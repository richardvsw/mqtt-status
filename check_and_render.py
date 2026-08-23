"""
Standalone MQTT broker status checker + page generator. Runs from TWO
independent places, same script either way:

- GitHub Actions (.github/workflows/check-status.yml), on a ~10 min
  schedule, no dependency on the LXC/bot at all -- if the LXC goes down,
  this keeps running and reporting real, live broker status.
- This box's own LXC (deploy/systemd/, via mqtt-status-lxc.timer), every
  2 min -- geographically near Indonesia (~5-30ms to these brokers, vs
  the Actions runner's ~600-750ms from its US/EU datacenter, confirmed
  2026-08-19), so this is the accurate/primary source whenever it's up.

Neither side needs to know about the other or detect "is the other one
alive" -- both independently check-and-commit-if-changed, so whichever
committed most recently is just what the page reflects, and latency from
each source is tracked and displayed separately (see lxc_latency_ms /
actions_latency_ms below) rather than one clobbering the other's number.
"""
import json
import os
import socket
import time
from datetime import datetime, timezone, timedelta

# GitHub Actions sets this on every runner automatically -- no config
# needed on either side to tell the two apart.
IS_CI = os.environ.get("GITHUB_ACTIONS") == "true"

# 2026-08-22 incident: a raw TCP connect (the old check_broker) reports a
# broker "up" even when the account's credentials are being rejected at
# the MQTT layer -- confirmed live during a real outage where all 5
# brokers accepted the TCP handshake but every one replied "Connection
# Refused: not authorised" (mosquitto CONNACK rc=5) to the real
# idmeshnode login meshtasticd actually uses. This page showed "all
# operational" the entire time. Doing a real authenticated CONNECT here
# (paho-mqtt, connect+disconnect only, no subscribe) catches that
# specific failure mode and reports it as its own "Auth Error" state,
# distinct from a genuine network-level "Down" -- an auth rejection means
# the broker itself is fine and the problem is the shared account, which
# reads very differently to anyone watching this page than "broker is
# down".
#
# Credentials: on the LXC this comes from the systemd unit's own
# Environment= lines (deploy/systemd/mqtt-status-lxc.service) since this
# box already has them for meshtasticd's own config; on GitHub Actions it
# comes from the repo's Actions secrets (MQTT_CHECK_USER/MQTT_CHECK_PASS)
# -- never committed in plaintext either place. A missing/empty
# credential just skips the auth check and falls back to the old
# TCP-only result (still better than crashing the whole page).
import paho.mqtt.client as mqtt

MQTT_CHECK_USER = os.environ.get("MQTT_CHECK_USER", "")
MQTT_CHECK_PASS = os.environ.get("MQTT_CHECK_PASS", "")


def get_ci_location():
    """Best-effort geolocation of the CURRENT run's own egress IP -- only
    called when IS_CI. GitHub-hosted runners are ephemeral VMs spun up
    fresh per job and are NOT guaranteed to be in the same datacenter
    every time, so this is looked up fresh on every run rather than
    hardcoded, and simply reflects wherever this particular run actually
    landed. Never raises -- a lookup failure just means the location is
    omitted from the display, not a broken page."""
    try:
        import urllib.request
        with urllib.request.urlopen("https://ipinfo.io/json", timeout=5) as r:
            data = json.load(r)
        parts = [p for p in (data.get("city"), data.get("region"), data.get("country")) if p]
        return ", ".join(parts) if parts else None
    except Exception as e:
        print(f"CI geolocation lookup failed: {e}")
        return None
LATENCY_STATE_KEY = "actions_latency_ms" if IS_CI else "lxc_latency_ms"

WIB = timezone(timedelta(hours=7))
MQTT_PORT = 1883
# Edit brokers.json to add/remove brokers -- no code change needed, this
# file is re-read fresh on every run.
with open("brokers.json") as _f:
    BROKERS = json.load(_f)

# 2026-08-22: was a single ever-growing history.json. Stayed genuinely
# tiny even at full 400-day retention (~150-200KB), so this isn't a
# size/perf fix -- it's for cleaner git diffs (a day's check only
# touches THAT month's file, not one file every host/day ever
# recorded lives in) and easier manual archiving/pruning of old
# months later if wanted. One file per calendar month, e.g.
# history/2026-08.json, each holding exactly the same per-day dict
# shape the old flat file did for just that month's dates.
HISTORY_DIR = "history"
LOG_PATH = "log.jsonl"  # append-only per-run detail log -- kept separate so index.html only ever holds the current rendered state, not accumulating history
STATE_PATH = "state.json"  # tracks current_outage_start per host, same purpose as mqtt_tap's own outage_state
OUT_PATH = "index.html"
# 2026-08-22: dropped from 90 -> 30 to match the reference layout
# (status.claude.com) -- at 90 days each bar was too thin on mobile to
# reliably tap; 30 keeps every bar wide enough for the new click-to-open
# popover below to feel deliberate rather than fiddly. This is ONLY the
# main page's display window now -- see HISTORY_RETENTION_DAYS below for
# how much history is actually kept (the calendar/uptime.html page reads
# the full retained range, not just this).
HISTORY_DAYS = 30
# 2026-08-22: was the SAME value as HISTORY_DAYS (history.json deleted
# anything older every run), which meant there was never more than 30
# days of data to show even once the status.claude.com-style calendar
# page (uptime.html) existed to show it. Decoupled so retention keeps
# accumulating (up to ~13 months) independent of the main page's
# display window. Per-day storage cost is trivial (a handful of int
# fields per host per day), so keeping over a year of it is cheap.
HISTORY_RETENTION_DAYS = 400
UPTIME_OUT_PATH = "uptime.html"

CHECK_RETRIES = 10
RETRY_DELAY_SECONDS = 1.5


def _mqtt_connect_attempt(host, timeout):
    """One real MQTT CONNECT (not just a TCP handshake). Returns "up",
    "auth_error", or "down". Falls back to a plain TCP check when no
    credentials are configured, so this degrades gracefully rather than
    reporting every broker as broken if the env vars are ever missing."""
    if not MQTT_CHECK_USER:
        try:
            with socket.create_connection((host, MQTT_PORT), timeout=timeout):
                return "up"
        except Exception:
            return "down"

    result = {"status": None}

    def on_connect(client, userdata, flags, rc, properties=None):
        code = rc.value if hasattr(rc, "value") else rc
        if code == 0:
            result["status"] = "up"
        elif code in (4, 5):  # bad username/password, not authorised
            result["status"] = "auth_error"
        else:
            result["status"] = "down"
        client.disconnect()

    # paho-mqtt 2.x defaults to the VERSION2 callback API, whose
    # on_connect signature/rc type differs from what's used below -- left
    # implicit, the mismatch doesn't raise, it just means on_connect never
    # actually assigns result["status"], silently degrading every real
    # auth_error into a "down" (the timeout fallback) and defeating the
    # entire point of this check. Confirmed via a live test run right
    # after writing this, caught before it ever reached the public page.
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1,
                          client_id=f"mqtt-status-check-{os.getpid()}-{int(time.time()*1000)}",
                          protocol=mqtt.MQTTv311)
    client.username_pw_set(MQTT_CHECK_USER, MQTT_CHECK_PASS)
    client.on_connect = on_connect
    try:
        client.connect(host, MQTT_PORT, timeout)
        client.loop_start()
        waited = 0.0
        while result["status"] is None and waited < timeout:
            time.sleep(0.1)
            waited += 0.1
        client.loop_stop()
        try:
            client.disconnect()
        except Exception:
            pass
    except Exception:
        return "down"
    return result["status"] or "down"


def check_broker(host, timeout=5, retries=CHECK_RETRIES):
    """A single bad moment on the GitHub Actions runner's own network/DNS
    used to be enough to mark a broker down -- confirmed 2026-08-18: all 5
    brokers failed in the same instant right after latencies had climbed
    unusually high, while this box's own live connection to the same
    brokers was working the entire time. Retries here so a transient
    runner-side blip doesn't get reported as a real outage on the public
    page; a genuinely down broker still fails every attempt and gets
    marked down same as before, just slower to confirm.

    Returns (status, latency_ms) where status is "up", "auth_error", or
    "down". auth_error does not retry through the full loop the way "down"
    does -- a rejected login is a deterministic result, not a transient
    network blip, so retrying it just burns time for the same answer."""
    t0 = time.time()
    last_status = "down"
    for attempt in range(retries):
        status = _mqtt_connect_attempt(host, timeout)
        if status == "up":
            return "up", round((time.time() - t0) * 1000)
        if status == "auth_error":
            return "auth_error", None
        last_status = status
        if attempt < retries - 1:
            time.sleep(RETRY_DELAY_SECONDS)
    return last_status, None


def load_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f)


def fmt_duration(seconds):
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}dtk"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}j {minutes % 60}m"
    days = hours // 24
    return f"{days}h {hours % 24}j"


# Same principle real status pages (this one deliberately mirrors
# Anthropic's) use: don't let a single check run's result flip the public
# status. CHECK_RETRIES already rides out sub-minute blips within one
# run; this rides out anything that outlasts even that, by requiring the
# SAME broker to fail CONFIRM_THRESHOLD separate scheduled runs (~10-20
# min apart, not just retries seconds apart) before it's shown as Down.
# Confirmed 2026-08-18: without this, a brief GitHub Actions runner-side
# network/DNS blip reported all 5 brokers down simultaneously while this
# box's own live connection to the same brokers never dropped.
CONFIRM_THRESHOLD = 2

now = time.time()
state = load_json(STATE_PATH, {})
def _load_history():
    """Merges every history/YYYY-MM.json into one {date: {host: {...}}}
    dict, same shape callers already expect from the old flat file --
    every existing reader (day_bar_html, host_uptime_pct,
    _months_with_data, _cal_day_cell, etc.) keeps working unchanged."""
    merged = {}
    if os.path.isdir(HISTORY_DIR):
        for fname in sorted(os.listdir(HISTORY_DIR)):
            if fname.endswith(".json"):
                merged.update(load_json(os.path.join(HISTORY_DIR, fname), {}))
    return merged


def _save_history(history):
    """Regroups by month and rewrites each month's file wholesale --
    simpler and safer than trying to patch individual files in place,
    and cheap given how small each month's slice is. A month that
    dropped out of `history` entirely (every day trimmed past
    HISTORY_RETENTION_DAYS) has its file deleted, not left stale."""
    os.makedirs(HISTORY_DIR, exist_ok=True)
    by_month = {}
    for d, hosts in history.items():
        by_month.setdefault(d[:7], {})[d] = hosts
    for fname in os.listdir(HISTORY_DIR):
        if fname.endswith(".json") and fname[:-5] not in by_month:
            os.remove(os.path.join(HISTORY_DIR, fname))
    for ym, days in by_month.items():
        save_json(os.path.join(HISTORY_DIR, f"{ym}.json"), days)


history = _load_history()

# Where each source's ping is measured from -- LXC location is fixed
# (this box doesn't move), CI location is looked up fresh every run
# since GitHub's runners aren't guaranteed to be in the same datacenter
# each time. Kept under a "_meta" key, distinct from the per-host entries
# state.json otherwise holds.
meta = state.setdefault("_meta", {})
meta["lxc_location"] = "Cikarang, ID"
if IS_CI:
    loc = get_ci_location()
    if loc:
        meta["actions_location"] = loc

# 2026-08-22: legend used to hardcode "RiV-meshBot" -- only the LXC run
# can actually query meshtasticd (run_and_push.sh sets BOT_LONG_NAME),
# so persist whatever it found into _meta and keep using that on every
# run including GitHub Actions ones, same pattern as lxc_location.
bot_long_name = os.environ.get("BOT_LONG_NAME", "").strip()
if bot_long_name:
    meta["lxc_bot_name"] = bot_long_name

brokers = {}
for host in BROKERS:
    raw_status, latency_ms = check_broker(host)  # "up" / "auth_error" / "down"
    raw_reachable = raw_status == "up"
    st = state.setdefault(host, {"current_outage_start": None, "consecutive_fails": 0, "provisional_start": None})
    st.setdefault("consecutive_fails", 0)
    st.setdefault("provisional_start", None)
    # Auth-error tracking mirrors the down-tracking fields above exactly,
    # just under its own keys, so the two failure modes get independent
    # confirm-threshold debouncing and independent displayed durations
    # instead of one clobbering the other's state.
    st.setdefault("current_auth_start", None)
    st.setdefault("consecutive_auth_fails", 0)
    st.setdefault("provisional_auth_start", None)

    if raw_status == "up":
        st["consecutive_fails"] = 0
        st["provisional_start"] = None
        st["current_outage_start"] = None
        st["consecutive_auth_fails"] = 0
        st["provisional_auth_start"] = None
        st["current_auth_start"] = None
    elif raw_status == "auth_error":
        # Not a network outage -- don't touch the down-tracking fields.
        st["consecutive_fails"] = 0
        st["provisional_start"] = None
        st["current_outage_start"] = None
        st["consecutive_auth_fails"] += 1
        if st["provisional_auth_start"] is None:
            st["provisional_auth_start"] = now
        if st["consecutive_auth_fails"] >= CONFIRM_THRESHOLD:
            st["current_auth_start"] = st["provisional_auth_start"]
        else:
            st["current_auth_start"] = None
    else:  # "down"
        st["consecutive_auth_fails"] = 0
        st["provisional_auth_start"] = None
        st["current_auth_start"] = None
        st["consecutive_fails"] += 1
        if st["provisional_start"] is None:
            st["provisional_start"] = now
        if st["consecutive_fails"] >= CONFIRM_THRESHOLD:
            # Promote to a confirmed, publicly-displayed outage -- keep the
            # TRUE first-failure time, not the moment it got confirmed, so
            # the displayed duration reflects the real total downtime.
            st["current_outage_start"] = st["provisional_start"]
        else:
            # Not yet confirmed -- current_outage_start MUST stay null here,
            # not just "untouched", or a stale value from before this
            # confirm-threshold logic existed (or from a prior confirmed
            # outage that only just recovered) would keep showing as a
            # false "Down" until a raw success happens to land. This is
            # also what self-heals the real repo's already-corrupted
            # state.json from the 2026-08-18 false-positive incident.
            st["current_outage_start"] = None

    # Persist latency under THIS source's own key only -- the other
    # source's last-known value is left untouched, so the page can show
    # both side by side (e.g. "5ms local / 650ms CI") instead of one
    # number that flips wildly depending on which side last committed.
    if raw_reachable:
        st[LATENCY_STATE_KEY] = latency_ms

    # Only shown as Down/Auth Error once confirmed -- a single run's raw
    # failure still shows Operational publicly, exactly so an unconfirmed
    # blip never produces a false reading.
    confirmed_down = st["current_outage_start"] is not None
    confirmed_auth = st["current_auth_start"] is not None
    brokers[host] = {
        "reachable": not confirmed_down and not confirmed_auth,
        "raw_reachable": raw_reachable,
        "auth_error": confirmed_auth,
        "latency_ms": latency_ms,
        "lxc_latency_ms": st.get("lxc_latency_ms"),
        "actions_latency_ms": st.get("actions_latency_ms"),
        "current_outage_start": st["current_outage_start"],
        "current_auth_start": st["current_auth_start"],
    }

save_json(STATE_PATH, state)

today_str = datetime.fromtimestamp(now, WIB).strftime("%Y-%m-%d")
today_bucket = history.setdefault(today_str, {})
for host, b in brokers.items():
    # "down"/"auth_error" keys added 2026-08-22 alongside the tri-state
    # check itself -- lets the per-day bar's tooltip say WHICH kind of
    # failure a day had (see day_bar_html below) instead of just a bare
    # percentage. Older days recorded before this existed simply lack
    # these keys; day_bar_html treats that as "breakdown unknown" rather
    # than assuming either value, since a plain TCP check genuinely
    # can't tell them apart after the fact.
    slot = today_bucket.setdefault(host, {"up": 0, "total": 0, "down": 0, "auth_error": 0})
    slot.setdefault("down", 0)
    slot.setdefault("auth_error", 0)
    slot["total"] += 1
    # Raw result, not the confirmed/public one -- the 90-day bar strip
    # already has a "warn" (partial) tier for exactly this kind of noise,
    # so a lone transient blip shows as a slightly-off day rather than
    # either full green (hiding it) or full red (the false alarm this
    # whole change exists to avoid on the CURRENT-status banner/rows).
    if b["raw_reachable"]:
        slot["up"] += 1
    elif b["auth_error"]:
        slot["auth_error"] += 1
    else:
        slot["down"] += 1
cutoff_date = (datetime.fromtimestamp(now, WIB) - timedelta(days=HISTORY_RETENTION_DAYS)).strftime("%Y-%m-%d")
for d in [d for d in history if d < cutoff_date]:
    del history[d]
_save_history(history)

# append this run'''s raw result to the detail log -- one line per check,
# never rewritten/truncated (unlike history.json/state.json which hold
# only the current rolled-up state)
with open(LOG_PATH, "a") as f:
    f.write(json.dumps({"ts": now, "brokers": brokers}) + "\n")

day_labels = [(datetime.fromtimestamp(now, WIB) - timedelta(days=i)).strftime("%Y-%m-%d")
              for i in range(HISTORY_DAYS - 1, -1, -1)]


def _fmt_clock_duration(seconds):
    """"3 jam 25 menit" style -- distinct from fmt_duration() above (which
    favors compact "3j 25m" for the current-outage banner/rows): the
    popover has room to spell it out, matching the reference layout's
    "3 hrs 25 mins" framing."""
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds} detik"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} menit"
    hours = minutes // 60
    rem_minutes = minutes % 60
    if rem_minutes == 0:
        return f"{hours} jam"
    return f"{hours} jam {rem_minutes} menit"


MIN_INCIDENT_SECONDS = 120  # shorter than this -> not a real tracked incident, just noise


def _build_real_incidents():
    """Reconstructs REAL per-incident start/end epoch timestamps per host
    by replaying log.jsonl's append-only per-check history -- a much
    better source than history.json's aggregate day counts, which can
    only give a proportional-elapsed-time ESTIMATE of duration, not real
    clock times. Every log.jsonl line already carries current_outage_start
    (every entry) / current_auth_start (entries since the 2026-08-22
    tri-state fix) -- the confirmed start timestamp of whatever incident
    was active at that check, persisted unchanged across checks until it
    clears. Watching each field flip null -> timestamp -> null across the
    log gives exact incident boundaries.

    Adjacent same-kind incidents separated by a short gap get merged --
    confirmed via a live run: the CONFIRM_THRESHOLD debounce (and having
    two independent writers, the LXC timer and GitHub Actions, racing
    commits) can momentarily reset current_*_start even mid-outage,
    fragmenting one real incident into several 1-9 minute ones. A gap
    under 10 minutes is treated as the same incident continuing, not a
    real recovery-then-fail.
    """
    MERGE_GAP_SECONDS = 600
    incidents = {}
    open_incident = {}
    try:
        with open(LOG_PATH) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts = rec.get("ts")
                for host, b in rec.get("brokers", {}).items():
                    incidents.setdefault(host, [])
                    for kind, field in (("down", "current_outage_start"), ("autherr", "current_auth_start")):
                        val = b.get(field)
                        key = (host, kind)
                        if val:
                            if key not in open_incident:
                                open_incident[key] = val
                        elif key in open_incident:
                            start = open_incident.pop(key)
                            lst = incidents[host]
                            if lst and lst[-1]["kind"] == kind and (start - lst[-1]["end"]) <= MERGE_GAP_SECONDS:
                                lst[-1]["end"] = ts
                            else:
                                lst.append({"kind": kind, "start": start, "end": ts})
    except FileNotFoundError:
        pass
    for (host, kind), start in open_incident.items():
        lst = incidents.setdefault(host, [])
        if lst and lst[-1]["kind"] == kind and (start - lst[-1]["end"]) <= MERGE_GAP_SECONDS:
            lst[-1]["end"] = None
        else:
            lst.append({"kind": kind, "start": start, "end": None})
    # 2026-08-23: drop anything under MIN_INCIDENT_SECONDS -- a blip
    # that happened to straddle exactly CONFIRM_THRESHOLD checks isn't
    # a meaningful incident on its own (raw single-check blips already
    # get their own clearly-labeled "(sesaat)" popover entry instead).
    for host in incidents:
        incidents[host] = [
            inc for inc in incidents[host]
            if (inc["end"] if inc["end"] is not None else time.time()) - inc["start"] >= MIN_INCIDENT_SECONDS
        ]
    return incidents


REAL_INCIDENTS = _build_real_incidents()

# 2026-08-23: lookup of each host's CURRENTLY-OPEN merged incident start
# time (kind -> start), so the live "Down · Xm"/"Autentikasi Ditolak ·
# Xm" badge can agree with the incident popover's duration instead of
# the two disagreeing whenever a brief recovery-then-fail blip resets
# state.json's current_outage_start mid-outage (confirmed live: mqtt5
# flickered reachable for ~2 checks mid-outage on 2026-08-23, resetting
# the badge to "2m" while the popover correctly still showed the whole
# ~28min merged span). Falls back to state.json's own current_*_start
# when there's no matching open REAL_INCIDENTS entry yet -- a fresh
# outage under MIN_INCIDENT_SECONDS hasn't been promoted into
# REAL_INCIDENTS yet, so the simple calculation is still correct there.
_OPEN_INCIDENT_START = {}
for _host, _incs in REAL_INCIDENTS.items():
    if _incs and _incs[-1]["end"] is None:
        _OPEN_INCIDENT_START[(_host, _incs[-1]["kind"])] = _incs[-1]["start"]
KIND_LABEL = {"down": "Down", "autherr": "Autentikasi Ditolak"}


BOT_ENTITIES = ["mesh_bot", "meshtasticd", "lxc-monitor"]
BOT_ENTITY_LABEL = {
    "mesh_bot": "mesh_bot.service",
    "meshtasticd": "meshtasticd.service",
    "lxc-monitor": "Server (Cikarang)",
}


def _build_bot_incidents():
    """Same reconstruction approach as _build_real_incidents() above,
    just against bot_log.jsonl's simpler {"checks": {svc: "up"/"down"}}
    shape (written by check_bot_status.py) instead of log.jsonl's
    per-broker down/auth_error tri-state. Only ever "down" as a kind --
    a systemd service doesn't have an auth-rejected equivalent."""
    MERGE_GAP_SECONDS = 600
    incidents = {}
    open_incident = {}
    try:
        with open("bot_log.jsonl") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts = rec.get("ts")
                for svc, status in rec.get("checks", {}).items():
                    incidents.setdefault(svc, [])
                    if status == "down":
                        if svc not in open_incident:
                            open_incident[svc] = ts
                    elif svc in open_incident:
                        start = open_incident.pop(svc)
                        lst = incidents[svc]
                        if lst and (start - lst[-1]["end"]) <= MERGE_GAP_SECONDS:
                            lst[-1]["end"] = ts
                        else:
                            lst.append({"start": start, "end": ts})
    except FileNotFoundError:
        pass
    for svc, start in open_incident.items():
        lst = incidents.setdefault(svc, [])
        if lst and lst[-1]["end"] is not None and (start - lst[-1]["end"]) <= MERGE_GAP_SECONDS:
            lst[-1]["end"] = None
        else:
            lst.append({"start": start, "end": None})
    for svc in incidents:
        incidents[svc] = [
            inc for inc in incidents[svc]
            if (inc["end"] if inc["end"] is not None else time.time()) - inc["start"] >= MIN_INCIDENT_SECONDS
        ]
    return incidents


BOT_REAL_INCIDENTS = _build_bot_incidents()


def _clip_incidents_to_day(host, d):
    """Real incidents clipped to day `d`'s [00:00, 24:00) WIB window,
    each carrying real clock start/end (or day boundaries for whatever
    portion of a multi-day incident falls on this particular day)."""
    day_start = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=WIB).timestamp()
    day_end = day_start + 86400
    out = []
    is_bot = host in BOT_ENTITIES
    source = BOT_REAL_INCIDENTS if is_bot else REAL_INCIDENTS
    for inc in source.get(host, []):
        seg_start = max(inc["start"], day_start)
        seg_end_raw = inc["end"] if inc["end"] is not None else now
        seg_end = min(seg_end_raw, day_end)
        if seg_end <= seg_start:
            continue
        # An incident that continues past this day's boundary clips to
        # exactly midnight-of-the-NEXT-day -- strftime on that instant
        # reads "00:00", which looks like it ended at the START of this
        # day, not that it was still running at the end of it. "24:00" is
        # the correct end-of-day reading; confirmed live against the real
        # 2026-08-15 outage (19:09 -> continues into the 16th), which
        # rendered as "19:09-00:00" before this fix -- looked like a
        # 9-hour gap-then-restart instead of one continuous outage.
        if inc["end"] is None and seg_end_raw <= day_end:
            end_clock = "sekarang"
        elif seg_end >= day_end:
            end_clock = "24:00"
        else:
            end_clock = datetime.fromtimestamp(seg_end, WIB).strftime("%H:%M")
        out.append({
            "kind": "down" if is_bot else inc["kind"],
            "label": "Down" if is_bot else KIND_LABEL[inc["kind"]],
            "seconds": seg_end - seg_start,
            "start_clock": datetime.fromtimestamp(seg_start, WIB).strftime("%H:%M"),
            "end_clock": end_clock,
        })
    return out


def host_uptime_pct(host):
    """Overall uptime % across the whole HISTORY_DAYS window for one
    host -- same "99.22% uptime" figure status.claude.com shows under
    each service's own bar strip. Summed from raw per-check counts
    (not a simple average of daily percentages), so a day with more
    checks recorded naturally carries more weight than a day with only
    a handful -- matches how the bars themselves are colored."""
    up_sum, total_sum = 0, 0
    for d in day_labels:
        slot = history.get(d, {}).get(host)
        if slot:
            up_sum += slot.get("up", 0)
            total_sum += slot.get("total", 0)
    if total_sum == 0:
        return None
    return up_sum / total_sum * 100


# 2026-08-22: status.claude.com's own calendar uses a continuous
# green->yellow->orange->red gradient keyed to that day's real uptime %,
# not a fixed 3-bucket scheme -- confirmed live against the reference
# (a 4-hour outage day and a 5-minute blip both used to render as the
# exact same flat red here, losing the severity signal a color gradient
# is supposed to carry). Piecewise-linear interpolation between a few
# named stops, closely matching the reference's own observed hues
# (100% green #76ad2a through 0% red #e04343). Used by both day_bar_html
# (main page) and _cal_day_cell (uptime.html) -- defined here, ahead of
# day_bar_html's own definition below, since it's called from inside
# that function and Python resolves module-level names at call time
# against whatever has ALREADY executed, not definition order alone.
_SEVERITY_STOPS = [
    (100.0, (0x76, 0xad, 0x2a)),
    (99.5,  (0x9a, 0xb4, 0x2a)),
    (97.0,  (0xd9, 0xa9, 0x2a)),
    (90.0,  (0xe6, 0xa8, 0x2a)),
    (75.0,  (0xe8, 0x61, 0x36)),
    (0.0,   (0xe0, 0x43, 0x43)),
]


_ID_MONTHS_FULL = [
    "", "Januari", "Februari", "Maret", "April", "Mei", "Juni",
    "Juli", "Agustus", "September", "Oktober", "November", "Desember",
]
_ID_MONTHS_ABBR = [
    "", "Jan", "Feb", "Mar", "Apr", "Mei", "Jun",
    "Jul", "Agu", "Sep", "Okt", "Nov", "Des",
]


def _id_strftime_dmy(ts, tz):
    # datetime.strftime's %b/%B are locale-dependent (usually English on
    # this server) -- the page is otherwise all Indonesian, so spell
    # out the Indonesian month name/abbreviation ourselves instead of
    # depending on system locale.
    dt = datetime.fromtimestamp(ts, tz)
    return dt, _ID_MONTHS_ABBR[dt.month], _ID_MONTHS_FULL[dt.month]


def _severity_color(pct):
    pct = max(0.0, min(100.0, pct))
    for (p1, c1), (p2, c2) in zip(_SEVERITY_STOPS, _SEVERITY_STOPS[1:]):
        if pct >= p1:
            return f"#{c1[0]:02x}{c1[1]:02x}{c1[2]:02x}"
        if pct >= p2:
            t = (p1 - pct) / (p1 - p2)
            r = round(c1[0] + (c2[0] - c1[0]) * t)
            g = round(c1[1] + (c2[1] - c1[1]) * t)
            b = round(c1[2] + (c2[2] - c1[2]) * t)
            return f"#{r:02x}{g:02x}{b:02x}"
    last = _SEVERITY_STOPS[-1][1]
    return f"#{last[0]:02x}{last[1]:02x}{last[2]:02x}"


def day_bar_html(host):
    bars = []
    for d in day_labels:
        slot = history.get(d, {}).get(host)
        if not slot or slot["total"] == 0:
            bars.append(f'<div class="bar nodata" data-date="{d}" data-status="nodata"></div>')
            continue
        total = slot["total"]
        pct = slot["up"] / total * 100
        # cls kept for data-status only (drives the popover's no-
        # incident-data fallback icon/label) -- the VISIBLE color is
        # the same continuous severity gradient uptime.html's calendar
        # uses (_severity_color, defined further down but resolved at
        # call time same as any Python name), applied here too so a
        # 5-minute blip and a 4-hour outage don't render as the exact
        # same flat red on the main page either.
        incidents = _clip_incidents_to_day(host, d)
        if incidents:
            cls = "up" if pct >= 99.5 else ("warn" if pct >= 90 else "down")
            color = _severity_color(pct)
        else:
            # No confirmed incident (>= MIN_INCIDENT_SECONDS) touched
            # this day -- render it fully green like a clean day, even
            # if raw pct dipped from sub-2-min blips that no longer
            # count as real incidents (matches the popover's own
            # unconditional "Beroperasi Normal" fallback).
            cls = "up"
            color = _severity_color(100.0)
        import html as _html
        incidents_attr = _html.escape(json.dumps(incidents), quote=True)
        # is_today marks the one bar whose percentage is against
        # ELAPsed time so far, not a completed 24h -- the popover needs
        # this to word the % line honestly ("so far today" vs "that
        # day") instead of implying a finished day's stats for a day
        # that's still in progress.
        is_today = "1" if d == day_labels[-1] else "0"
        bars.append(
            f'<div class="bar" style="background:{color}" data-date="{d}" data-status="{cls}" '
            f'data-pct="{pct:.0f}" data-today="{is_today}" data-incidents="{incidents_attr}"></div>')
    return "".join(bars)


# Just the city, not the full "RiV-meshBot server"/"GitHub Actions"
# phrase on every single row -- keeps rows to one line instead of
# wrapping, full explanation lives once in the footnote instead of
# repeated 5x on the page.
lxc_city = (meta.get("lxc_location") or "").split(",")[0]
actions_city = (meta.get("actions_location") or "").split(",")[0]
bot_display_name = meta.get("lxc_bot_name") or "RiV-meshBot"

rows = []
up_count = 0
down_hosts = []
auth_hosts = []
for host in BROKERS:
    b = brokers[host]
    if b["reachable"]:
        up_count += 1
        status_class = "up"
        # Color-coded instead of repeating "(city name)" text on every
        # row -- legend explaining what each color means lives once,
        # below the panel (see .ping-legend).
        parts = []
        if b["lxc_latency_ms"] is not None:
            parts.append(f'<span class="ping ping-lxc">{b["lxc_latency_ms"]}ms</span>')
        if b["actions_latency_ms"] is not None:
            parts.append(f'<span class="ping ping-ci">{b["actions_latency_ms"]}ms</span>')
        status_label = "Aktif · " + " · ".join(parts) if parts else "Aktif"
    elif b["auth_error"]:
        _start = _OPEN_INCIDENT_START.get((host, "autherr"), b["current_auth_start"])
        dur = fmt_duration(now - _start) if _start else "?"
        status_label, status_class = f"Autentikasi Ditolak · {dur}", "autherr"
        auth_hosts.append(host)
    else:
        _start = _OPEN_INCIDENT_START.get((host, "down"), b["current_outage_start"])
        dur = fmt_duration(now - _start) if _start else "?"
        status_label, status_class = f"Down · {dur}", "down"
        down_hosts.append(host)
    uptime_pct = host_uptime_pct(host)
    uptime_label = f"{uptime_pct:.2f} % uptime" if uptime_pct is not None else "belum ada data"
    rows.append(f'''
        <div class="row">
          <div class="row-top">
            <div class="row-left"><span class="dot {status_class}"></span><span class="host">{host}</span></div>
            <div class="status {status_class}">{status_label}</div>
          </div>
          <div class="bars">{day_bar_html(host)}</div>
          <div class="bars-caption row-caption">
            <span>{HISTORY_DAYS} hari lalu</span>
            <span class="caption-line"></span>
            <span>{uptime_label}</span>
            <span class="caption-line"></span>
            <span>Hari ini</span>
          </div>
        </div>''')

# "Past Incidents" -- same spirit as status.claude.com's own past-
# incidents log: a chronological, per-day list of what actually broke,
# not just the bar-strip summary. Reuses REAL_INCIDENTS (already built
# for the day-popovers) rather than a separate data source, and reuses
# _clip_incidents_to_day so a single multi-day outage still splits at
# midnight the same way it does in the popovers. Days with zero
# incidents across every broker are skipped entirely rather than
# padded with "no incidents" filler -- with 6 brokers over 30 days,
# an exhaustive per-day list (like Claude's) would mostly be noise;
# only days something actually happened are worth showing here.
incident_days = []
for d in reversed(day_labels):
    day_entries = []
    for host in BROKERS:
        for inc in _clip_incidents_to_day(host, d):
            day_entries.append({"host": host, **inc})
    if day_entries:
        day_entries.sort(key=lambda e: e["start_clock"])
        incident_days.append((d, day_entries))

incident_kind_icon = {"down": "✕", "autherr": "⚠"}


def _incident_log_html():
    # 2026-08-22: a genuinely bad day (today, mid-outage: 20+ rows across
    # 6 flapping brokers) made the page extremely long -- native <details>/
    # <summary> collapses each day to one line by default (no JS needed,
    # keyboard-accessible for free), showing just a count + which brokers
    # were affected. Clicking a day expands the full row list, same as
    # before.
    if not incident_days:
        return '<p class="note">Tidak ada insiden tercatat dalam 30 hari terakhir.</p>'
    blocks = []
    for d, entries in incident_days:
        rows_html = "".join(
            f'''<div class="incident-row">
              <span class="incident-icon {e["kind"]}">{incident_kind_icon.get(e["kind"], "✕")}</span>
              <span class="incident-host">{e["host"]}</span>
              <span class="incident-label">{e["label"]}</span>
              <span class="incident-time">{e["start_clock"]}–{e["end_clock"]} WIB</span>
            </div>'''
            for e in entries
        )
        n_hosts = len({e["host"] for e in entries})
        summary_text = f"{len(entries)} insiden · {n_hosts} broker terdampak"
        blocks.append(f'''
        <details class="incident-day">
          <summary class="incident-summary">
            <span class="incident-chevron">▸</span>
            <span class="incident-date">{d}</span>
            <span class="incident-count">{summary_text}</span>
          </summary>
          <div class="incident-rows">{rows_html}</div>
        </details>''')
    return "".join(blocks)


total = len(BROKERS)
if up_count == total:
    banner_class, banner_text, banner_icon = "ok", "Semua Broker Beroperasi Normal", "✓"
elif up_count == 0 and len(auth_hosts) == total:
    # Every broker reachable but rejecting the shared login -- a very
    # different (and differently actionable) situation than the network
    # actually being down, so it gets its own banner wording entirely
    # rather than folding into "Semua Broker Down".
    banner_class, banner_text, banner_icon = "crit", "Semua Broker Menolak Autentikasi", "✕"
elif up_count == 0:
    banner_class, banner_text, banner_icon = "crit", "Semua Broker Down", "✕"
else:
    parts = []
    if down_hosts:
        parts.append(f"{', '.join(down_hosts)} Down")
    if auth_hosts:
        parts.append(f"{', '.join(auth_hosts)} Autentikasi Ditolak")
    banner_class, banner_text, banner_icon = "warn", f"Gangguan Sebagian — {', '.join(parts)}", "!"

_upd_dt, _upd_abbr, _ = _id_strftime_dmy(now, WIB)
updated_str = _upd_dt.strftime(f"%d {_upd_abbr} %Y, %H:%M:%S WIB")
# notice.json mirrors brokers.json's own convention -- edit the JSON,
# no code change needed, re-read fresh every run. Missing file or a
# blank/absent "text" both mean no banner, so removing the notice
# once meshnode.id's migration settles is a one-line JSON edit, not a
# code change. Built OUTSIDE the big html f-string below, not inlined
# as a nested conditional expression there -- a nested triple-quoted
# string would prematurely close the outer f'''...''' literal.
notice_text = load_json("notice.json", {}).get("text", "").strip()
maintenance_banner_html = (
    f'<div class="banner info"><span class="banner-icon">\u2139</span>{notice_text}</div>'
    if notice_text else ""
)
commit_sha = os.environ.get("GITHUB_SHA", "")[:7] or "local"

html = f'''<!doctype html>
<html lang="id">
<script>
(function () {{
  try {{
    if (localStorage.getItem("mqtt-status-theme") === "light") {{
      document.documentElement.setAttribute("data-theme", "light");
    }}
  }} catch (e) {{}}
}})();
</script>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>meshnode.id MQTT Status</title>
<meta name="description" content="Status langsung broker MQTT publik meshnode.id">
<script>
  // 2026-08-22: was a plain <meta http-equiv="refresh" content="60">.
  // GitHub Pages serves this page with Cache-Control: max-age=600 (a
  // platform default we can't override, no custom-headers support) --
  // a bare meta-refresh just re-navigates to the SAME URL, which a
  // normal reload is allowed to satisfy straight from the browser's
  // HTTP cache without ever hitting the network. Confirmed live: a
  // real viewer kept seeing the identical incident data (down to the
  // exact same durations) across several manual refreshes minutes
  // apart, up to 10 minutes stale, silently contradicting the "tiap 2
  // menit" text on this same page. Appending a cache-busting query
  // param forces a genuinely new URL each cycle, so the disk cache
  // can never satisfy it -- location.replace (not .href) so this
  // doesn't pile up in browser history on every cycle.
  setTimeout(function () {{
    location.replace(location.pathname + "?_=" + Date.now());
  }}, 60000);
</script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;650;700&display=swap" rel="stylesheet">
<style>
  :root {{
    --bg: #111827; --surf: #1f2937; --surf2: #232f42; --border: #2e3c51; --border-soft: #253247;
    --text: #e5e7eb; --muted: #94a3b8; --faint: #64748b;
    --ok: #2fb344; --ok-dim: #1e4326; --ok-bg: #0f2115;
    --warn: #f76707; --warn-dim: #4a2c0d; --warn-bg: #271a0a;
    --crit: #d63939; --crit-dim: #4a2020; --crit-bg: #2a1414;
    --accent: #066fd1; --accent-bg: #0d2136;
    --tooltip-bg: #232f42;
    --shadow: 0 1px 2px rgba(0,0,0,.3), 0 8px 24px -8px rgba(0,0,0,.5);
  }}
  * {{ box-sizing: border-box; }}
  html {{ color-scheme: dark; }}
  html[data-theme="light"] {{ color-scheme: light; }}
  :root[data-theme="light"] {{
    --bg: #f9fafb; --surf: #ffffff; --surf2: #ffffff; --border: #e5e7eb; --border-soft: #eef0f2;
    --text: #1f2937; --muted: #67748c; --faint: #94a3b8;
    --ok: #2fb344; --ok-dim: #bfe8c8; --ok-bg: #eafbee;
    --warn: #f76707; --warn-dim: #ffd8ad; --warn-bg: #fff2e6;
    --crit: #d63939; --crit-dim: #f5b8b8; --crit-bg: #fdecec;
    --accent: #066fd1; --accent-bg: #e8f2fd;
    --tooltip-bg: #ffffff;
    --shadow: 0 1px 2px rgba(0,0,0,.05), 0 8px 24px -8px rgba(0,0,0,.12);
  }}
  body {{
    margin: 0; min-height: 100vh; color: var(--text);
    background: var(--bg);
    font-family: "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", sans-serif;
    -webkit-font-smoothing: antialiased;
  }}
  .banner {{
    display: flex; align-items: center; justify-content: center; gap: .55rem;
    padding: .85rem 1.2rem; text-align: center; font-size: .92rem; font-weight: 600;
    letter-spacing: .1px; border-bottom: 1px solid var(--border-soft);
  }}
  .banner-icon {{
    display: inline-flex; align-items: center; justify-content: center;
    width: 20px; height: 20px; border-radius: 50%; font-size: .7rem; flex-shrink: 0;
  }}
  .banner.ok {{ background: var(--ok-bg); color: var(--ok); }}
  .banner.ok .banner-icon {{ background: var(--ok); color: #05130d; }}
  .banner.warn {{ background: var(--warn-bg); color: var(--warn); }}
  .banner.warn .banner-icon {{ background: var(--warn); color: #241c0d; }}
  .banner.crit {{ background: var(--crit-bg); color: var(--crit); }}
  .banner.crit .banner-icon {{ background: var(--crit); color: #250f0d; }}
  .banner.info {{ background: var(--accent-bg); color: var(--accent); border-bottom: 1px solid var(--border-soft); }}
  .banner.info .banner-icon {{ background: var(--accent); color: #eef6ff; }}

  .wrap {{ max-width: 680px; margin: 0 auto; padding: 2.4rem 1.25rem 2rem; }}
  .titlebar {{ display: flex; align-items: center; gap: .55rem; margin-bottom: .35rem; }}
  .titlebar h1 {{ flex: 1; }}
  .theme-toggle {{
    display: inline-flex; align-items: center; justify-content: center; width: 30px; height: 30px;
    border-radius: 6px; border: 1px solid var(--border); background: var(--surf); color: var(--muted);
    cursor: pointer; flex-shrink: 0;
  }}
  .theme-toggle:hover {{ color: var(--text); border-color: var(--faint); }}
  .theme-toggle .icon-moon {{ display: none; }}
  :root[data-theme="light"] .theme-toggle .icon-sun {{ display: none; }}
  :root[data-theme="light"] .theme-toggle .icon-moon {{ display: block; }}
  .titlebar .glyph {{ font-size: 1.25rem; line-height: 1; }}
  .live-clock {{
    font-variant-numeric: tabular-nums; font-size: .82rem; color: var(--muted);
    font-family: ui-monospace, "SF Mono", Menlo, monospace; flex-shrink: 0; margin-right: .3rem;
  }}
  .titlebar h1 {{ font-size: 1.2rem; font-weight: 650; margin: 0; letter-spacing: -.2px; }}
  .sub {{ color: var(--muted); font-size: .86rem; margin-bottom: 1.8rem; }}
  .sub b {{ color: var(--text); font-weight: 600; }}

  /* No overflow:hidden here either -- same reason as .bars below: it
     would clip the first row's tooltip, which sits close to the panel's
     own top edge. .row handles its own corner rounding on first/last
     instead of relying on the parent to clip square corners into shape. */
  .panel {{
    background: var(--surf); border: 1px solid var(--border); border-radius: 6px;
    box-shadow: var(--shadow);
  }}
  .row {{
    padding: 1rem 1.25rem 1.1rem; border-top: 1px solid var(--border-soft);
    font-size: .92rem; transition: background .12s;
  }}
  .row:first-child {{ border-top: none; border-top-left-radius: 14px; border-top-right-radius: 14px; }}
  .row:last-child {{ border-bottom-left-radius: 14px; border-bottom-right-radius: 14px; }}
  .row:hover {{ background: var(--surf2); }}
  .row-top {{ display: flex; justify-content: space-between; align-items: center; gap: .8rem; margin-bottom: .65rem; }}
  .row-left {{ display: flex; align-items: center; gap: .7rem; min-width: 0; }}
  .dot {{ position: relative; width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }}
  .dot.up {{ background: var(--ok); box-shadow: 0 0 0 3px var(--ok-dim); }}
  .dot.up::after {{
    content: ""; position: absolute; inset: -3px; border-radius: 50%; border: 1px solid var(--ok);
    animation: pulse 2.2s ease-out infinite;
  }}
  .dot.down {{ background: var(--crit); box-shadow: 0 0 0 3px var(--crit-dim); }}
  .dot.autherr {{ background: var(--warn); box-shadow: 0 0 0 3px var(--warn-dim); }}
  @keyframes pulse {{
    0% {{ transform: scale(.6); opacity: .8; }}
    100% {{ transform: scale(2.1); opacity: 0; }}
  }}
  .host {{ font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: .88rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
  /* Now potentially "Operational · 5ms (RiV-meshBot server) · 650ms
     (GitHub Actions)" instead of just "Operational · 5ms" -- stack below
     the host name on narrow screens rather than squeezing both onto one
     row and truncating the host. */
  .status {{ font-weight: 600; font-variant-numeric: tabular-nums; font-size: .84rem; flex-shrink: 0; }}
  @media (max-width: 480px) {{
    .row-top {{ flex-wrap: wrap; }}
    .status {{ flex-basis: 100%; font-size: .78rem; }}
  }}
  .status.up {{ color: var(--ok); }}
  .status.down {{ color: var(--crit); }}
  .status.autherr {{ color: var(--warn); }}
  /* Ping values color-coded by source instead of repeating "(city name)"
     text on every row -- see .ping-legend for what each color means. */
  .ping-lxc {{ color: var(--ok); }}
  .ping-ci {{ color: var(--accent); }}
  .ping-legend {{ display: flex; justify-content: center; gap: 1.2rem; margin-top: .7rem; font-size: .74rem; color: var(--faint); }}
  .ping-legend span {{ display: inline-flex; align-items: center; gap: .35rem; }}
  .ping-legend i {{ width: 8px; height: 8px; border-radius: 50%; display: inline-block; }}
  .ping-legend .lg-lxc {{ background: var(--ok); }}
  .ping-legend .lg-ci {{ background: var(--accent); }}

  /* No overflow:hidden here -- that would also clip the tooltip
     pseudo-elements below, which sit above each .bar's own box and are
     positioned relative to it, not to .bars. Rounding is done on the
     first/last .bar directly instead, so the strip still reads as one
     rounded pill without needing to clip anything. */
  .bars {{ display: flex; gap: 3px; height: 34px; }}
  .bar {{ flex: 1 1 0; min-width: 0; position: relative; cursor: pointer; }}
  .bar:first-child {{ border-top-left-radius: 4px; border-bottom-left-radius: 4px; }}
  .bar:last-child {{ border-top-right-radius: 4px; border-bottom-right-radius: 4px; }}
  .bar.up {{ background: var(--ok); opacity: .9; }}
  .bar.warn {{ background: var(--warn); }}
  .bar.down {{ background: var(--crit); }}
  .bar.nodata {{ background: var(--border); }}
  .bar:hover, .bar.active {{ opacity: 1; transform: scaleY(1.06); }}

  /* 2026-08-22: replaced the old CSS-only ::after tooltip (a single line
     of text) with a JS-built popover card -- mirrors status.claude.com's
     "click a day, see a card with the incident type + duration" pattern,
     which a one-line tooltip can't express once a day has more than one
     kind of incident (e.g. both a real outage AND an auth rejection).
     Positioning/open-close logic lives in the <script> block below;
     this is just the card's visual shell. */
  .daypop {{
    position: fixed; z-index: 40; width: min(300px, calc(100vw - 2rem));
    background: var(--surf2); border: 1px solid var(--border); border-radius: 6px;
    box-shadow: var(--shadow); opacity: 0; pointer-events: none;
    transform: translateY(4px); transition: opacity .12s, transform .12s;
    /* 2026-08-22: a day with several incidents could grow taller than
       the viewport -- confirmed live on a phone screen: the card's
       TOP (date header + close button) scrolled off-screen with no way
       to reach it, since position:fixed doesn't scroll with the page.
       Capping height and scrolling internally instead fixes that;
       padding moved off this element onto .daypop-head/#daypop-body
       individually so the head can stay pinned while the body scrolls.
    */
    max-height: calc(100vh - 2rem); overflow-y: auto; padding: 0;
  }}
  .daypop.open {{ opacity: 1; pointer-events: auto; transform: translateY(0); }}
  /* 2026-08-22: the arrow used to be a ::before pseudo-element of
     .daypop itself, positioned at top:-6px/bottom:-6px (i.e. just
     outside the card's own box). That broke the moment .daypop grew
     overflow-y:auto for the scrollable-popover fix -- overflow clips
     ANY child positioned outside the element's box, pseudo-elements
     included, so the arrow was invisible (or showed a stray clipped
     sliver) any time the card was tall enough to scroll, confirmed
     live via screenshot. Pulled out into its own always-fixed sibling
     element so it's never inside .daypop's scrolling/clipping context
     -- its position is computed in JS from the popover's actual
     rendered edges (getBoundingClientRect), not a CSS offset relative
     to a box that might clip it. */
  .daypop-arrow {{
    position: fixed; z-index: 41; width: 10px; height: 10px; background: var(--surf2);
    border-left: 1px solid var(--border); border-top: 1px solid var(--border);
    opacity: 0; pointer-events: none; transition: opacity .12s;
  }}
  .daypop-arrow.open {{ opacity: 1; }}
  .daypop-arrow.arrow-up {{ transform: rotate(45deg); }}
  .daypop-arrow.arrow-down {{ transform: rotate(225deg); }}
  .daypop-head {{
    display: flex; align-items: center; justify-content: space-between;
    position: sticky; top: 0; background: var(--surf2); z-index: 1;
    padding: .9rem 1rem .55rem; border-bottom: 1px solid var(--border-soft);
  }}
  #daypop-body {{ padding: .6rem 1rem 1rem; }}
  .daypop-date {{ font-weight: 650; font-size: .9rem; }}
  .daypop-close {{
    background: none; border: none; color: var(--muted); cursor: pointer; font-size: 1rem;
    line-height: 1; padding: .15rem; border-radius: 5px;
  }}
  .daypop-close:hover {{ color: var(--text); background: var(--border-soft); }}
  .daypop-row {{
    display: flex; align-items: center; gap: .5rem; padding: .5rem .6rem; border-radius: 8px;
    font-size: .82rem; margin-bottom: .4rem;
  }}
  .daypop-row:last-child {{ margin-bottom: 0; }}
  .daypop-row.down {{ background: var(--crit-bg); color: var(--crit); }}
  .daypop-row.autherr {{ background: var(--warn-bg); color: var(--warn); }}
  .daypop-row.ok {{ background: var(--ok-bg); color: var(--ok); }}
  .daypop-row-icon {{ flex-shrink: 0; }}
  .daypop-row-main {{ flex: 1; min-width: 0; }}
  .daypop-row-label {{ font-weight: 600; }}
  .daypop-row-time {{
    font-variant-numeric: tabular-nums; font-size: .74rem; opacity: .8; margin-top: .1rem;
  }}
  .daypop-row-dur {{ font-variant-numeric: tabular-nums; color: var(--text); font-weight: 600; flex-shrink: 0; }}
  .daypop-pct {{ color: var(--faint); font-size: .74rem; margin-top: .5rem; }}

  .bars-caption {{ display: flex; justify-content: space-between; align-items: center; color: var(--faint); font-size: .72rem; margin-top: 1.1rem; }}
  /* Per-row variant (status.claude.com's own "30 days ago —— 99.22%
     uptime —— Today" line, one under each service's bar strip) --
     tighter than the page-level .bars-caption since there's now one of
     these per broker, not just one at the very bottom. */
  .row-caption {{ margin-top: .5rem; font-size: .68rem; gap: .5rem; }}
  .caption-line {{ flex: 1; height: 1px; background: var(--border); min-width: 1.2rem; }}
  .legend {{ display: flex; align-items: center; gap: 1rem; }}
  .legend span {{ display: inline-flex; align-items: center; gap: .35rem; }}
  .legend i {{ width: 8px; height: 8px; border-radius: 2px; display: inline-block; }}
  .legend .lg-up {{ background: var(--ok); }}
  .legend .lg-warn {{ background: var(--warn); }}
  .legend .lg-down {{ background: var(--crit); }}

  .note {{ color: var(--faint); font-size: .76rem; text-align: center; margin-top: 1.6rem; line-height: 1.5; max-width: 34rem; margin-left: auto; margin-right: auto; }}
  .uptime-link {{ text-align: center; margin: -1.2rem 0 1.8rem; font-size: .8rem; }}
  .uptime-link a {{ color: var(--accent); text-decoration: none; }}
  .uptime-link a:hover {{ text-decoration: underline; }}

  .section-title {{ font-size: .95rem; font-weight: 650; margin: 2rem 0 .8rem; letter-spacing: -.1px; }}
  .incident-log {{
    background: var(--surf); border: 1px solid var(--border); border-radius: 6px;
    box-shadow: var(--shadow); overflow: hidden;
  }}
  .incident-day {{ border-bottom: 1px solid var(--border-soft); }}
  .incident-day:last-child {{ border-bottom: none; }}
  .incident-day[open] > .incident-summary .incident-chevron {{ transform: rotate(90deg); }}
  .incident-summary {{
    display: flex; align-items: center; gap: .55rem; padding: .9rem 1.1rem;
    cursor: pointer; list-style: none; user-select: none;
  }}
  .incident-summary::-webkit-details-marker {{ display: none; }}
  .incident-summary:hover {{ background: var(--border-soft); }}
  .incident-chevron {{ color: var(--faint); font-size: .7rem; transition: transform .12s; flex-shrink: 0; }}
  .incident-date {{ font-weight: 600; font-size: .82rem; }}
  .incident-count {{ color: var(--faint); font-size: .76rem; margin-left: auto; }}
  .incident-rows {{ padding: 0 1.1rem 1rem; }}
  .incident-row {{
    display: flex; align-items: baseline; gap: .5rem; font-size: .78rem;
    padding: .3rem 0; flex-wrap: wrap;
  }}
  .incident-icon {{ flex-shrink: 0; font-size: .7rem; }}
  .incident-icon.down {{ color: var(--crit); }}
  .incident-icon.autherr {{ color: var(--warn); }}
  .incident-host {{ font-family: ui-monospace, "SF Mono", Menlo, monospace; color: var(--muted); flex-shrink: 0; }}
  .incident-label {{ font-weight: 600; }}
  .incident-time {{ color: var(--faint); font-variant-numeric: tabular-nums; margin-left: auto; }}

  footer {{ color: var(--faint); font-size: .78rem; text-align: center; margin-top: .8rem; }}
  footer a {{ color: var(--muted); text-decoration: none; border-bottom: 1px solid var(--border); }}
  footer a:hover {{ color: var(--text); border-color: var(--muted); }}
</style>
</head>
<body>
  <div class="banner {banner_class}"><span class="banner-icon">{banner_icon}</span>{banner_text}</div>
  {maintenance_banner_html}
  <div class="wrap">
    <div class="titlebar">
      <span class="glyph">📡</span><h1>meshnode.id MQTT Status</h1>
      <span class="live-clock" id="live-clock" title="Waktu sekarang (WIB)"></span>
      <button class="theme-toggle" id="theme-toggle" aria-label="Ganti tema terang/gelap" title="Ganti tema terang/gelap">
        <svg class="icon-sun" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="4"></circle><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"></path></svg>
        <svg class="icon-moon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"></path></svg>
      </button>
    </div>
    <div class="sub"><b>{up_count}/{total}</b> broker aktif — data diperbarui tiap 10 menit</div>
    <div class="uptime-link"><a href="uptime.html">Lihat riwayat uptime lengkap →</a> · <a href="bot-status.html">Status bot →</a></div>
    <div class="panel">{"".join(rows)}</div>
    <div class="bars-caption">
      <span>{HISTORY_DAYS} hari lalu</span>
      <span class="legend"><span><i class="lg-up"></i>Aktif</span><span><i class="lg-warn"></i>Sebagian</span><span><i class="lg-down"></i>Down</span></span>
      <span>Hari ini</span>
    </div>
    <div class="ping-legend">
      <span><i class="lg-lxc"></i>{bot_display_name}{f" ({lxc_city})" if lxc_city else ""}</span>
      <span><i class="lg-ci"></i>GitHub Actions{f" ({actions_city})" if actions_city else ""}</span>
    </div>
    <p class="note">Ping cadangan wajar lebih tinggi karena jaraknya — bukan tanda broker lambat.</p>
    <h2 class="section-title">Riwayat Insiden</h2>
    <div class="incident-log">{_incident_log_html()}</div>
    <footer>Commit {commit_sha} · Diperbarui {updated_str} · <a href="https://github.com/richardvsw/mqtt-status">Sumber di GitHub</a></footer>
  </div>
  <div class="daypop" id="daypop">
    <div class="daypop-head">
      <span class="daypop-date" id="daypop-date"></span>
      <button class="daypop-close" id="daypop-close" aria-label="Tutup">✕</button>
    </div>
    <div id="daypop-body"></div>
  </div>
  <div class="daypop-arrow" id="daypop-arrow"></div>
  <script>
    // Click-to-open day popover -- same interaction status.claude.com
    // uses (click a day cell, get a card with incident type + duration),
    // chosen over the old CSS ::after tooltip because a day can have more
    // than one kind of incident (a real Down AND a separate Auth Ditolak
    // period) that a single tooltip line can't represent cleanly.
    var pop = document.getElementById("daypop");
    var popDate = document.getElementById("daypop-date");
    var popBody = document.getElementById("daypop-body");
    var popClose = document.getElementById("daypop-close");
    var popArrow = document.getElementById("daypop-arrow");
    var activeBar = null;

    function closePop() {{
      pop.classList.remove("open");
      popArrow.classList.remove("open");
      if (activeBar) activeBar.classList.remove("active");
      activeBar = null;
    }}

    function rowHtml(kind, label, seconds, startClock, endClock) {{
      var icon = kind === "down" ? "\u2715" : (kind === "autherr" ? "\u26A0" : "\u2713");
      var mins = Math.round(seconds / 60);
      var dur;
      if (seconds < 30) {{
        dur = "sesaat";
      }} else if (mins < 60) {{
        dur = mins + " menit";
      }} else {{
        var h = Math.floor(mins / 60), m = mins % 60;
        dur = m === 0 ? (h + " jam") : (h + " jam " + m + " menit");
      }}
      var timeRange = (startClock && endClock)
        ? '<div class="daypop-row-time">' + startClock + '\u2013' + endClock + ' WIB</div>' : '';
      return '<div class="daypop-row ' + kind + '"><span class="daypop-row-icon">' + icon +
             '</span><div class="daypop-row-main"><span class="daypop-row-label">' + label + '</span>' + timeRange +
             '</div><span class="daypop-row-dur">' + dur + '</span></div>';
    }}

    // 2026-08-22: pulled out of the click handler so it can be re-run,
    // not just computed once at click time. Real-device testing kept
    // showing the arrow landing somewhere on the card's face instead of
    // its edge, despite the exact same logic measuring correctly every
    // time in automated headless testing -- the difference is Google
    // Fonts: "Inter" loads over the network async (see the <link> in
    // <head>), so a tap that lands before it's ready gets positioned
    // against fallback-font layout, then the row text reflows (FOUT)
    // once Inter arrives, changing the card's real height/edges out
    // from under an arrow that was never told to recheck. A headless
    // test with fonts already cached never hits this window. Re-running
    // this on fonts.ready + resize (mobile browsers can also resize the
    // viewport post-tap as the address bar collapses/expands) closes
    // that gap regardless of which of those actually fires.
    function positionPopover(bar) {{
        var r = bar.getBoundingClientRect();
        pop.classList.remove("arrow-up", "arrow-down");
        var popWidth = pop.offsetWidth || 300;
        // Prefer the VISUAL viewport over window.innerWidth/Height where
        // available -- on mobile Chrome the layout viewport (what
        // window.innerHeight reports) can be taller than what's actually
        // visible right now (address bar covering part of it), and using
        // the layout size here is what let the card's true bottom edge
        // land under the toolbar instead of the real visible fold.
        var vvw = window.visualViewport;
        var viewW = vvw ? vvw.width : window.innerWidth;
        var viewH = vvw ? vvw.height : window.innerHeight;
        // 2026-08-22: clamped against the real viewport edges (with an
        // 8px margin) instead of just the .wrap container's own bounds
        // -- .wrap's right edge IS effectively the viewport edge on
        // narrow phone widths, so the old clamp let the card render
        // flush against the actual screen edge with zero breathing
        // room, which read as "cut off" (confirmed live: a click on the
        // rightmost/"today" bar produced exactly this). The 8px margin
        // applies on both sides now.
        var margin = 8;
        var left = Math.min(
          Math.max(r.left + r.width / 2 - popWidth / 2, margin),
          viewW - popWidth - margin
        );
        // The card's own left edge can end up anywhere within that
        // clamp range, independent of the bar's true center -- so the
        // arrow needs its OWN position, not just "centered on the
        // card". This is the fix for the arrow pointing at the wrong
        // spot: it's the bar's center MINUS wherever the card actually
        // landed, further clamped so it can't render outside the
        // card's own rounded corners.
        var arrowX = r.left + r.width / 2 - left;
        arrowX = Math.min(Math.max(arrowX, 16), popWidth - 16);
        var spaceAbove = r.top;
        var vMargin = 8;
        pop.style.transform = "translateY(0)";
        if (spaceAbove > 220) {{
          // 2026-08-22: was `top: (r.top-12)px` + translateY(-100%) --
          // fine for a short card, but a day with several incidents
          // could render taller than the space actually available
          // above the bar, pushing the card's TOP (including the date
          // header and close button) above y=0 with no way to reach it
          // (confirmed live). Setting both top AND bottom instead lets
          // the browser compute the box's real height as whatever fits
          // between them -- it can never push past the vMargin safety
          // line at the top, and overflow-y:auto (see .daypop CSS)
          // scrolls internally for anything that still doesn't fit.
          pop.style.top = vMargin + "px";
          pop.style.bottom = (viewH - r.top + 12) + "px";
          pop.classList.add("arrow-down");
        }} else {{
          pop.style.top = (r.bottom + 12) + "px";
          pop.style.bottom = vMargin + "px";
          pop.classList.add("arrow-up");
        }}
        pop.style.left = left + "px";
        pop.classList.add("open");

        // Arrow position is read back from the popover's ACTUAL
        // rendered box (post-layout), not recomputed from the same
        // top/bottom/left values used to place .daypop -- this stays
        // correct even if content height changes what the browser
        // settles on between the two top/bottom constraints, and it's
        // immune to internal scrolling since this element lives
        // outside .daypop entirely (see .daypop-arrow CSS comment).
        var popRect = pop.getBoundingClientRect();
        popArrow.classList.remove("arrow-up", "arrow-down");
        if (pop.classList.contains("arrow-up")) {{
          popArrow.style.top = (popRect.top - 5) + "px";
          popArrow.classList.add("arrow-up");
        }} else {{
          popArrow.style.top = (popRect.bottom - 5) + "px";
          popArrow.classList.add("arrow-down");
        }}
        popArrow.style.left = (popRect.left + arrowX - 5) + "px";
        popArrow.classList.add("open");
    }}

    document.querySelectorAll(".bar").forEach(function (bar) {{
      bar.addEventListener("click", function (e) {{
        e.stopPropagation();
        var wasActive = bar.classList.contains("active");
        document.querySelectorAll(".bar.active").forEach(function (b) {{ b.classList.remove("active"); }});
        if (wasActive) {{ closePop(); return; }}

        var status = bar.dataset.status;
        if (status === "nodata") return;  // nothing to show yet for that day

        bar.classList.add("active");
        activeBar = bar;
        popDate.textContent = bar.dataset.date;

        var incidents = [];
        try {{ incidents = JSON.parse(bar.dataset.incidents || "[]"); }}
        catch (err) {{ incidents = []; }}

        var body = "";
        if (incidents.length === 0) {{
          // 2026-08-22: this branch used to hardcode the green "ok"
          // style/checkmark regardless of the real status -- confirmed
          // live on mqtt5.meshnode.id: its very first-ever check failed
          // (0% up that day), but since REAL_INCIDENTS only records a
          // CONFIRMED outage (2 consecutive fails) and this was only 1,
          // there was no incident to show, and the fallback showed a
          // green checkmark + "no details" as if it were healthy. The
          // icon/color now matches bar.dataset.status (the same value
          // that colors the bar itself), so a red/down day can't render
          // a green "no details available" row.
          body = '<div class="daypop-row ok"><span class="daypop-row-icon">✓</span>' +
                 '<span class="daypop-row-label">Beroperasi Normal</span></div>';
        }} else {{
          incidents.forEach(function (inc) {{
            body += rowHtml(inc.kind, inc.label, inc.seconds, inc.start_clock, inc.end_clock);
          }});
        }}
        if (bar.dataset.pct) {{
          var pctLabel = bar.dataset.today === "1"
            ? 'Aktif ' + bar.dataset.pct + '% dari waktu berjalan hari ini'
            : 'Aktif ' + bar.dataset.pct + '%';
          body += '<div class="daypop-pct">' + pctLabel + '</div>';
        }}
        popBody.innerHTML = body;
        positionPopover(bar);
      }});
    }});

    // Re-position (never re-open) if the active popover's own layout
    // might have shifted out from under it -- see positionPopover's own
    // comment for why fonts/resize specifically.
    function repositionIfOpen() {{
      if (activeBar && pop.classList.contains("open")) positionPopover(activeBar);
    }}
    if (document.fonts && document.fonts.ready) {{
      document.fonts.ready.then(repositionIfOpen);
    }}
    window.addEventListener("resize", repositionIfOpen, {{ passive: true }});
    // 2026-08-22: window's own "resize" event does NOT reliably fire for
    // Android Chrome's address-bar collapse/expand -- that's specifically
    // what visualViewport's own separate resize event exists for (the
    // layout viewport window.innerHeight uses and the visual viewport the
    // user actually sees can diverge exactly while the toolbar animates).
    // Confirmed as the live gap: a taller/scrollable popover (more likely
    // to be open across a toolbar transition, simply by being open a bit
    // longer while the user reads more rows) kept mispositioning even
    // after the window-resize + fonts.ready listeners above landed, while
    // short non-scrolling popovers were already fine. window.resize is
    // kept too since visualViewport isn't universal (older WebKit).
    if (window.visualViewport) {{
      window.visualViewport.addEventListener("resize", repositionIfOpen, {{ passive: true }});
    }}

    var themeToggle = document.getElementById("theme-toggle");
    themeToggle.addEventListener("click", function () {{
      var root = document.documentElement;
      var isLight = root.getAttribute("data-theme") === "light";
      if (isLight) {{
        root.removeAttribute("data-theme");
        try {{ localStorage.setItem("mqtt-status-theme", "dark"); }} catch (e) {{}}
      }} else {{
        root.setAttribute("data-theme", "light");
        try {{ localStorage.setItem("mqtt-status-theme", "light"); }} catch (e) {{}}
      }}
      closePop();
    }});

    popClose.addEventListener("click", function (e) {{ e.stopPropagation(); closePop(); }});
    document.addEventListener("click", function (e) {{
      if (pop.classList.contains("open") && !pop.contains(e.target)) closePop();
    }});
    window.addEventListener("scroll", closePop, {{ passive: true }});

    // 2026-08-22: live-ticking clock -- WIB is a fixed UTC+7 offset
    // (no DST, no historical changes to account for), so computing it
    // by hand from the visitor's own UTC clock is simpler and more
    // portable than fighting Intl.DateTimeFormat timezone-name
    // support across older mobile browsers. Ticks independently of
    // the page's own 60s data refresh -- this is just wall-clock
    // time, not tied to when the data was last checked.
    var liveClock = document.getElementById("live-clock");
    function tickClock() {{
      var wib = new Date(Date.now() + 7 * 3600 * 1000);
      var hh = String(wib.getUTCHours()).padStart(2, "0");
      var mm = String(wib.getUTCMinutes()).padStart(2, "0");
      var ss = String(wib.getUTCSeconds()).padStart(2, "0");
      liveClock.textContent = hh + ":" + mm + ":" + ss + " WIB";
    }}
    tickClock();
    setInterval(tickClock, 1000);
  </script>
</body>
</html>
'''

with open(OUT_PATH, "w") as f:
    f.write(html)
print(f"wrote {OUT_PATH} ({up_count}/{total} brokers up)")

# 2026-08-22: everything from here on (uptime.html) reads a MERGED
# history that also includes mesh_bot/meshtasticd/lxc-monitor, written
# by check_bot_status.py (same repo, same LXC run) -- reassigning the
# `history` name rather than threading a second dict through every
# function below, since day_bar_html/host_uptime_pct for index.html's
# own rows have ALL already run and finished by this point.
_bot_history = {}
if os.path.isdir("bot_history"):
    for _fname in sorted(os.listdir("bot_history")):
        if _fname.endswith(".json"):
            _bot_history.update(load_json(os.path.join("bot_history", _fname), {}))
_merged_history = {d: dict(hosts) for d, hosts in history.items()}
for _d, _entities in _bot_history.items():
    _merged_history.setdefault(_d, {}).update(_entities)

# 2026-08-22: was 3 separate dropdown entries (mesh_bot, meshtasticd,
# lxc-monitor) -- collapsed into one "Bot & Server" combined entry so
# the dropdown reads as MQTT brokers + one bot row, not 6+3=9 items
# where 3 of them are really facets of the same thing. The 3 underlying
# per-service rows/history are completely untouched -- bot-status.html
# still shows them individually; this combined slot is synthesized
# ADDITIONALLY, summed across all 3 services' up/total for that day, so
# a day where any one service degraded shows as partially degraded here
# rather than needing its own separate row.
BOT_COMBINED_KEY = "bot-combined"
BOT_COMBINED_LABEL = "Bot & Server (RiV-meshBot)"
for _d, _entities in _bot_history.items():
    _up_sum = sum(e.get("up", 0) for e in _entities.values())
    _total_sum = sum(e.get("total", 0) for e in _entities.values())
    if _total_sum:
        _merged_history.setdefault(_d, {})[BOT_COMBINED_KEY] = {
            "up": _up_sum, "total": _total_sum, "down": _total_sum - _up_sum,
        }
history = _merged_history


# ── Historical uptime calendar page (uptime.html) ──────────────────────────
# Mirrors status.claude.com's own /uptime page (confirmed live against the
# real site, 2026-08-22): ONE broker's calendar visible at a time (picked
# via dropdown, not all six stacked), a 3-month sliding window with
# prev/next arrows, and clicking a day opens the SAME incident popover the
# main page already uses (reuses _clip_incidents_to_day -- real clock
# start/end per incident, not just a color) rather than a plain tooltip.
import calendar as _calendar_mod
import html as _html_mod

_today_str_cal = datetime.fromtimestamp(now, WIB).strftime("%Y-%m-%d")


def _combined_bot_incidents(d):
    out = []
    for _svc in BOT_ENTITIES:
        for inc in _clip_incidents_to_day(_svc, d):
            inc = dict(inc)
            inc["label"] = f"{BOT_ENTITY_LABEL[_svc]} Down"
            out.append(inc)
    out.sort(key=lambda x: x["start_clock"])
    return out


def _cal_day_cell(host, d):
    slot = history.get(d, {}).get(host)
    if not slot or slot.get("total", 0) == 0:
        return f'<div class="cal-day nodata" data-date="{d}" data-status="nodata"></div>'
    total = slot["total"]
    pct = slot["up"] / total * 100
    # cls kept for data-status (drives the popover's no-incident-data
    # fallback icon/label -- see the click handler's noDataStyle map) --
    # the VISIBLE color comes from the gradient below, not this bucket.
    incidents = _combined_bot_incidents(d) if host == BOT_COMBINED_KEY else _clip_incidents_to_day(host, d)
    if incidents:
        cls = "up" if pct >= 99.5 else ("warn" if pct >= 90 else "down")
        color = _severity_color(pct)
    else:
        cls = "up"
        color = _severity_color(100.0)
    incidents_attr = _html_mod.escape(json.dumps(incidents), quote=True)
    is_today = "1" if d == _today_str_cal else "0"
    return (
        f'<div class="cal-day" style="background:{color}" data-date="{d}" data-status="{cls}" '
        f'data-pct="{pct:.0f}" data-today="{is_today}" data-incidents="{incidents_attr}"></div>'
    )


def _month_uptime_pct(host, year, month):
    up_sum, total_sum = 0, 0
    _, days_in_month = _calendar_mod.monthrange(year, month)
    for day in range(1, days_in_month + 1):
        slot = history.get(f"{year:04d}-{month:02d}-{day:02d}", {}).get(host)
        if slot:
            up_sum += slot.get("up", 0)
            total_sum += slot.get("total", 0)
    return (up_sum / total_sum * 100) if total_sum else None


def _months_with_data(host, max_months=13):
    """(year, month) tuples ascending, from the earliest month this host
    has ANY recorded data through the current month -- capped at
    max_months so a broker never accumulates an unbounded month list."""
    host_days = sorted(d for d, hosts in history.items() if host in hosts)
    if not host_days:
        return []
    start_y, start_m = (int(x) for x in host_days[0].split("-")[:2])
    end_dt = datetime.fromtimestamp(now, WIB)
    months = []
    y, m = start_y, start_m
    while (y, m) <= (end_dt.year, end_dt.month):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months[-max_months:]


def _month_block_html(host, year, month):
    first_weekday, days_in_month = _calendar_mod.monthrange(year, month)
    cell_parts = ['<div class="cal-day empty"></div>'] * first_weekday
    for day in range(1, days_in_month + 1):
        d = f"{year:04d}-{month:02d}-{day:02d}"
        if d > _today_str_cal:
            break  # don't render future days at all, not even as nodata
        cell_parts.append(_cal_day_cell(host, d))
    cells_html = "".join(cell_parts)
    pct = _month_uptime_pct(host, year, month)
    pct_label = f"{pct:.2f}%" if pct is not None else "-"
    month_name = _ID_MONTHS_FULL[month]
    return (
        f'\n      <div class="cal-month" data-ym="{year:04d}-{month:02d}" data-label="{month_name} {year}">\n'
        f'        <div class="cal-month-head"><span>{month_name} {year}</span><span class="cal-month-pct">{pct_label}</span></div>\n'
        f'        <div class="cal-grid">{cells_html}</div>\n'
        '      </div>'
    )


def _broker_section_html(host, is_first):
    months = _months_with_data(host)
    display = "" if is_first else "display:none"
    if not months:
        return f'<div class="cal-broker" data-host="{host}" style="{display}"><p class="note">Belum ada data.</p></div>'
    blocks = "".join(_month_block_html(host, y, m) for y, m in months)
    return f'<div class="cal-broker" data-host="{host}" style="{display}">{blocks}</div>'


_upd_dt2, _upd_abbr2, _ = _id_strftime_dmy(now, WIB)
uptime_updated_str = _upd_dt2.strftime(f"%d {_upd_abbr2} %Y, %H:%M:%S WIB")
_UPTIME_HOSTS = BROKERS + [BOT_COMBINED_KEY]
_UPTIME_LABELS = {h: h for h in BROKERS}
_UPTIME_LABELS[BOT_COMBINED_KEY] = BOT_COMBINED_LABEL
_broker_sections = "".join(_broker_section_html(h, h == _UPTIME_HOSTS[0]) for h in _UPTIME_HOSTS)
_broker_options = "".join(f'<option value="{h}">{_UPTIME_LABELS[h]}</option>' for h in _UPTIME_HOSTS)

uptime_html = f"""<!doctype html>
<html lang="id">
<script>
(function () {{
  try {{
    if (localStorage.getItem("mqtt-status-theme") === "light") {{
      document.documentElement.setAttribute("data-theme", "light");
    }}
  }} catch (e) {{}}
}})();
</script>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Riwayat Uptime — meshnode.id MQTT Status</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;650;700&display=swap" rel="stylesheet">
<style>
  :root {{
    --bg: #111827; --surf: #1f2937; --surf2: #232f42; --border: #2e3c51; --border-soft: #253247;
    --text: #e5e7eb; --muted: #94a3b8; --faint: #64748b;
    --ok: #2fb344; --ok-dim: #1e4326; --ok-bg: #0f2115;
    --warn: #f76707; --warn-dim: #4a2c0d; --warn-bg: #271a0a;
    --crit: #d63939; --crit-dim: #4a2020; --crit-bg: #2a1414;
    --accent: #066fd1; --shadow: 0 1px 2px rgba(0,0,0,.3), 0 8px 24px -8px rgba(0,0,0,.5);
  }}
  html {{ color-scheme: dark; }}
  html[data-theme="light"] {{ color-scheme: light; }}
  :root[data-theme="light"] {{
    --bg: #f9fafb; --surf: #ffffff; --surf2: #ffffff; --border: #e5e7eb; --border-soft: #eef0f2;
    --text: #1f2937; --muted: #67748c; --faint: #94a3b8;
    --ok: #2fb344; --ok-dim: #bfe8c8; --ok-bg: #eafbee;
    --warn: #f76707; --warn-dim: #ffd8ad; --warn-bg: #fff2e6;
    --crit: #d63939; --crit-dim: #f5b8b8; --crit-bg: #fdecec;
    --accent: #066fd1; --shadow: 0 1px 2px rgba(0,0,0,.05), 0 8px 24px -8px rgba(0,0,0,.12);
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; min-height: 100vh; color: var(--text); background: var(--bg);
    font-family: "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    -webkit-font-smoothing: antialiased;
  }}
  .wrap {{ max-width: 680px; margin: 0 auto; padding: 2.4rem 1.25rem 2rem; }}
  h1 {{ font-size: 1.15rem; font-weight: 650; margin: .8rem 0 .3rem; }}
  .back {{ color: var(--accent); text-decoration: none; font-size: .85rem; }}
  .back:hover {{ text-decoration: underline; }}
  select {{
    width: 100%; margin-top: 1.2rem; padding: .65rem .8rem; border-radius: 6px;
    border: 1px solid var(--border); background: var(--surf); color: var(--text);
    font-family: inherit; font-size: .88rem; font-weight: 600;
  }}
  .cal-nav {{
    display: flex; align-items: center; justify-content: space-between;
    margin-top: 1.4rem; margin-bottom: .6rem;
  }}
  .cal-nav button {{
    width: 32px; height: 32px; border-radius: 6px; border: 1px solid var(--border);
    background: var(--surf); color: var(--text); cursor: pointer; font-size: .9rem;
  }}
  .cal-nav button:disabled {{ opacity: .35; cursor: default; }}
  .cal-nav button:not(:disabled):hover {{ background: var(--surf2); }}
  .cal-range {{ font-size: .85rem; color: var(--muted); font-weight: 600; }}
  .cal-months {{ display: flex; flex-wrap: wrap; gap: 1.4rem; }}
  /* 2026-08-22: was min(220px, 100%), left over from the old design
     where multiple brokers' month blocks sat side-by-side in a
     wrapping row -- now only one broker/month shows at a time (see
     the dropdown + 3-month-window rework), so capping width just
     wasted most of the screen on mobile for no reason. */
  .cal-month {{ width: 100%; max-width: 420px; margin: 0 auto; }}
  .cal-month-head {{
    display: flex; justify-content: space-between; font-size: .78rem; font-weight: 600;
    color: var(--muted); margin-bottom: .4rem;
  }}
  .cal-month-pct {{ font-variant-numeric: tabular-nums; color: var(--text); }}
  .cal-grid {{ display: grid; grid-template-columns: repeat(7, 1fr); gap: 3px; width: 100%; }}
  .cal-day {{ aspect-ratio: 1; border-radius: 3px; background: var(--border); cursor: pointer; }}
  .cal-day.empty {{ background: transparent; cursor: default; }}
  .cal-day.up {{ background: var(--ok); }}
  .cal-day.warn {{ background: var(--warn); }}
  .cal-day.down {{ background: var(--crit); }}
  .cal-day.nodata {{ background: var(--border); cursor: default; }}
  .cal-day:hover:not(.empty):not(.nodata) {{ opacity: .85; transform: scale(1.08); }}
  .note {{ color: var(--faint); font-size: .82rem; }}
  footer {{ color: var(--faint); font-size: .78rem; text-align: center; margin-top: 1.5rem; }}

  .daypop {{
    position: fixed; z-index: 40; width: min(300px, calc(100vw - 2rem));
    background: var(--surf2); border: 1px solid var(--border); border-radius: 6px;
    box-shadow: var(--shadow); opacity: 0; pointer-events: none;
    transform: translateY(4px); transition: opacity .12s, transform .12s;
    max-height: calc(100vh - 2rem); overflow-y: auto; padding: 0;
  }}
  .daypop.open {{ opacity: 1; pointer-events: auto; transform: translateY(0); }}
  .daypop-arrow {{
    position: fixed; z-index: 41; width: 10px; height: 10px; background: var(--surf2);
    border-left: 1px solid var(--border); border-top: 1px solid var(--border);
    opacity: 0; pointer-events: none; transition: opacity .12s;
  }}
  .daypop-arrow.open {{ opacity: 1; }}
  .daypop-arrow.arrow-up {{ transform: rotate(45deg); }}
  .daypop-arrow.arrow-down {{ transform: rotate(225deg); }}
  .daypop-head {{
    display: flex; align-items: center; justify-content: space-between;
    position: sticky; top: 0; background: var(--surf2); z-index: 1;
    padding: .9rem 1rem .55rem; border-bottom: 1px solid var(--border-soft);
  }}
  #daypop-body {{ padding: .6rem 1rem 1rem; }}
  .daypop-date {{ font-weight: 650; font-size: .9rem; }}
  .daypop-close {{
    background: none; border: none; color: var(--muted); cursor: pointer; font-size: 1rem;
    line-height: 1; padding: .15rem; border-radius: 5px;
  }}
  .daypop-close:hover {{ color: var(--text); background: var(--border-soft); }}
  .daypop-row {{
    display: flex; align-items: center; gap: .5rem; padding: .5rem .6rem; border-radius: 8px;
    font-size: .82rem; margin-bottom: .4rem;
  }}
  .daypop-row:last-child {{ margin-bottom: 0; }}
  .daypop-row.down {{ background: var(--crit-bg); color: var(--crit); }}
  .daypop-row.autherr {{ background: var(--warn-bg); color: var(--warn); }}
  .daypop-row.ok {{ background: var(--ok-bg); color: var(--ok); }}
  .daypop-row-icon {{ flex-shrink: 0; }}
  .daypop-row-main {{ flex: 1; min-width: 0; }}
  .daypop-row-label {{ font-weight: 600; }}
  .daypop-row-time {{ font-variant-numeric: tabular-nums; font-size: .74rem; opacity: .8; margin-top: .1rem; }}
  .daypop-row-dur {{ font-variant-numeric: tabular-nums; color: var(--text); font-weight: 600; flex-shrink: 0; }}
  .daypop-pct {{ color: var(--faint); font-size: .74rem; margin-top: .5rem; }}
</style>
</head>
<body>
  <div class="wrap">
    <a class="back" href="index.html">← Kembali ke status</a>
    <h1>Riwayat Uptime</h1>
    <select id="broker-select">{_broker_options}</select>
    <div class="cal-nav">
      <button id="cal-prev" aria-label="Bulan sebelumnya">‹</button>
      <span class="cal-range" id="cal-range"></span>
      <button id="cal-next" aria-label="Bulan berikutnya">›</button>
    </div>
    <div id="cal-container">{_broker_sections}</div>
    <footer>Diperbarui {uptime_updated_str}</footer>
  </div>
  <div class="daypop" id="daypop">
    <div class="daypop-head">
      <span class="daypop-date" id="daypop-date"></span>
      <button class="daypop-close" id="daypop-close" aria-label="Tutup">✕</button>
    </div>
    <div id="daypop-body"></div>
  </div>
  <div class="daypop-arrow" id="daypop-arrow"></div>
  <script>
    // ── day popover -- identical interaction/positioning to the main
    // status page's (see check_and_render.py's own comments there for
    // the font-reflow/visualViewport reasoning); only the trigger
    // selector (.cal-day instead of .bar) differs.
    var pop = document.getElementById("daypop");
    var popDate = document.getElementById("daypop-date");
    var popBody = document.getElementById("daypop-body");
    var popClose = document.getElementById("daypop-close");
    var popArrow = document.getElementById("daypop-arrow");
    var activeCell = null;

    function closePop() {{
      pop.classList.remove("open");
      popArrow.classList.remove("open");
      if (activeCell) activeCell.classList.remove("active");
      activeCell = null;
    }}

    function rowHtml(kind, label, seconds, startClock, endClock) {{
      var icon = kind === "down" ? "✕" : (kind === "autherr" ? "⚠" : "✓");
      var mins = Math.round(seconds / 60);
      var dur;
      if (seconds < 30) {{ dur = "sesaat"; }}
      else if (mins < 60) {{ dur = mins + " menit"; }}
      else {{ var h = Math.floor(mins / 60), m = mins % 60; dur = m === 0 ? (h + " jam") : (h + " jam " + m + " menit"); }}
      var timeRange = (startClock && endClock)
        ? '<div class="daypop-row-time">' + startClock + '–' + endClock + ' WIB</div>' : '';
      return '<div class="daypop-row ' + kind + '"><span class="daypop-row-icon">' + icon +
             '</span><div class="daypop-row-main"><span class="daypop-row-label">' + label + '</span>' + timeRange +
             '</div><span class="daypop-row-dur">' + dur + '</span></div>';
    }}

    function positionPopover(cell) {{
      var r = cell.getBoundingClientRect();
      pop.classList.remove("arrow-up", "arrow-down");
      var popWidth = pop.offsetWidth || 300;
      var vvw = window.visualViewport;
      var viewW = vvw ? vvw.width : window.innerWidth;
      var viewH = vvw ? vvw.height : window.innerHeight;
      var margin = 8;
      var left = Math.min(Math.max(r.left + r.width / 2 - popWidth / 2, margin), viewW - popWidth - margin);
      var arrowX = r.left + r.width / 2 - left;
      arrowX = Math.min(Math.max(arrowX, 16), popWidth - 16);
      var spaceAbove = r.top;
      var vMargin = 8;
      pop.style.transform = "translateY(0)";
      if (spaceAbove > 220) {{
        pop.style.top = vMargin + "px";
        pop.style.bottom = (viewH - r.top + 12) + "px";
        pop.classList.add("arrow-down");
      }} else {{
        pop.style.top = (r.bottom + 12) + "px";
        pop.style.bottom = vMargin + "px";
        pop.classList.add("arrow-up");
      }}
      pop.style.left = left + "px";
      pop.classList.add("open");
      var popRect = pop.getBoundingClientRect();
      popArrow.classList.remove("arrow-up", "arrow-down");
      if (pop.classList.contains("arrow-up")) {{
        popArrow.style.top = (popRect.top - 5) + "px";
        popArrow.classList.add("arrow-up");
      }} else {{
        popArrow.style.top = (popRect.bottom - 5) + "px";
        popArrow.classList.add("arrow-down");
      }}
      popArrow.style.left = (popRect.left + arrowX - 5) + "px";
      popArrow.classList.add("open");
    }}

    function openDayCell(cell) {{
      var wasActive = cell.classList.contains("active");
      document.querySelectorAll(".cal-day.active").forEach(function (c) {{ c.classList.remove("active"); }});
      if (wasActive) {{ closePop(); return; }}
      var status = cell.dataset.status;
      if (!status || status === "nodata") return;
      cell.classList.add("active");
      activeCell = cell;
      popDate.textContent = cell.dataset.date;
      var incidents = [];
      try {{ incidents = JSON.parse(cell.dataset.incidents || "[]"); }} catch (err) {{ incidents = []; }}
      var body = "";
      if (incidents.length === 0) {{
        body = '<div class="daypop-row ok"><span class="daypop-row-icon">✓</span>' +
               '<span class="daypop-row-label">Beroperasi Normal</span></div>';
      }} else {{
        incidents.forEach(function (inc) {{ body += rowHtml(inc.kind, inc.label, inc.seconds, inc.start_clock, inc.end_clock); }});
      }}
      if (cell.dataset.pct) {{
        body += '<div class="daypop-pct">Aktif ' + cell.dataset.pct + '%</div>';
      }}
      popBody.innerHTML = body;
      positionPopover(cell);
    }}

    document.getElementById("cal-container").addEventListener("click", function (e) {{
      var cell = e.target.closest(".cal-day");
      if (cell) {{ e.stopPropagation(); openDayCell(cell); }}
    }});

    function repositionIfOpen() {{ if (activeCell && pop.classList.contains("open")) positionPopover(activeCell); }}
    if (document.fonts && document.fonts.ready) {{ document.fonts.ready.then(repositionIfOpen); }}
    window.addEventListener("resize", repositionIfOpen, {{ passive: true }});
    if (window.visualViewport) {{ window.visualViewport.addEventListener("resize", repositionIfOpen, {{ passive: true }}); }}
    popClose.addEventListener("click", function (e) {{ e.stopPropagation(); closePop(); }});
    document.addEventListener("click", function (e) {{ if (pop.classList.contains("open") && !pop.contains(e.target)) closePop(); }});
    window.addEventListener("scroll", closePop, {{ passive: true }});

    // ── broker dropdown + 3-month sliding window ────────────────────────
    var brokerSelect = document.getElementById("broker-select");
    var prevBtn = document.getElementById("cal-prev");
    var nextBtn = document.getElementById("cal-next");
    var rangeLabel = document.getElementById("cal-range");
    var WINDOW_SIZE = 3;
    var windowStart = 0;

    function activeBrokerEl() {{
      return document.querySelector('.cal-broker[data-host="' + brokerSelect.value + '"]');
    }}

    function renderWindow() {{
      closePop();
      var broker = activeBrokerEl();
      if (!broker) return;
      var months = Array.from(broker.querySelectorAll(".cal-month"));
      if (months.length === 0) {{ rangeLabel.textContent = ""; prevBtn.disabled = nextBtn.disabled = true; return; }}
      windowStart = Math.max(0, Math.min(windowStart, months.length - 1));
      var visible = months.slice(windowStart, windowStart + WINDOW_SIZE);
      months.forEach(function (m) {{ m.style.display = visible.includes(m) ? "" : "none"; }});
      var first = visible[0], last = visible[visible.length - 1];
      rangeLabel.textContent = visible.length > 1
        ? (first.dataset.label + " to " + last.dataset.label)
        : first.dataset.label;
      prevBtn.disabled = windowStart <= 0;
      nextBtn.disabled = windowStart + WINDOW_SIZE >= months.length;
    }}

    brokerSelect.addEventListener("change", function () {{
      document.querySelectorAll(".cal-broker").forEach(function (b) {{ b.style.display = "none"; }});
      var broker = activeBrokerEl();
      if (broker) broker.style.display = "";
      var months = broker ? broker.querySelectorAll(".cal-month").length : 0;
      windowStart = Math.max(0, months - WINDOW_SIZE);
      renderWindow();
    }});
    prevBtn.addEventListener("click", function () {{ windowStart -= WINDOW_SIZE; renderWindow(); }});
    nextBtn.addEventListener("click", function () {{ windowStart += WINDOW_SIZE; renderWindow(); }});

    (function initWindow() {{
      var broker = activeBrokerEl();
      var months = broker ? broker.querySelectorAll(".cal-month").length : 0;
      windowStart = Math.max(0, months - WINDOW_SIZE);
      renderWindow();
    }})();

    var themeToggle = null; // no theme toggle button on this page (yet) -- theme still applied via the inline localStorage check in <head>
  </script>
</body>
</html>
"""

with open(UPTIME_OUT_PATH, "w") as f:
    f.write(uptime_html)
print(f"wrote {UPTIME_OUT_PATH}")
