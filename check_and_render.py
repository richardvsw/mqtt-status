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

HISTORY_PATH = "history.json"
LOG_PATH = "log.jsonl"  # append-only per-run detail log -- kept separate so index.html only ever holds the current rendered state, not accumulating history
STATE_PATH = "state.json"  # tracks current_outage_start per host, same purpose as mqtt_tap's own outage_state
OUT_PATH = "index.html"
# 2026-08-22: dropped from 90 -> 30 to match the reference layout
# (status.claude.com) -- at 90 days each bar was too thin on mobile to
# reliably tap; 30 keeps every bar wide enough for the new click-to-open
# popover below to feel deliberate rather than fiddly. Existing history
# beyond 30 days simply stops being *displayed* -- the trim logic a few
# lines down only deletes what's now unreachably old, so this is a
# display-window change, not a data-loss one for anything within 30 days.
HISTORY_DAYS = 30


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
history = load_json(HISTORY_PATH, {})

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
cutoff_date = (datetime.fromtimestamp(now, WIB) - timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%d")
for d in [d for d in history if d < cutoff_date]:
    del history[d]
save_json(HISTORY_PATH, history)

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
    return incidents


REAL_INCIDENTS = _build_real_incidents()
KIND_LABEL = {"down": "Down", "autherr": "Autentikasi Ditolak"}


def _clip_incidents_to_day(host, d):
    """Real incidents clipped to day `d`'s [00:00, 24:00) WIB window,
    each carrying real clock start/end (or day boundaries for whatever
    portion of a multi-day incident falls on this particular day)."""
    day_start = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=WIB).timestamp()
    day_end = day_start + 86400
    out = []
    for inc in REAL_INCIDENTS.get(host, []):
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
            "kind": inc["kind"],
            "label": KIND_LABEL[inc["kind"]],
            "seconds": seg_end - seg_start,
            "start_clock": datetime.fromtimestamp(seg_start, WIB).strftime("%H:%M"),
            "end_clock": end_clock,
        })
    return out


def day_bar_html(host):
    bars = []
    for d in day_labels:
        slot = history.get(d, {}).get(host)
        if not slot or slot["total"] == 0:
            bars.append(f'<div class="bar nodata" data-date="{d}" data-status="nodata"></div>')
            continue
        total = slot["total"]
        pct = slot["up"] / total * 100
        cls = "up" if pct >= 99.5 else ("warn" if pct >= 90 else "down")
        incidents = _clip_incidents_to_day(host, d)
        import html as _html
        incidents_attr = _html.escape(json.dumps(incidents), quote=True)
        # is_today marks the one bar whose percentage is against
        # ELAPsed time so far, not a completed 24h -- the popover needs
        # this to word the % line honestly ("so far today" vs "that
        # day") instead of implying a finished day's stats for a day
        # that's still in progress.
        is_today = "1" if d == day_labels[-1] else "0"
        bars.append(
            f'<div class="bar {cls}" data-date="{d}" data-status="{cls}" '
            f'data-pct="{pct:.0f}" data-today="{is_today}" data-incidents="{incidents_attr}"></div>')
    return "".join(bars)


# Just the city, not the full "RiV-meshBot server"/"GitHub Actions"
# phrase on every single row -- keeps rows to one line instead of
# wrapping, full explanation lives once in the footnote instead of
# repeated 5x on the page.
lxc_city = (meta.get("lxc_location") or "").split(",")[0]
actions_city = (meta.get("actions_location") or "").split(",")[0]

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
        dur = fmt_duration(now - b["current_auth_start"]) if b["current_auth_start"] else "?"
        status_label, status_class = f"Autentikasi Ditolak · {dur}", "autherr"
        auth_hosts.append(host)
    else:
        dur = fmt_duration(now - b["current_outage_start"]) if b["current_outage_start"] else "?"
        status_label, status_class = f"Down · {dur}", "down"
        down_hosts.append(host)
    rows.append(f'''
        <div class="row">
          <div class="row-top">
            <div class="row-left"><span class="dot {status_class}"></span><span class="host">{host}</span></div>
            <div class="status {status_class}">{status_label}</div>
          </div>
          <div class="bars">{day_bar_html(host)}</div>
        </div>''')

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

updated_str = datetime.fromtimestamp(now, WIB).strftime("%d %b %Y, %H:%M:%S WIB")
commit_sha = os.environ.get("GITHUB_SHA", "")[:7] or "local"

html = f'''<!doctype html>
<html lang="id">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>meshnode.id MQTT Status</title>
<meta name="description" content="Status langsung broker MQTT publik meshnode.id">
<meta http-equiv="refresh" content="60">
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
    --accent: #066fd1;
    --tooltip-bg: #232f42;
    --shadow: 0 1px 2px rgba(0,0,0,.3), 0 8px 24px -8px rgba(0,0,0,.5);
  }}
  * {{ box-sizing: border-box; }}
  html {{ color-scheme: dark; }}
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

  .wrap {{ max-width: 680px; margin: 0 auto; padding: 2.4rem 1.25rem 2rem; }}
  .titlebar {{ display: flex; align-items: baseline; gap: .55rem; margin-bottom: .35rem; }}
  .titlebar .glyph {{ font-size: 1.25rem; line-height: 1; }}
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
    box-shadow: var(--shadow); padding: .9rem 1rem 1rem; opacity: 0; pointer-events: none;
    transform: translateY(4px); transition: opacity .12s, transform .12s;
  }}
  .daypop.open {{ opacity: 1; pointer-events: auto; transform: translateY(0); }}
  .daypop::before {{
    content: ""; position: absolute; width: 10px; height: 10px; background: var(--surf2);
    border-left: 1px solid var(--border); border-top: 1px solid var(--border);
    transform: rotate(45deg);
  }}
  .daypop.arrow-up::before {{ top: -6px; }}
  .daypop.arrow-down::before {{ bottom: -6px; transform: rotate(225deg); }}
  .daypop-head {{ display: flex; align-items: center; justify-content: space-between; margin-bottom: .55rem; }}
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
  .legend {{ display: flex; align-items: center; gap: 1rem; }}
  .legend span {{ display: inline-flex; align-items: center; gap: .35rem; }}
  .legend i {{ width: 8px; height: 8px; border-radius: 2px; display: inline-block; }}
  .legend .lg-up {{ background: var(--ok); }}
  .legend .lg-warn {{ background: var(--warn); }}
  .legend .lg-down {{ background: var(--crit); }}

  .note {{ color: var(--faint); font-size: .76rem; text-align: center; margin-top: 1.6rem; line-height: 1.5; max-width: 34rem; margin-left: auto; margin-right: auto; }}
  footer {{ color: var(--faint); font-size: .78rem; text-align: center; margin-top: .8rem; }}
  footer a {{ color: var(--muted); text-decoration: none; border-bottom: 1px solid var(--border); }}
  footer a:hover {{ color: var(--text); border-color: var(--muted); }}
</style>
</head>
<body>
  <div class="banner {banner_class}"><span class="banner-icon">{banner_icon}</span>{banner_text}</div>
  <div class="wrap">
    <div class="titlebar"><span class="glyph">📡</span><h1>meshnode.id MQTT Status</h1></div>
    <div class="sub"><b>{up_count}/{total}</b> broker aktif — data diperbarui tiap 10 menit</div>
    <div class="panel">{"".join(rows)}</div>
    <div class="bars-caption">
      <span>{HISTORY_DAYS} hari lalu</span>
      <span class="legend"><span><i class="lg-up"></i>Aktif</span><span><i class="lg-warn"></i>Sebagian</span><span><i class="lg-down"></i>Down</span></span>
      <span>Hari ini</span>
    </div>
    <div class="ping-legend">
      <span><i class="lg-lxc"></i>RiV-meshBot{f" ({lxc_city})" if lxc_city else ""}</span>
      <span><i class="lg-ci"></i>GitHub Actions{f" ({actions_city})" if actions_city else ""}</span>
    </div>
    <p class="note">Ping cadangan wajar lebih tinggi karena jaraknya — bukan tanda broker lambat.</p>
    <footer>Commit {commit_sha} · Diperbarui {updated_str} · <a href="https://github.com/richardvsw/mqtt-status">Sumber di GitHub</a></footer>
  </div>
  <div class="daypop" id="daypop">
    <div class="daypop-head">
      <span class="daypop-date" id="daypop-date"></span>
      <button class="daypop-close" id="daypop-close" aria-label="Tutup">✕</button>
    </div>
    <div id="daypop-body"></div>
  </div>
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
    var activeBar = null;

    function closePop() {{
      pop.classList.remove("open");
      if (activeBar) activeBar.classList.remove("active");
      activeBar = null;
    }}

    function rowHtml(kind, label, seconds, startClock, endClock) {{
      var icon = kind === "down" ? "\u2715" : (kind === "autherr" ? "\u26A0" : "\u2713");
      var mins = Math.round(seconds / 60);
      var dur;
      if (mins < 60) {{
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
          var okLabel = status === "up" ? "Beroperasi Normal" : "Tidak ada rincian tersedia";
          body = '<div class="daypop-row ok"><span class="daypop-row-icon">\u2713</span>' +
                 '<span class="daypop-row-label">' + okLabel + '</span></div>';
        }} else {{
          incidents.forEach(function (inc) {{
            body += rowHtml(inc.kind, inc.label, inc.seconds, inc.start_clock, inc.end_clock);
          }});
        }}
        if (bar.dataset.pct) {{
          // Both phrasings lead with "Aktif {{pct}}%..." so the pair reads
          // as one consistent sentence pattern -- only the qualifier
          // changes depending on whether the day is still in progress.
          var pctLabel = bar.dataset.today === "1"
            ? 'Aktif ' + bar.dataset.pct + '% dari waktu berjalan hari ini'
            : 'Aktif ' + bar.dataset.pct + '%';
          body += '<div class="daypop-pct">' + pctLabel + '</div>';
        }}
        popBody.innerHTML = body;

        var r = bar.getBoundingClientRect();
        var wrap = document.querySelector(".wrap").getBoundingClientRect();
        pop.classList.remove("arrow-up", "arrow-down");
        var popWidth = pop.offsetWidth || 300;
        var left = Math.min(Math.max(r.left + r.width / 2 - popWidth / 2, wrap.left), wrap.right - popWidth);
        var spaceAbove = r.top;
        if (spaceAbove > 220) {{
          pop.style.top = (r.top - 12) + "px";
          pop.style.left = left + "px";
          pop.style.transform = "translateY(-100%)";
          pop.classList.add("arrow-down");
        }} else {{
          pop.style.top = (r.bottom + 12) + "px";
          pop.style.left = left + "px";
          pop.style.transform = "translateY(0)";
          pop.classList.add("arrow-up");
        }}
        pop.classList.add("open");
      }});
    }});

    popClose.addEventListener("click", function (e) {{ e.stopPropagation(); closePop(); }});
    document.addEventListener("click", function (e) {{
      if (pop.classList.contains("open") && !pop.contains(e.target)) closePop();
    }});
    window.addEventListener("scroll", closePop, {{ passive: true }});
  </script>
</body>
</html>
'''

with open(OUT_PATH, "w") as f:
    f.write(html)
print(f"wrote {OUT_PATH} ({up_count}/{total} brokers up)")
