"""
Standalone bot/service status checker + page generator (bot-status.html).
Mirrors check_and_render.py's own structure closely (sharded history,
CONFIRM_THRESHOLD debounce, REAL_INCIDENTS reconstruction, severity
gradient, click-to-open day popover) -- same shape, different data
source and different "hosts".

Runs from TWO places, same script either way, but with very different
capabilities:

- This box's own LXC (deploy/systemd/, via mqtt-status-lxc.timer, every
  2 min): the ONLY place that can actually check mesh_bot/meshtasticd --
  they're local systemd services and a local API (rivbot-ui on
  localhost:8080), neither reachable from outside this box. This is the
  real, primary data source for those two rows.
- GitHub Actions (.github/workflows/check-status.yml), ~10 min via
  external cron: CANNOT check mesh_bot/meshtasticd at all -- no network
  path to a private service on a home LXC. All it can do is notice that
  THIS repo hasn't received a fresh LXC-side commit in a while, and
  track that as its own "lxc-monitor" outage -- answering "how long has
  our own monitoring been silent", not "is the bot itself up", which
  are genuinely different questions. If this box goes down, "mesh_bot"/
  "meshtasticd" rows simply stop gaining new data points (their last
  known state stays displayed, clearly timestamped) while "LXC Monitor"
  is the one row that keeps recording throughout the outage.
"""
import json
import os
import socket
import subprocess
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

# 2026-09-04: this always runs from GitHub Actions now (see
# PUBLIC_STATUS_URL below) -- the old IS_CI split and the local-file
# EVENT_LOG_* constants (this box's own bot_events.jsonl, unreachable
# from a GitHub Actions runner) are gone; event data comes over HTTP
# from /api/public/events instead (see _load_bot_events()).

WIB = timezone(timedelta(hours=7))
HISTORY_DIR = "bot_history"
LOG_PATH = "bot_log.jsonl"
STATE_PATH = "bot_state.json"
OUT_PATH = "bot-status.html"
HISTORY_DAYS = 30
HISTORY_RETENTION_DAYS = 400
CONFIRM_THRESHOLD = 2
# How stale the LXC's own last check can get before Actions marks a
# "lxc-monitor" outage -- comfortably more than 2x the LXC's own 2-min
# cadence (same reasoning check-status.yml's publish-guard already uses
# for the exact same kind of margin), so a normal single missed cycle
# never false-alarms.
LXC_STALE_SECONDS = 15 * 60

def _current_failover_target():
    """mqtt.rivi.my.id is a CNAME an external DNS failover job
    (update_dns.py, GitHub-Actions-only) repoints at whichever broker
    is currently healthy -- resolving it live here shows what's
    ACTUALLY configured right now, not just what the failover job last
    intended. Matched by resolved IP rather than reading the CNAME
    target directly (stdlib socket doesn't expose a CNAME lookup, only
    final A-record resolution). brokers.json is read directly here
    (not imported from check_and_render.py -- these run as separate
    top-level scripts, not modules)."""
    try:
        target_ip = socket.gethostbyname("mqtt.rivi.my.id")
    except Exception:
        return None
    try:
        with open("brokers.json") as f:
            candidates = json.load(f)
    except Exception:
        return None
    for host in candidates:
        try:
            if socket.gethostbyname(host) == target_ip:
                return host
        except Exception:
            continue
    return None


_failover_target = _current_failover_target()

SERVICES = ["mesh_bot", "meshtasticd"]
SERVICE_LABEL = {
    "mesh_bot": "mesh_bot.service",
    "meshtasticd": "meshtasticd.service",
}

# 2026-09-04: replaces the old two-sided design (a local LXC check that
# git-committed its results, plus a GitHub-Actions-side "has the LXC
# committed recently" staleness inference standing in for a real check
# it couldn't make itself) with one direct HTTP check against the bot's
# own public status API (meshbot.rivi.my.id), reachable from anywhere,
# including this script running on a GitHub Actions runner. The local
# LXC-side git push/pull/discard dance is gone entirely -- no more
# credential-helper HOME hack, no more risk of an uncommitted local edit
# to this very file getting silently discarded by the next automated
# pull (confirmed live: that happened to this file's own staleness-badge
# fix before this rewrite existed). A failed/timed-out request now
# directly means "can't confirm the bot is up," reported as down for
# BOTH services rather than an ambiguous "last known state, possibly
# stale" -- more accurate than the git-staleness proxy ever was, since
# it's testing actual reachability instead of inferring it.
PUBLIC_STATUS_URL = "https://meshbot.rivi.my.id/api/public/status"
PUBLIC_EVENTS_URL = "https://meshbot.rivi.my.id/api/public/events"
PUBLIC_API_TIMEOUT = 15


def sh(cmd):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=15).stdout.strip()
    except Exception:
        return ""


def load_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f)


def _load_history():
    merged = {}
    if os.path.isdir(HISTORY_DIR):
        for fname in sorted(os.listdir(HISTORY_DIR)):
            if fname.endswith(".json"):
                merged.update(load_json(os.path.join(HISTORY_DIR, fname), {}))
    return merged


def _save_history(history):
    os.makedirs(HISTORY_DIR, exist_ok=True)
    by_month = {}
    for d, hosts in history.items():
        by_month.setdefault(d[:7], {})[d] = hosts
    for fname in os.listdir(HISTORY_DIR):
        if fname.endswith(".json") and fname[:-5] not in by_month:
            os.remove(os.path.join(HISTORY_DIR, fname))
    for ym, days in by_month.items():
        save_json(os.path.join(HISTORY_DIR, f"{ym}.json"), days)


now = time.time()
state = load_json(STATE_PATH, {})
meta = state.setdefault("_meta", {})

checks = {}  # service -> "up" / "down"

try:
    with urllib.request.urlopen(PUBLIC_STATUS_URL, timeout=PUBLIC_API_TIMEOUT) as resp:
        pub = json.load(resp)
    checks["mesh_bot"] = "up" if pub.get("bot_service") == "active" else "down"
    checks["meshtasticd"] = "up" if pub.get("meshtasticd_service") == "active" else "down"
    if pub.get("last_reply_ago") is not None:
        meta["last_reply_ts"] = now - pub["last_reply_ago"]
    if pub.get("restart_count") is not None:
        meta["restart_count"] = pub["restart_count"]
except Exception as e:
    print(f"public status API unreachable, reporting down: {e}")
    checks["mesh_bot"] = "down"
    checks["meshtasticd"] = "down"

for svc, status in checks.items():
    st = state.setdefault(svc, {"current_outage_start": None, "consecutive_fails": 0, "provisional_start": None})
    st.setdefault("consecutive_fails", 0)
    st.setdefault("provisional_start", None)
    if status == "up":
        st["consecutive_fails"] = 0
        st["provisional_start"] = None
        st["current_outage_start"] = None
    else:
        st["consecutive_fails"] += 1
        if st["provisional_start"] is None:
            st["provisional_start"] = now
        if st["consecutive_fails"] >= CONFIRM_THRESHOLD:
            st["current_outage_start"] = st["provisional_start"]
        else:
            st["current_outage_start"] = None

save_json(STATE_PATH, state)

if checks:
    with open(LOG_PATH, "a") as f:
        f.write(json.dumps({"ts": now, "checks": checks}) + "\n")

# 2026-08-23: recomputed fresh from bot_log.jsonl every run instead of
# an incrementally-updated counter -- see this file's own module
# docstring note above load_json/save_json for the full reasoning.
# cutoff matches the same HISTORY_RETENTION_DAYS trim the old
# increment-based version applied to the loaded dict.
history = {}
_cutoff_date = (datetime.fromtimestamp(now, WIB) - timedelta(days=HISTORY_RETENTION_DAYS)).strftime("%Y-%m-%d")
try:
    with open(LOG_PATH) as f:
        for _line in f:
            _line = _line.strip()
            if not _line:
                continue
            try:
                _rec = json.loads(_line)
            except json.JSONDecodeError:
                continue
            _d = datetime.fromtimestamp(_rec["ts"], WIB).strftime("%Y-%m-%d")
            if _d < _cutoff_date:
                continue
            _bucket = history.setdefault(_d, {})
            for _svc, _status in _rec.get("checks", {}).items():
                _slot = _bucket.setdefault(_svc, {"up": 0, "total": 0, "down": 0})
                _slot["total"] += 1
                if _status == "up":
                    _slot["up"] += 1
                else:
                    _slot["down"] += 1
except FileNotFoundError:
    pass
_save_history(history)

day_labels = [(datetime.fromtimestamp(now, WIB) - timedelta(days=i)).strftime("%Y-%m-%d")
              for i in range(HISTORY_DAYS - 1, -1, -1)]


def _fmt_clock_duration(seconds):
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds} detik"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} menit"
    hours = minutes // 60
    rem_minutes = minutes % 60
    return f"{hours} jam" if rem_minutes == 0 else f"{hours} jam {rem_minutes} menit"


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


def _build_real_incidents():
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
                for svc, status in rec.get("checks", {}).items():
                    incidents.setdefault(svc, [])
                    key = svc
                    if status == "down":
                        if key not in open_incident:
                            open_incident[key] = ts
                    elif key in open_incident:
                        start = open_incident.pop(key)
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
    return incidents


REAL_INCIDENTS = _build_real_incidents()

# 2026-08-23: see check_and_render.py's identical comment -- lets the
# live "Down · Xm" badge agree with the incident popover's merged
# duration instead of resetting on a brief mid-outage recovery blip.
_OPEN_INCIDENT_START = {}
for _svc, _incs in REAL_INCIDENTS.items():
    if _incs and _incs[-1]["end"] is None:
        _OPEN_INCIDENT_START[_svc] = _incs[-1]["start"]


def _clip_incidents_to_day(svc, d):
    day_start = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=WIB).timestamp()
    day_end = day_start + 86400
    out = []
    for inc in REAL_INCIDENTS.get(svc, []):
        seg_start = max(inc["start"], day_start)
        seg_end_raw = inc["end"] if inc["end"] is not None else now
        seg_end = min(seg_end_raw, day_end)
        if seg_end <= seg_start:
            continue
        if inc["end"] is None and seg_end_raw <= day_end:
            end_clock = "sekarang"
        elif seg_end >= day_end:
            end_clock = "24:00"
        else:
            end_clock = datetime.fromtimestamp(seg_end, WIB).strftime("%H:%M")
        out.append({
            "kind": "down",
            "label": "Down",
            "seconds": seg_end - seg_start,
            "start_clock": datetime.fromtimestamp(seg_start, WIB).strftime("%H:%M"),
            "end_clock": end_clock,
        })
    return out


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


def host_uptime_pct(svc):
    up_sum, total_sum = 0, 0
    for d in day_labels:
        slot = history.get(d, {}).get(svc)
        if slot:
            up_sum += slot.get("up", 0)
            total_sum += slot.get("total", 0)
    if total_sum == 0:
        return None
    return up_sum / total_sum * 100


def day_bar_html(svc):
    import html as _html
    bars = []
    for d in day_labels:
        slot = history.get(d, {}).get(svc)
        if not slot or slot["total"] == 0:
            bars.append(f'<div class="bar nodata" data-date="{d}" data-status="nodata"></div>')
            continue
        total = slot["total"]
        pct = slot["up"] / total * 100
        incidents = _clip_incidents_to_day(svc, d)
        if incidents:
            color = _severity_color(pct)
            cls = "up" if pct >= 99.5 else ("warn" if pct >= 90 else "down")
        else:
            color = _severity_color(100.0)
            cls = "up"
        incidents_attr = _html.escape(json.dumps(incidents), quote=True)
        is_today = "1" if d == day_labels[-1] else "0"
        bars.append(
            f'<div class="bar" style="background:{color}" data-date="{d}" data-status="{cls}" '
            f'data-pct="{pct:.0f}" data-today="{is_today}" data-incidents="{incidents_attr}"></div>')
    return "".join(bars)


rows = []
up_count = 0
down_svcs = []
for svc in SERVICES:
    st = state.get(svc, {})
    down = st.get("current_outage_start") is not None
    if not down:
        up_count += 1
    else:
        down_svcs.append(SERVICE_LABEL[svc])
    if down:
        _start = _OPEN_INCIDENT_START.get(svc, st["current_outage_start"])
        dur = fmt_duration(now - _start)
        status_label, status_class = f"Down · {dur}", "down"
    else:
        status_label, status_class = "Aktif", "up"
    uptime_pct = host_uptime_pct(svc)
    uptime_label = f"{uptime_pct:.2f} % uptime" if uptime_pct is not None else "belum ada data"
    extra = ""
    if svc == "mesh_bot" and meta.get("last_reply_ts"):
        ago = fmt_duration(now - meta["last_reply_ts"])
        extra = f'<div class="row-extra">Balasan terakhir: {ago} lalu</div>'
    if svc == "mesh_bot" and meta.get("restart_count") is not None:
        extra += f'<div class="row-extra">Restart tercatat: {meta["restart_count"]}x</div>'
    rows.append(f'''
        <div class="row">
          <div class="row-top">
            <div class="row-left"><span class="dot {status_class}"></span><span class="host">{SERVICE_LABEL[svc]}</span></div>
            <div class="status {status_class}">{status_label}</div>
          </div>
          {extra}
          <div class="bars">{day_bar_html(svc)}</div>
          <div class="bars-caption row-caption">
            <span>{HISTORY_DAYS} hari lalu</span>
            <span class="caption-line"></span>
            <span>{uptime_label}</span>
            <span class="caption-line"></span>
            <span>Hari ini</span>
          </div>
        </div>''')

total = len(SERVICES)
if up_count == total:
    banner_class, banner_text, banner_icon = "ok", "Semua Layanan Bot Normal", "✓"
elif up_count == 0:
    banner_class, banner_text, banner_icon = "crit", "Semua Layanan Bot Down", "✕"
else:
    banner_class, banner_text, banner_icon = "warn", f"Gangguan — {', '.join(down_svcs)} Down", "!"

_upd_dt, _upd_abbr, _ = _id_strftime_dmy(now, WIB)
updated_str = _upd_dt.strftime(f"%d {_upd_abbr} %Y, %H:%M:%S WIB")

def _load_bot_events():
    """Every entry ntfy.notify() has ever logged (restarts, broker
    switches, NodeDB resets, etc.) -- real human-written context from
    whoever/whatever triggered the action, not re-derived from the
    generic up/down checks above.

    2026-09-04: used to read EVENT_LOG_PATH directly, which only ever
    worked when this script ran ON the LXC itself (the file it wanted
    doesn't exist on a GitHub Actions runner). Now that this always
    runs from Actions (see the PUBLIC_STATUS_URL switch above), it
    fetches the same data via /api/public/events instead -- that route
    already applies the same age/count capping this used to do locally,
    so this is a thin pass-through, best-effort (a fetch failure just
    means an empty events section this run, not a hard error)."""
    try:
        with urllib.request.urlopen(PUBLIC_EVENTS_URL, timeout=PUBLIC_API_TIMEOUT) as resp:
            return json.load(resp)
    except Exception as e:
        print(f"public events API fetch failed: {e}")
        return []


def _event_row_time(ts):
    dt, abbr, _ = _id_strftime_dmy(ts, WIB)
    return dt.strftime(f"%d {abbr}, %H:%M WIB")


def _event_log_html():
    events = _load_bot_events()
    if not events:
        return '<p class="note">Belum ada kejadian tercatat.</p>'
    rows_html = "".join(
        f'''<div class="event-row">
          <div class="event-time">{_event_row_time(e["ts"])}</div>
          <div class="event-body"><div class="event-title">{e.get("title", "")}</div><div class="event-msg">{e.get("message", "")}</div></div>
        </div>'''
        for e in events
    )
    return f'''
    <h2 class="section-title">Riwayat Kejadian</h2>
    <div class="event-log">{rows_html}</div>'''

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
<title>Status Bot — RiV-meshBot</title>
<meta name="description" content="Status langsung layanan RiV-meshBot (mesh_bot, meshtasticd)">
<script>
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
    --accent: #066fd1;
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
    --accent: #066fd1;
    --shadow: 0 1px 2px rgba(0,0,0,.05), 0 8px 24px -8px rgba(0,0,0,.12);
  }}
  body {{
    margin: 0; min-height: 100vh; color: var(--text); background: var(--bg);
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
  .titlebar {{ display: flex; align-items: center; gap: .55rem; margin-bottom: .35rem; }}
  .titlebar h1 {{ flex: 1; font-size: 1.2rem; font-weight: 650; margin: 0; letter-spacing: -.2px; }}
  .live-clock {{
    font-variant-numeric: tabular-nums; font-size: .82rem; color: var(--muted);
    font-family: ui-monospace, "SF Mono", Menlo, monospace;
  }}
  .back {{ color: var(--accent); text-decoration: none; font-size: .85rem; }}
  .back:hover {{ text-decoration: underline; }}
  .sub {{ color: var(--muted); font-size: .86rem; margin: .6rem 0 1.8rem; }}
  .sub b {{ color: var(--text); font-weight: 600; }}

  .panel {{ background: var(--surf); border: 1px solid var(--border); border-radius: 6px; box-shadow: var(--shadow); }}
  .row {{ padding: 1rem 1.25rem 1.1rem; border-top: 1px solid var(--border-soft); font-size: .92rem; }}
  .row:first-child {{ border-top: none; border-top-left-radius: 14px; border-top-right-radius: 14px; }}
  .row:last-child {{ border-bottom-left-radius: 14px; border-bottom-right-radius: 14px; }}
  .row-top {{ display: flex; justify-content: space-between; align-items: center; gap: .8rem; margin-bottom: .3rem; }}
  .row-left {{ display: flex; align-items: center; gap: .7rem; min-width: 0; }}
  .row-extra {{ color: var(--faint); font-size: .76rem; margin-bottom: .25rem; }}
  .dot {{ position: relative; width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }}
  .dot.up {{ background: var(--ok); box-shadow: 0 0 0 3px var(--ok-dim); }}
  .dot.down {{ background: var(--crit); box-shadow: 0 0 0 3px var(--crit-dim); }}
  .host {{ font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: .88rem; }}
  .status {{ font-weight: 600; font-variant-numeric: tabular-nums; font-size: .84rem; flex-shrink: 0; }}
  .status.up {{ color: var(--ok); }}
  .status.down {{ color: var(--crit); }}

  .bars {{ display: flex; gap: 3px; height: 34px; margin-top: .6rem; }}
  .bar {{ flex: 1 1 0; min-width: 0; position: relative; cursor: pointer; }}
  .bar:first-child {{ border-top-left-radius: 4px; border-bottom-left-radius: 4px; }}
  .bar:last-child {{ border-top-right-radius: 4px; border-bottom-right-radius: 4px; }}
  .bar.nodata {{ background: var(--border); }}
  .bar:hover, .bar.active {{ opacity: 1; transform: scaleY(1.06); }}

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
  .daypop-close {{ background: none; border: none; color: var(--muted); cursor: pointer; font-size: 1rem; line-height: 1; padding: .15rem; border-radius: 5px; }}
  .daypop-close:hover {{ color: var(--text); background: var(--border-soft); }}
  .daypop-row {{ display: flex; align-items: center; gap: .5rem; padding: .5rem .6rem; border-radius: 8px; font-size: .82rem; margin-bottom: .4rem; }}
  .daypop-row:last-child {{ margin-bottom: 0; }}
  .daypop-row.down {{ background: var(--crit-bg); color: var(--crit); }}
  .daypop-row.ok {{ background: var(--ok-bg); color: var(--ok); }}
  .daypop-row-icon {{ flex-shrink: 0; }}
  .daypop-row-main {{ flex: 1; min-width: 0; }}
  .daypop-row-label {{ font-weight: 600; }}
  .daypop-row-time {{ font-variant-numeric: tabular-nums; font-size: .74rem; opacity: .8; margin-top: .1rem; }}
  .daypop-row-dur {{ font-variant-numeric: tabular-nums; color: var(--text); font-weight: 600; flex-shrink: 0; }}
  .daypop-pct {{ color: var(--faint); font-size: .74rem; margin-top: .5rem; }}

  .bars-caption {{ display: flex; justify-content: space-between; align-items: center; color: var(--faint); font-size: .72rem; }}
  .row-caption {{ margin-top: .5rem; font-size: .68rem; gap: .5rem; }}
  .caption-line {{ flex: 1; height: 1px; background: var(--border); min-width: 1.2rem; }}
  .note {{ color: var(--faint); font-size: .76rem; text-align: center; margin-top: 1.6rem; line-height: 1.5; max-width: 34rem; margin-left: auto; margin-right: auto; }}
  .section-title {{ font-size: .95rem; font-weight: 650; margin: 2rem 0 .8rem; letter-spacing: -.1px; }}
  .event-log {{ background: var(--surf); border: 1px solid var(--border); border-radius: 6px; box-shadow: var(--shadow); overflow: hidden; }}
  .event-row {{ display: flex; gap: .9rem; padding: .8rem 1.1rem; border-top: 1px solid var(--border-soft); }}
  .event-row:first-child {{ border-top: none; }}
  .event-time {{ flex-shrink: 0; width: 6.5rem; color: var(--faint); font-size: .72rem; font-variant-numeric: tabular-nums; padding-top: .1rem; }}
  .event-title {{ font-weight: 600; font-size: .84rem; }}
  .event-msg {{ color: var(--muted); font-size: .78rem; margin-top: .15rem; line-height: 1.4; }}
  footer {{ color: var(--faint); font-size: .78rem; text-align: center; margin-top: 1.5rem; }}
  footer a {{ color: var(--muted); text-decoration: none; border-bottom: 1px solid var(--border); }}
  footer a:hover {{ color: var(--text); border-color: var(--muted); }}
  .info-btn {{
    display: inline-flex; align-items: center; justify-content: center;
    width: 1.05rem; height: 1.05rem; margin-left: .3rem; border-radius: 50%;
    border: 1px solid var(--border); background: var(--surf2); color: var(--muted);
    font-size: .68rem; font-weight: 700; line-height: 1; cursor: pointer;
    vertical-align: .1rem; padding: 0;
  }}
  .info-btn:hover {{ color: var(--text); border-color: var(--muted); }}
  .info-modal-backdrop {{
    position: fixed; inset: 0; background: rgba(0,0,0,.45); z-index: 50;
    display: flex; align-items: center; justify-content: center; padding: 1.2rem;
    opacity: 0; pointer-events: none; transition: opacity .15s ease;
  }}
  .info-modal-backdrop.open {{ opacity: 1; pointer-events: auto; }}
  .info-modal {{
    background: var(--surf2); border: 1px solid var(--border); border-radius: 12px;
    max-width: 380px; width: 100%; padding: 1rem 1.1rem; box-shadow: 0 12px 32px rgba(0,0,0,.3);
    transform: translateY(6px); transition: transform .15s ease;
  }}
  .info-modal-backdrop.open .info-modal {{ transform: translateY(0); }}
  .info-modal-head {{ display: flex; align-items: center; justify-content: space-between; margin-bottom: .6rem; }}
  .info-modal-head span {{ font-weight: 650; font-size: .92rem; }}
  .info-modal-close {{ background: none; border: none; color: var(--muted); cursor: pointer; font-size: 1rem; line-height: 1; padding: .15rem; border-radius: 5px; }}
  .info-modal-close:hover {{ color: var(--text); background: var(--border-soft); }}
  .info-modal-body {{ font-size: .84rem; line-height: 1.55; color: var(--muted); }}
  .info-modal-body b {{ color: var(--text); }}
  .info-modal-body ol {{ margin: .5rem 0 0; padding-left: 1.2rem; }}
  .info-modal-body li {{ margin-bottom: .35rem; }}
</style>
</head>
<body>
  <div class="banner {banner_class}"><span class="banner-icon">{banner_icon}</span>{banner_text}</div>
  <div class="wrap">
    <a class="back" href="index.html">← Status broker MQTT</a>
    <div class="titlebar">
      <h1>Status Bot — RiV-meshBot</h1>
      <span class="live-clock" id="live-clock" title="Waktu sekarang (WIB)"></span>
    </div>
    <div class="sub"><b>{up_count}/{total}</b> layanan normal</div>
    <p class="note">Bot menggunakan <b>mqtt.rivi.my.id</b>, otomatis memilih broker yang sehat (DNS failover)<button class="info-btn" id="dnsInfoBtn" aria-label="Cara kerja DNS failover" title="Cara kerja DNS failover">?</button>.{f" Saat ini terkoneksi ke: <b>{_failover_target}</b>." if _failover_target else ""}</p>
    <div class="info-modal-backdrop" id="dnsInfoBackdrop">
      <div class="info-modal">
        <div class="info-modal-head">
          <span>Cara kerja DNS failover</span>
          <button class="info-modal-close" id="dnsInfoClose" aria-label="Tutup">✕</button>
        </div>
        <div class="info-modal-body">
          <p><b>mqtt.rivi.my.id</b> adalah alamat MQTT tetap yang otomatis diarahkan (via Cloudflare DNS) ke broker yang sedang sehat, sehingga node tidak perlu mengganti alamat broker secara manual saat terjadi gangguan.</p>
          <ol>
            <li>Secara default mengarah ke <b>mqtt.meshnode.id</b> (broker utama) selama broker itu sehat.</li>
            <li>Jika broker utama down atau menolak autentikasi, DNS otomatis dialihkan ke <b>mqtt1.meshnode.id</b> sampai <b>mqtt5.meshnode.id</b> secara berurutan, memilih broker cadangan pertama yang sehat.</li>
            <li>Begitu broker utama pulih, DNS otomatis kembali ke <b>mqtt.meshnode.id</b>.</li>
          </ol>
        </div>
      </div>
    </div>
    <div class="panel">{"".join(rows)}</div>
    {_event_log_html()}
    <footer>Diperbarui {updated_str} · <a href="https://github.com/richardvsw/mqtt-status">Sumber di GitHub</a></footer>
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
      var icon = kind === "down" ? "✕" : "✓";
      var mins = Math.round(seconds / 60);
      var dur;
      if (mins < 60) {{ dur = mins + " menit"; }}
      else {{ var h = Math.floor(mins / 60), m = mins % 60; dur = m === 0 ? (h + " jam") : (h + " jam " + m + " menit"); }}
      var timeRange = (startClock && endClock)
        ? '<div class="daypop-row-time">' + startClock + '–' + endClock + ' WIB</div>' : '';
      return '<div class="daypop-row ' + kind + '"><span class="daypop-row-icon">' + icon +
             '</span><div class="daypop-row-main"><span class="daypop-row-label">' + label + '</span>' + timeRange +
             '</div><span class="daypop-row-dur">' + dur + '</span></div>';
    }}

    function positionPopover(bar) {{
      var r = bar.getBoundingClientRect();
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

    document.querySelectorAll(".bar").forEach(function (bar) {{
      bar.addEventListener("click", function (e) {{
        e.stopPropagation();
        var wasActive = bar.classList.contains("active");
        document.querySelectorAll(".bar.active").forEach(function (b) {{ b.classList.remove("active"); }});
        if (wasActive) {{ closePop(); return; }}
        var status = bar.dataset.status;
        if (status === "nodata") return;
        bar.classList.add("active");
        activeBar = bar;
        popDate.textContent = bar.dataset.date;
        var incidents = [];
        try {{ incidents = JSON.parse(bar.dataset.incidents || "[]"); }} catch (err) {{ incidents = []; }}
        var body = "";
        if (incidents.length === 0) {{
          body = '<div class="daypop-row ok"><span class="daypop-row-icon">✓</span>' +
                 '<span class="daypop-row-label">Beroperasi Normal</span></div>';
        }} else {{
          incidents.forEach(function (inc) {{ body += rowHtml(inc.kind, inc.label, inc.seconds, inc.start_clock, inc.end_clock); }});
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

    function repositionIfOpen() {{ if (activeBar && pop.classList.contains("open")) positionPopover(activeBar); }}
    if (document.fonts && document.fonts.ready) {{ document.fonts.ready.then(repositionIfOpen); }}
    window.addEventListener("resize", repositionIfOpen, {{ passive: true }});
    if (window.visualViewport) {{ window.visualViewport.addEventListener("resize", repositionIfOpen, {{ passive: true }}); }}
    popClose.addEventListener("click", function (e) {{ e.stopPropagation(); closePop(); }});

    var dnsInfoBtn = document.getElementById("dnsInfoBtn");
    var dnsInfoBackdrop = document.getElementById("dnsInfoBackdrop");
    var dnsInfoClose = document.getElementById("dnsInfoClose");
    if (dnsInfoBtn && dnsInfoBackdrop) {{
      dnsInfoBtn.addEventListener("click", function (e) {{
        e.stopPropagation();
        dnsInfoBackdrop.classList.add("open");
      }});
      dnsInfoClose.addEventListener("click", function () {{ dnsInfoBackdrop.classList.remove("open"); }});
      dnsInfoBackdrop.addEventListener("click", function (e) {{
        if (e.target === dnsInfoBackdrop) dnsInfoBackdrop.classList.remove("open");
      }});
      document.addEventListener("keydown", function (e) {{
        if (e.key === "Escape") dnsInfoBackdrop.classList.remove("open");
      }});
    }}
    document.addEventListener("click", function (e) {{ if (pop.classList.contains("open") && !pop.contains(e.target)) closePop(); }});
    window.addEventListener("scroll", closePop, {{ passive: true }});

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
print(f"wrote {OUT_PATH} ({up_count}/{total} services up)")
