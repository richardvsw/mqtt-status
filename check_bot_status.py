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
import subprocess
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

IS_CI = os.environ.get("GITHUB_ACTIONS") == "true"

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

SERVICES = ["mesh_bot", "meshtasticd", "lxc-monitor"]
SERVICE_LABEL = {
    "mesh_bot": "mesh_bot.service",
    "meshtasticd": "meshtasticd.service",
    "lxc-monitor": "LXC Monitor",
}


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
history = _load_history()
meta = state.setdefault("_meta", {})

checks = {}  # service -> "up" / "down", only services actually checked THIS run

if not IS_CI:
    # ── Real local checks (LXC only) ────────────────────────────────────
    mesh_bot_active = sh("systemctl is-active mesh_bot") == "active"
    meshtasticd_active = sh("systemctl is-active meshtasticd") == "active"
    checks["mesh_bot"] = "up" if mesh_bot_active else "down"
    checks["meshtasticd"] = "up" if meshtasticd_active else "down"
    # This code only runs when the LXC is clearly alive (it's the one
    # executing right now), so lxc-monitor is definitionally "up" here --
    # the interesting case (down) can only ever be detected from the
    # OUTSIDE (the IS_CI branch below), same as how a heartbeat only
    # means something to someone ELSE watching for its absence.
    checks["lxc-monitor"] = "up"
    meta["last_lxc_check_ts"] = now

    try:
        health = json.load(urllib.request.urlopen("http://localhost:8080/api/bot/health", timeout=8))
        if health.get("last_reply_ts"):
            meta["last_reply_ts"] = health["last_reply_ts"]
    except Exception as e:
        print(f"bot health API fetch failed: {e}")

    restarts_raw = sh("systemctl show mesh_bot --property=NRestarts").replace("NRestarts=", "")
    try:
        meta["restart_count"] = int(restarts_raw)
    except ValueError:
        pass
else:
    # ── GitHub Actions: can only judge LXC staleness, nothing else ──────
    last_ts = meta.get("last_lxc_check_ts")
    stale = (last_ts is None) or (now - last_ts > LXC_STALE_SECONDS)
    checks["lxc-monitor"] = "down" if stale else "up"
    # mesh_bot/meshtasticd deliberately get NO data point this run --
    # neither service is reachable from here, and writing a fabricated
    # "unknown" status would just be noise; their last real (LXC-
    # sourced) state stays exactly as it was, clearly timestamped by
    # whichever day it's still showing.

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

today_str = datetime.fromtimestamp(now, WIB).strftime("%Y-%m-%d")
today_bucket = history.setdefault(today_str, {})
for svc, status in checks.items():
    slot = today_bucket.setdefault(svc, {"up": 0, "total": 0, "down": 0})
    slot["total"] += 1
    if status == "up":
        slot["up"] += 1
    else:
        slot["down"] += 1
cutoff_date = (datetime.fromtimestamp(now, WIB) - timedelta(days=HISTORY_RETENTION_DAYS)).strftime("%Y-%m-%d")
for d in [d for d in history if d < cutoff_date]:
    del history[d]
_save_history(history)

if checks:
    with open(LOG_PATH, "a") as f:
        f.write(json.dumps({"ts": now, "checks": checks}) + "\n")

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
        color = _severity_color(pct)
        cls = "up" if pct >= 99.5 else ("warn" if pct >= 90 else "down")
        incidents = _clip_incidents_to_day(svc, d)
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
        dur = fmt_duration(now - st["current_outage_start"])
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

updated_str = datetime.fromtimestamp(now, WIB).strftime("%d %b %Y, %H:%M:%S WIB")

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
  footer {{ color: var(--faint); font-size: .78rem; text-align: center; margin-top: 1.5rem; }}
  footer a {{ color: var(--muted); text-decoration: none; border-bottom: 1px solid var(--border); }}
  footer a:hover {{ color: var(--text); border-color: var(--muted); }}
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
    <div class="sub"><b>{up_count}/{total}</b> layanan normal — data diperbarui tiap 2 menit (LXC) / dicek tiap ~10 menit (GitHub Actions, hanya memantau apakah LXC masih hidup)</div>
    <div class="panel">{"".join(rows)}</div>
    <p class="note">"LXC Monitor" bukan layanan bot itu sendiri -- ini menunjukkan apakah box yang menjalankan pengecekan mesh_bot/meshtasticd masih hidup. GitHub Actions tidak bisa memeriksa mesh_bot/meshtasticd secara langsung (layanan privat di jaringan rumah), jadi baris itu hanya bisa dikonfirmasi dari LXC sendiri -- kalau LXC down, baris ini yang akan menunjukkan berapa lama.</p>
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
          var ok = status === "up";
          body = '<div class="daypop-row ' + (ok ? "ok" : "down") + '"><span class="daypop-row-icon">' + (ok ? "✓" : "✕") + '</span>' +
                 '<span class="daypop-row-label">' + (ok ? "Beroperasi Normal" : "Tidak ada rincian tersedia") + '</span></div>';
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
print(f"wrote {OUT_PATH} ({up_count}/{total} services up, IS_CI={IS_CI})")
