"""
Fully standalone MQTT broker status checker + page generator -- runs
entirely inside GitHub Actions (see .github/workflows/check-status.yml),
with NO dependency on the LXC/bot at all. Does its own plain TCP connect
to each broker (same check mqtt_tap.py does on the LXC), keeps its own
uptime history as a JSON file committed alongside the page, and
regenerates index.html. If the LXC goes down, this keeps running and
reporting real, live broker status -- the whole point of moving it here.
"""
import json
import os
import socket
import time
from datetime import datetime, timezone, timedelta

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
HISTORY_DAYS = 90


def check_broker(host, timeout=5):
    t0 = time.time()
    try:
        with socket.create_connection((host, MQTT_PORT), timeout=timeout):
            return True, round((time.time() - t0) * 1000)
    except Exception:
        return False, None


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


now = time.time()
state = load_json(STATE_PATH, {})
history = load_json(HISTORY_PATH, {})

brokers = {}
for host in BROKERS:
    reachable, latency_ms = check_broker(host)
    st = state.setdefault(host, {"current_outage_start": None})
    if not reachable and st["current_outage_start"] is None:
        st["current_outage_start"] = now
    elif reachable and st["current_outage_start"] is not None:
        st["current_outage_start"] = None
    brokers[host] = {"reachable": reachable, "latency_ms": latency_ms, "current_outage_start": st["current_outage_start"]}

save_json(STATE_PATH, state)

today_str = datetime.fromtimestamp(now, WIB).strftime("%Y-%m-%d")
today_bucket = history.setdefault(today_str, {})
for host, b in brokers.items():
    slot = today_bucket.setdefault(host, {"up": 0, "total": 0})
    slot["total"] += 1
    if b["reachable"]:
        slot["up"] += 1
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


def day_bar_html(host):
    bars = []
    for d in day_labels:
        slot = history.get(d, {}).get(host)
        if not slot or slot["total"] == 0:
            cls, tip = "nodata", f"{d}: belum ada data"
        else:
            pct = slot["up"] / slot["total"] * 100
            cls = "up" if pct >= 99.5 else ("warn" if pct >= 90 else "down")
            tip = f"{d}: {pct:.0f}% aktif"
        bars.append(f'<div class="bar {cls}" data-tip="{tip}"></div>')
    return "".join(bars)


rows = []
up_count = 0
down_hosts = []
for host in BROKERS:
    b = brokers[host]
    if b["reachable"]:
        up_count += 1
        status_label, status_class = "Operational", "up"
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
elif up_count == 0:
    banner_class, banner_text, banner_icon = "crit", "Semua Broker Down", "✕"
else:
    banner_class, banner_text, banner_icon = "warn", f"Gangguan Sebagian — {', '.join(down_hosts)} Down", "!"

updated_str = datetime.fromtimestamp(now, WIB).strftime("%d %b %Y, %H:%M:%S WIB")
commit_sha = os.environ.get("GITHUB_SHA", "")[:7] or "local"

html = f'''<!doctype html>
<html lang="id">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>meshnode.id MQTT Status</title>
<meta http-equiv="refresh" content="60">
<style>
  :root {{
    --bg: #0b0f14; --surf: #121821; --border: #232c38; --text: #e7edf3;
    --muted: #8a96a3;
    --ok: #45d9ae; --ok-bg: #12271f;
    --warn: #e3b341; --warn-bg: #2b2413;
    --crit: #f0665a; --crit-bg: #2b1715;
    --tooltip-bg: #1c2430;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; background: var(--bg); color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  }}
  .banner {{ padding: 1.4rem 1.5rem; text-align: center; font-size: 1.15rem; font-weight: 600; }}
  .banner.ok {{ background: var(--ok-bg); color: var(--ok); }}
  .banner.warn {{ background: var(--warn-bg); color: var(--warn); }}
  .banner.crit {{ background: var(--crit-bg); color: var(--crit); }}
  .wrap {{ max-width: 640px; margin: 0 auto; padding: 2rem 1.2rem; }}
  .titlebar h1 {{ font-size: 1.15rem; margin: 0 0 .3rem; }}
  .sub {{ color: var(--muted); font-size: .85rem; margin-bottom: 1.6rem; }}
  .panel {{ background: var(--surf); border: 1px solid var(--border); border-radius: 10px; }}
  .row {{ padding: .9rem 1.2rem 1rem; border-top: 1px solid var(--border); font-size: .92rem; }}
  .row:first-child {{ border-top: none; }}
  .row-top {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: .55rem; }}
  .row-left {{ display: flex; align-items: center; gap: .65rem; }}
  .dot {{ width: 9px; height: 9px; border-radius: 50%; flex-shrink: 0; }}
  .dot.up {{ background: var(--ok); }}
  .dot.down {{ background: var(--crit); }}
  .host {{ font-family: ui-monospace, "SF Mono", Menlo, monospace; }}
  .status {{ font-weight: 600; font-variant-numeric: tabular-nums; }}
  .status.up {{ color: var(--ok); }}
  .status.down {{ color: var(--crit); }}
  .bars {{ display: flex; gap: 1px; height: 28px; overflow: hidden; }}
  .bar {{ flex: 1 1 0; min-width: 0; border-radius: 1px; position: relative; cursor: pointer; }}
  @media (min-width: 480px) {{ .bars {{ gap: 2px; }} .bar {{ border-radius: 2px; }} }}
  .bar.up {{ background: var(--ok); }}
  .bar.warn {{ background: var(--warn); }}
  .bar.down {{ background: var(--crit); }}
  .bar.nodata {{ background: var(--border); }}
  .bar:hover {{ opacity: .75; }}
  .bar::after {{
    content: attr(data-tip); position: absolute; bottom: calc(100% + 8px); left: 50%; transform: translateX(-50%);
    background: var(--tooltip-bg); color: var(--text); border: 1px solid var(--border);
    padding: .35rem .6rem; border-radius: 6px; font-size: .74rem; white-space: nowrap;
    opacity: 0; pointer-events: none; transition: opacity .1s; z-index: 10; box-shadow: 0 4px 12px rgba(0,0,0,.4);
  }}
  .bar::before {{
    content: ""; position: absolute; bottom: 100%; left: 50%; transform: translateX(-50%);
    border: 5px solid transparent; border-top-color: var(--tooltip-bg);
    margin-bottom: -1px; opacity: 0; pointer-events: none; transition: opacity .1s; z-index: 10;
  }}
  .bar:hover::after, .bar:hover::before {{ opacity: 1; }}
  .bars-caption {{ display: flex; justify-content: space-between; color: var(--muted); font-size: .72rem; margin-top: 1rem; }}
  footer {{ color: var(--muted); font-size: .78rem; text-align: center; margin-top: 1.6rem; }}
</style>
</head>
<body>
  <div class="banner {banner_class}">{banner_icon} {banner_text}</div>
  <div class="wrap">
    <div class="titlebar"><h1>📡 meshnode.id MQTT Status</h1></div>
    <div class="sub">Status broker MQTT publik meshnode.id — data diperbarui tiap 10 menit</div>
    <div class="panel">{"".join(rows)}</div>
    <div class="bars-caption"><span>{HISTORY_DAYS} hari lalu</span><span>Hari ini</span></div>
    <footer>Commit {commit_sha} · Diperbarui {updated_str}</footer>
  </div>
</body>
</html>
'''

with open(OUT_PATH, "w") as f:
    f.write(html)
print(f"wrote {OUT_PATH} ({up_count}/{total} brokers up)")
