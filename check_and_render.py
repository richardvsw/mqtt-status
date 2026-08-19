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
HISTORY_DAYS = 90


CHECK_RETRIES = 10
RETRY_DELAY_SECONDS = 1.5


def check_broker(host, timeout=5, retries=CHECK_RETRIES):
    """A single bad moment on the GitHub Actions runner's own network/DNS
    used to be enough to mark a broker down -- confirmed 2026-08-18: all 5
    brokers failed in the same instant right after latencies had climbed
    unusually high, while this box's own live connection to the same
    brokers was working the entire time. Retries here so a transient
    runner-side blip doesn't get reported as a real outage on the public
    page; a genuinely down broker still fails every attempt and gets
    marked down same as before, just slower to confirm."""
    t0 = time.time()
    for attempt in range(retries):
        try:
            with socket.create_connection((host, MQTT_PORT), timeout=timeout):
                return True, round((time.time() - t0) * 1000)
        except Exception:
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY_SECONDS)
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

brokers = {}
for host in BROKERS:
    raw_reachable, latency_ms = check_broker(host)
    st = state.setdefault(host, {"current_outage_start": None, "consecutive_fails": 0, "provisional_start": None})
    st.setdefault("consecutive_fails", 0)
    st.setdefault("provisional_start", None)

    if raw_reachable:
        st["consecutive_fails"] = 0
        st["provisional_start"] = None
        st["current_outage_start"] = None
    else:
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

    # Only shown as Down once confirmed -- a single run's raw failure
    # (consecutive_fails == 1) still shows Operational publicly, exactly
    # so an unconfirmed blip never produces a false "down" reading.
    confirmed_down = st["current_outage_start"] is not None
    brokers[host] = {
        "reachable": not confirmed_down,
        "raw_reachable": raw_reachable,
        "latency_ms": latency_ms,
        "lxc_latency_ms": st.get("lxc_latency_ms"),
        "actions_latency_ms": st.get("actions_latency_ms"),
        "current_outage_start": st["current_outage_start"],
    }

save_json(STATE_PATH, state)

today_str = datetime.fromtimestamp(now, WIB).strftime("%Y-%m-%d")
today_bucket = history.setdefault(today_str, {})
for host, b in brokers.items():
    slot = today_bucket.setdefault(host, {"up": 0, "total": 0})
    slot["total"] += 1
    # Raw result, not the confirmed/public one -- the 90-day bar strip
    # already has a "warn" (partial) tier for exactly this kind of noise,
    # so a lone transient blip shows as a slightly-off day rather than
    # either full green (hiding it) or full red (the false alarm this
    # whole change exists to avoid on the CURRENT-status banner/rows).
    if b["raw_reachable"]:
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
        status_class = "up"
        # Both shown side by side, each labeled with where it was measured
        # from, rather than one number that jumps between ~5ms and ~650ms
        # depending on which source (LXC vs GitHub Actions) last committed.
        parts = []
        if b["lxc_latency_ms"] is not None:
            parts.append(f"{b['lxc_latency_ms']}ms (RiV-meshBot server)")
        if b["actions_latency_ms"] is not None:
            parts.append(f"{b['actions_latency_ms']}ms (GitHub Actions)")
        status_label = "Operational · " + " · ".join(parts) if parts else "Operational"
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
<meta name="description" content="Status langsung broker MQTT publik meshnode.id">
<meta http-equiv="refresh" content="60">
<style>
  :root {{
    --bg: #0a0d12; --bg2: #0d1117; --surf: #10151d; --surf2: #151b25; --border: #212a37; --border-soft: #1a222d;
    --text: #eaeef3; --muted: #8b96a5; --faint: #566173;
    --ok: #3ddc97; --ok-dim: #2a4a3d; --ok-bg: #0f2019;
    --warn: #e8b64a; --warn-dim: #4a3d1f; --warn-bg: #241c0d;
    --crit: #f2685c; --crit-dim: #4a2521; --crit-bg: #251311;
    --accent: #5b8cff;
    --tooltip-bg: #1a2230;
    --shadow: 0 1px 2px rgba(0,0,0,.3), 0 8px 24px -8px rgba(0,0,0,.5);
  }}
  * {{ box-sizing: border-box; }}
  html {{ color-scheme: dark; }}
  body {{
    margin: 0; min-height: 100vh; color: var(--text);
    background:
      radial-gradient(900px 420px at 50% -10%, rgba(91,140,255,.10), transparent 60%),
      linear-gradient(180deg, var(--bg2), var(--bg) 340px);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Inter, Roboto, sans-serif;
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
    background: var(--surf); border: 1px solid var(--border); border-radius: 14px;
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

  /* No overflow:hidden here -- that would also clip the tooltip
     pseudo-elements below, which sit above each .bar's own box and are
     positioned relative to it, not to .bars. Rounding is done on the
     first/last .bar directly instead, so the strip still reads as one
     rounded pill without needing to clip anything. */
  .bars {{ display: flex; gap: 2px; height: 30px; }}
  .bar {{ flex: 1 1 0; min-width: 0; position: relative; cursor: pointer; }}
  .bar:first-child {{ border-top-left-radius: 4px; border-bottom-left-radius: 4px; }}
  .bar:last-child {{ border-top-right-radius: 4px; border-bottom-right-radius: 4px; }}
  .bar.up {{ background: var(--ok); opacity: .9; }}
  .bar.warn {{ background: var(--warn); }}
  .bar.down {{ background: var(--crit); }}
  .bar.nodata {{ background: var(--border); }}
  .bar:hover, .bar.active {{ opacity: 1; transform: scaleY(1.06); }}
  .bar::after {{
    content: attr(data-tip); position: absolute; bottom: calc(100% + 9px); left: 50%; transform: translateX(-50%);
    background: var(--tooltip-bg); color: var(--text); border: 1px solid var(--border);
    padding: .38rem .65rem; border-radius: 7px; font-size: .74rem; white-space: nowrap;
    opacity: 0; pointer-events: none; transition: opacity .12s; z-index: 10; box-shadow: var(--shadow);
  }}
  .bar::before {{
    content: ""; position: absolute; bottom: 100%; left: 50%; transform: translateX(-50%);
    border: 5px solid transparent; border-top-color: var(--tooltip-bg);
    margin-bottom: -1px; opacity: 0; pointer-events: none; transition: opacity .12s; z-index: 10;
  }}
  /* :hover covers desktop; .active is toggled by the click/tap handler below
     for touch devices, which have no :hover state at all. Same pattern
     Anthropic's own status page uses -- tap a day cell on mobile to pin
     its tooltip open instead of it being unreachable there. */
  .bar:hover::after, .bar:hover::before,
  .bar.active::after, .bar.active::before {{ opacity: 1; }}
  .bar:last-child::after {{ left: auto; right: 0; transform: none; }}
  .bar:last-child::before {{ left: auto; right: 10px; transform: none; }}
  .bar:first-child::after {{ left: 0; transform: none; }}
  .bar:first-child::before {{ left: 10px; transform: none; }}

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
    <p class="note">Ping diukur dari dua sumber independen: <b>RiV-meshBot server</b> (dekat Indonesia) dan <b>GitHub Actions</b> (server pengecekan cadangan, berlokasi di luar Indonesia). Ping dari GitHub Actions yang jauh lebih tinggi itu wajar/normal — bukan tanda broker lambat, cuma jarak geografis ke server pengecekannya.</p>
    <footer>Commit {commit_sha} · Diperbarui {updated_str} · <a href="https://github.com/richardvsw/mqtt-status">Sumber di GitHub</a></footer>
  </div>
  <script>
    // Tap-to-pin tooltips for touch devices, which have no :hover state --
    // same UX as Anthropic's own status page (tap a day cell on mobile).
    // Desktop keeps working via plain CSS :hover regardless of this script.
    document.querySelectorAll(".bar").forEach(function (bar) {{
      bar.addEventListener("click", function (e) {{
        var wasActive = bar.classList.contains("active");
        document.querySelectorAll(".bar.active").forEach(function (b) {{ b.classList.remove("active"); }});
        if (!wasActive) bar.classList.add("active");
        e.stopPropagation();
      }});
    }});
    document.addEventListener("click", function () {{
      document.querySelectorAll(".bar.active").forEach(function (b) {{ b.classList.remove("active"); }});
    }});
  </script>
</body>
</html>
'''

with open(OUT_PATH, "w") as f:
    f.write(html)
print(f"wrote {OUT_PATH} ({up_count}/{total} brokers up)")
