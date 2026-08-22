#!/bin/bash
# LXC-side runner for the mqtt-status page -- mirrors .github/workflows/
# check-status.yml's own check-and-commit logic exactly, just running
# locally instead of on a GitHub Actions runner.
#
# Why this exists alongside the GitHub Actions workflow, not instead of
# it: this box sits near Indonesia (~5-30ms to these brokers); the
# Actions runner is on US/EU infra (~600-750ms to the same brokers,
# confirmed 2026-08-19). Running the check from here gives accurate
# latency numbers and a much tighter check interval whenever this box is
# up. THIS side is the primary publisher; check-status.yml's own "Skip
# publish if the LXC already did recently" step (added 2026-08-22) makes
# it defer to whatever this script just pushed, only actually publishing
# when this box has been quiet for a while -- that's what makes it a
# real fallback instead of two independent writers racing each other
# every cycle (confirmed live: that race was failing the Actions job
# and emailing failure notifications regularly before the guard existed).
#
# Commits under a distinct identity (not the "RiV-Bot" identity used for
# manual/design-work commits, not "github-actions[bot]") so the commit
# history makes it obvious which system produced which data point.
set -euo pipefail
cd "$(dirname "$0")/../.."

# 2026-08-23: discard any leftover uncommitted state before pulling --
# confirmed live, repeatedly: a dirty tree (always regenerated-output
# files here, e.g. from manual dev/debug work over SSH -- real source
# edits go through their own separate commit, never accumulate as
# stray uncommitted diffs in this checkout) makes `git pull --rebase`
# fail at the very first line, which cascades into the lxc-monitor
# staleness detector correctly-but-misleadingly reporting this box as
# down. Every one of these files gets regenerated fresh moments later
# regardless of what was on disk before this line runs, so discarding
# whatever is here is always safe -- this is what actually makes the
# script self-healing against that whole failure class, instead of
# relying on remembering to `git status` clean before every SSH
# session ends.
if [ -n "$(git status --porcelain)" ]; then
    echo "mqtt-status-lxc: discarding dirty working tree before pull"
    git checkout --quiet -- .
    git clean --quiet -fd
fi

git pull --rebase --quiet origin main

# 2026-08-22: check_and_render.py now does a real authenticated MQTT
# CONNECT (not just a TCP check) so it can tell "broker down" apart from
# "broker up but login rejected" -- see check_and_render.py's own
# comments for why. Pulled fresh from secrets.json on every run (same
# source meshtasticd's own config was seeded from) rather than baked
# into this unit file, so a password rotation only needs updating in one
# place.
MQTT_CHECK_USER="$(python3 -c "import json; print(json.load(open('/opt/rivbot-ui/data/secrets.json')).get('mqtt_user',''))")"
MQTT_CHECK_PASS="$(python3 -c "import json; print(json.load(open('/opt/rivbot-ui/data/secrets.json')).get('mqtt_pass',''))")"
export MQTT_CHECK_USER MQTT_CHECK_PASS

# 2026-08-22: the ping-legend label used to hardcode "RiV-meshBot" --
# the bot's own node can be (and has been) renamed via
# `meshtastic --set-owner`, so pull the live longName from meshtasticd
# on every run instead of a string that goes stale the moment someone
# renames it. Best-effort: a failed/slow query just leaves
# BOT_LONG_NAME empty, and check_and_render.py falls back to whatever
# was last persisted in state.json's _meta rather than failing the
# whole publish over this.
BOT_LONG_NAME="$(timeout 8 python3 -c "
from meshtastic.tcp_interface import TCPInterface
iface = TCPInterface(hostname='localhost')
name = iface.getMyNodeInfo().get('user', {}).get('longName', '')
iface.close()
print(name)
" 2>/dev/null || true)"
export BOT_LONG_NAME

python3 check_and_render.py

# 2026-08-22: bot-status.html (mesh_bot/meshtasticd service uptime) --
# only THIS side can actually check them (local systemd + local API,
# neither reachable from GitHub Actions). See check_bot_status.py's own
# docstring for the full LXC-vs-Actions split.
python3 check_bot_status.py

git add index.html history state.json log.jsonl brokers.json notice.json bot-status.html bot_history bot_state.json bot_log.jsonl
if git diff --cached --quiet; then
    echo "mqtt-status-lxc: no changes to publish"
else
    git -c user.name="mqtt-status-lxc" -c user.email="lxc@rivbot.local" \
        commit --quiet -m "Update status (LXC) $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    # This box is the primary publisher and Actions now defers to it, so
    # a real collision should be rare -- but not impossible right at the
    # boundary of Actions' own 5-min defer window. 2 retries is enough
    # margin for that narrow case without this oneshot unit hanging
    # around; -X ours keeps this run's freshly-computed data on
    # conflict, same reasoning as the Actions side.
    pushed=0
    for i in 1 2; do
        if git push --quiet origin main; then
            pushed=1
            break
        fi
        echo "mqtt-status-lxc: push rejected (attempt $i), rebasing and retrying"
        git fetch --quiet origin main
        git -c user.name="mqtt-status-lxc" -c user.email="lxc@rivbot.local" \
            rebase --quiet -X ours origin/main
    done
    if [ "$pushed" = "1" ]; then
        echo "mqtt-status-lxc: published"
    else
        echo "mqtt-status-lxc: failed to push after retries"
        exit 1
    fi
fi
