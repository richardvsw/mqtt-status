#!/bin/bash
# LXC-side runner for the mqtt-status page -- mirrors .github/workflows/
# check-status.yml's own check-and-commit logic exactly, just running
# locally instead of on a GitHub Actions runner.
#
# Why this exists alongside the still-unchanged GitHub Actions workflow,
# not instead of it: this box sits near Indonesia (~5-30ms to these
# brokers); the Actions runner is on US/EU infra (~600-750ms to the same
# brokers, confirmed 2026-08-19). Running the check from here gives
# accurate latency numbers and a much tighter check interval whenever
# this box is up. The Actions workflow needs NO changes to act as a
# fallback -- it already just checks-and-commits-if-changed every ~10
# min regardless of what else pushed since; if this box goes quiet, the
# next Actions run is simply the freshest thing again, no explicit
# "is the LXC alive" detection required anywhere.
#
# Commits under a distinct identity (not the "RiV-Bot" identity used for
# manual/design-work commits, not "github-actions[bot]") so the commit
# history makes it obvious which system produced which data point.
set -euo pipefail
cd "$(dirname "$0")/../.."

git pull --quiet origin main

python3 check_and_render.py

git add index.html history.json state.json log.jsonl brokers.json
if git diff --cached --quiet; then
    echo "mqtt-status-lxc: no changes to publish"
else
    git -c user.name="mqtt-status-lxc" -c user.email="lxc@rivbot.local" \
        commit --quiet -m "Update status (LXC) $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    git push --quiet origin main
    echo "mqtt-status-lxc: published"
fi
