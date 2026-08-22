"""
Points mqtt.rivi.my.id at whichever meshnode.id broker is currently
healthy, via Cloudflare's DNS API -- lets meshtasticd (and any future
hardware node) use ONE fixed address forever instead of needing its
mqtt.address reconfigured on every failover.

Deliberately GitHub-Actions-only (see check-status.yml), never run from
the LXC-side timer: the whole point is that broker selection keeps
working even if the homelab itself is offline. A device's DNS lookup
hits Cloudflare's global network directly and never touches anything
we run at home.

Reuses the SAME debounced health signal check_and_render.py already
computes and persists to state.json (current_outage_start /
current_auth_start) rather than doing its own raw check -- avoids
flapping the DNS record on a single transient blip that hasn't even
been confirmed down/auth-rejected on the public status page yet.

CNAME, not A record: points at the broker's own hostname rather than
resolving it to an IP ourselves, so if meshnode.id ever moves a broker
to a new IP, nothing here needs to change. Must stay unproxied (DNS
only, grey cloud) -- MQTT is a plain TCP protocol on port 1883, and
Cloudflare's orange-cloud proxy only understands HTTP(S) without the
paid Spectrum add-on.
"""
import json
import os
import sys
import urllib.request
import urllib.error

CF_API = "https://api.cloudflare.com/client/v4"
ZONE_ID = os.environ.get("CLOUDFLARE_ZONE_ID", "")
API_TOKEN = os.environ.get("CLOUDFLARE_API_TOKEN", "")
RECORD_NAME = "mqtt.rivi.my.id"
TTL = 60  # low on purpose -- this is the whole failover mechanism, staleness here IS the outage


def _cf_request(method, path, body=None):
    url = f"{CF_API}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        print(f"Cloudflare API error {e.code}: {e.read().decode()}", file=sys.stderr)
        raise


def pick_best_broker(brokers, state):
    """Always prefers brokers.json's FIRST entry (mqtt.meshnode.id) when
    it's healthy -- the primary broker, not just "whatever we happen to
    be on already". Only falls through to the next healthy one in
    brokers.json's own priority order while the primary is down/auth-
    rejected, and reverts back to it automatically the moment it
    recovers (this function is re-evaluated fresh every run, so recovery
    just falls out of the same ordered scan rather than needing separate
    revert logic)."""
    healthy = [h for h in brokers if not state.get(h, {}).get("current_outage_start")
               and not state.get(h, {}).get("current_auth_start")]
    if not healthy:
        return None  # every broker confirmed down/auth-rejected -- leave DNS as-is rather than pointing at a known-bad host
    return healthy[0]


def get_current_target():
    try:
        r = _cf_request("GET", f"/zones/{ZONE_ID}/dns_records?type=CNAME&name={RECORD_NAME}")
    except Exception:
        return None
    records = r.get("result") or []
    return records[0]["content"] if records else None


def get_current_record_id():
    r = _cf_request("GET", f"/zones/{ZONE_ID}/dns_records?type=CNAME&name={RECORD_NAME}")
    records = r.get("result") or []
    return records[0]["id"] if records else None


def main():
    if not ZONE_ID or not API_TOKEN:
        print("CLOUDFLARE_ZONE_ID/CLOUDFLARE_API_TOKEN not set -- skipping DNS update")
        return

    with open("brokers.json") as f:
        brokers = json.load(f)
    with open("state.json") as f:
        state = json.load(f)

    target = pick_best_broker(brokers, state)
    if not target:
        print("all brokers currently unhealthy -- leaving DNS record unchanged")
        return

    record_id = get_current_record_id()
    payload = {"type": "CNAME", "name": RECORD_NAME, "content": target,
               "ttl": TTL, "proxied": False}

    if record_id is None:
        _cf_request("POST", f"/zones/{ZONE_ID}/dns_records", payload)
        print(f"created {RECORD_NAME} -> {target}")
        return

    current = get_current_target()
    if current == target:
        print(f"{RECORD_NAME} already points at {target} -- no change")
        return

    _cf_request("PUT", f"/zones/{ZONE_ID}/dns_records/{record_id}", payload)
    print(f"updated {RECORD_NAME}: {current} -> {target}")


if __name__ == "__main__":
    main()
