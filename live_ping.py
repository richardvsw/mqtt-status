"""
Fast, best-effort broker check for the mqtt-status page's LIVE layer --
distinct from check_and_render.py's own check_broker(), which retries
and only reports a status once CONFIRM_THRESHOLD runs agree (built that
way specifically to avoid false-positive "Down" flickers on the durable,
git-committed record).

This script deliberately skips all of that: single attempt, short
timeout, no retries, no git commit, no history/incident tracking. It
exists purely to push a fast, best-effort snapshot to the mqtt-status
page's own homelab box (CT104/rivbot-ui) for a client-side "live" dot
next to the durable status -- worth being fast and cheap even at the
cost of occasionally flashing a false blip, since the SAME durable
check_and_render.py run (this workflow's other step) is what actually
decides the publicly-recorded up/down state.

Runs from GitHub Actions only, same as check_and_render.py -- never
from the homelab LXC. See public_site.py's own /api/public/broker-status
route docstring on the receiving end for why: that box is instructed to
never open a connection to the broker cluster itself, so the fast
checking has to happen out here regardless of how it's triggered.
"""
import json
import os
import socket
import time
import urllib.request
import urllib.error

MQTT_PORT = 1883
MQTT_CHECK_USER = os.environ.get("MQTT_CHECK_USER", "")
MQTT_CHECK_PASS = os.environ.get("MQTT_CHECK_PASS", "")
PUSH_URL = os.environ.get("BROKER_STATUS_PUSH_URL", "")
PUSH_TOKEN = os.environ.get("BROKER_STATUS_PUSH_TOKEN", "")
TIMEOUT = 4

with open("brokers.json") as _f:
    BROKERS = json.load(_f)


def _classify_connect_error(exc):
    if isinstance(exc, socket.gaierror):
        return "dns_error"
    if isinstance(exc, ConnectionRefusedError):
        return "refused"
    if isinstance(exc, (socket.timeout, TimeoutError)):
        return "timeout"
    return "down"


def check_once(host):
    t0 = time.time()
    if not MQTT_CHECK_USER:
        try:
            with socket.create_connection((host, MQTT_PORT), timeout=TIMEOUT):
                return {"status": "up", "latency_ms": round((time.time() - t0) * 1000)}
        except Exception as e:
            return {"status": "down", "reason": _classify_connect_error(e)}

    import paho.mqtt.client as mqtt
    result = {"status": None, "reason": None}

    def on_connect(client, userdata, flags, rc, properties=None):
        code = rc.value if hasattr(rc, "value") else rc
        if code == 0:
            result["status"] = "up"
        elif code in (4, 5):
            result["status"] = "auth_error"
        else:
            result["status"] = "down"
            result["reason"] = "mqtt_rejected"
        client.disconnect()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1,
                          client_id=f"mqtt-status-live-{os.getpid()}-{int(time.time()*1000)}",
                          protocol=mqtt.MQTTv311)
    client.username_pw_set(MQTT_CHECK_USER, MQTT_CHECK_PASS)
    client.on_connect = on_connect
    try:
        client.connect(host, MQTT_PORT, TIMEOUT)
        client.loop_start()
        waited = 0.0
        while result["status"] is None and waited < TIMEOUT:
            time.sleep(0.1)
            waited += 0.1
        client.loop_stop()
        try:
            client.disconnect()
        except Exception:
            pass
    except Exception as e:
        return {"status": "down", "reason": _classify_connect_error(e)}

    if result["status"] == "up":
        return {"status": "up", "latency_ms": round((time.time() - t0) * 1000)}
    if result["status"] == "auth_error":
        return {"status": "auth_error"}
    return {"status": "down", "reason": result["reason"] or "down"}


def main():
    brokers = {host: check_once(host) for host in BROKERS}
    if not PUSH_URL or not PUSH_TOKEN:
        print("live_ping: BROKER_STATUS_PUSH_URL/TOKEN not set, skipping push")
        return
    body = json.dumps({"brokers": brokers}).encode()
    req = urllib.request.Request(
        PUSH_URL, data=body, method="POST",
        headers={"Content-Type": "application/json", "X-Push-Token": PUSH_TOKEN},
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            print(f"live_ping: pushed, status {resp.status}")
    except urllib.error.URLError as e:
        # Best-effort -- the homelab being offline (the whole reason the
        # durable record doesn't depend on it) is an EXPECTED case here,
        # not a failure worth failing this workflow step over.
        print(f"live_ping: push failed (homelab likely offline): {e}")


if __name__ == "__main__":
    main()
