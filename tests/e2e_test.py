#!/usr/bin/env python3
"""End-to-end test for the realtime-fleet-telemetry stack (spec §9 milestones 0-5)."""
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

PASS, FAIL = [], []


def check(name, ok, detail=""):
    (PASS if ok else FAIL).append(name)
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  -- {detail}" if detail else ""))


def sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True)


def cql(q):
    r = sh(f"docker exec iot-cassandra cqlsh cassandra 9042 -e \"{q}\" 2>/dev/null")
    return r.stdout


print("== 1. Container status ==")
ps = sh("docker compose -f docker-compose.yml ps --format json").stdout.strip().splitlines()
services = {}
for line in ps:
    d = json.loads(line)
    services[d["Service"]] = (d["State"], d.get("Health", "none"))
for svc in ["kafka", "cassandra", "redis", "simulator", "beam", "grafana"]:
    state = services.get(svc, ("missing",))[0]
    check(f"container {svc} running", state == "running", f"state={state}")

print("\n== 2. Kafka flow ==")
topics = sh("docker exec iot-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list").stdout.split()
check("topic iot.telemetry.raw exists", "iot.telemetry.raw" in topics)
check("topic iot.events.raw exists", "iot.events.raw" in topics)

def last_offset(topic):
    r = sh(f"docker exec iot-kafka /opt/kafka/bin/kafka-get-offsets.sh "
           f"--bootstrap-server localhost:9092 --topic {topic} --time -1")
    return sum(int(l.rsplit(":", 1)[1]) for l in r.stdout.strip().splitlines() if l)

t1, o1 = last_offset("iot.telemetry.raw"), None
o1 = t1
time.sleep(6)
o2 = last_offset("iot.telemetry.raw")
check("telemetry messages flowing (~2/s)", o2 > o1, f"{o1} -> {o2} (+{o2-o1} in 6s)")

sample = sh("timeout 8 docker exec iot-kafka /opt/kafka/bin/kafka-console-consumer.sh "
            "--bootstrap-server localhost:9092 --topic iot.telemetry.raw --from-beginning --max-messages 1").stdout.strip()
try:
    msg = json.loads(sample)
    required = {"vehicle_id", "ts", "lat", "lon", "speed_kmh", "engine_temp_c"}
    check("telemetry payload has expected shape", required.issubset(msg.keys()),
          f"keys={sorted(msg.keys())[:8]}...")
except Exception as exc:
    check("telemetry payload parses as JSON", False, str(exc))

print("\n== 3. Cassandra sinks ==")
def count(table):
    out = cql(f"SELECT COUNT(*) FROM fleet_telemetry.{table};")
    m = [l.strip() for l in out.splitlines() if l.strip().isdigit()]
    return int(m[0]) if m else -1

c1 = count("telemetry_by_vehicle_time")
time.sleep(4)
c2 = count("telemetry_by_vehicle_time")
check("raw telemetry rows growing", c2 > c1, f"{c1} -> {c2}")
check("raw events written", count("events_by_vehicle_time") > 0)
agg = cql("SELECT DISTINCT vehicle_id FROM fleet_telemetry.vehicle_window_aggregates;")
vids = re.findall(r"veh_\d+", agg)
check("window aggregates present for both vehicles", len(set(vids)) == 2, f"vehicles={sorted(set(vids))}")

print("\n== 4. Redis latest-state ==")
keys = sh("docker exec iot-redis redis-cli keys 'vehicle:latest:*'").stdout.split()
check("both vehicles have latest-state blobs", len(keys) == 2, str(keys))
fresh = False
blob = ""
for k in keys:
    blob = sh(f"docker exec iot-redis redis-cli get {k}").stdout.strip()
    try:
        d = json.loads(blob)
        age = time.time() - datetime.strptime(d["updated_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
        if age < 15:
            fresh = True
        fields_ok = {"lat", "lon", "speed_kmh", "engine_temp_c", "updated_at"}.issubset(d.keys())
    except Exception:
        fields_ok = False
    check(f"blob {k} valid & fresh", fields_ok and fresh, blob[:80])

print("\n== 5. Beam pipeline health ==")
logs = sh("docker logs iot-beam --since 5m 2>&1").stdout
check("no beam tracebacks in last 5m", "Traceback" not in logs)
check("beam flushing aggregates", "flush" in logs, "")

print("\n== 6. Grafana ==")
import urllib.request
import base64
auth = base64.b64encode(b"admin:admin").decode()

def api(path, body=None):
    req = urllib.request.Request(f"http://localhost:3000{path}",
                                 data=json.dumps(body).encode() if body else None,
                                 headers={"Authorization": f"Basic {auth}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

health = api("/api/health")
check("grafana healthy", health.get("database") == "ok")

ds = {d["uid"]: d["name"] for d in api("/api/datasources")}
check("datasources provisioned", {"cassandra-fleet", "redis-latest"}.issubset(ds.keys()), str(ds))

dash = api("/api/dashboards/uid/fleet-overview")["dashboard"]
check("dashboard provisioned with 5 panels", len(dash["panels"]) == 5,
      ",".join(p["title"] for p in dash["panels"]))

def query(body):
    return api("/api/ds/query", {"queries": [body], "from": "now-30m", "to": "now"})

redis_q = query({"refId": "A", "datasource": {"type": "redis-datasource", "uid": "redis-latest"},
                 "queryType": "cli", "query": "GET vehicle:latest:veh_0001"})
frames = redis_q["results"]["A"].get("frames", [])
check("Redis datasource returns live blob", bool(frames) and "speed_kmh" in frames[0]["data"]["values"][0][0],
      "" if frames else str(redis_q)[:120])

cass_q = query({"refId": "A", "datasource": {"type": "hadesarchitect-cassandra-datasource", "uid": "cassandra-fleet"},
                "rawQuery": True, "queryType": "query",
                "target": "SELECT event_time AS time, speed_kmh FROM fleet_telemetry.telemetry_by_vehicle_time WHERE vehicle_id = 'veh_0001' LIMIT 100"})
pts = sum(len(f["data"]["values"][1]) for f in cass_q["results"]["A"].get("frames", []))
check("Cassandra datasource returns speed series", pts > 50, f"{pts} points")

agg_q = query({"refId": "A", "datasource": {"type": "hadesarchitect-cassandra-datasource", "uid": "cassandra-fleet"},
               "rawQuery": True, "queryType": "query",
               "target": "SELECT window_start AS time, harsh_event_count FROM fleet_telemetry.vehicle_window_aggregates WHERE vehicle_id = 'veh_0001' LIMIT 60"})
check("aggregates queryable for dashboard", agg_q["results"]["A"].get("status") == 200)

print("\n== 7. WebSocket dashboard ==")
http = sh("curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/")
check("frontend served on :8080", http.stdout.strip() == "200", f"status={http.stdout.strip()!r}")

probe = Path(__file__).parent / "ws_probe.py"
ws_probe = sh(f"docker exec -i iot-ws python - < {probe}")
ok = ws_probe.returncode == 0 and "OK" in ws_probe.stdout
check("WS /ws pushes valid fleet snapshot", ok, (ws_probe.stdout + ws_probe.stderr).strip()[:120])

print("\n== 8. Sliding fleet windows + sessions (spec §6) ==")
def fleet_rows():
    out = cql("SELECT COUNT(*) FROM fleet_telemetry.fleet_window_aggregates "
              "WHERE day_bucket = '" + datetime.now(timezone.utc).strftime("%Y-%m-%d") + "';")
    m = [l.strip() for l in out.splitlines() if l.strip().isdigit()]
    return int(m[0]) if m else -1

f1 = fleet_rows()
time.sleep(35)
f2 = fleet_rows()
check("fleet sliding window emitting every ~30s", f2 > f1 and f2 >= 1, f"{f1} -> {f2} rows today")
fleet_vals = cql("SELECT vehicles_active, fleet_avg_speed_kmh FROM fleet_telemetry.fleet_window_aggregates "
                 "WHERE day_bucket = '" + datetime.now(timezone.utc).strftime("%Y-%m-%d") + "' LIMIT 5;")
check("fleet aggregates populated", "null" not in fleet_vals, "")

beam_logs = sh("docker logs iot-beam 2>&1").stdout
check("trip session detection active", "TRIP OPENED" in beam_logs or "TRIP CLOSED" in beam_logs)

# Force a late element: replay a telemetry tick stamped 15 min in the past.
# The aggregator must DROP it loudly (spec §6: log dropped-after-lateness).
old_ts = (datetime.now(timezone.utc) - timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
stale = ('{"vehicle_id":"veh_0001","ts":"' + old_ts + '","lat":-6.2,"lon":106.8,'
         '"heading_deg":90,"speed_kmh":55,"gear":3,"rpm":2500,"throttle_pct":40,'
         '"maf_g_per_s":12,"engine_temp_c":90,"coolant_temp_c":85,"intake_air_temp_c":30,'
         '"fuel_pct":60,"fuel_consumed_l":1.2,"odometer_km":1000,"ignition_on":true}')
sh("printf '%s' " + json.dumps(stale) +
   " | docker exec -i iot-kafka /opt/kafka/bin/kafka-console-producer.sh "
   "--bootstrap-server localhost:9092 --topic iot.telemetry.raw >/dev/null 2>&1")
time.sleep(4)
late_logs = sh("docker logs iot-beam --since 2m 2>&1").stdout
check("late element dropped with explicit log", "[late-data]" in late_logs and "DROPPED" in late_logs,
      next((l.split("beam:")[-1].strip() for l in late_logs.splitlines() if "[late-data]" in l), "no drop logged"))

print(f"\n===== RESULT: {len(PASS)} passed, {len(FAIL)} failed =====")
for f in FAIL:
    print(f"  FAILED: {f}")
sys.exit(1 if FAIL else 0)
