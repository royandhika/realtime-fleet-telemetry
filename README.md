# Realtime Fleet Telemetry

**Live fleet intelligence, end to end.** Two delivery vehicles stream their engine and GPS telemetry through a real streaming stack, and turn it into a live fleet map, per-minute operational KPIs, and driving-safety signals a fleet operator could actually act on.

Everything runs from a single `docker compose up`.

![demo](docs/demo.gif)

## Why this exists

Fleet businesses run on questions their data can't answer fast enough:

- *Where is every vehicle right now?*
- *Is anyone driving in a way that burns fuel or risks an accident?*
- *Which engines are heading for a costly breakdown?*

The data to answer all of this already exists inside every vehicle's ECU. The hard part is
moving it continuously and real-timey from the moving vehicles to the people
making decisions.

## What an operator gets

| Business question | Answered by |
|---|---|
| Where is the fleet *right now*? | Live map with per-second positions and speeds |
| How is the fleet performing? | 1-minute per-vehicle KPIs + 5-minute rolling fleet view (avg/max speed, harsh events) |
| Is anyone driving dangerously? | Harsh braking / acceleration / cornering, over-speed and idling flagged as they happen |
| Which vehicles need attention? | Engine temperature, fuel level, and fault codes streamed continuously |
| What exactly happened last Tuesday? | Every raw reading kept in queryable history for audits and analysis |

## The system in one picture

![High-level architecture](docs/architecture-high-level.png)

The vehicles are simulated, but the pipeline is real. Two virtual cars move around Jakarta and Bandung with traffic-aware speeds, engine signals derived from the motion physics, scripted driver behaviors producing harsh events. 

Every data point takes the same journey:

1. **Publish** 

    Each tick (1 Hz), vehicles emit telemetry to the Kafka topic `iot.telemetry.raw`. Driving events (harsh braking, over-speed, …) go to a second topic `iot.events.raw` only when a threshold is crossed.

2. **Process** 
    
    An Apache Beam pipeline runs on *event time* and fans the stream out to two types of storage, Cassandra and Redis.

3. **Store** 
    
    Two stores, split by function. 
     
    **Cassandra** holds the write-optimized history: raw readings and windowed aggregates (`fleet_telemetry.vehicle_window_aggregates`), with tables modeled one per business question so every read is a single lookup. 
    
    **Redis** holds `vehicle:latest:*`, one blob per vehicle overwritten every tick, for sub-millisecond "where is it *right now*" reads.

4. **Serve** 

    Two layers for two different purposes.

   **Grafana** for trend analysis. 
   
   **Custom dashboard** for the live view -> it reads Redis and pushes updates to browsers over WebSocket the moment something changes.

## Screenshots

![Grafana — Fleet Overview](docs/screenshot-grafana.jpg)

![Live dashboard — map + rolling charts](docs/screenshot-dashboard.jpg)

## Run it

```bash
docker compose up --build
```

| URL | What you'll see |
|---|---|
| http://localhost:8080 | Live dashboard — fleet map + rolling charts, updating every second |
| http://localhost:3000 | Grafana (`admin`/`admin`) → "Fleet Overview" |

<details>
<summary>Peek at the raw data flowing through</summary>

```bash
# raw telemetry from Kafka
docker exec iot-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic iot.telemetry.raw

# latest-state blobs in Redis (one per vehicle, overwritten every tick)
docker exec iot-redis redis-cli --scan --pattern 'vehicle:latest:*'

# per-minute KPIs in Cassandra
docker exec iot-cassandra cqlsh cassandra 9042 \
  -e "SELECT vehicle_id, window_start, avg_speed_kmh, harsh_event_count
      FROM fleet_telemetry.vehicle_window_aggregates LIMIT 10;"
```

</details>

## Testing, testing

A streaming system that *looks* alive is not the same as one that *is* alive. This repo ships a 30-assertion end-to-end suite that runs against the live stack and checks what an operator would care about: data is actually flowing (not stale), storage is growing,
dashboards return real query results, browsers receive valid live updates, aggregates
arrive on schedule, trips are detected, and **late-arriving data is
dropped loudly instead of silently corrupting the KPIs**.

```bash
python3 tests/e2e_test.py
```

## What could happen next?

- **Alerting**, engine-temp and harsh-driving thresholds pushed to operators *before*
  they become breakdowns or accidents.
- **Scale-out runner**, swap in Apache Flink to go from demo scale to real fleet volumes
  without changing the pipeline logic.