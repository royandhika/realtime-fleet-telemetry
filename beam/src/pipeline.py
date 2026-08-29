"""Fleet telemetry Beam pipeline (DirectRunner, streaming).

Reads iot.telemetry.raw + iot.events.raw from Kafka, writes raw rows to
Cassandra, emits a per-vehicle 1-minute tumbling-window aggregate
(avg/max speed, harsh-event count) to vehicle_window_aggregates and a
5-min fleet sliding window (30s slide) to fleet_window_aggregates,
detects trips via session windows (5-min gap / ignition transitions),
drops-and-logs elements past the late-data hold, and overwrites the
Redis latest-state blob vehicle:latest:{vehicle_id} (spec §4.6) on
every telemetry tick.

Kafka is consumed via a Splittable DoFn (SDF) — the modern pure-Python
replacement for the removed `UnboundedSource`. The SDF reports the output
watermark from the consumed messages' own event_time (via a
ManualWatermarkEstimator), so tumbling windows fire correctly instead of
being dropped as "later than allowed lateness" (which happened with the
earlier bounded `Create`-fed infinite generator, whose watermark jumped to
+inf). No JVM is required.
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
import uuid
from collections import deque
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners import sdf_utils
from apache_beam.transforms import core
from apache_beam.transforms.window import FixedWindows, TimestampedValue
from apache_beam.utils.timestamp import Timestamp

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("beam")

# ---- Config ---------------------------------------------------------------
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TELEMETRY_TOPIC = os.environ.get("KAFKA_TELEMETRY_TOPIC", "iot.telemetry.raw")
EVENTS_TOPIC = os.environ.get("KAFKA_EVENTS_TOPIC", "iot.events.raw")
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "beam-fleet-pipeline")
CONTACT_POINTS = os.environ.get("CASSANDRA_CONTACT_POINTS", "localhost:9042")
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "fleet_telemetry")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

HARSH_EVENT_TYPES = {"harsh_braking", "harsh_accel", "harsh_cornering", "over_speed"}


# ---- Parsing --------------------------------------------------------------
def _to_dt(s: str) -> datetime:
    # simulator ISO strings are tz-aware UTC, e.g. "...+00:00"; handle literal Z.
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _to_uuid(v):
    if not v:
        return None
    return v if isinstance(v, uuid.UUID) else uuid.UUID(str(v))


def _json_value(raw):
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        return json.loads(raw)
    return raw


def parse_telemetry(raw):
    d = dict(_json_value(raw))
    d["event_time"] = _to_dt(d.pop("ts"))
    d["trip_id"] = _to_uuid(d.get("trip_id"))
    return d


def parse_event(raw):
    d = dict(_json_value(raw))
    d["event_time"] = _to_dt(d.pop("ts"))
    d["trip_id"] = _to_uuid(d.get("trip_id"))
    d["event_id"] = _to_uuid(d.get("event_id"))
    return d


def _peek_event_ts(raw):
    """Decode a raw Kafka value and return (raw_value, event_datetime or None)."""
    try:
        d = _json_value(raw)
        ts = d.get("ts")
        return raw, (_to_dt(ts) if ts else None)
    except Exception:
        return raw, None


def _ts_telemetry(pair):
    """Parse telemetry and attach event_time as the element timestamp (window key)."""
    raw, event_dt = pair
    d = parse_telemetry(raw)
    if event_dt is None:
        event_dt = d["event_time"]
    return TimestampedValue(d, Timestamp(event_dt.timestamp()))


def _ts_event(pair):
    """Parse event and attach event_time as the element timestamp (window key)."""
    raw, event_dt = pair
    d = parse_event(raw)
    if event_dt is None:
        event_dt = d["event_time"]
    return TimestampedValue(d, Timestamp(event_dt.timestamp()))


# ---- Kafka Splittable DoFn (unbounded source) -----------------------------
class KafkaRestrictionProvider(core.RestrictionProvider):
    """Unbounded restriction: a monotonically-increasing poll counter.

    Modelled after `ImpulseSeqGenRestrictionProvider` in
    apache_beam.transforms.periodicsequence. ``stop`` is effectively infinite;
    progress is driven by per-message ``try_claim`` + ``defer_remainder``.
    """

    def initial_restriction(self, _element):
        return OffsetRange(0, 1 << 62)

    def create_tracker(self, restriction):
        return OffsetRestrictionTracker(restriction)

    def restriction_size(self, _element, restriction):
        return float(restriction.size())

    def truncate(self, _element, _restriction):
        # On drain (pipeline stop), claim no further positions.
        return None


def _advance_watermark(estimator, ts: Timestamp):
    """Advance the watermark, never backward.

    The idle branch sets the watermark to wall-clock time; when real messages
    then arrive with slightly older event times, ManualWatermarkEstimator
    raises on a non-monotonic update (which previously crashed the whole
    pipeline). Watermarks must only ever move forward, so ignore regressions.
    """
    try:
        estimator.set_watermark(ts)
    except ValueError:
        log.debug("watermark regression ignored (%s behind current)", ts)


class KafkaConsumeSDF(beam.DoFn):
    """Unbounded Kafka consumer as a Splittable DoFn.

    One SDF instance polls its assigned topic indefinitely. Each process()
    invocation polls one batch, claims one restriction position per consumed
    message (so Beam tracks progress and can checkpoint/split), yields each
    message as ``TimestampedValue(value, event_time)``, advances the
    watermark to the max event_time seen, then ``defer_remainder`` schedules
    the next poll.
    """

    def __init__(self, bootstrap: str, topic: str, group_id: str,
                 poll_ms: int = 500, max_records: int = 2000):
        super().__init__()
        self._bootstrap = bootstrap
        self._topic = topic
        self._group_id = group_id
        self._poll_ms = poll_ms
        self._max_records = max_records
        self._consumer = None

    def _ensure_consumer(self):
        if self._consumer is None:
            from kafka import KafkaConsumer
            self._consumer = KafkaConsumer(
                self._topic,
                bootstrap_servers=self._bootstrap.split(","),
                group_id=f"{self._group_id}-{self._topic}",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda b: b,
                key_deserializer=lambda b: b,
            )
            log.info("KafkaConsumer attached to topic %s at %s", self._topic, self._bootstrap)

    @beam.DoFn.unbounded_per_element()
    def process(
        self,
        _element,
        restriction_tracker=beam.DoFn.RestrictionParam(KafkaRestrictionProvider()),
        watermark_estimator=beam.DoFn.WatermarkEstimatorParam(
            ManualWatermarkEstimator.default_provider()),
    ):
        assert isinstance(restriction_tracker, sdf_utils.RestrictionTrackerView)
        self._ensure_consumer()
        pos = restriction_tracker.current_restriction().start
        polled = self._consumer.poll(timeout_ms=self._poll_ms, max_records=self._max_records)
        max_ts = None
        n = 0
        for _tp, msgs in polled.items():
            for m in msgs:
                if not restriction_tracker.try_claim(pos):
                    return
                raw, event_dt = _peek_event_ts(m.value)
                if event_dt is not None:
                    tsf = event_dt.timestamp()
                    if max_ts is None or tsf > max_ts:
                        max_ts = tsf
                # Yield (raw, event_dt); a downstream Map attaches the
                # TimestampedValue instead of doing it inside the SDF (follows
                # the ImpulseSeqGenDoFn idiom so element timestamps propagate).
                yield (raw, event_dt)
                pos += 1
                n += 1
        if max_ts is not None:
            _advance_watermark(watermark_estimator, Timestamp(max_ts))
        else:
            # Idle source: advance the watermark to wall-clock time so a sparse
            # topic (e.g. iot.events.raw, which fires only on threshold crossings)
            # does not sink the pipeline-wide watermark (the min across sources)
            # and block other streams' tumbling windows from firing.
            _advance_watermark(watermark_estimator, Timestamp(time.time()))
        log.info("[%s] polled %d msgs, watermark=%s", self._topic, n,
                 Timestamp(max_ts) if max_ts else "now")
        # Schedule next poll shortly so we don't idle-spin but stay responsive.
        restriction_tracker.defer_remainder(Timestamp(time.time() + 0.25))

    def teardown(self):
        try:
            if self._consumer is not None:
                self._consumer.close()
        except Exception:
            pass


# ---- Cassandra write DoFn -------------------------------------------------
CQL_INSERT_TELEMETRY = (
    f"INSERT INTO {KEYSPACE}.telemetry_by_vehicle_time ("
    "vehicle_id, event_time, trip_id, lat, lon, heading_deg, speed_kmh, gear, "
    "rpm, throttle_pct, maf_g_per_s, engine_temp_c, coolant_temp_c, "
    "intake_air_temp_c, fuel_pct, fuel_consumed_l, odometer_km, ignition_on"
    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
)
CQL_INSERT_EVENT = (
    f"INSERT INTO {KEYSPACE}.events_by_vehicle_time ("
    "event_id, vehicle_id, trip_id, event_time, event_type, detail"
    ") VALUES (?,?,?,?,?,?)"
)


class CassandraWriteDoFn(beam.DoFn):
    """Generic Cassandra writer. `params_fn(element, window_start_dt) -> tuple`.

    `window` is read via beam.DoFn.WindowParam so windowed writers can pull
    the pane's window start. Non-windowed writers ignore it (pass None).
    """

    def __init__(self, contact_points: str, cql: str, params_fn, label: str):
        super().__init__()
        # Accept "host:9042" or "host" entries (strip any embedded port).
        self._contact_points = [h.split(":")[0] for h in contact_points.split(",") if h]
        self._cql = cql
        self._params_fn = params_fn
        self._label = label
        self._cluster = None
        self._session = None
        self._prepared = None

    def setup(self):
        self._connect_and_prepare()

    def _connect_and_prepare(self, attempts: int = 90, delay: float = 2.0):
        from cassandra.cluster import Cluster
        last = None
        for i in range(attempts):
            try:
                self._cluster = Cluster(contact_points=self._contact_points, port=9042)
                self._session = self._cluster.connect()
                self._prepared = self._session.prepare(self._cql)
                log.info("[%s] Cassandra prepared (%s)", self._label, self._cql[:60])
                return
            except Exception as exc:  # noqa: BLE001
                last = exc
                log.warning("[%s] cassandra prepare failed (%s), retry %d/%d",
                            self._label, exc, i + 1, attempts)
                try:
                    if self._cluster is not None:
                        self._cluster.shutdown()
                except Exception:
                    pass
                self._cluster = None
                self._session = None
                time.sleep(delay)
        raise RuntimeError(f"[{self._label}] could not prepare Cassandra statement: {last}")

    def process(self, element, window=beam.DoFn.WindowParam):
        if self._session is None:
            self._connect_and_prepare()
        ws = None
        try:
            # BoundedWindow.start is a Timestamp; convert micros -> UTC datetime.
            ws = datetime.fromtimestamp(int(window.start) / 1_000_000.0, tz=timezone.utc)
        except Exception:
            ws = None
        params = self._params_fn(element, ws)
        try:
            self._session.execute(self._prepared, params)
        except Exception as exc:
            log.warning("[%s] execute failed: %s (params=%r)", self._label, exc, params)
            try:
                self._connect_and_prepare()
                self._session.execute(self._prepared, params)
            except Exception:
                pass

    def teardown(self):
        try:
            if self._cluster is not None:
                self._cluster.shutdown()
        except Exception:
            pass


# ---- param-packing functions (module-level so they're picklable) ----------
def _telemetry_params(el, _ws):
    return (
        el["vehicle_id"], el["event_time"], el.get("trip_id"),
        float(el["lat"]), float(el["lon"]), float(el["heading_deg"]),
        float(el["speed_kmh"]), int(el["gear"]), int(el["rpm"]),
        float(el["throttle_pct"]), float(el["maf_g_per_s"]),
        float(el["engine_temp_c"]), float(el["coolant_temp_c"]), float(el["intake_air_temp_c"]),
        float(el["fuel_pct"]), float(el["fuel_consumed_l"]), float(el["odometer_km"]),
        bool(el["ignition_on"]),
    )


def _event_params(el, _ws):
    return (
        el["event_id"], el["vehicle_id"], el.get("trip_id"),
        el["event_time"], el["event_type"], el.get("detail") or "",
    )


# ---- Redis latest-state write DoFn ----------------------------------------
class RedisLatestStateDoFn(beam.DoFn):
    """Overwrites vehicle:latest:{vehicle_id} with a JSON blob on every
    telemetry tick (spec §4.6). Powers "where is the fleet right now" reads
    without touching Cassandra."""

    KEY_PREFIX = "vehicle:latest:"

    def __init__(self, redis_url: str):
        super().__init__()
        self._redis_url = redis_url
        self._client = None

    def _connect(self, attempts: int = 90, delay: float = 2.0):
        import redis as redis_lib
        last = None
        for i in range(attempts):
            try:
                client = redis_lib.Redis.from_url(self._redis_url, decode_responses=True)
                client.ping()
                self._client = client
                log.info("[latest-state] Redis ready (%s)", self._redis_url)
                return
            except Exception as exc:  # noqa: BLE001
                last = exc
                log.warning("[latest-state] redis connect failed (%s), retry %d/%d",
                            exc, i + 1, attempts)
                time.sleep(delay)
        raise RuntimeError(f"[latest-state] could not reach Redis at {self._redis_url}: {last}")

    def process(self, el):
        if self._client is None:
            self._connect()
        blob = json.dumps({
            "vehicle_id": el["vehicle_id"],
            "lat": float(el["lat"]),
            "lon": float(el["lon"]),
            "speed_kmh": float(el["speed_kmh"]),
            "engine_temp_c": float(el["engine_temp_c"]),
            "updated_at": el["event_time"].astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        key = f"{self.KEY_PREFIX}{el['vehicle_id']}"
        try:
            self._client.set(key, blob)
        except Exception as exc:
            log.warning("[latest-state] set failed: %s (key=%s)", exc, key)
            try:
                self._client = None
                self._connect(attempts=3, delay=1.0)
                self._client.set(key, blob)
            except Exception:
                pass


# ---- tumbling aggregator (manual; see note) -------------------------------
# NOTE on Beam + DirectRunner: stateful windowed CombinePerKey over an
# UNBOUNDED source does NOT emit panes under the Python DirectRunner in
# streaming mode (verified: neither AfterWatermark nor AfterProcessingTime
# triggers fire; beam.Flatten-driven downstream delivery into a single sink
# DoFn also stalled). The Flink/Dataflow runner handles windowing natively.
# For a self-contained local demo we compute the 1-minute tumbling aggregate
# in-process inside two DoFns, one each on the telemetry and events branches
# (the same per-element pattern proven by the raw writers). They update
# different columns of vehicle_window_aggregates for the same (vehicle, minute)
# key, so the row assembles from two idempotent UPDATEs. Each bucket flushes
# once its minute closes; late stragglers within ~75s update the same row.
_AGG_WINDOW_S = 60.0
_AGG_LATE_HOLD_S = 75.0

_CQL_UPSERT_SPEED = (
    f"UPDATE {KEYSPACE}.vehicle_window_aggregates "
    "SET avg_speed_kmh=?, max_speed_kmh=? WHERE vehicle_id=? AND window_start=?"
)
_CQL_UPSERT_HARSH = (
    f"UPDATE {KEYSPACE}.vehicle_window_aggregates "
    "SET harsh_event_count=? WHERE vehicle_id=? AND window_start=?"
)


class _BaseWindowAgg(beam.DoFn):
    """Shared 1-minute bucketing + Cassandra flush plumbing for the two
    per-vehicle aggregates."""

    BUCKET_S = _AGG_WINDOW_S
    LATE_HOLD_S = _AGG_LATE_HOLD_S

    def __init__(self, contact_points, cql, label):
        super().__init__()
        self._contact_points = [h.split(":")[0] for h in contact_points.split(",") if h]
        self._cql = cql
        self._label = label
        self._cluster = None
        self._session = None
        self._prepared = None
        self._buffers = {}   # (vehicle, bucket_epoch) -> buffer dict

    def setup(self):
        from cassandra.cluster import Cluster
        last = None
        for i in range(90):
            try:
                self._cluster = Cluster(contact_points=self._contact_points, port=9042)
                self._session = self._cluster.connect()
                self._prepared = self._session.prepare(self._cql)
                log.info("[%s] Cassandra prepared (%s)", self._label, self._cql[:60])
                return
            except Exception as exc:  # noqa: BLE001
                last = exc
                log.warning("[%s] cassandra prepare failed (%s), retry %d/90",
                            self._label, exc, i + 1)
                try:
                    if self._cluster is not None:
                        self._cluster.shutdown()
                except Exception:
                    pass
                self._cluster = None
                time.sleep(2.0)
        raise RuntimeError(f"[{self._label}] could not prepare Cassandra statement: {last}")

    def _new_buffer(self):  # pragma: no cover - overridden
        raise NotImplementedError

    def _accumulate(self, buf, item):  # pragma: no cover - overridden
        raise NotImplementedError

    def _flush_params(self, buf, vehicle, bucket_epoch):
        return self._flush_params_impl(buf, vehicle, bucket_epoch)  # overridden

    def _flush_closed(self, now):
        for key in list(self._buffers.keys()):
            vehicle, bucket = key
            bucket_end = bucket + self.BUCKET_S
            buf = self._buffers[key]
            # Delete buckets past their late-hold window that have no pending
            # changes. This is checked BEFORE the flush below (and preserves a
            # still-arriving burst for a historical bucket) so the bucket keeps
            # accumulating its late/replayed elements instead of being dropped
            # after its first element.
            if bucket_end + self.LATE_HOLD_S < now and not buf.get("dirty"):
                del self._buffers[key]
                continue
            if bucket_end < now and buf.get("dirty"):
                params = self._flush_params(buf, vehicle, bucket)
                try:
                    self._session.execute(self._prepared, params)
                except Exception as exc:
                    log.warning("[%s] execute failed: %s (params=%r)",
                                self._label, exc, params)
                buf["dirty"] = False

    def process(self, item, window=beam.DoFn.WindowParam):
        if self._session is None:
            self.setup()
        vehicle = item["vehicle_id"]
        ts = item["event_time"]  # tz-aware datetime
        now = time.time()
        bucket = int(ts.timestamp() // self.BUCKET_S) * int(self.BUCKET_S)
        # Explicit late-data handling (spec §6): elements arriving after the
        # bucket's minute has closed AND its lateness hold has expired are
        # dropped LOUDLY instead of silently updating stale rows.
        if bucket + self.BUCKET_S + self.LATE_HOLD_S < now:
            log.warning("[late-data] %s DROPPED %s for window closed at %s "
                        "(arrived %.0fs late, hold=%ds)",
                        self._label, vehicle,
                        datetime.fromtimestamp(bucket + self.BUCKET_S, tz=timezone.utc).strftime("%H:%M:%S"),
                        now - (bucket + self.BUCKET_S), int(self.LATE_HOLD_S))
            return
        key = (vehicle, bucket)
        buf = self._buffers.get(key)
        if buf is None:
            buf = self._buffers[key] = self._new_buffer()
        self._accumulate(buf, item)
        self._flush_closed(now)

    def teardown(self):
        try:
            if self._cluster is not None:
                self._cluster.shutdown()
        except Exception:
            pass


class SpeedWindowAggDoFn(_BaseWindowAgg):
    """Telemetry -> avg/max speed per (vehicle, minute)."""

    def __init__(self, contact_points):
        super().__init__(contact_points, _CQL_UPSERT_SPEED, "agg-speed")

    def _new_buffer(self):
        return {"sum": 0.0, "cnt": 0, "mx": float("-inf"), "dirty": True}

    def _accumulate(self, buf, item):
        spd = float(item["speed_kmh"])
        buf["sum"] += spd
        buf["cnt"] += 1
        if spd > buf["mx"]:
            buf["mx"] = spd
        buf["dirty"] = True

    def _flush_params(self, buf, vehicle, bucket_epoch):
        avg = (buf["sum"] / buf["cnt"]) if buf["cnt"] else 0.0
        ws = datetime.fromtimestamp(bucket_epoch, tz=timezone.utc)
        log.info("[agg-speed] flush %s @%s avg=%.1f max=%.1f",
                 vehicle, ws.strftime("%H:%M:%S"), avg, buf["mx"])
        return (float(avg), float(buf["mx"]), vehicle, ws)


class HarshWindowAggDoFn(_BaseWindowAgg):
    """Events -> harsh-event count per (vehicle, minute)."""

    def __init__(self, contact_points):
        super().__init__(contact_points, _CQL_UPSERT_HARSH, "agg-harsh")

    def _new_buffer(self):
        return {"harsh": 0, "dirty": True}

    def _accumulate(self, buf, item):
        if item["event_type"] in HARSH_EVENT_TYPES:
            buf["harsh"] += 1
            buf["dirty"] = True

    def _flush_params(self, buf, vehicle, bucket_epoch):
        ws = datetime.fromtimestamp(bucket_epoch, tz=timezone.utc)
        log.info("[agg-harsh] flush %s @%s harsh=%d",
                 vehicle, ws.strftime("%H:%M:%S"), buf["harsh"])
        return (int(buf["harsh"]), vehicle, ws)


# ---- sliding fleet windows + session windows (spec §6) --------------------
# Same DirectRunner constraint as the tumbling aggregates above: no native
# windowed pane firing for unbounded sources in Python streaming, so the
# 5-min fleet window (sliding every 30s) and the 5-min-gap trip sessions are
# computed in-process. The two sliding writers UPDATE different columns of
# the same fleet_window_aggregates row (day_bucket, window_start) following
# the idempotent partial-UPDATE pattern used by the vehicle aggregates.
_SLIDE_S = 30
_SLIDING_WINDOW_S = 300
_SESSION_GAP_S = 300

CQL_FLEET_SPEED = (
    f"UPDATE {KEYSPACE}.fleet_window_aggregates "
    "SET vehicles_active=?, fleet_avg_speed_kmh=? WHERE day_bucket=? AND window_start=?"
)
CQL_FLEET_HARSH = (
    f"UPDATE {KEYSPACE}.fleet_window_aggregates "
    "SET total_harsh_events=? WHERE day_bucket=? AND window_start=?"
)


class _CassandraSink(beam.DoFn):
    """Minimal Cassandra plumbing (lazy connect + retry) shared by the new
    sliding-window and session writers."""

    def __init__(self, contact_points, cql, label):
        super().__init__()
        self._contact_points = [h.split(":")[0] for h in contact_points.split(",") if h]
        self._cql = cql
        self._label = label
        self._cluster = None
        self._session = None
        self._prepared = None

    def _ensure_session(self, attempts: int = 90, delay: float = 2.0):
        if self._session is not None:
            return
        from cassandra.cluster import Cluster
        last = None
        for i in range(attempts):
            try:
                self._cluster = Cluster(contact_points=self._contact_points, port=9042)
                self._session = self._cluster.connect()
                self._prepared = self._session.prepare(self._cql)
                log.info("[%s] Cassandra prepared (%s)", self._label, self._cql[:60])
                return
            except Exception as exc:  # noqa: BLE001
                last = exc
                log.warning("[%s] cassandra prepare failed (%s), retry %d/%d",
                            self._label, exc, i + 1, attempts)
                try:
                    if self._cluster is not None:
                        self._cluster.shutdown()
                except Exception:
                    pass
                self._cluster = None
                self._session = None
                time.sleep(delay)
        raise RuntimeError(f"[{self._label}] could not prepare Cassandra statement: {last}")

    def _execute(self, params):
        try:
            self._session.execute(self._prepared, params)
        except Exception as exc:
            log.warning("[%s] execute failed: %s (params=%r)", self._label, exc, params)
            try:
                self._session = None
                self._ensure_session(attempts=3, delay=1.0)
                self._session.execute(self._prepared, params)
            except Exception:
                pass

    def teardown(self):
        try:
            if self._cluster is not None:
                self._cluster.shutdown()
        except Exception:
            pass


def _fleet_row_params(window_epoch: int) -> tuple:
    ws = datetime.fromtimestamp(window_epoch, tz=timezone.utc)
    return (ws.date(), ws)  # day_bucket, window_start


class FleetSlidingSpeedDoFn(_CassandraSink):
    """Telemetry -> 5-min fleet rolling avg speed + active vehicle count,
    re-emitted every 30s into fleet_window_aggregates."""

    def __init__(self, contact_points):
        super().__init__(contact_points, CQL_FLEET_SPEED, "fleet-slide-speed")
        self._samples = {}   # vehicle_id -> deque[(event_ts, speed)]
        self._last_slot = None

    def process(self, el):
        self._ensure_session()
        v = el["vehicle_id"]
        t = el["event_time"].timestamp()
        self._samples.setdefault(v, deque()).append((t, float(el["speed_kmh"])))
        slot = int(time.time()) // _SLIDE_S
        if slot == self._last_slot:
            return
        self._last_slot = slot
        window_end = slot * _SLIDE_S          # aligned to the 30s slide grid
        cutoff = window_end - _SLIDING_WINDOW_S
        speeds, active = [], 0
        for vid, dq in list(self._samples.items()):
            while dq and dq[0][0] < cutoff:   # prune samples outside the window
                dq.popleft()
            if not dq:
                del self._samples[vid]
                continue
            active += 1
            speeds.extend(s for _, s in dq)
        avg = sum(speeds) / len(speeds) if speeds else 0.0
        log.info("[fleet-slide-speed] @%s vehicles=%d avg=%.1f km/h (%d samples)",
                 datetime.fromtimestamp(window_end, tz=timezone.utc).strftime("%H:%M:%S"),
                 active, avg, len(speeds))
        self._execute((active, float(avg)) + _fleet_row_params(window_end))


class FleetSlidingHarshDoFn(_CassandraSink):
    """Events -> harsh-event count in the trailing 5-min fleet window,
    re-emitted every 30s (same slide grid as the speed writer)."""

    def __init__(self, contact_points):
        super().__init__(contact_points, CQL_FLEET_HARSH, "fleet-slide-harsh")
        self._events = deque()               # event_ts of harsh events
        self._last_slot = None

    def process(self, el):
        self._ensure_session()
        if el["event_type"] not in HARSH_EVENT_TYPES:
            return
        self._events.append(el["event_time"].timestamp())
        slot = int(time.time()) // _SLIDE_S
        if slot == self._last_slot:
            return
        self._last_slot = slot
        window_end = slot * _SLIDE_S
        cutoff = window_end - _SLIDING_WINDOW_S
        while self._events and self._events[0] < cutoff:
            self._events.popleft()
        log.info("[fleet-slide-harsh] @%s total=%d",
                 datetime.fromtimestamp(window_end, tz=timezone.utc).strftime("%H:%M:%S"),
                 len(self._events))
        self._execute((len(self._events),) + _fleet_row_params(window_end))


def _haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


class TripSessionDoFn(beam.DoFn):
    """Session windows (spec §6): groups a vehicle's telemetry into discrete
    trips. A session closes on a >=5 min stream gap OR an ignition_off
    transition; closure logs duration/distance/max speed so the demo shows
    trip detection explicitly."""

    GAP_S = _SESSION_GAP_S

    def __init__(self):
        super().__init__()
        self._sessions = {}   # vehicle -> {"start","last","dist","max","n","prev_ign"}

    def process(self, el):
        v = el["vehicle_id"]
        t = el["event_time"].timestamp()
        spd = float(el["speed_kmh"])
        ign = bool(el["ignition_on"])
        s = self._sessions.get(v)

        if s is not None and (t - s["last"] > self.GAP_S or not s["prev_ign"]):
            self._close(v, s)
            s = None
        if s is None:
            self._sessions[v] = s = {"start": t, "last": t, "dist": 0.0,
                                     "max": spd, "n": 0, "lat": None, "lon": None,
                                     "prev_ign": ign}
            log.info("[sessions] %s TRIP OPENED at %s (ignition=%s)",
                     v, el["event_time"].strftime("%H:%M:%S"), ign)

        if s["lat"] is not None:
            s["dist"] += _haversine_km(s["lat"], s["lon"], float(el["lat"]), float(el["lon"]))
        s["lat"], s["lon"] = float(el["lat"]), float(el["lon"])
        s["max"] = max(s["max"], spd)
        s["last"] = t
        s["n"] += 1
        s["prev_ign"] = ign
        if not ign:
            self._close(v, s)
            del self._sessions[v]

    def _close(self, v, s):
        dur_min = (s["last"] - s["start"]) / 60.0
        log.info("[sessions] %s TRIP CLOSED dur=%.1fmin dist=%.2fkm max_speed=%.1fkph samples=%d",
                 v, dur_min, s["dist"], s["max"], s["n"])


# ---- pipeline -------------------------------------------------------------
def build_pipeline(root):
    # Telemetry branch (SDF assigns each message's event_time as element timestamp)
    telemetry_raw = (
        root
        | "seed-telemetry" >> beam.Create([None])
        | "consume-telemetry" >> beam.ParDo(KafkaConsumeSDF(BOOTSTRAP, TELEMETRY_TOPIC, GROUP_ID))
        | "ts-telemetry" >> beam.Map(_ts_telemetry)
    )

    telemetry_raw | "write-raw-telemetry" >> beam.ParDo(
        CassandraWriteDoFn(CONTACT_POINTS, CQL_INSERT_TELEMETRY, _telemetry_params, "raw-telemetry"))

    # Redis latest-state blob per vehicle (spec §4.6) — overwritten every tick.
    telemetry_raw | "write-latest-state" >> beam.ParDo(RedisLatestStateDoFn(REDIS_URL))

    # Events branch
    events_raw = (
        root
        | "seed-events" >> beam.Create([None])
        | "consume-events" >> beam.ParDo(KafkaConsumeSDF(BOOTSTRAP, EVENTS_TOPIC, GROUP_ID))
        | "ts-events" >> beam.Map(_ts_event)
    )

    events_raw | "write-raw-events" >> beam.ParDo(
        CassandraWriteDoFn(CONTACT_POINTS, CQL_INSERT_EVENT, _event_params, "raw-events"))

    # 1-minute tumbling aggregate per vehicle. NOTE: Beam Python DirectRunner in
    # streaming does not fire stateful windowed CombinePerKey panes for unbounded
    # sources (verified), and beam.Flatten into a single sink DoFn stalled too,
    # so the aggregation is computed in-process by two DoFns each on its own
    # branch (the same per-element pattern the raw writers use). They UPDATE
    # different columns of vehicle_window_aggregates for the same
    # (vehicle, minute) key. The Flink/Dataflow runner would enable native Beam
    # windowing (spec §10 stretch).
    telemetry_raw | "agg-speed" >> beam.ParDo(SpeedWindowAggDoFn(CONTACT_POINTS))
    events_raw | "agg-harsh" >> beam.ParDo(HarshWindowAggDoFn(CONTACT_POINTS))

    # Fleet sliding window (5 min, sliding every 30s) -> fleet_window_aggregates.
    telemetry_raw | "fleet-slide-speed" >> beam.ParDo(FleetSlidingSpeedDoFn(CONTACT_POINTS))
    events_raw | "fleet-slide-harsh" >> beam.ParDo(FleetSlidingHarshDoFn(CONTACT_POINTS))

    # Session windows: trip detection from 5-min gaps / ignition transitions (logged).
    telemetry_raw | "trip-sessions" >> beam.ParDo(TripSessionDoFn())


def main():
    options = PipelineOptions(["--streaming"])
    with beam.Pipeline(options=options) as p:
        build_pipeline(p)
    # The pipeline runs until cancelled (SIGINT). The SDF keeps polling Kafka
    # indefinitely via defer_remainder.


if __name__ == "__main__":
    main()