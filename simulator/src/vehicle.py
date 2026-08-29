"""Per-vehicle asyncio simulator — the integration glue over physics,
events, state, and output.

One ``VehicleSimulator`` instance owns one vehicle's tick loop. Each tick
either advances a trip (driving/stopped) or dwells in idle, emitting one
telemetry row and zero-or-more events to the sink.
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
import uuid
from dataclasses import dataclass
from datetime import datetime

import config
from events import detect_tick_conditions, make_event, pick_dtc_code
from models import (
    DrivingEvent,
    EngineType,
    EventType,
    Route,
    Telemetry,
    Trip,
    Vehicle,
    VehicleState,
    utcnow,
)
from output import KafkaSink, StdoutSink
from physics import (
    bearing_deg,
    compute_fuel_rate_lps,
    compute_gear,
    compute_intake_air_temp,
    compute_maf,
    compute_rpm,
    compute_throttle,
    interpolate_point,
    haversine_m,
    update_coolant_temp,
    update_engine_temp,
)
from routes import routes_for_city
from state import StateStore

log = logging.getLogger("simulator")


# ---------------------------------------------------------------------------
# Planned trip segment — pre-computed at trip start
# ---------------------------------------------------------------------------

@dataclass
class PlannedSegment:
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
    distance_m: float
    target_speed_kmh: float  # already × traffic-factor at plan time
    stop_at_end: bool        # traffic-light stop scheduled here
    stop_dwell_s: float     # dwell when stopped (0.0 if not stopping)


# ---------------------------------------------------------------------------
# Vehicle simulator
# ---------------------------------------------------------------------------

class VehicleSimulator:
    """One vehicle's tick loop."""

    def __init__(
        self,
        vehicle: Vehicle,
        store: StateStore,
        sink: StdoutSink | KafkaSink,
    ) -> None:
        self.vehicle: Vehicle = vehicle
        self.store: StateStore = store
        self.sink: StdoutSink | KafkaSink = sink

        kin = config.PERSONA_KINEMATICS[vehicle.persona]
        self.accel_max_mps2: float = kin["accel_max_mps2"]
        self.brake_max_mps2: float = kin["brake_max_mps2"]
        self.jitter_pct: float = kin["jitter_pct"]
        self.traffic_light_p: float = kin["traffic_light_p"]
        self.congestion_p: float = kin["congestion_p"]
        self.tank_capacity_L: float = config.TANK_CAPACITY_L[vehicle.vehicle_id]
        self.ambient_c: float = config.CITY_AMBIENT_C.get(
            vehicle.home_city, config.AMBIENT_TEMP_C
        )

        # Persistent state, loaded from SQLite (or defaults if first boot)
        self.lat: float = 0.0
        self.lon: float = 0.0
        self.odometer_km: float = 0.0
        self.fuel_pct: float = 100.0
        self.fuel_consumed_l: float = 0.0
        self.current_trip_id: str | None = None
        self.ignition_on: bool = False
        self.seconds_since_ignition: float = 0.0

        # ECU filter state (lives only in memory; engine-cools when parked)
        self.rpm: int = 800
        self.engine_temp_c: float = self.ambient_c
        self.coolant_temp_c: float = self.ambient_c

        # Per-trip state (re-initialized on each trip start)
        self.trip: Trip | None = None
        self.trip_start_odometer_km: float = 0.0
        self.plan: list[PlannedSegment] = []
        self.segment_idx: int = 0
        self.distance_into_segment_m: float = 0.0
        self.target_speed_kmh: float = 0.0  # current tick's chosen target
        self.stopped_at_end: bool = False
        self.stopped_remaining_dwell_s: float = 0.0
        self.idle_seconds: float = 0.0
        self.over_speed_sustained_s: float = 0.0
        self.congestion_active: bool = False
        self.congestion_remaining_s: float = 0.0
        self.dtc_pending: bool = False
        self.dtc_fire_seconds: float = 0.0
        self.last_position_lat: float | None = None  # for heading calc
        self.last_position_lon: float | None = None
        self.last_heading_deg: float = 0.0
        self.last_speed_mps: float = 0.0
        self.prev_active: set[EventType] = set()
        self.fuel_low_emitted: bool = False

        # Idle dwell after a trip
        self.idle_dwell_s: float = 0.0

        # Bookkeeping
        self._tick_count: int = 0

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    def _bootstrap(self) -> None:
        """Seed initial position from a home-city first-route first waypoint."""
        city_routes = routes_for_city(self.vehicle.home_city)
        if not city_routes:
            raise RuntimeError(f"no routes for city {self.vehicle.home_city}")
        start_route = random.choice(city_routes)
        first_node = start_route.nodes[0]
        self.lat = first_node.lat
        self.lon = first_node.lon
        self.fuel_pct = 100.0
        self.fuel_consumed_l = 0.0
        self.odometer_km = 0.0

    def load_or_bootstrap(self) -> None:
        row = self.store.load(self.vehicle.vehicle_id)
        if row is None:
            self._bootstrap()
            log.info(
                "bootstrapped %s at (%.5f, %.5f) fuel=%.1f%%",
                self.vehicle.vehicle_id, self.lat, self.lon, self.fuel_pct,
            )
            return
        self.lat = row["lat"]
        self.lon = row["lon"]
        self.odometer_km = row["odometer_km"]
        self.fuel_pct = row["fuel_pct"]
        self.fuel_consumed_l = row["fuel_consumed_l"]
        self.current_trip_id = row["current_trip_id"]
        self.ignition_on = row["ignition_on"]
        self.seconds_since_ignition = row["seconds_since_ignition"]
        self.engine_temp_c = max(self.ambient_c, self.ambient_c + 5.0)
        self.coolant_temp_c = max(self.ambient_c, self.ambient_c + 5.0)
        log.info(
            "restored %s at (%.5f, %.5f) odo=%.2f fuel=%.1f%% ign=%s",
            self.vehicle.vehicle_id, self.lat, self.lon,
            self.odometer_km, self.fuel_pct, self.ignition_on,
        )

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        self.load_or_bootstrap()
        log.info("%s starting tick loop", self.vehicle.vehicle_id)
        try:
            while True:
                self.tick()
                self._tick_count += 1
                if self._tick_count % 5 == 0:
                    self.persist()
                await asyncio.sleep(config.TICK_SECONDS)
        except asyncio.CancelledError:
            log.info("%s cancelled, persisting state", self.vehicle.vehicle_id)
            self.persist()
            raise

    def tick(self) -> None:
        ts = utcnow()
        if not self.ignition_on:
            self._tick_idle(ts)
        else:
            self._tick_driving(ts)

    # ------------------------------------------------------------------
    # Idle state tick — engine off, parked, possibly planning a new trip
    # ------------------------------------------------------------------

    def _tick_idle(self, ts: datetime) -> None:
        # Cool engine slowly toward ambient
        self.engine_temp_c += (self.ambient_c - self.engine_temp_c) * 0.001
        self.coolant_temp_c += (self.ambient_c - self.coolant_temp_c) * 0.002
        self.rpm = 800
        self.idle_dwell_s += config.TICK_SECONDS

        # Telemetry still emitted at 1 Hz so consumers see "alive" signal
        tel = Telemetry(
            vehicle_id=self.vehicle.vehicle_id,
            trip_id=None,
            ts=ts,
            lat=self.lat,
            lon=self.lon,
            heading_deg=self.last_heading_deg,
            speed_kmh=0.0,
            gear=1,
            rpm=self.rpm,
            throttle_pct=0.0,
            maf_g_per_s=0.0,
            engine_temp_c=self.engine_temp_c,
            coolant_temp_c=self.coolant_temp_c,
            intake_air_temp_c=compute_intake_air_temp(self.ambient_c, self.rpm),
            fuel_pct=self.fuel_pct,
            fuel_consumed_l=self.fuel_consumed_l,
            odometer_km=self.odometer_km,
            ignition_on=False,
        )
        self.sink.emit_telemetry(tel)

        # Decide whether to start a new trip
        min_dwell = config.IDLE_DWELL_MIN_S
        if self.idle_dwell_s >= min_dwell and random.random() < config.TRIP_START_P:
            self._start_trip(ts)

    # ------------------------------------------------------------------
    # Trip planning — pre-compute segments at trip start
    # ------------------------------------------------------------------

    def _traffic_factor_for_wib(self, ts: datetime) -> float:
        """Time-of-day traffic factor (spec §11); ts is UTC, convert to WIB."""
        wib_hour = (ts.hour + 7) % 24
        for start, end in config.RUSH_HOURS:
            if start <= wib_hour < end:
                return config.RUSH_FACTOR
        return config.NON_RUSH_FACTOR

    def _plan_route(self, route: Route, traffic_factor: float) -> list[PlannedSegment]:
        plan: list[PlannedSegment] = []
        nodes = route.nodes
        # Roll congestion state for the whole trip
        trip_congestion = random.random() < self.congestion_p
        for i in range(len(nodes) - 1):
            a, b = nodes[i], nodes[i + 1]
            dist_m = haversine_m(a.lat, a.lon, b.lat, b.lon)
            target = a.speed_limit_kmh * traffic_factor
            if trip_congestion and i % 2 == 1:
                target *= 0.55  # one or two congestion pockets
            target = max(8.0, target)
            jitter = 1.0 + random.uniform(-self.jitter_pct, self.jitter_pct)
            target = max(8.0, target * jitter)
            stop_at_end = (
                i < len(nodes) - 2 and b.road_class.value != "toll"
                and random.random() < self.traffic_light_p
            )
            dwell = random.uniform(20.0, 60.0) if stop_at_end else 0.0
            plan.append(PlannedSegment(
                start_lat=a.lat, start_lon=a.lon,
                end_lat=b.lat, end_lon=b.lon,
                distance_m=max(1.0, dist_m),
                target_speed_kmh=round(target, 2),
                stop_at_end=stop_at_end,
                stop_dwell_s=dwell,
            ))
        return plan

    def _start_trip(self, ts: datetime) -> None:
        city_routes = routes_for_city(self.vehicle.home_city)
        route = random.choice(city_routes)
        traffic = self._traffic_factor_for_wib(ts)
        self.plan = self._plan_route(route, traffic)
        self.segment_idx = 0
        self.distance_into_segment_m = 0.0
        self.stopped_at_end = False
        self.idle_seconds = 0.0
        self.over_speed_sustained_s = 0.0
        self.seconds_since_ignition = 0.0
        self.congestion_active = False
        self.fuel_low_emitted = False
        self.prev_active = set()
        self.ignition_on = True

        trip_id = uuid.uuid4()
        self.current_trip_id = str(trip_id)
        self.trip_start_odometer_km = self.odometer_km
        self.trip = Trip(
            trip_id=trip_id,
            vehicle_id=self.vehicle.vehicle_id,
            route_id=route.route_id,
            start_ts=ts,
            state=VehicleState.DRIVING,
            start_lat=self.lat,
            start_lon=self.lon,
        )

        # Schedule DTC (rare)
        kn = self.vehicle.vehicle_id
        if random.random() < config.DTC_TRIP_RATE:
            self.dtc_pending = True
            self.dtc_fire_seconds = random.uniform(30.0, 240.0)
        else:
            self.dtc_pending = False
            self.dtc_fire_seconds = 0.0

        log.info(
            "%s trip %s started on %s (%d segments, traffic=%.2f)",
            self.vehicle.vehicle_id, str(trip_id)[:8], route.route_id,
            len(self.plan), traffic,
        )
        # Emit ignition_on event
        ev = make_event(
            vehicle_id=self.vehicle.vehicle_id,
            trip_id=self.current_trip_id,
            ts=ts,
            event_type=EventType.IGNITION_ON,
            detail=f"route={route.route_id}",
        )
        self.sink.emit_event(ev)

    # ------------------------------------------------------------------
    # Driving tick
    # ------------------------------------------------------------------

    def _tick_driving(self, ts: datetime) -> None:
        if not self.plan:
            log.error("%s has no plan; ending trip", self.vehicle.vehicle_id)
            self._end_trip(ts)
            return

        dt = config.TICK_SECONDS
        seg = self.plan[self.segment_idx]
        is_last = self.segment_idx >= len(self.plan) - 1

        # --- decide target speed ---
        target = seg.target_speed_kmh

        if self.stopped_at_end:
            target = 0.0
            self.stopped_remaining_dwell_s -= dt
            if self.stopped_remaining_dwell_s <= 0:
                self.stopped_at_end = False
        else:
            frac = (
                self.distance_into_segment_m / seg.distance_m
                if seg.distance_m > 0 else 1.0
            )
            # Anticipate next node (last 15% of segment): ramp down to ~20
            if not is_last and frac > 0.85:
                ramp_factor = (frac - 0.85) / 0.15
                floor = 15.0
                target = min(target, max(floor, seg.target_speed_kmh * (1.0 - 0.6 * ramp_factor)))
            # Approach traffic-light stop: be near 0 if very close to end
            if (not is_last and seg.stop_at_end
                and (seg.distance_m - self.distance_into_segment_m) < 40.0):
                target = min(target, 8.0)
            # Last segment: prepare to stop at end
            if is_last and frac > 0.92:
                target = min(target, 10.0)

        # --- kinematic update ---
        speed_mps = self.last_speed_mps
        target_mps = target / 3.6
        # Required accel to hit target this tick — drives comfort vs cap logic.
        desired_accel_mps2 = (target_mps - speed_mps) / dt if dt > 0 else 0.0
        if desired_accel_mps2 > 0.1:
            # Accelerating — usually comfortable, occasionally aggressive.
            aggressive_roll = random.random()
            if aggressive_roll < 0.04:
                cap = self.accel_max_mps2
            else:
                cap = self.accel_max_mps2 * 0.5 * random.uniform(0.7, 1.2)
            accel = min(desired_accel_mps2, cap)
            accel = max(0.0, accel)
        elif desired_accel_mps2 < -0.1:
            # Decelerating — usually gentle, occasionally hard.
            aggressive_roll = random.random()
            if aggressive_roll < 0.04:
                cap = self.brake_max_mps2
            else:
                cap = self.brake_max_mps2 * 0.45 * random.uniform(0.7, 1.2)
            accel = -min(abs(desired_accel_mps2), cap)
            accel = min(0.0, accel)
        else:
            accel = 0.0
        new_speed_mps = max(0.0, speed_mps + accel * dt)
        if abs(new_speed_mps - target_mps) < 0.3:
            new_speed_mps = target_mps
        new_speed_kmh = new_speed_mps * 3.6
        accel_mps2 = (new_speed_mps - speed_mps) / dt if dt > 0 else 0.0

        # --- if stopping at end and we've come to a halt near it, mark dwell ---
        if (not is_last and seg.stop_at_end and not self.stopped_at_end
            and new_speed_mps < 0.5
            and (seg.distance_m - self.distance_into_segment_m) < 25.0):
            self.stopped_at_end = True
            self.stopped_remaining_dwell_s = seg.stop_dwell_s

        # --- advance along polyline ---
        if new_speed_mps > 0.01:
            advance_m = new_speed_mps * dt
            self.distance_into_segment_m += advance_m
            while (
                not is_last
                and self.distance_into_segment_m >= self.plan[self.segment_idx].distance_m
            ):
                self.distance_into_segment_m -= self.plan[self.segment_idx].distance_m
                self.segment_idx += 1
                if self.segment_idx >= len(self.plan) - 1:
                    is_last = True
                seg = self.plan[self.segment_idx]
                # reset traffic-light state across segment boundary
                self.stopped_at_end = False
                self.stopped_remaining_dwell_s = 0.0

        seg = self.plan[self.segment_idx]
        is_last = self.segment_idx >= len(self.plan) - 1
        seg_complete = (
            is_last and self.distance_into_segment_m >= seg.distance_m
        )
        if seg_complete:
            self.distance_into_segment_m = seg.distance_m

        # new position
        frac_on_seg = (
            self.distance_into_segment_m / seg.distance_m if seg.distance_m else 0.0
        )
        new_lat, new_lon = interpolate_point(
            seg.start_lat, seg.start_lon, seg.end_lat, seg.end_lon, frac_on_seg,
        )
        # Snap to end node if segment complete — guards against interp drift
        if seg_complete:
            new_lat, new_lon = seg.end_lat, seg.end_lon

        # heading
        if (self.last_position_lat is not None and new_speed_mps > 0.6):
            heading = bearing_deg(
                self.last_position_lat, self.last_position_lon, new_lat, new_lon,
            )
        else:
            heading = self.last_heading_deg
        heading_delta_deg: float | None = None
        if new_speed_mps > 0.6:
            hd = ((heading - self.last_heading_deg + 180) % 360) - 180
            heading_delta_deg = hd

        # --- ECU PIDs derived from motion ---
        gear = compute_gear(new_speed_kmh)
        rpm = compute_rpm(new_speed_kmh, gear, self.rpm, idle_rpm=800)
        throttle = compute_throttle(
            new_speed_kmh, target, max(0.0, accel_mps2), self.accel_max_mps2,
        )
        maf = compute_maf(rpm, throttle, self.vehicle.displacement_L)
        self.engine_temp_c = update_engine_temp(
            self.engine_temp_c, rpm, self.seconds_since_ignition, self.ambient_c,
        )
        self.coolant_temp_c = update_coolant_temp(
            self.coolant_temp_c, rpm, self.seconds_since_ignition, self.ambient_c,
        )
        intake_air = compute_intake_air_temp(self.ambient_c, rpm)
        fuel_rate_lps = compute_fuel_rate_lps(
            maf, rpm, self.vehicle.fuel_type, idle_rpm=800,
        )
        fuel_burned = fuel_rate_lps * dt
        new_fuel_pct = max(0.0, self.fuel_pct - (fuel_burned / self.tank_capacity_L) * 100.0)
        new_fuel_consumed_l = self.fuel_consumed_l + fuel_burned
        distance_km_this_tick = new_speed_mps * dt / 1000.0
        new_odometer_km = self.odometer_km + distance_km_this_tick

        # Update engine-on idle seconds (idle = engine on, speed near 0)
        if new_speed_mps < 0.5:
            self.idle_seconds += dt
        else:
            self.idle_seconds = 0.0

        # speed limit for this segment (used for over_speed event detection)
        speed_limit = seg.target_speed_kmh

        # Persisted state updates
        self.lat = new_lat
        self.lon = new_lon
        self.odometer_km = new_odometer_km
        self.fuel_pct = new_fuel_pct
        self.fuel_consumed_l = new_fuel_consumed_l
        self.rpm = rpm
        self.last_speed_mps = new_speed_mps
        self.last_position_lat = new_lat
        self.last_position_lon = new_lon
        self.last_heading_deg = heading
        self.seconds_since_ignition += dt

        # --- telemetry emit ---
        tel = Telemetry(
            vehicle_id=self.vehicle.vehicle_id,
            trip_id=self.current_trip_id,
            ts=ts,
            lat=new_lat,
            lon=new_lon,
            heading_deg=heading,
            speed_kmh=new_speed_kmh,
            gear=gear,
            rpm=rpm,
            throttle_pct=throttle,
            maf_g_per_s=maf,
            engine_temp_c=self.engine_temp_c,
            coolant_temp_c=self.coolant_temp_c,
            intake_air_temp_c=intake_air,
            fuel_pct=self.fuel_pct,
            fuel_consumed_l=self.fuel_consumed_l,
            odometer_km=self.odometer_km,
            ignition_on=True,
        )
        self.sink.emit_telemetry(tel)

        # Over-speed sustained tracking
        if new_speed_kmh > speed_limit + 10.0:
            self.over_speed_sustained_s += dt
        else:
            self.over_speed_sustained_s = 0.0

        # DTC firing
        dtc_fire = False
        if self.dtc_pending and self.seconds_since_ignition >= self.dtc_fire_seconds:
            dtc_fire = True
            self.dtc_pending = False

        # --- event detection ---
        conditions = detect_tick_conditions(
            accel_mps2=accel_mps2,
            speed_kmh=new_speed_kmh,
            heading_delta_deg=heading_delta_deg,
            route_speed_limit_kmh=speed_limit,
            idle_seconds=self.idle_seconds,
            fuel_pct=self.fuel_pct,
            over_speed_sustained_s=self.over_speed_sustained_s,
            dtc_fire=dtc_fire,
            fuel_low_active=self.fuel_low_emitted,
        )

        # Edge-triggered emission: only fire for conditions newly active
        newly_active = conditions - self.prev_active
        declined = self.prev_active - conditions
        for et in newly_active:
            detail = ""
            if et is EventType.DTC_CODE:
                code, desc = pick_dtc_code(self.vehicle.engine_type)
                detail = f"{code} {desc}"
            elif et is EventType.IGNITION_ON:
                continue
            ev = make_event(
                vehicle_id=self.vehicle.vehicle_id,
                trip_id=self.current_trip_id,
                ts=ts,
                event_type=et,
                detail=detail,
            )
            self.sink.emit_event(ev)
            if et is EventType.FUEL_LOW:
                self.fuel_low_emitted = True
        # Reset fuel_low if refueled (rare in-flight; usually between trips)
        if EventType.FUEL_LOW in declined and self.fuel_pct >= 15.0:
            self.fuel_low_emitted = False
        self.prev_active = conditions

        # --- trip completion ---
        if seg_complete:
            self._end_trip(ts)

    def _end_trip(self, ts: datetime) -> None:
        if self.trip is not None:
            self.trip.end_ts = ts
            self.trip.end_lat = self.lat
            self.trip.end_lon = self.lon
            self.trip.state = VehicleState.IDLE
            self.trip.distance_km = round(
                self.odometer_km - self.trip_start_odometer_km, 4
            )
        log.info(
            "%s trip %s ended at (%.5f, %.5f) after %.1fs",
            self.vehicle.vehicle_id,
            (self.current_trip_id or "")[:8],
            self.lat, self.lon, self.seconds_since_ignition,
        )
        ev = make_event(
            vehicle_id=self.vehicle.vehicle_id,
            trip_id=self.current_trip_id,
            ts=ts,
            event_type=EventType.IGNITION_OFF,
            detail="trip_complete",
        )
        self.sink.emit_event(ev)

        self.ignition_on = False
        self.current_trip_id = None
        self.seconds_since_ignition = 0.0
        self.idle_dwell_s = 0.0
        self.idle_seconds = 0.0
        # Refuel between trips (full tank) — only if fuel low; otherwise stays
        if self.fuel_pct < 30.0:
            self.fuel_pct = 100.0
            log.info("%s refuelled to 100%%", self.vehicle.vehicle_id)
        self.persist()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def persist(self) -> None:
        self.store.save(
            vehicle_id=self.vehicle.vehicle_id,
            lat=self.lat,
            lon=self.lon,
            odometer_km=self.odometer_km,
            fuel_pct=self.fuel_pct,
            fuel_consumed_l=self.fuel_consumed_l,
            current_trip_id=self.current_trip_id,
            ignition_on=self.ignition_on,
            seconds_since_ignition=self.seconds_since_ignition,
            last_updated=utcnow().isoformat(),
        )