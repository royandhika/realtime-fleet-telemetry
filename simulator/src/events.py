from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import datetime

from config import DTC_CATALOG
from models import DrivingEvent, EngineType, EventType


@dataclass(frozen=True)
class EventThresholds:
    harsh_accel_mps2: float = 2.5
    harsh_brake_mps2: float = -2.5
    over_speed_margin_kmh: float = 10.0
    over_speed_sustained_s: float = 3.0
    harsh_corner_speed_kmh: float = 40.0
    harsh_corner_heading_deg: float = 25.0
    idling_threshold_s: float = 120.0
    fuel_low_pct: float = 10.0


THRESHOLDS = EventThresholds()


def detect_tick_conditions(
    *,
    accel_mps2: float,
    speed_kmh: float,
    heading_delta_deg: float | None,
    route_speed_limit_kmh: int | None,
    idle_seconds: float,
    fuel_pct: float,
    over_speed_sustained_s: float,
    dtc_fire: bool,
    fuel_low_active: bool,
) -> set[EventType]:
    conditions: set[EventType] = set()
    if accel_mps2 > THRESHOLDS.harsh_accel_mps2:
        conditions.add(EventType.HARSH_ACCEL)
    if accel_mps2 < THRESHOLDS.harsh_brake_mps2:
        conditions.add(EventType.HARSH_BRAKING)
    if (
        heading_delta_deg is not None
        and speed_kmh > THRESHOLDS.harsh_corner_speed_kmh
        and abs(heading_delta_deg) > THRESHOLDS.harsh_corner_heading_deg
    ):
        conditions.add(EventType.HARSH_CORNERING)
    if (
        route_speed_limit_kmh is not None
        and speed_kmh >= route_speed_limit_kmh + THRESHOLDS.over_speed_margin_kmh
        and over_speed_sustained_s >= THRESHOLDS.over_speed_sustained_s
    ):
        conditions.add(EventType.OVER_SPEED)
    if idle_seconds >= THRESHOLDS.idling_threshold_s:
        conditions.add(EventType.IDLING)
    if fuel_pct < THRESHOLDS.fuel_low_pct and not fuel_low_active:
        conditions.add(EventType.FUEL_LOW)
    if dtc_fire:
        conditions.add(EventType.DTC_CODE)
    return conditions


def pick_dtc_code(engine_type: EngineType) -> tuple[str, str]:
    eligible = [
        (code, info["description"])
        for code, info in DTC_CATALOG.items()
        if engine_type in info["engine_types"]
    ]
    if not eligible:
        raise ValueError(f"no DTC codes available for {engine_type}")
    return random.choice(eligible)


def make_event(
    vehicle_id: str,
    trip_id: str | None,
    ts: datetime,
    event_type: EventType,
    detail: str = "",
) -> DrivingEvent:
    return DrivingEvent(
        event_id=uuid.uuid4(),
        vehicle_id=vehicle_id,
        trip_id=trip_id,
        ts=ts,
        event_type=event_type,
        detail=detail,
    )
