"""Source/conceptual data model for the simulator.

This is the *producer-side* (entity-oriented) model from spec §4.0.
It is deliberately distinct from the *query-driven* Cassandra tables in
spec §4.1-§4.6 — the ERD is normalized, the Cassandra schema is not.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional
import uuid


class RoadClass(str, Enum):
    TOLL = "toll"
    ARTERIAL = "arterial"
    LOCAL = "local"
    RESIDENTIAL = "residential"


ROAD_CLASS_SPEED_LIMIT_KMH: dict[RoadClass, int] = {
    RoadClass.TOLL: 100,
    RoadClass.ARTERIAL: 60,
    RoadClass.LOCAL: 40,
    RoadClass.RESIDENTIAL: 20,
}


class VehiclePersona(str, Enum):
    DELIVERY_VAN = "delivery_van"
    CITY_COMMUTER = "city_commuter"
    LONG_HAUL = "long_haul"


class VehicleState(str, Enum):
    IDLE = "idle"        # engine off, parked
    DRIVING = "driving"  # engine on, moving
    STOPPED = "stopped"  # engine on, stationary (traffic/light)


class EventType(str, Enum):
    HARSH_BRAKING = "harsh_braking"
    HARSH_ACCEL = "harsh_accel"
    HARSH_CORNERING = "harsh_cornering"
    OVER_SPEED = "over_speed"
    IDLING = "idling"
    FUEL_LOW = "fuel_low"
    DTC_CODE = "dtc_code"
    IGNITION_ON = "ignition_on"
    IGNITION_OFF = "ignition_off"


class EngineType(str, Enum):
    GASOLINE = "gasoline"
    DIESEL = "diesel"


@dataclass(frozen=True)
class RouteNode:
    """One waypoint in a route polyline."""
    lat: float
    lon: float
    road_class: RoadClass
    speed_limit_kmh: int  # explicit override; falls back to ROAD_CLASS_SPEED_LIMIT_KMH


@dataclass(frozen=True)
class Route:
    route_id: str
    name: str
    city: str
    nodes: tuple[RouteNode, ...]


@dataclass(frozen=True)
class Vehicle:
    vehicle_id: str
    plate_number: str
    brand: str
    model: str
    year: int
    persona: VehiclePersona
    engine_type: EngineType
    fuel_type: str          # e.g. "Pertamax 92", "Pertamax 95"
    displacement_L: float  # engine displacement in litres (drives MAF + fuel rate)
    fleet_group_id: str
    home_city: str


@dataclass
class Trip:
    trip_id: uuid.UUID
    vehicle_id: str
    route_id: str
    start_ts: datetime
    end_ts: Optional[datetime] = None
    state: VehicleState = VehicleState.DRIVING
    start_lat: float = 0.0
    start_lon: float = 0.0
    end_lat: Optional[float] = None
    end_lon: Optional[float] = None
    distance_km: float = 0.0


@dataclass
class Telemetry:
    """One tick of ECU + GPS output (spec §4.2 shape)."""
    vehicle_id: str
    trip_id: Optional[str]
    ts: datetime
    lat: float
    lon: float
    heading_deg: float
    speed_kmh: float
    gear: int
    rpm: int
    throttle_pct: float
    maf_g_per_s: float
    engine_temp_c: float
    coolant_temp_c: float
    intake_air_temp_c: float
    fuel_pct: float
    fuel_consumed_l: float
    odometer_km: float
    ignition_on: bool

    def to_dict(self) -> dict:
        return {
            "vehicle_id": self.vehicle_id,
            "trip_id": str(self.trip_id) if self.trip_id else None,
            "ts": self.ts.isoformat(),
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "heading_deg": round(self.heading_deg, 1),
            "speed_kmh": round(self.speed_kmh, 2),
            "gear": self.gear,
            "rpm": self.rpm,
            "throttle_pct": round(self.throttle_pct, 2),
            "maf_g_per_s": round(self.maf_g_per_s, 3),
            "engine_temp_c": round(self.engine_temp_c, 2),
            "coolant_temp_c": round(self.coolant_temp_c, 2),
            "intake_air_temp_c": round(self.intake_air_temp_c, 2),
            "fuel_pct": round(self.fuel_pct, 3),
            "fuel_consumed_l": round(self.fuel_consumed_l, 4),
            "odometer_km": round(self.odometer_km, 4),
            "ignition_on": self.ignition_on,
        }


@dataclass
class DrivingEvent:
    event_id: uuid.UUID
    vehicle_id: str
    trip_id: Optional[str]
    ts: datetime
    event_type: EventType
    detail: str

    def to_dict(self) -> dict:
        return {
            "event_id": str(self.event_id),
            "vehicle_id": self.vehicle_id,
            "trip_id": str(self.trip_id) if self.trip_id else None,
            "ts": self.ts.isoformat(),
            "event_type": self.event_type.value,
            "detail": self.detail,
        }


def utcnow() -> datetime:
    return datetime.now(timezone.utc)