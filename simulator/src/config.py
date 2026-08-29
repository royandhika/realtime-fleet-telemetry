"""Static configuration: vehicles, city bboxes, personas, DTC catalog.

No runtime logic — purely declarative. Edit values here to change the
simulated fleet.
"""

from __future__ import annotations

import os

from models import EngineType, Route, RouteNode, RoadClass, Vehicle, VehiclePersona


# ---------------------------------------------------------------------------
# Vehicles (spec §3.1) — 2 fixed hand-defined vehicles
# ---------------------------------------------------------------------------

VEHICLES: tuple[Vehicle, ...] = (
    Vehicle(
        vehicle_id="veh_0001",
        plate_number="B 1234 DEL",
        brand="Toyota",
        model="Innova Reborn",
        year=2021,
        persona=VehiclePersona.DELIVERY_VAN,
        engine_type=EngineType.GASOLINE,
        fuel_type="Pertamax 95",
        displacement_L=2.7,
        fleet_group_id="jakarta",
        home_city="Jakarta",
    ),
    Vehicle(
        vehicle_id="veh_0002",
        plate_number="D 5678 AZR",
        brand="Mitsubishi",
        model="Xpander",
        year=2020,
        persona=VehiclePersona.CITY_COMMUTER,
        engine_type=EngineType.GASOLINE,
        fuel_type="Pertamax 92",
        displacement_L=1.5,
        fleet_group_id="bandung",
        home_city="Bandung",
    ),
)


# ---------------------------------------------------------------------------
# City bounding boxes (spec §11) — vehicles never leave their home bbox
# ---------------------------------------------------------------------------

CITY_BBOXES: dict[str, dict[str, float]] = {
    "Jakarta":  {"center": (-6.2090, 106.8456), "lat_min": -6.30, "lat_max": -6.10, "lon_min": 106.70, "lon_max": 106.95},
    "Bandung":  {"center": (-6.9175, 107.6191), "lat_min": -7.00, "lat_max": -6.85, "lon_min": 107.55, "lon_max": 107.70},
}


# ---------------------------------------------------------------------------
# Persona kinematics (spec §3.1) — accel/brake envelopes
# ---------------------------------------------------------------------------

PERSONA_KINEMATICS: dict[VehiclePersona, dict[str, float]] = {
    VehiclePersona.DELIVERY_VAN: {
        "accel_max_mps2": 2.0,
        "brake_max_mps2": 3.0,
        "jitter_pct": 0.08,        # ±8% on target speed
        "traffic_light_p": 0.35,   # per node chance of stopping at a light
        "congestion_p": 0.20,      # per-trip chance of a congestion pocket
    },
    VehiclePersona.CITY_COMMUTER: {
        "accel_max_mps2": 3.0,
        "brake_max_mps2": 4.0,
        "jitter_pct": 0.10,
        "traffic_light_p": 0.45,
        "congestion_p": 0.25,
    },
    VehiclePersona.LONG_HAUL: {
        "accel_max_mps2": 1.5,
        "brake_max_mps2": 2.5,
        "jitter_pct": 0.05,
        "traffic_light_p": 0.05,
        "congestion_p": 0.10,
    },
}


# ---------------------------------------------------------------------------
# Ambient climate (spec §11) — tropical
# ---------------------------------------------------------------------------

AMBIENT_TEMP_C: float = 30.0   # ~mean diurnal ambient in Jakarta/Bandung
AMBIENT_TEMP_JITTER_C: float = 2.5  # ±2.5°C around the mean

CITY_AMBIENT_C: dict[str, float] = {
    # Jakarta: coastal tropical; Bandung: highland, ~5°C cooler
    "Jakarta": 31.0,
    "Bandung": 25.0,
}


# ---------------------------------------------------------------------------
# Fuel tanks — needed for fuel_pct decay math
# ---------------------------------------------------------------------------

TANK_CAPACITY_L: dict[str, float] = {
    "veh_0001": 55.0,   # Toyota Innova Reborn
    "veh_0002": 42.0,   # Mitsubishi Xpander
}


# ---------------------------------------------------------------------------
# Time-of-day traffic factor (WIB local hours)
# spec §11: rush 07-09 / 17-19 → target speed × 0.6
# ---------------------------------------------------------------------------

RUSH_HOURS: tuple[tuple[int, int], ...] = (
    (7, 9),
    (17, 19),
)
RUSH_FACTOR: float = 0.6       # multiply segment target speed during rush
NON_RUSH_FACTOR: float = 0.95 # mild calibration loss vs posted speed


# ---------------------------------------------------------------------------
# DTC catalog (spec §11) — feasible codes per engine type
# ---------------------------------------------------------------------------

DTC_CATALOG: dict[str, dict[str, object]] = {
    "P0171": {"description": "System Too Lean (Bank 1)",                        "severity": "med",  "engine_types": (EngineType.GASOLINE,)},
    "P0301": {"description": "Cylinder 1 Misfire Detected",                    "severity": "high", "engine_types": (EngineType.GASOLINE,)},
    "P0420": {"description": "Catalyst System Efficiency Below Threshold",      "severity": "med",  "engine_types": (EngineType.GASOLINE,)},
    "P0128": {"description": "Coolant Thermostat Below Regulating Temp",         "severity": "low",  "engine_types": (EngineType.GASOLINE, EngineType.DIESEL)},
    "P0455": {"description": "EVAP System Leak Detected (large)",               "severity": "low",  "engine_types": (EngineType.GASOLINE,)},
    "P2002": {"description": "DPF Efficiency Below Threshold",                   "severity": "med",  "engine_types": (EngineType.DIESEL,)},
}

DTC_TRIP_RATE: float = 0.01  # ~1% of trips emit one DTC event near trip midpoint


# ---------------------------------------------------------------------------
# Fuel rates — pertamax blends (spec §11)
# ---------------------------------------------------------------------------

FUEL_RATE_BY_TYPE: dict[str, float] = {
    "Pertamax 92": 1.00,   # baseline multiplier vs MAF-derived liters per hour
    "Pertamax 95": 0.97,   # slightly denser energy → marginally lower volume rate
}

# Low fuel threshold (spec §3.1 events table)
FUEL_LOW_PCT: float = 10.0


# ---------------------------------------------------------------------------
# Idling parameters (spec §3.1 events table)
# ---------------------------------------------------------------------------

IDLING_THRESHOLD_S: float = 120.0   # 2 min engine-on stop → idling event


# ---------------------------------------------------------------------------
# Pace
# ---------------------------------------------------------------------------

TICK_SECONDS: float = 1.0    # 1 Hz telemetry cadence
IDLE_DWELL_MIN_S: float = 30.0
IDLE_DWELL_MAX_S: float = 90.0
TRIP_START_P: float = 0.18   # per idle tick, chance to start a trip after min dwell


# ---------------------------------------------------------------------------
# SQLite state store path (mount as volume in compose) — spec §3.1
# ---------------------------------------------------------------------------

STATE_DB_PATH: str = "/data/simulator_state.db"
STATE_DB_FALLBACK_PATH: str = "./simulator_state.db"


# ---------------------------------------------------------------------------
# Output sink — stdout for local dev, kafka for the pipeline
# ---------------------------------------------------------------------------

OUTPUT_SINK: str = os.environ.get("OUTPUT_SINK", "stdout")   # "stdout" | "kafka"

KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TELEMETRY_TOPIC: str = os.environ.get("KAFKA_TELEMETRY_TOPIC", "iot.telemetry.raw")
KAFKA_EVENTS_TOPIC: str = os.environ.get("KAFKA_EVENTS_TOPIC", "iot.events.raw")
