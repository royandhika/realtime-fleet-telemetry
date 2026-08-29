from __future__ import annotations

import math
import random


_GEAR_RATIOS = (3.5, 2.1, 1.4, 1.0, 0.8)


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_m = 6_371_000.0
    lat1_rad, lat2_rad = math.radians(lat1), math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    a = math.sin(delta_lat / 2.0) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2.0) ** 2
    return 2.0 * radius_m * math.asin(math.sqrt(a))


def interpolate_point(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
    fraction: float,
) -> tuple[float, float]:
    return lat1 + (lat2 - lat1) * fraction, lon1 + (lon2 - lon1) * fraction


def bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1_rad, lat2_rad = math.radians(lat1), math.radians(lat2)
    delta_lon = math.radians(lon2 - lon1)
    x = math.sin(delta_lon) * math.cos(lat2_rad)
    y = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(delta_lon)
    return math.degrees(math.atan2(x, y)) % 360.0


def compute_gear(speed_kmh: float) -> int:
    if speed_kmh <= 0.5 or speed_kmh < 20.0:
        return 1
    if speed_kmh < 40.0:
        return 2
    if speed_kmh < 60.0:
        return 3
    if speed_kmh < 80.0:
        return 4
    return 5


def compute_rpm(speed_kmh: float, gear: int, prev_rpm: int, idle_rpm: int = 800) -> int:
    ratio = _GEAR_RATIOS[min(max(gear, 1), len(_GEAR_RATIOS)) - 1]
    target_rpm = idle_rpm + (speed_kmh / ratio) * 300.0
    if speed_kmh <= 0.5:
        target_rpm = idle_rpm + random.uniform(-30.0, 30.0)
    rpm = prev_rpm + (target_rpm - prev_rpm) * 0.35
    return round(min(6500.0, max(800.0, rpm)))


def compute_throttle(
    speed_kmh: float,
    v_target_kmh: float,
    accel_demand_mps2: float,
    max_accel_mps2: float = 3.0,
) -> float:
    if v_target_kmh <= 0.5:
        return 0.0
    speed_gap = max(0.0, v_target_kmh - speed_kmh)
    base = min(80.0, 100.0 * speed_gap / max(20.0, v_target_kmh))
    accel_bonus = 20.0 * max(0.0, accel_demand_mps2) / max_accel_mps2
    return min(100.0, max(0.0, base + accel_bonus))


def compute_maf(rpm: int, throttle_pct: float, displacement_L: float) -> float:
    return displacement_L * (rpm / 60.0) * (throttle_pct / 100.0) * 1.2 * 0.5


def update_engine_temp(
    prev_temp_c: float,
    rpm: int,
    seconds_since_ignition: float,
    ambient_c: float,
) -> float:
    target = 95.0
    warmup = 1.0 - math.exp(-max(0.0, seconds_since_ignition) / 240.0)
    target_warm = ambient_c + (target - ambient_c) * warmup
    rpm_heat = max(0.0, (rpm - 800) / 5700.0) * 8.0
    new_temp = prev_temp_c + (target_warm + rpm_heat - prev_temp_c) * 0.05
    return min(110.0, max(ambient_c - 2.0, new_temp))


def update_coolant_temp(
    prev_c: float,
    rpm: int,
    seconds_since_ignition: float,
    ambient_c: float,
) -> float:
    target = 90.0
    warmup = 1.0 - math.exp(-max(0.0, seconds_since_ignition) / 180.0)
    target_warm = ambient_c + (target - ambient_c) * warmup
    rpm_heat = max(0.0, (rpm - 5000) / 1500.0) * 12.0
    new_temp = prev_c + (target_warm + rpm_heat - prev_c) * 0.07
    return min(110.0, max(ambient_c - 2.0, new_temp))


def compute_intake_air_temp(ambient_c: float, rpm: int) -> float:
    return ambient_c + min(8.0, rpm / 800.0)


def compute_fuel_rate_lps(
    maf_g_per_s: float,
    rpm: int,
    fuel_type: str,
    idle_rpm: int = 800,
) -> float:
    density_factor = 0.97 if fuel_type == "Pertamax 95" else 1.0
    fuel_lps = (maf_g_per_s / 14.7) / 740.0 * density_factor
    return max(fuel_lps, 0.0002 if rpm <= idle_rpm else 0.0)
