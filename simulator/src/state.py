from __future__ import annotations

import os
import sqlite3

import config


class StateStore:
    def __init__(self, db_path: str) -> None:
        directory: str = os.path.dirname(db_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._connection: sqlite3.Connection = sqlite3.connect(
            db_path,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS vehicle_state (
                vehicle_id TEXT PRIMARY KEY,
                lat REAL NOT NULL,
                lon REAL NOT NULL,
                odometer_km REAL NOT NULL,
                fuel_pct REAL NOT NULL,
                fuel_consumed_l REAL NOT NULL,
                current_trip_id TEXT,
                ignition_on INTEGER NOT NULL,
                seconds_since_ignition REAL NOT NULL DEFAULT 0,
                last_updated TEXT NOT NULL
            )
            """
        )

    def load(self, vehicle_id: str) -> dict | None:
        cursor: sqlite3.Cursor = self._connection.execute(
            """
            SELECT vehicle_id, lat, lon, odometer_km, fuel_pct,
                   fuel_consumed_l, current_trip_id, ignition_on,
                   seconds_since_ignition, last_updated
            FROM vehicle_state
            WHERE vehicle_id = ?
            """,
            (vehicle_id,),
        )
        row: tuple[object, ...] | None = cursor.fetchone()
        if row is None:
            return None
        return {
            "vehicle_id": row[0],
            "lat": row[1],
            "lon": row[2],
            "odometer_km": row[3],
            "fuel_pct": row[4],
            "fuel_consumed_l": row[5],
            "current_trip_id": row[6],
            "ignition_on": bool(row[7]),
            "seconds_since_ignition": row[8],
            "last_updated": row[9],
        }

    def save(
        self,
        *,
        vehicle_id: str,
        lat: float,
        lon: float,
        odometer_km: float,
        fuel_pct: float,
        fuel_consumed_l: float,
        current_trip_id: str | None,
        ignition_on: bool,
        seconds_since_ignition: float,
        last_updated: str,
    ) -> None:
        self._connection.execute(
            """
            INSERT OR REPLACE INTO vehicle_state (
                vehicle_id, lat, lon, odometer_km, fuel_pct, fuel_consumed_l,
                current_trip_id, ignition_on, seconds_since_ignition,
                last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                vehicle_id,
                lat,
                lon,
                odometer_km,
                fuel_pct,
                fuel_consumed_l,
                current_trip_id,
                int(ignition_on),
                seconds_since_ignition,
                last_updated,
            ),
        )

    def close(self) -> None:
        self._connection.commit()
        self._connection.close()


def init_state_store(db_path: str | None = None) -> StateStore:
    return StateStore(db_path if db_path is not None else config.STATE_DB_FALLBACK_PATH)
