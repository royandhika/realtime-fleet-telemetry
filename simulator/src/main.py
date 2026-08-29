"""Entry point for the fleet telemetry simulator.

Wires up state store, output sink, and one asyncio task per vehicle.
Honours ``SIM_ENABLED`` env (spec §3.1): 0 → idle forever, 1 → run.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

# Make sibling modules importable both under `python -m src.main` and `python main.py`
sys.path.insert(0, os.path.dirname(__file__))

import config
from output import make_sink
from state import init_state_store
from vehicle import VehicleSimulator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("simulator")


async def _idle_loop() -> None:
    log.info("SIM_ENABLED=0 — simulator idle, sleeping forever")
    while True:
        await asyncio.sleep(60.0)


async def _run_vehicles() -> None:
    store = init_state_store(config.STATE_DB_FALLBACK_PATH)
    sink = make_sink()
    sims = [
        VehicleSimulator(vehicle=v, store=store, sink=sink)
        for v in config.VEHICLES
    ]

    async def runner(s: VehicleSimulator) -> None:
        try:
            await s.run()
        except asyncio.CancelledError:
            log.info("%s cancelled", s.vehicle.vehicle_id)
            raise
        except Exception:
            log.exception("%s crashed", s.vehicle.vehicle_id)

    tasks = [asyncio.create_task(runner(s)) for s in sims]
    try:
        await asyncio.gather(*tasks)
    finally:
        log.info("shutting down — persisting all vehicle states")
        for s in sims:
            s.persist()
        sink.close()
        store.close()


async def _main_async() -> None:
    sim_enabled = os.environ.get("SIM_ENABLED", "1") == "1"
    if not sim_enabled:
        await _idle_loop()
        return
    log.info(
        "starting fleet simulator with %d vehicles, tick=%.1fs",
        len(config.VEHICLES), config.TICK_SECONDS,
    )
    try:
        await _run_vehicles()
    except asyncio.CancelledError:
        log.info("main loop cancelled")
        raise


def main() -> None:
    try:
        asyncio.run(_main_async())
    except KeyboardInterrupt:
        log.info("interrupted by user")


if __name__ == "__main__":
    main()