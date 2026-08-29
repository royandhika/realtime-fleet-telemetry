"""Fleet live dashboard server (spec §7 "Dashboard B").

Polls Redis vehicle:latest:{vehicle_id} blobs on a short interval (the spec
sanctions this until the iot.telemetry.windowed fan-out topic exists) and
pushes snapshots to all connected WebSocket clients. Serves the static
frontend from ./static.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import redis.asyncio as aioredis
from aiohttp import WSMsgType, web

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("ws")

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
PORT = int(os.environ.get("WS_PORT", "8080"))
POLL_S = float(os.environ.get("WS_POLL_SECONDS", "1.0"))
KEY_PREFIX = "vehicle:latest:"
STATIC = Path(__file__).parent / "static"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class FleetHub:
    """Tracks connected clients and fans out fleet snapshots."""

    def __init__(self):
        self.clients: set[web.WebSocketResponse] = set()
        self.redis = None
        self.last_snapshot: dict | None = None

    async def start(self, app):
        self.redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        await self.redis.ping()
        log.info("Redis ready (%s)", REDIS_URL)
        self._task = asyncio.create_task(self._poll_loop())

    async def stop(self, app):
        self._task.cancel()
        if self.redis is not None:
            await self.redis.aclose()

    async def _poll_loop(self):
        while True:
            try:
                vehicles = []
                for key in sorted(await self.redis.keys(f"{KEY_PREFIX}*")):
                    raw = await self.redis.get(key)
                    if raw is None:
                        continue
                    blob = json.loads(raw)
                    vehicles.append({"id": key.rsplit(":", 1)[-1], **blob})
                if vehicles:
                    snapshot = {"type": "snapshot", "ts": _now_iso(), "vehicles": vehicles}
                    self.last_snapshot = snapshot
                    await self.broadcast(snapshot)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("poll failed: %s", exc)
            await asyncio.sleep(POLL_S)

    async def broadcast(self, message: dict):
        data = json.dumps(message)
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_str(data)
            except Exception:  # noqa: BLE001
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)
        if self.clients:
            log.info("pushed snapshot to %d client(s) (%d vehicles)",
                     len(self.clients), len(message["vehicles"]))


async def ws_handler(request: web.Request) -> web.WebSocketResponse:
    hub: FleetHub = request.app["hub"]
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    hub.clients.add(ws)
    log.info("client connected (%d total)", len(hub.clients))
    if hub.last_snapshot is not None:
        await ws.send_str(json.dumps(hub.last_snapshot))
    try:
        async for msg in ws:
            if msg.type == WSMsgType.ERROR:
                break
    finally:
        hub.clients.discard(ws)
        log.info("client disconnected (%d total)", len(hub.clients))
    return ws


async def index_handler(_request: web.Request) -> web.FileResponse:
    return web.FileResponse(STATIC / "index.html")


def build_app() -> web.Application:
    app = web.Application()
    hub = FleetHub()
    app["hub"] = hub
    app.on_startup.append(hub.start)
    app.on_cleanup.append(hub.stop)
    app.router.add_get("/", index_handler)
    app.router.add_get("/ws", ws_handler)
    app.router.add_static("/static/", STATIC)
    return app


if __name__ == "__main__":
    web.run_app(build_app(), host="0.0.0.0", port=PORT)
