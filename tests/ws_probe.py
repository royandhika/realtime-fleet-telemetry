"""WS probe: runs INSIDE the ws-dashboard container (has aiohttp)."""
import asyncio
import json

import aiohttp


async def main():
    async with aiohttp.ClientSession() as s:
        async with s.ws_connect("http://localhost:8080/ws") as ws:
            msg = json.loads((await asyncio.wait_for(ws.receive(), 10)).data)
            assert msg["type"] == "snapshot", msg.get("type")
            v = msg["vehicles"][0]
            assert {"id", "lat", "lon", "speed_kmh", "engine_temp_c", "updated_at"} <= set(v), v.keys()
            print(f"OK vehicles={len(msg['vehicles'])} id={v['id']} lat={v['lat']} speed={v['speed_kmh']}")


asyncio.run(main())
