"""WebSocket support with event broadcasting."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import Any

from aiohttp import WSMsgType, web

logger = logging.getLogger(__name__)

_WS_CLOSE_TIMEOUT = 2.0


class EventBroadcaster:
    """Maintains a set of WebSocket clients and broadcasts events to all."""

    def __init__(self) -> None:
        self._clients: set[web.WebSocketResponse] = set()

    def register(self, ws: web.WebSocketResponse) -> None:
        """Add a WebSocket client to the broadcast set."""
        self._clients.add(ws)

    def unregister(self, ws: web.WebSocketResponse) -> None:
        """Remove a WebSocket client from the broadcast set."""
        self._clients.discard(ws)

    async def broadcast(self, event: dict[str, Any]) -> None:
        """Send a JSON event to all connected clients."""
        payload = json.dumps(event)
        closed: list[web.WebSocketResponse] = []
        for ws in self._clients:
            try:
                await ws.send_str(payload)
            except ConnectionResetError:
                closed.append(ws)
            except Exception:
                logger.exception("Error sending to WebSocket client")
                closed.append(ws)
        for ws in closed:
            self._clients.discard(ws)

    async def close(self) -> None:
        """Close all WebSocket clients with a per-connection timeout."""
        for ws in list(self._clients):
            with contextlib.suppress(asyncio.TimeoutError):
                async with asyncio.timeout(_WS_CLOSE_TIMEOUT):
                    await ws.close()
        self._clients.clear()

    @property
    def client_count(self) -> int:
        """Number of connected WebSocket clients."""
        return len(self._clients)


async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    """Handle WebSocket connections at /api/v1/ws."""
    ws = web.WebSocketResponse(heartbeat=30.0)
    await ws.prepare(request)

    broadcaster: EventBroadcaster = request.app["broadcaster"]
    broadcaster.register(ws)

    try:
        async for msg in ws:
            if msg.type == WSMsgType.ERROR:
                logger.warning("WebSocket error: %s", ws.exception())
    finally:
        broadcaster.unregister(ws)

    return ws
