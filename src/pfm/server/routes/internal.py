"""Internal endpoints for in-process / sibling-process daemon coordination.

These routes are local-only (daemon binds 127.0.0.1) and are not part of the
public API. They exist so out-of-process callers (e.g. the MCP server) can
trigger WebSocket events after committing a write directly to the shared
SQLite database.
"""

from __future__ import annotations

from typing import Any

from aiohttp import web

from pfm.server.state import get_broadcaster

routes = web.RouteTableDef()

_ALLOWED_EVENT_TYPES = frozenset({"snapshot_updated", "sources_changed"})


@routes.post("/api/v1/internal/broadcast")
async def broadcast_event(request: web.Request) -> web.Response:
    """Broadcast an allow-listed event to all connected WebSocket clients."""
    try:
        body: dict[str, Any] = await request.json()
    except (ValueError, TypeError):
        return web.json_response({"error": "Invalid JSON body"}, status=400)

    event_type = body.get("type")
    if not isinstance(event_type, str) or event_type not in _ALLOWED_EVENT_TYPES:
        return web.json_response(
            {"error": f"event type must be one of {sorted(_ALLOWED_EVENT_TYPES)}"},
            status=400,
        )

    await get_broadcaster(request.app).broadcast({"type": event_type})
    return web.json_response({"broadcast": True, "type": event_type})
