"""Health check endpoint."""

from __future__ import annotations

from aiohttp import web

routes = web.RouteTableDef()


@routes.get("/api/v1/health")
async def health(_request: web.Request) -> web.Response:
    """Return server health status."""
    return web.json_response({"status": "ok"})
