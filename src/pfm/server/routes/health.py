"""Health check endpoint."""

from __future__ import annotations

from aiohttp import web

from pfm import __version__

routes = web.RouteTableDef()


@routes.get("/api/v1/health")
async def health(request: web.Request) -> web.Response:
    """Return server health status."""
    return web.json_response(
        {
            "status": "ok",
            "version": __version__,
            "collecting": request.app["collecting"],
        }
    )
