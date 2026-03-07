"""Route registration hub."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aiohttp import web


def setup_routes(app: web.Application) -> None:
    """Register all route tables with the application."""
    from pfm.server.routes.ai import routes as ai_routes
    from pfm.server.routes.analytics import routes as analytics_routes
    from pfm.server.routes.apy_rules import routes as apy_rules_routes
    from pfm.server.routes.collect import routes as collect_routes
    from pfm.server.routes.earn import routes as earn_routes
    from pfm.server.routes.ext_snapshot import routes as ext_snapshot_routes
    from pfm.server.routes.health import routes as health_routes
    from pfm.server.routes.portfolio import routes as portfolio_routes
    from pfm.server.routes.report import routes as report_routes
    from pfm.server.routes.settings import routes as settings_routes
    from pfm.server.routes.sources import routes as sources_routes
    from pfm.server.routes.updates import routes as updates_routes
    from pfm.server.ws import websocket_handler

    app.router.add_routes(health_routes)
    app.router.add_routes(sources_routes)
    app.router.add_routes(portfolio_routes)
    app.router.add_routes(analytics_routes)
    app.router.add_routes(ai_routes)
    app.router.add_routes(collect_routes)
    app.router.add_routes(earn_routes)
    app.router.add_routes(ext_snapshot_routes)
    app.router.add_routes(report_routes)
    app.router.add_routes(settings_routes)
    app.router.add_routes(apy_rules_routes)
    app.router.add_routes(updates_routes)
    app.router.add_route("GET", "/api/v1/ws", websocket_handler)
