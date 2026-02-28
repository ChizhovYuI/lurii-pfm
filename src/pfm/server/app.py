"""Application factory for the aiohttp server."""

from __future__ import annotations

from typing import TYPE_CHECKING

from aiohttp import web

from pfm.server.middleware import error_handling_middleware, local_only_middleware
from pfm.server.routes import setup_routes
from pfm.server.ws import EventBroadcaster

if TYPE_CHECKING:
    from pathlib import Path


async def _on_startup(app: web.Application) -> None:
    """Open shared Repository, PricingService, and EventBroadcaster."""
    from pfm.config import get_settings
    from pfm.db.models import init_db
    from pfm.db.repository import Repository
    from pfm.pricing.coingecko import PricingService

    db_path: Path = app["db_path"]
    await init_db(db_path)

    settings = get_settings()

    repo = Repository(db_path)
    await repo.__aenter__()
    app["repo"] = repo

    pricing = PricingService(
        api_key=settings.coingecko_api_key,
        cache_db_path=db_path,
    )
    app["pricing"] = pricing
    app["broadcaster"] = EventBroadcaster()
    app["collecting"] = False


async def _on_cleanup(app: web.Application) -> None:
    """Close shared resources."""
    repo = app.get("repo")
    if repo is not None:
        await repo.__aexit__(None, None, None)

    pricing = app.get("pricing")
    if pricing is not None:
        await pricing.close()

    broadcaster = app.get("broadcaster")
    if broadcaster is not None:
        await broadcaster.close()


def create_app(db_path: Path) -> web.Application:
    """Create and configure the aiohttp application."""
    app = web.Application(
        middlewares=[local_only_middleware, error_handling_middleware],
    )
    app["db_path"] = db_path
    app.on_startup.append(_on_startup)
    app.on_cleanup.append(_on_cleanup)
    setup_routes(app)
    return app
