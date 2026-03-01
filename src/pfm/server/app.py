"""Application factory for the aiohttp server."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from typing import TYPE_CHECKING

from aiohttp import web

from pfm.server.middleware import (
    api_logging_middleware,
    db_locked_middleware,
    error_handling_middleware,
    local_only_middleware,
)
from pfm.server.routes import setup_routes
from pfm.server.ws import EventBroadcaster

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)


async def _on_startup(app: web.Application) -> None:
    """Open shared Repository, PricingService, and EventBroadcaster."""
    from pfm.config import get_settings
    from pfm.db.models import init_db
    from pfm.db.repository import Repository
    from pfm.pricing.coingecko import PricingService

    db_path: Path = app["db_path"]
    key_hex: str | None = app.get("db_key") or os.environ.get("PFM_DB_KEY")

    if key_hex:
        app["db_key"] = key_hex

    encryption_enabled = app.get("encryption_enabled", False)

    if encryption_enabled and not key_hex:
        # Encryption is configured but no key provided — start locked.
        app["db_locked"] = True
        app["broadcaster"] = EventBroadcaster()
        app["collecting"] = False
        logger.warning("Encryption enabled but no key provided — starting in locked state")
        return

    await init_db(db_path, key_hex=key_hex)

    settings = get_settings()

    repo = Repository(db_path, key_hex=key_hex)
    await repo.__aenter__()
    app["repo"] = repo

    pricing = PricingService(
        api_key=settings.coingecko_api_key,
        cache_db_path=db_path,
    )
    app["pricing"] = pricing
    app["broadcaster"] = EventBroadcaster()
    app["collecting"] = False

    from pfm.server.scheduler import run_daily_collector

    app["_scheduler_task"] = asyncio.create_task(run_daily_collector(app))

    logger.info("Startup complete — DB unlocked, services ready")


async def _on_cleanup(app: web.Application) -> None:
    """Close shared resources and clear sensitive state."""
    scheduler: asyncio.Task[None] | None = app.get("_scheduler_task")
    if scheduler is not None:
        scheduler.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await scheduler

    repo = app.get("repo")
    if repo is not None:
        await repo.__aexit__(None, None, None)

    pricing = app.get("pricing")
    if pricing is not None:
        await pricing.close()

    broadcaster = app.get("broadcaster")
    if broadcaster is not None:
        await broadcaster.close()

    # Clear key from memory
    app.pop("db_key", None)


def create_app(db_path: Path) -> web.Application:
    """Create and configure the aiohttp application."""
    app = web.Application(
        middlewares=[local_only_middleware, db_locked_middleware, api_logging_middleware, error_handling_middleware],
    )
    app["db_path"] = db_path
    app["db_key"] = None
    app["db_locked"] = False
    app["encryption_enabled"] = False
    app.on_startup.append(_on_startup)
    app.on_cleanup.append(_on_cleanup)
    setup_routes(app)
    return app
