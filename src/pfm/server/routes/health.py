"""Health check, unlock, and encryption status endpoints."""

from __future__ import annotations

import logging
import sqlite3

from aiohttp import web

from pfm import __version__

logger = logging.getLogger(__name__)

routes = web.RouteTableDef()


@routes.get("/api/v1/health")
async def health(request: web.Request) -> web.Response:
    """Return server health status."""
    return web.json_response(
        {
            "status": "ok",
            "version": __version__,
            "collecting": request.app.get("collecting", False),
            "locked": request.app.get("db_locked", False),
        }
    )


@routes.get("/api/v1/encryption/status")
async def encryption_status(request: web.Request) -> web.Response:
    """Return encryption and lock state for the SwiftUI client."""
    return web.json_response(
        {
            "encryption_enabled": request.app.get("encryption_enabled", False),
            "locked": request.app.get("db_locked", False),
        }
    )


@routes.post("/api/v1/unlock")
async def unlock(request: web.Request) -> web.Response:
    """Unlock the encrypted database with the provided key."""
    import sqlcipher3

    from pfm.config import get_settings
    from pfm.db.encryption import connect_encrypted, validate_key_hex
    from pfm.db.models import init_db
    from pfm.db.repository import Repository
    from pfm.pricing.coingecko import PricingService

    try:
        body = await request.json()
    except (ValueError, KeyError):
        return web.json_response({"error": "Invalid JSON body"}, status=400)

    key_hex = body.get("key", "")
    if not isinstance(key_hex, str) or not validate_key_hex(key_hex):
        return web.json_response(
            {"error": "Key must be a 64-character hex string (256-bit)"},
            status=400,
        )

    db_path = request.app["db_path"]

    # Test the key by running a query against the encrypted DB.
    try:
        conn = connect_encrypted(db_path, key_hex)
        async with conn:
            cursor = await conn.execute("SELECT count(*) FROM sqlite_master")
            await cursor.fetchone()
    except (sqlite3.DatabaseError, sqlcipher3.DatabaseError):
        logger.debug("Unlock attempt failed for %s", db_path, exc_info=True)
        return web.json_response({"error": "Invalid encryption key"}, status=401)

    # Key is valid — initialize DB, open shared resources.
    await init_db(db_path, key_hex=key_hex)

    settings = get_settings()

    repo = Repository(db_path, key_hex=key_hex)
    await repo.__aenter__()
    request.app["repo"] = repo

    pricing = PricingService(
        api_key=settings.coingecko_api_key,
        cache_db_path=db_path,
    )
    request.app["pricing"] = pricing

    request.app["db_key"] = key_hex
    request.app["db_locked"] = False

    logger.info("Database unlocked successfully")
    return web.json_response({"status": "unlocked"})
