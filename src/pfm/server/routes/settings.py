"""Application settings REST endpoints."""

from __future__ import annotations

from typing import Any

import aiosqlite
from aiohttp import web

routes = web.RouteTableDef()


@routes.get("/api/v1/settings")
async def get_settings(request: web.Request) -> web.Response:
    """Read all key-value settings from app_settings table."""
    db_path = request.app["db_path"]
    async with aiosqlite.connect(str(db_path)) as db:
        rows = await (await db.execute("SELECT key, value FROM app_settings")).fetchall()
    settings_dict = {str(row[0]): str(row[1]) for row in rows}
    return web.json_response(settings_dict)


@routes.put("/api/v1/settings")
async def update_settings(request: web.Request) -> web.Response:
    """Update key-value settings in app_settings table."""
    db_path = request.app["db_path"]
    body: dict[str, Any] = await request.json()

    if not body:
        return web.json_response({"error": "Request body must be a non-empty JSON object"}, status=400)

    async with aiosqlite.connect(str(db_path)) as db:
        for key, value in body.items():
            await db.execute(
                "INSERT INTO app_settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, "
                "updated_at = datetime('now')",
                (key, str(value)),
            )
        await db.commit()

    return web.json_response({"updated": True})
