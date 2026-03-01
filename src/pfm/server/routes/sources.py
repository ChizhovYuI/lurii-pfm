"""Source management REST endpoints."""

from __future__ import annotations

from typing import Any

from aiohttp import web

from pfm.db.source_store import (
    DuplicateSourceError,
    InvalidCredentialsError,
    InvalidSourceTypeError,
    SourceNotFoundError,
    SourceStore,
)
from pfm.server.serializers import source_to_dict

routes = web.RouteTableDef()


@routes.get("/api/v1/source-types")
async def list_source_types(_request: web.Request) -> web.Response:
    """Return credential field schemas for all known source types."""
    from pfm.source_types import SOURCE_TYPES

    return web.json_response(
        {
            name: [
                {"name": f.name, "prompt": f.prompt, "required": f.required, "secret": f.secret, "tip": f.tip}
                for f in fields
            ]
            for name, fields in SOURCE_TYPES.items()
        }
    )


@routes.get("/api/v1/sources")
async def list_sources(request: web.Request) -> web.Response:
    """List all configured sources."""
    store = SourceStore(request.app["db_path"])
    sources = await store.list_all()
    return web.json_response([source_to_dict(s) for s in sources])


@routes.post("/api/v1/sources")
async def add_source(request: web.Request) -> web.Response:
    """Add a new data source."""
    body: dict[str, Any] = await request.json()
    name = body.get("name")
    source_type = body.get("type")
    credentials = body.get("credentials")

    if not name or not source_type or credentials is None:
        return web.json_response(
            {"error": "name, type, and credentials are required"},
            status=400,
        )

    store = SourceStore(request.app["db_path"])
    try:
        source = await store.add(name, source_type, credentials)
    except DuplicateSourceError as exc:
        return web.json_response({"error": str(exc)}, status=409)
    except (InvalidSourceTypeError, InvalidCredentialsError) as exc:
        return web.json_response({"error": str(exc)}, status=400)

    return web.json_response(source_to_dict(source), status=201)


@routes.get("/api/v1/sources/{name}")
async def get_source(request: web.Request) -> web.Response:
    """Get a single source by name (secrets masked)."""
    name = request.match_info["name"]
    store = SourceStore(request.app["db_path"])
    try:
        source = await store.get(name)
    except SourceNotFoundError:
        return web.json_response({"error": f"Source {name!r} not found"}, status=404)

    return web.json_response(source_to_dict(source))


@routes.delete("/api/v1/sources/{name}")
async def delete_source(request: web.Request) -> web.Response:
    """Delete a source by name."""
    name = request.match_info["name"]
    store = SourceStore(request.app["db_path"])
    try:
        await store.delete(name)
    except SourceNotFoundError:
        return web.json_response({"error": f"Source {name!r} not found"}, status=404)

    return web.json_response({"deleted": True})


@routes.patch("/api/v1/sources/{name}")
async def update_source(request: web.Request) -> web.Response:
    """Update a source's credentials and/or enabled flag."""
    name = request.match_info["name"]
    body: dict[str, Any] = await request.json()

    store = SourceStore(request.app["db_path"])
    try:
        source = await store.update(
            name,
            credentials=body.get("credentials"),
            enabled=body.get("enabled"),
        )
    except SourceNotFoundError:
        return web.json_response({"error": f"Source {name!r} not found"}, status=404)
    except InvalidCredentialsError as exc:
        return web.json_response({"error": str(exc)}, status=400)

    return web.json_response(source_to_dict(source))
