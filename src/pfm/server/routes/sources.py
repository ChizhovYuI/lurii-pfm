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
from pfm.server.connection_validation import (
    ConnectionValidationError,
)
from pfm.server.connection_validation import (
    validate_source_connection as run_source_validation,
)
from pfm.server.serializers import source_to_dict

routes = web.RouteTableDef()


@routes.get("/api/v1/source-types")
async def list_source_types(_request: web.Request) -> web.Response:
    """Return credential field schemas for all known source types."""
    from pfm.source_types import APY_RULES_TYPES, SOURCE_TYPES

    return web.json_response(
        {
            name: {
                "fields": [
                    {"name": f.name, "prompt": f.prompt, "required": f.required, "secret": f.secret, "tip": f.tip}
                    for f in fields
                ],
                "supported_apy_rules": [
                    {"protocol": p.protocol, "coins": list(p.coins)} for p in APY_RULES_TYPES.get(name, ())
                ],
            }
            for name, fields in SOURCE_TYPES.items()
        }
    )


@routes.get("/api/v1/sources")
async def list_sources(request: web.Request) -> web.Response:
    """List all configured sources."""
    store = SourceStore(request.app["db_path"])
    sources = await store.list_all()
    return web.json_response([source_to_dict(s) for s in sources])


@routes.post("/api/v1/source-connections/validate")
@routes.post("/api/v1/sources/validate")
async def validate_source_connection(request: web.Request) -> web.Response:
    """Validate source credentials without saving them."""
    body: dict[str, Any] = await request.json()

    try:
        message = await run_source_validation(
            request.app["db_path"],
            source_name=body.get("name"),
            source_type=body.get("type"),
            credentials=body.get("credentials"),
        )
    except ConnectionValidationError as exc:
        return web.json_response({"error": exc.message}, status=exc.status_code)

    return web.json_response({"ok": True, "message": message})


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
    repo = request.app["repo"]
    try:
        result = await repo.delete_source_cascade(name)
    except SourceNotFoundError:
        return web.json_response({"error": f"Source {name!r} not found"}, status=404)

    await request.app["broadcaster"].broadcast({"type": "snapshot_updated"})
    return web.json_response(
        {
            "deleted": True,
            "name": result.name,
            "removed": {
                "snapshots": result.snapshots,
                "transactions": result.transactions,
                "analytics_metrics": result.analytics_metrics,
                "apy_rules": result.apy_rules,
            },
        }
    )


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
