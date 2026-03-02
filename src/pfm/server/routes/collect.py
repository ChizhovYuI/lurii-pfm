"""Collection trigger REST endpoint."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Any

from aiohttp import web

if TYPE_CHECKING:
    from pfm.db.models import Source
    from pfm.db.repository import Repository
    from pfm.db.source_store import SourceStore

from pfm.server.serializers import collector_result_to_dict

logger = logging.getLogger(__name__)

routes = web.RouteTableDef()


@routes.get("/api/v1/collect/status")
async def collect_status(request: web.Request) -> web.Response:
    """Return current collection state."""
    return web.json_response({"collecting": request.app["collecting"]})


@routes.post("/api/v1/collect")
async def start_collection(request: web.Request) -> web.Response:
    """Spawn a background collection task. Returns 202 immediately."""
    if request.app["collecting"]:
        return web.json_response(
            {"error": "Collection already in progress"},
            status=409,
        )

    body: dict[str, Any] = {}
    if request.can_read_body:
        try:
            body = await request.json()
        except json.JSONDecodeError:
            logger.debug("Could not parse collection request body as JSON")

    source_name: str | None = body.get("source")

    request.app["collecting"] = True
    task = asyncio.ensure_future(_run_collection(request.app, source_name))
    request.app["_collection_task"] = task

    return web.json_response({"status": "started"}, status=202)


async def _run_collection(app: web.Application, source_name: str | None) -> None:
    """Background task: collect from sources, analyze, broadcast events."""
    from pfm.collectors import COLLECTOR_REGISTRY
    from pfm.db.models import CollectorResult
    from pfm.db.source_store import SourceNotFoundError, SourceStore

    broadcaster = app["broadcaster"]
    db_path = app["db_path"]
    repo = app["repo"]
    pricing = app["pricing"]

    try:
        await broadcaster.broadcast({"type": "collection_started"})

        store = SourceStore(db_path)

        if source_name:
            try:
                sources = [await store.get(source_name)]
            except SourceNotFoundError:
                await broadcaster.broadcast(
                    {
                        "type": "collection_failed",
                        "error": f"Source {source_name!r} not found",
                    }
                )
                return
        else:
            sources = await store.list_enabled()

        if not source_name:
            await _cleanup_disabled_snapshots(store, repo, sources)

        if not sources:
            await broadcaster.broadcast(
                {
                    "type": "collection_completed",
                    "results": [],
                }
            )
            return

        results: list[CollectorResult] = []

        for i, src in enumerate(sources):
            collector_cls = COLLECTOR_REGISTRY.get(src.type)
            if collector_cls is None:
                logger.warning("No collector registered for type '%s'", src.type)
                continue

            await broadcaster.broadcast(
                {
                    "type": "collection_progress",
                    "source": src.name,
                    "current": i + 1,
                    "total": len(sources),
                }
            )

            creds = json.loads(src.credentials)
            collector = collector_cls(pricing, **creds)
            collector.instance_name = src.name
            try:
                result = await collector.collect(repo)
                results.append(result)
            except Exception as exc:
                logger.exception("Unhandled collector error from '%s'", src.name)
                results.append(
                    CollectorResult(
                        source=src.name,
                        snapshots_count=0,
                        transactions_count=0,
                        errors=[f"Unhandled collector error: {exc}"],
                        duration_seconds=0.0,
                    ),
                )

        # Auto-run analyze after collection
        try:
            await _run_analyze(repo)
            await broadcaster.broadcast({"type": "snapshot_updated"})
        except Exception:
            logger.exception("Post-collection analyze failed")

        await broadcaster.broadcast(
            {
                "type": "collection_completed",
                "results": [collector_result_to_dict(r) for r in results],
            }
        )

    except Exception as exc:
        logger.exception("Collection background task failed")
        await broadcaster.broadcast(
            {
                "type": "collection_failed",
                "error": str(exc),
            }
        )
    finally:
        app["collecting"] = False


async def _cleanup_disabled_snapshots(
    store: SourceStore,
    repo: Repository,
    enabled_sources: list[Source],
) -> None:
    """Delete snapshots belonging to disabled (or removed) sources."""
    all_sources = await store.list_all()
    enabled_names = {s.name for s in enabled_sources}
    disabled_names = [s.name for s in all_sources if s.name not in enabled_names]
    if disabled_names:
        deleted = await repo.delete_snapshots_by_source_names(disabled_names)
        if deleted:
            logger.info("Deleted %d snapshots for disabled sources: %s", deleted, disabled_names)


async def _run_analyze(repo: object) -> None:
    """No-op — analytics are now computed on-the-fly from snapshots."""
    del repo
