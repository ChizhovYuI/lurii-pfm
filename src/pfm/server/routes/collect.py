"""Collection trigger REST endpoint."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Any

from aiohttp import web

if TYPE_CHECKING:
    from pathlib import Path

    from pfm.collectors.base import BaseCollector
    from pfm.db.models import Source
    from pfm.db.repository import Repository
    from pfm.db.source_store import SourceStore
    from pfm.pricing.coingecko import PricingService

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
    from pfm.collectors.pipeline import run_parallel_pipeline
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
            await broadcaster.broadcast({"type": "collection_completed", "results": []})
            return

        collectors = await _build_collectors(sources, pricing, db_path)
        total = len(collectors)

        async def _on_progress(current: float, total: float, message: str) -> None:
            await broadcaster.broadcast(
                {"type": "collection_progress", "current": current, "total": total, "message": message}
            )

        await _on_progress(0, 1, f"Fetching from {total} source(s)...")

        results = await run_parallel_pipeline(collectors, pricing, repo, on_progress=_on_progress)

        ok_count = sum(1 for r in results if not r.errors)
        err_count = sum(1 for r in results if r.errors)
        summary = f"Done. {ok_count} ok"
        if err_count:
            summary += f". {err_count} error{'s' if err_count != 1 else ''}"

        try:
            await _run_analyze(repo)
            await broadcaster.broadcast({"type": "snapshot_updated"})
        except Exception:
            logger.exception("Post-collection analyze failed")

        await broadcaster.broadcast(
            {
                "type": "collection_completed",
                "results": [collector_result_to_dict(r) for r in results],
                "message": summary,
            }
        )

    except Exception as exc:
        logger.exception("Collection background task failed")
        await broadcaster.broadcast({"type": "collection_failed", "error": str(exc)})
    finally:
        app["collecting"] = False


async def _build_collectors(
    sources: list[Source],
    pricing: PricingService,
    db_path: str | Path,
) -> list[tuple[Source, BaseCollector]]:
    """Create and configure collector instances from source configs."""
    from pfm.collectors import COLLECTOR_REGISTRY

    collectors: list[tuple[Source, BaseCollector]] = []
    for src in sources:
        collector_cls = COLLECTOR_REGISTRY.get(src.type)
        if collector_cls is None:
            logger.warning("No collector registered for type '%s'", src.type)
            continue
        creds = json.loads(src.credentials)
        collector = collector_cls(pricing, **creds)
        collector.instance_name = src.name
        await _inject_apy_rules(collector, src, db_path)
        collectors.append((src, collector))
    return collectors


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


async def _inject_apy_rules(collector: object, src: Source, db_path: str | Path) -> None:
    """Load APY rules for sources that support them."""
    from pfm.source_types import APY_RULES_TYPES

    if src.type not in APY_RULES_TYPES:
        return
    from pfm.db.apy_rules_store import ApyRulesStore

    store = ApyRulesStore(db_path)
    collector.apy_rules = await store.load_rules(src.name)  # type: ignore[attr-defined]


async def _run_analyze(repo: object) -> None:
    """No-op — analytics are now computed on-the-fly from snapshots."""
    del repo
