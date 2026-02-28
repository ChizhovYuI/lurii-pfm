"""Collection trigger REST endpoint."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Any

from aiohttp import web

if TYPE_CHECKING:
    from pfm.db.repository import Repository

from pfm.server.serializers import (
    build_asset_type_map,
    collector_result_to_dict,
    fmt_amount,
    fmt_pct,
    fmt_price,
    fmt_usd,
    pnl_result_to_dict,
)

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


async def _run_analyze(repo: Repository) -> None:
    """Compute all analytics and cache them (same as pfm analyze)."""
    from pfm.analytics import (
        PnlPeriod,
        compute_allocation_by_asset,
        compute_allocation_by_category,
        compute_allocation_by_source,
        compute_currency_exposure,
        compute_net_worth,
        compute_pnl,
        compute_risk_metrics,
    )

    latest = await repo.get_latest_snapshots()
    if not latest:
        return

    analysis_date = latest[0].date
    analysis_snapshots = await repo.get_snapshots_by_date(analysis_date)
    asset_type_map = build_asset_type_map(analysis_snapshots)

    net_worth = await compute_net_worth(repo, analysis_date)
    alloc_asset = await compute_allocation_by_asset(repo, analysis_date)
    alloc_source = await compute_allocation_by_source(repo, analysis_date)
    alloc_category = await compute_allocation_by_category(repo, analysis_date)
    currency_exposure = await compute_currency_exposure(repo, analysis_date)
    risk = await compute_risk_metrics(repo, analysis_date)

    pnl_daily = await compute_pnl(repo, analysis_date, PnlPeriod.DAILY)
    pnl_weekly = await compute_pnl(repo, analysis_date, PnlPeriod.WEEKLY)
    pnl_monthly = await compute_pnl(repo, analysis_date, PnlPeriod.MONTHLY)
    pnl_all_time = await compute_pnl(repo, analysis_date, PnlPeriod.ALL_TIME)

    await repo.save_analytics_metric(
        analysis_date,
        "net_worth",
        json.dumps({"usd": fmt_usd(net_worth)}),
    )
    await repo.save_analytics_metric(
        analysis_date,
        "allocation_by_asset",
        json.dumps(
            [
                {
                    "asset": row.asset,
                    "source": row.source,
                    "amount": fmt_amount(row.amount),
                    "usd_value": fmt_usd(row.usd_value),
                    "price": fmt_price(row.usd_value / row.amount) if row.amount else "0",
                    "percentage": fmt_pct(row.percentage),
                    "asset_type": asset_type_map.get(row.asset.upper(), "other"),
                }
                for row in alloc_asset
            ]
        ),
    )
    await repo.save_analytics_metric(
        analysis_date,
        "allocation_by_source",
        json.dumps(
            [
                {"source": row.bucket, "usd_value": fmt_usd(row.usd_value), "percentage": fmt_pct(row.percentage)}
                for row in alloc_source
            ]
        ),
    )
    await repo.save_analytics_metric(
        analysis_date,
        "allocation_by_category",
        json.dumps(
            [
                {
                    "category": row.bucket,
                    "usd_value": fmt_usd(row.usd_value),
                    "percentage": fmt_pct(row.percentage),
                }
                for row in alloc_category
            ]
        ),
    )
    await repo.save_analytics_metric(
        analysis_date,
        "currency_exposure",
        json.dumps(
            [
                {
                    "currency": row.currency,
                    "usd_value": fmt_usd(row.usd_value),
                    "percentage": fmt_pct(row.percentage),
                }
                for row in currency_exposure
            ]
        ),
    )
    await repo.save_analytics_metric(
        analysis_date,
        "risk_metrics",
        json.dumps(
            {
                "concentration_percentage": fmt_pct(risk.concentration_percentage),
                "hhi_index": fmt_pct(risk.hhi_index),
                "top_5_assets": [
                    {
                        "asset": row.asset,
                        "source": row.source,
                        "usd_value": fmt_usd(row.usd_value),
                        "price": fmt_price(row.usd_value / row.amount) if row.amount else "0",
                        "percentage": fmt_pct(row.percentage),
                    }
                    for row in risk.top_5_assets
                ],
            }
        ),
    )
    await repo.save_analytics_metric(
        analysis_date,
        "pnl",
        json.dumps(
            {
                "daily": pnl_result_to_dict(pnl_daily),
                "weekly": pnl_result_to_dict(pnl_weekly),
                "monthly": pnl_result_to_dict(pnl_monthly),
                "all_time": pnl_result_to_dict(pnl_all_time),
            }
        ),
    )
    await repo.save_analytics_metric(
        analysis_date,
        "weekly_pnl_by_asset",
        json.dumps(
            [
                {
                    "asset": row.asset,
                    "start_value": fmt_usd(row.start_value),
                    "end_value": fmt_usd(row.end_value),
                    "absolute_change": fmt_usd(row.absolute_change),
                    "percentage_change": fmt_pct(row.percentage_change),
                }
                for row in pnl_weekly.by_asset
            ]
        ),
    )
