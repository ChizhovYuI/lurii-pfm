"""Analytics REST endpoints."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from aiohttp import web

from pfm.server.serializers import _str_decimal
from pfm.server.state import get_repo

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pfm.db.models import Snapshot

routes = web.RouteTableDef()


def _sum_usd_by_source_name(snapshots: Sequence[Snapshot]) -> dict[str, Decimal]:
    totals: dict[str, Decimal] = {}
    for snapshot in snapshots:
        totals[snapshot.source] = totals.get(snapshot.source, Decimal(0)) + snapshot.usd_value
    return totals


def _serialize_source_mover(
    source: str,
    *,
    absolute_change: Decimal,
    current_usd_value: Decimal,
    previous_usd_value: Decimal,
) -> dict[str, str]:
    return {
        "source": source,
        "absolute_change": _str_decimal(absolute_change),
        "current_usd_value": _str_decimal(current_usd_value),
        "previous_usd_value": _str_decimal(previous_usd_value),
    }


@routes.get("/api/v1/analytics/allocation")
async def analytics_allocation(request: web.Request) -> web.Response:
    """Return live-computed allocation breakdowns."""
    from pfm.analytics import (
        compute_allocation_by_asset,
        compute_allocation_by_category,
        compute_allocation_by_source,
        compute_data_warnings,
        compute_risk_metrics,
    )
    from pfm.db.source_store import SourceStore

    repo = get_repo(request.app)
    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    analysis_date = max(s.date for s in latest)

    alloc_asset = await compute_allocation_by_asset(repo, analysis_date)
    alloc_source = await compute_allocation_by_source(repo, analysis_date)
    alloc_category = await compute_allocation_by_category(repo, analysis_date)
    risk_metrics = await compute_risk_metrics(repo, analysis_date)

    store = SourceStore(request.app["db_path"])
    enabled_types = {s.type for s in await store.list_enabled()}
    warnings = compute_data_warnings(latest, enabled_types, analysis_date)

    return web.json_response(
        {
            "date": analysis_date.isoformat(),
            "by_asset": [
                {
                    "asset": row.asset,
                    "asset_type": row.asset_type,
                    "sources": list(row.sources),
                    "amount": _str_decimal(row.amount),
                    "usd_value": _str_decimal(row.usd_value),
                    "price": _str_decimal(row.price),
                    "percentage": _str_decimal(row.percentage),
                }
                for row in alloc_asset
            ],
            "by_source": [
                {
                    "source": row.bucket,
                    "usd_value": _str_decimal(row.usd_value),
                    "percentage": _str_decimal(row.percentage),
                }
                for row in alloc_source
            ],
            "by_category": [
                {
                    "category": row.bucket,
                    "usd_value": _str_decimal(row.usd_value),
                    "percentage": _str_decimal(row.percentage),
                }
                for row in alloc_category
            ],
            "risk_metrics": {
                "concentration_percentage": _str_decimal(risk_metrics.concentration_percentage),
                "hhi_index": _str_decimal(risk_metrics.hhi_index),
                "top_5_assets": [
                    {
                        "asset": row.asset,
                        "sources": list(row.sources),
                        "usd_value": _str_decimal(row.usd_value),
                        "price": _str_decimal(row.price),
                        "percentage": _str_decimal(row.percentage),
                    }
                    for row in risk_metrics.top_5_assets
                ],
            },
            "warnings": warnings,
        }
    )


@routes.get("/api/v1/analytics/pnl")
async def analytics_pnl(request: web.Request) -> web.Response:
    """Return portfolio PnL for a supported period ending at the latest portfolio date."""
    from pfm.analytics.pnl import AssetPnl, PnlPeriod, compute_pnl

    repo = get_repo(request.app)
    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    period_raw = request.query.get("period")
    if not period_raw:
        return web.json_response({"error": "period query parameter is required"}, status=400)

    try:
        period = PnlPeriod(period_raw)
    except ValueError:
        return web.json_response({"error": "Unsupported period"}, status=400)

    analysis_date = max(s.date for s in latest)
    result = await compute_pnl(repo, analysis_date, period)

    def serialize_asset_row(asset_row: AssetPnl) -> dict[str, str | None]:
        return {
            "asset": asset_row.asset,
            "start_value": _str_decimal(asset_row.start_value),
            "end_value": _str_decimal(asset_row.end_value),
            "absolute_change": _str_decimal(asset_row.absolute_change),
            "percentage_change": _str_decimal(asset_row.percentage_change),
            "cost_basis_value": _str_decimal(asset_row.cost_basis_value)
            if asset_row.cost_basis_value is not None
            else None,
        }

    return web.json_response(
        {
            "date": analysis_date.isoformat(),
            "period": period.value,
            "pnl": {
                "start_date": result.start_date.isoformat() if result.start_date else None,
                "end_date": result.end_date.isoformat() if result.end_date else None,
                "start_value": _str_decimal(result.start_value),
                "end_value": _str_decimal(result.end_value),
                "absolute_change": _str_decimal(result.absolute_change),
                "percentage_change": _str_decimal(result.percentage_change),
                "by_asset": [serialize_asset_row(row) for row in result.by_asset],
                "top_gainers": [serialize_asset_row(row) for row in result.top_gainers],
                "top_losers": [serialize_asset_row(row) for row in result.top_losers],
                "notes": result.notes,
            },
        }
    )


@routes.get("/api/v1/analytics/exposure")
async def analytics_exposure(request: web.Request) -> web.Response:
    """Return live-computed currency exposure."""
    from pfm.analytics import compute_currency_exposure

    repo = get_repo(request.app)
    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    analysis_date = max(s.date for s in latest)
    exposure = await compute_currency_exposure(repo, analysis_date)

    return web.json_response(
        {
            "date": analysis_date.isoformat(),
            "exposure": [
                {
                    "currency": row.currency,
                    "usd_value": _str_decimal(row.usd_value),
                    "percentage": _str_decimal(row.percentage),
                }
                for row in exposure
            ],
        }
    )


@routes.get("/api/v1/analytics/source-movers")
async def analytics_source_movers(request: web.Request) -> web.Response:
    """Return top day-over-day allocation movers by source."""
    repo = get_repo(request.app)
    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    analysis_date = max(s.date for s in latest)
    comparison_date = analysis_date - timedelta(days=1)
    previous_snapshots = await repo.get_snapshots_resolved(comparison_date)

    if not previous_snapshots:
        return web.json_response(
            {
                "date": analysis_date.isoformat(),
                "previous_date": None,
                "gainers": [],
                "reducers": [],
            }
        )

    current_by_source = _sum_usd_by_source_name(latest)
    previous_by_source = _sum_usd_by_source_name(previous_snapshots)

    gainers: list[dict[str, str]] = []
    reducers: list[dict[str, str]] = []
    for source in sorted(set(current_by_source) | set(previous_by_source)):
        current_usd_value = current_by_source.get(source, Decimal(0))
        previous_usd_value = previous_by_source.get(source, Decimal(0))
        absolute_change = current_usd_value - previous_usd_value
        if absolute_change > 0:
            gainers.append(
                _serialize_source_mover(
                    source,
                    absolute_change=absolute_change,
                    current_usd_value=current_usd_value,
                    previous_usd_value=previous_usd_value,
                )
            )
        elif absolute_change < 0:
            reducers.append(
                _serialize_source_mover(
                    source,
                    absolute_change=absolute_change,
                    current_usd_value=current_usd_value,
                    previous_usd_value=previous_usd_value,
                )
            )

    gainers.sort(key=lambda row: Decimal(row["absolute_change"]), reverse=True)
    reducers.sort(key=lambda row: Decimal(row["absolute_change"]))

    return web.json_response(
        {
            "date": analysis_date.isoformat(),
            "previous_date": comparison_date.isoformat(),
            "gainers": gainers[:2],
            "reducers": reducers[:2],
        }
    )


@routes.get("/api/v1/analytics/yield")
async def analytics_yield(request: web.Request) -> web.Response:
    """Compute yield for a source/asset over a date range."""
    repo = get_repo(request.app)
    source = request.query.get("source")
    asset = request.query.get("asset")
    start_str = request.query.get("start")
    end_str = request.query.get("end")

    if not source or not asset or not start_str or not end_str:
        return web.json_response(
            {"error": "source, asset, start, and end query parameters are required"},
            status=400,
        )

    try:
        start = date.fromisoformat(start_str)
        end = date.fromisoformat(end_str)
    except ValueError:
        return web.json_response({"error": "Invalid date format (use YYYY-MM-DD)"}, status=400)

    from pfm.analytics import compute_yield

    result = await compute_yield(repo, source, asset, start, end)
    return web.json_response(
        {
            "source": result.source,
            "asset": result.asset,
            "start_date": result.start_date.isoformat() if result.start_date else None,
            "end_date": result.end_date.isoformat() if result.end_date else None,
            "principal_estimate": str(result.principal_estimate),
            "current_value": str(result.current_value),
            "yield_amount": str(result.yield_amount),
            "yield_percentage": str(result.yield_percentage),
            "annualized_rate": str(result.annualized_rate),
            "notes": result.notes,
        }
    )
