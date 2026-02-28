"""Analytics REST endpoints."""

from __future__ import annotations

import json
from datetime import date

from aiohttp import web

from pfm.server.serializers import pnl_result_to_dict

routes = web.RouteTableDef()


@routes.get("/api/v1/analytics/pnl")
async def analytics_pnl(request: web.Request) -> web.Response:
    """Return PnL data — from cache or live-computed."""
    repo = request.app["repo"]
    period = request.query.get("period", "weekly")

    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    analysis_date = latest[0].date
    metrics = await repo.get_analytics_metrics_by_date(analysis_date)

    if "pnl" in metrics:
        pnl_data = json.loads(metrics["pnl"])
        if period in pnl_data:
            return web.json_response(
                {
                    "date": analysis_date.isoformat(),
                    "period": period,
                    "pnl": pnl_data[period],
                }
            )

    # Live compute fallback
    from pfm.analytics import PnlPeriod, compute_pnl

    try:
        period_enum = PnlPeriod(period)
    except ValueError:
        return web.json_response(
            {"error": f"Invalid period: {period!r}. Use daily/weekly/monthly/all_time"},
            status=400,
        )

    result = await compute_pnl(repo, analysis_date, period_enum)
    return web.json_response(
        {
            "date": analysis_date.isoformat(),
            "period": period,
            "pnl": pnl_result_to_dict(result),
        }
    )


@routes.get("/api/v1/analytics/allocation")
async def analytics_allocation(request: web.Request) -> web.Response:
    """Return cached allocation breakdowns."""
    repo = request.app["repo"]
    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    analysis_date = latest[0].date
    metrics = await repo.get_analytics_metrics_by_date(analysis_date)

    if "allocation_by_asset" not in metrics:
        return web.json_response(
            {"error": "Analytics not computed. Run 'pfm analyze' first."},
            status=404,
        )

    return web.json_response(
        {
            "date": analysis_date.isoformat(),
            "by_asset": json.loads(metrics["allocation_by_asset"]),
            "by_source": json.loads(metrics.get("allocation_by_source", "[]")),
            "by_category": json.loads(metrics.get("allocation_by_category", "[]")),
        }
    )


@routes.get("/api/v1/analytics/exposure")
async def analytics_exposure(request: web.Request) -> web.Response:
    """Return cached currency exposure."""
    repo = request.app["repo"]
    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    analysis_date = latest[0].date
    metrics = await repo.get_analytics_metrics_by_date(analysis_date)

    if "currency_exposure" not in metrics:
        return web.json_response(
            {"error": "Analytics not computed. Run 'pfm analyze' first."},
            status=404,
        )

    return web.json_response(
        {
            "date": analysis_date.isoformat(),
            "exposure": json.loads(metrics["currency_exposure"]),
        }
    )


@routes.get("/api/v1/analytics/yield")
async def analytics_yield(request: web.Request) -> web.Response:
    """Compute yield for a source/asset over a date range."""
    repo = request.app["repo"]
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
