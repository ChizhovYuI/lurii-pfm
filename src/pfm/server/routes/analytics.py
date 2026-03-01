"""Analytics REST endpoints."""

from __future__ import annotations

from datetime import date

from aiohttp import web

from pfm.server.serializers import _str_decimal

routes = web.RouteTableDef()


@routes.get("/api/v1/analytics/allocation")
async def analytics_allocation(request: web.Request) -> web.Response:
    """Return live-computed allocation breakdowns."""
    from pfm.analytics import (
        compute_allocation_by_asset,
        compute_allocation_by_category,
        compute_allocation_by_source,
    )

    repo = request.app["repo"]
    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    analysis_date = latest[0].date

    alloc_asset = await compute_allocation_by_asset(repo, analysis_date)
    alloc_source = await compute_allocation_by_source(repo, analysis_date)
    alloc_category = await compute_allocation_by_category(repo, analysis_date)

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
        }
    )


@routes.get("/api/v1/analytics/exposure")
async def analytics_exposure(request: web.Request) -> web.Response:
    """Return live-computed currency exposure."""
    from pfm.analytics import compute_currency_exposure

    repo = request.app["repo"]
    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    analysis_date = latest[0].date
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
