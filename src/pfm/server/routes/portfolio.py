"""Portfolio REST endpoints."""

from __future__ import annotations

import json
from datetime import date

from aiohttp import web

from pfm.server.serializers import snapshot_to_dict

routes = web.RouteTableDef()


@routes.get("/api/v1/portfolio/summary")
async def portfolio_summary(request: web.Request) -> web.Response:
    """Return cached net_worth + allocation for the latest snapshot date."""
    repo = request.app["repo"]
    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    analysis_date = latest[0].date
    metrics = await repo.get_analytics_metrics_by_date(analysis_date)

    net_worth = json.loads(metrics["net_worth"]) if "net_worth" in metrics else None
    allocation = json.loads(metrics["allocation_by_asset"]) if "allocation_by_asset" in metrics else []

    return web.json_response(
        {
            "date": analysis_date.isoformat(),
            "net_worth": net_worth,
            "allocation": allocation,
        }
    )


@routes.get("/api/v1/portfolio/snapshots")
async def portfolio_snapshots(request: web.Request) -> web.Response:
    """Return snapshots for a date range."""
    repo = request.app["repo"]
    start_str = request.query.get("start")
    end_str = request.query.get("end")

    if not start_str or not end_str:
        return web.json_response(
            {"error": "start and end query parameters are required"},
            status=400,
        )

    try:
        start = date.fromisoformat(start_str)
        end = date.fromisoformat(end_str)
    except ValueError:
        return web.json_response({"error": "Invalid date format (use YYYY-MM-DD)"}, status=400)

    snapshots = await repo.get_snapshots_for_range(start, end)
    return web.json_response([snapshot_to_dict(s) for s in snapshots])


@routes.get("/api/v1/portfolio/holdings")
async def portfolio_holdings(request: web.Request) -> web.Response:
    """Return latest snapshots as a holdings list."""
    repo = request.app["repo"]
    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    return web.json_response(
        {
            "date": latest[0].date.isoformat(),
            "holdings": [snapshot_to_dict(s) for s in latest],
        }
    )
