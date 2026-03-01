"""Portfolio REST endpoints."""

from __future__ import annotations

from datetime import date

from aiohttp import web

from pfm.server.serializers import _str_decimal, asset_type_for_snapshot, snapshot_to_dict

routes = web.RouteTableDef()


@routes.get("/api/v1/portfolio/summary")
async def portfolio_summary(request: web.Request) -> web.Response:
    """Return live-computed net_worth + per-snapshot holdings for the latest date."""
    from pfm.analytics import compute_net_worth

    repo = request.app["repo"]
    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    analysis_date = latest[0].date
    net_worth = await compute_net_worth(repo, analysis_date)

    return web.json_response(
        {
            "date": analysis_date.isoformat(),
            "net_worth": {"usd": _str_decimal(net_worth)},
            "holdings": [
                {
                    "source": snap.source,
                    "asset": snap.asset,
                    "asset_type": asset_type_for_snapshot(snap.source, snap.asset),
                    "amount": _str_decimal(snap.amount),
                    "usd_value": _str_decimal(snap.usd_value),
                    "price": _str_decimal(snap.price),
                    "apy": _str_decimal(snap.apy),
                }
                for snap in latest
            ],
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
