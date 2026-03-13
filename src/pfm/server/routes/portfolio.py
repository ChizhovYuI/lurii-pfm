"""Portfolio REST endpoints."""

from __future__ import annotations

from datetime import date, timedelta

from aiohttp import web

from pfm.db.models import is_sync_marker_snapshot
from pfm.server.serializers import _str_decimal, asset_type_for_snapshot, snapshot_to_dict
from pfm.server.state import get_repo

routes = web.RouteTableDef()


@routes.get("/api/v1/portfolio/summary")
async def portfolio_summary(request: web.Request) -> web.Response:
    """Return live-computed net_worth + per-snapshot holdings for the latest date."""
    from pfm.analytics import compute_data_warnings, compute_net_worth
    from pfm.db.source_store import SourceStore

    repo = get_repo(request.app)
    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    analysis_date = max(s.date for s in latest)
    net_worth = await compute_net_worth(repo, analysis_date)

    store = SourceStore(request.app["db_path"])
    enabled_sources = await store.list_enabled()
    warnings = compute_data_warnings(latest, enabled_sources, analysis_date)

    return web.json_response(
        {
            "date": analysis_date.isoformat(),
            "net_worth": {"usd": _str_decimal(net_worth)},
            "holdings": [
                {
                    "source": snap.source,
                    "source_name": snap.source_name or snap.source,
                    "asset": snap.asset,
                    "asset_type": asset_type_for_snapshot(snap.source, snap.asset),
                    "amount": _str_decimal(snap.amount),
                    "usd_value": _str_decimal(snap.usd_value),
                    "price": _str_decimal(snap.price),
                    "apy": _str_decimal(snap.apy),
                }
                for snap in latest
                if not is_sync_marker_snapshot(snap)
            ],
            "warnings": warnings,
        }
    )


@routes.get("/api/v1/portfolio/net-worth-history")
async def portfolio_net_worth_history(request: web.Request) -> web.Response:
    """Return daily resolved net worth points ending at the latest portfolio date."""
    from pfm.analytics import compute_net_worth

    repo = get_repo(request.app)
    days_param = request.query.get("days", "30")

    try:
        days = int(days_param)
    except ValueError:
        return web.json_response({"error": "days query parameter must be a positive integer"}, status=400)

    if days <= 0:
        return web.json_response({"error": "days query parameter must be a positive integer"}, status=400)

    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    end_date = max(s.date for s in latest)
    earliest_date = await repo.get_earliest_snapshot_date()
    if earliest_date is None:
        return web.json_response({"error": "No snapshots available"}, status=404)

    start_date = max(earliest_date, end_date - timedelta(days=days - 1))
    total_days = (end_date - start_date).days + 1

    points: list[dict[str, str]] = []
    for offset in range(total_days):
        point_date = start_date + timedelta(days=offset)
        net_worth = await compute_net_worth(repo, point_date)
        points.append({"date": point_date.isoformat(), "usd_value": _str_decimal(net_worth)})

    return web.json_response(
        {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "currency": "usd",
            "points": points,
        }
    )


@routes.get("/api/v1/portfolio/snapshots")
async def portfolio_snapshots(request: web.Request) -> web.Response:
    """Return snapshots for a date range."""
    repo = get_repo(request.app)
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
    repo = get_repo(request.app)
    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    return web.json_response(
        {
            "date": max(s.date for s in latest).isoformat(),
            "holdings": [snapshot_to_dict(s) for s in latest if not is_sync_marker_snapshot(s)],
        }
    )
