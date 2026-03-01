"""Earn (yield) REST endpoints."""

from __future__ import annotations

from decimal import Decimal

from aiohttp import web

from pfm.server.serializers import _str_decimal, asset_type_for_snapshot

routes = web.RouteTableDef()


@routes.get("/api/v1/earn/summary")
async def earn_summary(request: web.Request) -> web.Response:
    """Return yield-earning positions and aggregate totals."""
    repo = request.app["repo"]
    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    analysis_date = max(s.date for s in latest)
    positions = [s for s in latest if s.apy > 0]

    total_usd_value = sum((s.usd_value for s in positions), Decimal(0))
    weighted_avg_apy = (
        sum((s.apy * s.usd_value for s in positions), Decimal(0)) / total_usd_value if total_usd_value else Decimal(0)
    )

    return web.json_response(
        {
            "date": analysis_date.isoformat(),
            "total_usd_value": _str_decimal(total_usd_value),
            "weighted_avg_apy": _str_decimal(weighted_avg_apy),
            "positions": [
                {
                    "source": snap.source,
                    "asset": snap.asset,
                    "asset_type": asset_type_for_snapshot(snap.source, snap.asset),
                    "amount": _str_decimal(snap.amount),
                    "usd_value": _str_decimal(snap.usd_value),
                    "price": _str_decimal(snap.price),
                    "apy": _str_decimal(snap.apy),
                }
                for snap in positions
            ],
        }
    )
