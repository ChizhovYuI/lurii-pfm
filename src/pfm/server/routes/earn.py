"""Earn (yield) REST endpoints."""

from __future__ import annotations

import json
from datetime import timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from aiohttp import web

from pfm.server.serializers import _str_decimal, asset_type_for_snapshot
from pfm.server.state import get_repo

if TYPE_CHECKING:
    from pfm.db.models import Snapshot

routes = web.RouteTableDef()


@routes.get("/api/v1/earn/summary")
async def earn_summary(request: web.Request) -> web.Response:
    """Return yield-earning positions and aggregate totals."""
    repo = get_repo(request.app)
    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    analysis_date = max(s.date for s in latest)
    positions = [s for s in latest if _is_earn_position(s)]
    total_usd_value, weighted_avg_apy = _earn_totals(positions)

    return web.json_response(
        {
            "date": analysis_date.isoformat(),
            "total_usd_value": _str_decimal(total_usd_value),
            "weighted_avg_apy": _str_decimal(weighted_avg_apy),
            "positions": [_position_to_dict(snap) for snap in positions],
        }
    )


@routes.get("/api/v1/earn/history")
async def earn_history(request: web.Request) -> web.Response:
    """Return daily earn totals and weighted APY ending at the latest snapshot date."""
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
        resolved = await repo.get_snapshots_resolved(point_date)
        positions = [snap for snap in resolved if _is_earn_position(snap)]
        total_usd_value, weighted_avg_apy = _earn_totals(positions)
        points.append(
            {
                "date": point_date.isoformat(),
                "total_usd_value": _str_decimal(total_usd_value),
                "weighted_avg_apy": _str_decimal(weighted_avg_apy),
            }
        )

    return web.json_response(
        {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "points": points,
        }
    )


def _position_to_dict(snap: Snapshot) -> dict[str, str | int | None]:
    return {
        "id": snap.id,
        "source": snap.source,
        "source_name": snap.source_name or snap.source,
        "asset": snap.asset,
        "asset_type": asset_type_for_snapshot(snap.source, snap.asset),
        "amount": _str_decimal(snap.amount),
        "usd_value": _str_decimal(snap.usd_value),
        "price": _str_decimal(snap.price),
        "apy": _str_decimal(snap.apy),
    }


def _earn_totals(positions: list[Snapshot]) -> tuple[Decimal, Decimal]:
    total_usd_value = sum((snap.usd_value for snap in positions), Decimal(0))
    weighted_avg_apy = (
        sum((snap.apy * snap.usd_value for snap in positions), Decimal(0)) / total_usd_value
        if total_usd_value
        else Decimal(0)
    )
    return total_usd_value, weighted_avg_apy


def _is_earn_position(snapshot: Snapshot) -> bool:
    if snapshot.apy > 0:
        return True
    if snapshot.source != "coinex":
        return False
    if not snapshot.raw_json:
        return False
    try:
        raw = json.loads(snapshot.raw_json)
    except json.JSONDecodeError:
        return False
    if not isinstance(raw, dict):
        return False
    account_type = raw.get("account_type")
    return isinstance(account_type, str) and account_type.lower() == "financial"
