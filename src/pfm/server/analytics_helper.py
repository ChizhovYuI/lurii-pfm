"""Shared helper to build AnalyticsSummary from live-computed metrics."""

from __future__ import annotations

import json
from datetime import timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from pfm.server.serializers import _str_decimal

_MIN_TX_AMOUNT = 10

if TYPE_CHECKING:
    from datetime import date
    from pathlib import Path

    from pfm.ai import AnalyticsSummary
    from pfm.db.repository import Repository


async def build_analytics_summary(
    repo: Repository, snapshot_date: date, *, db_path: Path | None = None
) -> AnalyticsSummary:
    """Compute all analytics metrics live and return an AnalyticsSummary."""
    from pfm.ai import AnalyticsSummary
    from pfm.analytics import (
        compute_allocation_by_asset,
        compute_allocation_by_category,
        compute_allocation_by_source,
        compute_currency_exposure,
        compute_data_warnings,
        compute_net_worth,
        compute_risk_metrics,
    )
    from pfm.analytics.pnl import PnlPeriod, compute_pnl

    snapshots = await repo.get_snapshots_resolved(snapshot_date)

    # Compute data warnings (stale KBank, missing sources)
    enabled_types: set[str] = set()
    if db_path is not None:
        from pfm.db.source_store import SourceStore

        store = SourceStore(db_path)
        enabled_types = {s.type for s in await store.list_enabled()}
    warnings = compute_data_warnings(snapshots, enabled_types, snapshot_date)

    net_worth = await compute_net_worth(repo, snapshot_date)
    alloc_asset = await compute_allocation_by_asset(repo, snapshot_date)
    alloc_source = await compute_allocation_by_source(repo, snapshot_date)
    alloc_category = await compute_allocation_by_category(repo, snapshot_date)
    currency_exposure = await compute_currency_exposure(repo, snapshot_date)
    risk = await compute_risk_metrics(repo, snapshot_date)

    # Earn positions: snapshots with APY > 0
    earn_positions = ""
    earn_snaps = [s for s in snapshots if s.apy > 0]
    if earn_snaps:
        earn_positions = json.dumps(
            [
                {
                    "asset": s.asset,
                    "source": s.source_name or s.source,
                    "usd_value": _str_decimal(s.usd_value),
                    "apy": _str_decimal(s.apy * 100),
                    "portfolio_pct": _str_decimal((s.usd_value / net_worth * 100) if net_worth else Decimal(0)),
                }
                for s in earn_snaps
            ]
        )

    # Weekly PnL: skip section if no prior snapshot exists
    weekly_pnl = ""
    pnl = await compute_pnl(repo, snapshot_date, PnlPeriod.WEEKLY)
    if pnl.start_date is not None and pnl.start_date != pnl.end_date:
        weekly_pnl = json.dumps(
            {
                "start_date": pnl.start_date.isoformat(),
                "end_date": pnl.end_date.isoformat() if pnl.end_date else "",
                "start_value": _str_decimal(pnl.start_value),
                "end_value": _str_decimal(pnl.end_value),
                "absolute_change": _str_decimal(pnl.absolute_change),
                "percentage_change": _str_decimal(pnl.percentage_change),
                "top_gainers": [
                    {
                        "asset": r.asset,
                        "absolute_change": _str_decimal(r.absolute_change),
                        "percentage_change": _str_decimal(r.percentage_change),
                    }
                    for r in pnl.top_gainers[:3]
                ],
                "top_losers": [
                    {
                        "asset": r.asset,
                        "absolute_change": _str_decimal(r.absolute_change),
                        "percentage_change": _str_decimal(r.percentage_change),
                    }
                    for r in pnl.top_losers[:3]
                ],
            }
        )

    # Recent transactions (last 7 days) for AI context on fund movements
    recent_transactions = ""
    tx_start = snapshot_date - timedelta(days=7)
    txs = await repo.get_transactions(start=tx_start, end=snapshot_date)
    move_types = {"deposit", "withdrawal", "transfer"}
    move_txs = [t for t in txs if t.tx_type in move_types and t.amount >= _MIN_TX_AMOUNT]
    if move_txs:
        recent_transactions = json.dumps(
            [
                {
                    "date": t.date.isoformat(),
                    "source": t.source,
                    "type": t.tx_type,
                    "asset": t.asset,
                    "amount": _str_decimal(t.amount),
                }
                for t in move_txs
            ]
        )

    return AnalyticsSummary(
        as_of_date=snapshot_date,
        net_worth_usd=net_worth,
        allocation_by_asset=json.dumps(
            [
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
            ]
        ),
        allocation_by_source=json.dumps(
            [
                {
                    "source": row.bucket,
                    "usd_value": _str_decimal(row.usd_value),
                    "percentage": _str_decimal(row.percentage),
                }
                for row in alloc_source
            ]
        ),
        allocation_by_category=json.dumps(
            [
                {
                    "category": row.bucket,
                    "usd_value": _str_decimal(row.usd_value),
                    "percentage": _str_decimal(row.percentage),
                }
                for row in alloc_category
            ]
        ),
        currency_exposure=json.dumps(
            [
                {
                    "currency": row.currency,
                    "usd_value": _str_decimal(row.usd_value),
                    "percentage": _str_decimal(row.percentage),
                }
                for row in currency_exposure
            ]
        ),
        risk_metrics=json.dumps(
            {
                "concentration_percentage": _str_decimal(risk.concentration_percentage),
                "hhi_index": _str_decimal(risk.hhi_index),
                "top_5_assets": [
                    {
                        "asset": row.asset,
                        "sources": list(row.sources),
                        "usd_value": _str_decimal(row.usd_value),
                        "price": _str_decimal(row.price),
                        "percentage": _str_decimal(row.percentage),
                    }
                    for row in risk.top_5_assets
                ],
            }
        ),
        warnings=tuple(warnings),
        earn_positions=earn_positions,
        weekly_pnl=weekly_pnl,
        recent_transactions=recent_transactions,
    )
