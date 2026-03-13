"""Shared helper to build AnalyticsSummary from live-computed metrics."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import TYPE_CHECKING

from pfm.server.serializers import _str_decimal

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

    snapshots = await repo.get_snapshots_resolved(snapshot_date)

    # Compute data warnings (stale KBank, missing sources)
    enabled_sources = []
    if db_path is not None:
        from pfm.db.source_store import SourceStore

        store = SourceStore(db_path)
        enabled_sources = await store.list_enabled()
    warnings = compute_data_warnings(snapshots, enabled_sources, snapshot_date)

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
    )
