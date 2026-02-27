"""Tests for portfolio PnL analytics."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pfm.analytics.pnl import PnlPeriod, compute_pnl
from pfm.db.models import Snapshot, Transaction, TransactionType


async def test_compute_pnl_daily(repo):
    await repo.save_snapshots(
        [
            Snapshot(date=date(2024, 1, 14), source="s", asset="BTC", amount=Decimal(1), usd_value=Decimal(100)),
            Snapshot(date=date(2024, 1, 14), source="s", asset="ETH", amount=Decimal(1), usd_value=Decimal(50)),
        ]
    )
    await repo.save_snapshots(
        [
            Snapshot(date=date(2024, 1, 15), source="s", asset="BTC", amount=Decimal(1), usd_value=Decimal(120)),
            Snapshot(date=date(2024, 1, 15), source="s", asset="ETH", amount=Decimal(1), usd_value=Decimal(40)),
        ]
    )

    result = await compute_pnl(repo, date(2024, 1, 15), PnlPeriod.DAILY)
    assert result.start_date == date(2024, 1, 14)
    assert result.end_date == date(2024, 1, 15)
    assert result.start_value == Decimal(150)
    assert result.end_value == Decimal(160)
    assert result.absolute_change == Decimal(10)
    assert result.percentage_change == Decimal("6.666666666666666666666666667")
    assert result.top_gainers[0].asset == "BTC"
    assert result.top_losers[0].asset == "ETH"


async def test_compute_pnl_missing_start_date_fallback(repo):
    await repo.save_snapshot(
        Snapshot(date=date(2024, 1, 1), source="s", asset="BTC", amount=Decimal(1), usd_value=Decimal(100))
    )
    await repo.save_snapshot(
        Snapshot(date=date(2024, 1, 8), source="s", asset="BTC", amount=Decimal(1), usd_value=Decimal(120))
    )

    result = await compute_pnl(repo, date(2024, 1, 9), PnlPeriod.WEEKLY)
    assert result.start_date == date(2024, 1, 1)
    assert result.end_date == date(2024, 1, 8)
    assert result.absolute_change == Decimal(20)
    assert any("No snapshot on" in note for note in result.notes)


async def test_compute_pnl_all_time_with_cost_basis(repo):
    await repo.save_snapshots(
        [
            Snapshot(date=date(2024, 1, 1), source="s", asset="BTC", amount=Decimal(1), usd_value=Decimal(100)),
            Snapshot(date=date(2024, 1, 15), source="s", asset="BTC", amount=Decimal(2), usd_value=Decimal(260)),
        ]
    )
    await repo.save_transactions(
        [
            Transaction(
                date=date(2024, 1, 5),
                source="s",
                tx_type=TransactionType.TRADE,
                asset="BTC",
                amount=Decimal(1),
                usd_value=Decimal(120),
            ),
            Transaction(
                date=date(2024, 1, 10),
                source="s",
                tx_type=TransactionType.DEPOSIT,
                asset="BTC",
                amount=Decimal(1),
                usd_value=Decimal(140),
            ),
        ]
    )

    result = await compute_pnl(repo, date(2024, 1, 15), PnlPeriod.ALL_TIME)
    assert result.start_date == date(2024, 1, 1)
    assert result.end_date == date(2024, 1, 15)
    assert result.absolute_change == Decimal(160)
    assert result.top_gainers[0].cost_basis_value == Decimal(260)


async def test_compute_pnl_no_data(repo):
    result = await compute_pnl(repo, date(2024, 1, 15), PnlPeriod.MONTHLY)
    assert result.start_date is None
    assert result.end_date is None
    assert result.start_value == Decimal(0)
    assert result.end_value == Decimal(0)
    assert result.absolute_change == Decimal(0)
    assert result.percentage_change == Decimal(0)
    assert result.notes
