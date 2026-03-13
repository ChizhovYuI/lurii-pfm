"""Tests for portfolio PnL analytics."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pfm.analytics.pnl import PnlPeriod, compute_pnl, compute_pnl_exact
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


async def test_compute_pnl_uses_resolved_snapshots_for_stale_sources(repo):
    await repo.save_snapshots(
        [
            Snapshot(date=date(2024, 1, 1), source="wise", asset="USD", amount=Decimal(100), usd_value=Decimal(100)),
            Snapshot(date=date(2024, 1, 1), source="okx", asset="BTC", amount=Decimal("0.01"), usd_value=Decimal(400)),
            Snapshot(date=date(2024, 1, 2), source="wise", asset="USD", amount=Decimal(110), usd_value=Decimal(110)),
        ]
    )

    result = await compute_pnl(repo, date(2024, 1, 2), PnlPeriod.DAILY)
    assert result.start_value == Decimal(500)
    assert result.end_value == Decimal(510)
    assert result.absolute_change == Decimal(10)


async def test_compute_pnl_exact_returns_unavailable_when_start_date_missing(repo):
    await repo.save_snapshots(
        [
            Snapshot(date=date(2024, 1, 1), source="s", asset="BTC", amount=Decimal(1), usd_value=Decimal(100)),
            Snapshot(date=date(2024, 1, 31), source="s", asset="BTC", amount=Decimal(1), usd_value=Decimal(130)),
        ]
    )

    result = await compute_pnl_exact(repo, date(2024, 1, 31), PnlPeriod.ONE_MONTH)
    assert result.start_date is None
    assert result.end_date is None
    assert any("PnL is unavailable" in note for note in result.notes)


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


async def test_compute_pnl_sell_trade_keeps_average_cost(repo):
    await repo.save_snapshots(
        [
            Snapshot(date=date(2024, 1, 1), source="s", asset="BTC", amount=Decimal(0), usd_value=Decimal(0)),
            Snapshot(date=date(2024, 1, 15), source="s", asset="BTC", amount=Decimal(1), usd_value=Decimal(130)),
        ]
    )
    await repo.save_transactions(
        [
            Transaction(
                date=date(2024, 1, 5),
                source="s",
                source_name="s-main",
                tx_type=TransactionType.TRADE,
                asset="BTC",
                amount=Decimal(2),
                usd_value=Decimal(240),
                trade_side="buy",
            ),
            Transaction(
                date=date(2024, 1, 10),
                source="s",
                source_name="s-main",
                tx_type=TransactionType.TRADE,
                asset="BTC",
                amount=Decimal(1),
                usd_value=Decimal(150),
                trade_side="sell",
            ),
        ]
    )

    result = await compute_pnl(repo, date(2024, 1, 15), PnlPeriod.ALL_TIME)
    assert result.top_gainers[0].cost_basis_value == Decimal(120)


async def test_compute_pnl_trade_without_trade_side_keeps_legacy_behavior(repo):
    await repo.save_snapshots(
        [
            Snapshot(date=date(2024, 1, 1), source="s", asset="BTC", amount=Decimal(0), usd_value=Decimal(0)),
            Snapshot(date=date(2024, 1, 15), source="s", asset="BTC", amount=Decimal(1), usd_value=Decimal(130)),
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
                trade_side="",
            ),
        ]
    )

    result = await compute_pnl(repo, date(2024, 1, 15), PnlPeriod.ALL_TIME)
    assert result.top_gainers[0].cost_basis_value == Decimal(120)


async def test_compute_pnl_no_data(repo):
    result = await compute_pnl(repo, date(2024, 1, 15), PnlPeriod.MONTHLY)
    assert result.start_date is None
    assert result.end_date is None
    assert result.start_value == Decimal(0)
    assert result.end_value == Decimal(0)
    assert result.absolute_change == Decimal(0)
    assert result.percentage_change == Decimal(0)
    assert result.notes


async def test_compute_pnl_monthly_uses_first_day_of_month(repo):
    await repo.save_snapshots(
        [
            Snapshot(date=date(2024, 1, 1), source="s", asset="BTC", amount=Decimal(1), usd_value=Decimal(100)),
            Snapshot(date=date(2024, 1, 15), source="s", asset="BTC", amount=Decimal(1), usd_value=Decimal(120)),
            Snapshot(date=date(2024, 1, 31), source="s", asset="BTC", amount=Decimal(1), usd_value=Decimal(150)),
        ]
    )

    result = await compute_pnl(repo, date(2024, 1, 31), PnlPeriod.MONTHLY)
    assert result.start_date == date(2024, 1, 1)
    assert result.end_date == date(2024, 1, 31)
    assert result.start_value == Decimal(100)
    assert result.end_value == Decimal(150)
    assert result.absolute_change == Decimal(50)


async def test_compute_pnl_cost_basis_removes_asset_when_fully_exited(repo):
    await repo.save_snapshots(
        [
            Snapshot(date=date(2024, 1, 1), source="s", asset="BTC", amount=Decimal(1), usd_value=Decimal(100)),
            Snapshot(date=date(2024, 1, 31), source="s", asset="BTC", amount=Decimal(0), usd_value=Decimal(0)),
        ]
    )
    await repo.save_transactions(
        [
            Transaction(
                date=date(2024, 1, 5),
                source="s",
                tx_type=TransactionType.DEPOSIT,
                asset="BTC",
                amount=Decimal(1),
                usd_value=Decimal(100),
            ),
            Transaction(
                date=date(2024, 1, 10),
                source="s",
                tx_type=TransactionType.DEPOSIT,
                asset="BTC",
                amount=Decimal(1),
                usd_value=Decimal(0),
            ),
            Transaction(
                date=date(2024, 1, 20),
                source="s",
                tx_type=TransactionType.WITHDRAWAL,
                asset="BTC",
                amount=Decimal(2),
                usd_value=Decimal(200),
            ),
        ]
    )

    result = await compute_pnl(repo, date(2024, 1, 31), PnlPeriod.ALL_TIME)
    btc_row = next(row for row in result.top_losers if row.asset == "BTC")
    assert btc_row.end_value == Decimal(0)
    assert btc_row.cost_basis_value is None
