"""Tests for yield tracker analytics."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from pfm.analytics.yield_tracker import compute_yield
from pfm.db.models import Snapshot, Transaction, TransactionType


async def test_compute_yield_basic_balance_diff(repo):
    await repo.save_snapshots(
        [
            Snapshot(date=date(2024, 1, 1), source="blend", asset="USDC", amount=Decimal(100), usd_value=Decimal(100)),
            Snapshot(date=date(2024, 1, 31), source="blend", asset="USDC", amount=Decimal(110), usd_value=Decimal(110)),
        ]
    )

    result = await compute_yield(repo, "blend", "USDC", date(2024, 1, 1), date(2024, 1, 31))
    assert result.principal_estimate == Decimal(100)
    assert result.current_value == Decimal(110)
    assert result.yield_amount == Decimal(10)
    assert result.yield_percentage == Decimal(10)
    assert result.annualized_rate > Decimal(0)


async def test_compute_yield_with_contributions(repo):
    await repo.save_snapshots(
        [
            Snapshot(date=date(2024, 1, 1), source="okx", asset="USDT", amount=Decimal(100), usd_value=Decimal(100)),
            Snapshot(date=date(2024, 1, 31), source="okx", asset="USDT", amount=Decimal(170), usd_value=Decimal(170)),
        ]
    )
    await repo.save_transactions(
        [
            Transaction(
                date=date(2024, 1, 15),
                source="okx",
                tx_type=TransactionType.DEPOSIT,
                asset="USDT",
                amount=Decimal(50),
                usd_value=Decimal(50),
            )
        ]
    )

    result = await compute_yield(repo, "okx", "USDT", date(2024, 1, 1), date(2024, 1, 31))
    assert result.principal_estimate == Decimal(150)
    assert result.current_value == Decimal(170)
    assert result.yield_amount == Decimal(20)
    assert result.yield_percentage == Decimal("13.33333333333333333333333333")


async def test_compute_yield_fallback_dates(repo):
    await repo.save_snapshots(
        [
            Snapshot(date=date(2024, 1, 1), source="blend", asset="USDC", amount=Decimal(100), usd_value=Decimal(100)),
            Snapshot(date=date(2024, 1, 10), source="blend", asset="USDC", amount=Decimal(102), usd_value=Decimal(102)),
        ]
    )

    result = await compute_yield(repo, "blend", "USDC", date(2024, 1, 5), date(2024, 1, 20))
    assert result.start_date == date(2024, 1, 1)
    assert result.end_date == date(2024, 1, 10)
    assert result.notes


async def test_compute_yield_no_data(repo):
    result = await compute_yield(repo, "blend", "USDC", date(2024, 1, 1), date(2024, 1, 31))
    assert result.principal_estimate == Decimal(0)
    assert result.current_value == Decimal(0)
    assert result.yield_amount == Decimal(0)
    assert result.yield_percentage == Decimal(0)
    assert result.annualized_rate == Decimal(0)
    assert result.notes


async def test_compute_yield_invalid_date_range(repo):
    with pytest.raises(ValueError, match="end date must be on or after start date"):
        await compute_yield(repo, "blend", "USDC", date(2024, 2, 1), date(2024, 1, 31))


async def test_compute_yield_clamps_negative_principal(repo):
    await repo.save_snapshots(
        [
            Snapshot(date=date(2024, 1, 1), source="okx", asset="USDT", amount=Decimal(100), usd_value=Decimal(100)),
            Snapshot(date=date(2024, 1, 31), source="okx", asset="USDT", amount=Decimal(20), usd_value=Decimal(20)),
        ]
    )
    await repo.save_transactions(
        [
            Transaction(
                date=date(2024, 1, 10),
                source="okx",
                tx_type=TransactionType.WITHDRAWAL,
                asset="USDT",
                amount=Decimal(90),
                usd_value=Decimal(90),
            ),
            Transaction(
                date=date(2024, 1, 20),
                source="okx",
                tx_type=TransactionType.FEE,
                asset="USDT",
                amount=Decimal(50),
                usd_value=Decimal(50),
            ),
        ]
    )

    result = await compute_yield(repo, "okx", "USDT", date(2024, 1, 1), date(2024, 1, 31))
    assert result.principal_estimate == Decimal(0)
    assert result.current_value == Decimal(20)
    assert result.yield_amount == Decimal(20)
    assert result.yield_percentage == Decimal(0)
    assert result.annualized_rate == Decimal(0)
    assert any("clamped to zero" in note for note in result.notes)


async def test_compute_yield_counts_transfer_and_ignores_non_matching_txs(repo):
    await repo.save_snapshots(
        [
            Snapshot(date=date(2024, 1, 1), source="blend", asset="USDC", amount=Decimal(100), usd_value=Decimal(100)),
            Snapshot(date=date(2024, 1, 31), source="blend", asset="USDC", amount=Decimal(110), usd_value=Decimal(110)),
        ]
    )
    await repo.save_transactions(
        [
            Transaction(
                date=date(2024, 1, 10),
                source="blend",
                tx_type=TransactionType.TRANSFER,
                asset="USDC",
                amount=Decimal(5),
                usd_value=Decimal(5),
            ),
            Transaction(
                date=date(2024, 1, 11),
                source="blend",
                tx_type=TransactionType.DEPOSIT,
                asset="USDC",
                amount=Decimal(5),
                usd_value=Decimal(0),
            ),
            Transaction(
                date=date(2024, 1, 12),
                source="blend",
                tx_type=TransactionType.DEPOSIT,
                asset="BTC",
                amount=Decimal("0.01"),
                usd_value=Decimal(400),
            ),
        ]
    )

    result = await compute_yield(repo, "blend", "USDC", date(2024, 1, 1), date(2024, 1, 31))
    assert result.principal_estimate == Decimal(105)
    assert result.yield_amount == Decimal(5)
