"""Tests for database models and repository."""

from datetime import date
from decimal import Decimal

import pytest

from pfm.db.models import Price, Snapshot, Transaction, TransactionType, init_db
from pfm.db.repository import Repository


@pytest.fixture
async def repo(tmp_path):
    db_path = tmp_path / "test.db"
    async with Repository(db_path) as r:
        yield r


async def test_init_db(tmp_path):
    db_path = tmp_path / "init_test.db"
    await init_db(db_path)
    assert db_path.exists()


async def test_save_and_get_snapshot(repo):
    snapshot = Snapshot(
        date=date(2024, 1, 15),
        source="test",
        asset="BTC",
        amount=Decimal("1.5"),
        usd_value=Decimal("67500.00"),
    )
    await repo.save_snapshot(snapshot)
    results = await repo.get_snapshots_by_date(date(2024, 1, 15))
    assert len(results) == 1
    assert results[0].source == "test"
    assert results[0].asset == "BTC"
    assert results[0].amount == Decimal("1.5")
    assert results[0].usd_value == Decimal("67500.00")


async def test_save_snapshots_batch(repo):
    snapshots = [
        Snapshot(date=date(2024, 1, 15), source="test", asset="BTC", amount=Decimal(1), usd_value=Decimal(45000)),
        Snapshot(date=date(2024, 1, 15), source="test", asset="ETH", amount=Decimal(10), usd_value=Decimal(25000)),
    ]
    await repo.save_snapshots(snapshots)
    results = await repo.get_snapshots_by_date(date(2024, 1, 15))
    assert len(results) == 2


async def test_get_latest_snapshots_empty(repo):
    results = await repo.get_latest_snapshots()
    assert results == []


async def test_get_latest_snapshots(repo):
    await repo.save_snapshot(
        Snapshot(date=date(2024, 1, 14), source="s1", asset="BTC", amount=Decimal(1), usd_value=Decimal(1))
    )
    await repo.save_snapshot(
        Snapshot(date=date(2024, 1, 15), source="s1", asset="ETH", amount=Decimal(2), usd_value=Decimal(2))
    )
    results = await repo.get_latest_snapshots()
    assert len(results) == 1
    assert results[0].date == date(2024, 1, 15)


async def test_save_and_get_transaction(repo):
    tx = Transaction(
        date=date(2024, 1, 15),
        source="test",
        tx_type=TransactionType.TRADE,
        asset="BTC",
        amount=Decimal("0.5"),
        usd_value=Decimal(22500),
    )
    await repo.save_transaction(tx)
    results = await repo.get_transactions(source="test")
    assert len(results) == 1
    assert results[0].tx_type == TransactionType.TRADE


async def test_get_transactions_with_filters(repo):
    txs = [
        Transaction(
            date=date(2024, 1, 10),
            source="a",
            tx_type=TransactionType.DEPOSIT,
            asset="BTC",
            amount=Decimal(1),
            usd_value=Decimal(1),
        ),
        Transaction(
            date=date(2024, 1, 20),
            source="b",
            tx_type=TransactionType.WITHDRAWAL,
            asset="ETH",
            amount=Decimal(2),
            usd_value=Decimal(2),
        ),
    ]
    await repo.save_transactions(txs)

    # Filter by source
    results = await repo.get_transactions(source="a")
    assert len(results) == 1
    assert results[0].source == "a"

    # Filter by date range
    results = await repo.get_transactions(start=date(2024, 1, 15))
    assert len(results) == 1
    assert results[0].source == "b"


async def test_save_and_get_price(repo):
    price = Price(date=date(2024, 1, 15), asset="BTC", currency="USD", price=Decimal(45000))
    await repo.save_price(price)
    result = await repo.get_price("BTC", "USD", date(2024, 1, 15))
    assert result is not None
    assert result.price == Decimal(45000)


async def test_get_price_not_found(repo):
    result = await repo.get_price("BTC", "USD", date(2024, 1, 15))
    assert result is None
