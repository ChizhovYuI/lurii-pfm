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


async def test_save_snapshots_replaces_same_source_and_date(repo):
    first_batch = [
        Snapshot(date=date(2024, 1, 15), source="wise", asset="GBP", amount=Decimal(100), usd_value=Decimal(125)),
        Snapshot(date=date(2024, 1, 15), source="wise", asset="EUR", amount=Decimal(50), usd_value=Decimal(55)),
    ]
    second_batch = [
        Snapshot(date=date(2024, 1, 15), source="wise", asset="GBP", amount=Decimal(120), usd_value=Decimal(150)),
    ]

    await repo.save_snapshots(first_batch)
    await repo.save_snapshots(second_batch)

    results = await repo.get_snapshots_by_date(date(2024, 1, 15))
    assert len(results) == 1
    assert results[0].source == "wise"
    assert results[0].asset == "GBP"
    assert results[0].amount == Decimal(120)
    assert results[0].usd_value == Decimal(150)


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


async def test_get_latest_snapshots_resolves_per_source(repo):
    """Stale sources (e.g., KBank) are included by resolving per-source latest date."""
    await repo.save_snapshot(
        Snapshot(date=date(2024, 1, 10), source="kbank", asset="THB", amount=Decimal(1000), usd_value=Decimal(28))
    )
    await repo.save_snapshot(
        Snapshot(date=date(2024, 1, 15), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(45000))
    )
    results = await repo.get_latest_snapshots()
    assert len(results) == 2
    sources = {r.source for r in results}
    assert sources == {"kbank", "okx"}


async def test_get_snapshots_resolved(repo):
    """get_snapshots_resolved returns latest per source up to target date."""
    await repo.save_snapshots(
        [
            Snapshot(date=date(2024, 1, 5), source="kbank", asset="THB", amount=Decimal(900), usd_value=Decimal(25)),
            Snapshot(date=date(2024, 1, 10), source="kbank", asset="THB", amount=Decimal(1000), usd_value=Decimal(28)),
            Snapshot(date=date(2024, 1, 12), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(45000)),
            Snapshot(date=date(2024, 1, 12), source="okx", asset="ETH", amount=Decimal(5), usd_value=Decimal(12000)),
        ]
    )
    results = await repo.get_snapshots_resolved(date(2024, 1, 12))
    assert len(results) == 3  # kbank(Jan 10) + okx BTC(Jan 12) + okx ETH(Jan 12)
    kbank = [r for r in results if r.source == "kbank"]
    assert len(kbank) == 1
    assert kbank[0].date == date(2024, 1, 10)
    assert kbank[0].amount == Decimal(1000)
    okx = [r for r in results if r.source == "okx"]
    assert len(okx) == 2


async def test_get_snapshots_resolved_ignores_future(repo):
    """Snapshots after target date are excluded."""
    await repo.save_snapshots(
        [
            Snapshot(date=date(2024, 1, 10), source="kbank", asset="THB", amount=Decimal(1000), usd_value=Decimal(28)),
            Snapshot(date=date(2024, 1, 20), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(45000)),
        ]
    )
    results = await repo.get_snapshots_resolved(date(2024, 1, 15))
    assert len(results) == 1
    assert results[0].source == "kbank"


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


async def test_save_and_get_analytics_metric(repo):
    metric_date = date(2024, 1, 15)
    await repo.save_analytics_metric(metric_date, "net_worth", '{"usd":"123.45"}')
    await repo.save_analytics_metric(metric_date, "allocation", '{"top":"BTC"}')

    metrics = await repo.get_analytics_metrics_by_date(metric_date)
    assert metrics["net_worth"] == '{"usd":"123.45"}'
    assert metrics["allocation"] == '{"top":"BTC"}'


async def test_save_analytics_metric_replaces_existing(repo):
    metric_date = date(2024, 1, 15)
    await repo.save_analytics_metric(metric_date, "net_worth", '{"usd":"100"}')
    await repo.save_analytics_metric(metric_date, "net_worth", '{"usd":"200"}')

    metrics = await repo.get_analytics_metrics_by_date(metric_date)
    assert metrics["net_worth"] == '{"usd":"200"}'
