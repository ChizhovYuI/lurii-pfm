"""Tests for the abstract collector base class."""

import socket
from datetime import date
from decimal import Decimal

import httpx
import pytest

from pfm.collectors.base import BaseCollector
from pfm.collectors.cash import CashCollector
from pfm.db.models import SYNC_MARKER_ASSET, Snapshot, Transaction, TransactionType
from pfm.db.repository import Repository
from pfm.pricing.coingecko import PricingService


class MockCollector(BaseCollector):
    """Test collector that returns fixed data."""

    source_name = "mock"

    def __init__(self, pricing, snapshots=None, transactions=None, fail_balances=False, fail_transactions=False):
        super().__init__(pricing)
        self._snapshots = snapshots or []
        self._transactions = transactions or []
        self._fail_balances = fail_balances
        self._fail_transactions = fail_transactions

    async def fetch_raw_balances(self):
        return []

    async def fetch_balances(self):
        if self._fail_balances:
            msg = "balance fetch failed"
            raise ValueError(msg)
        return self._snapshots

    async def fetch_transactions(self, since=None):
        if self._fail_transactions:
            msg = "transaction fetch failed"
            raise ValueError(msg)
        return self._transactions


class RawOnlyCollector(BaseCollector):
    source_name = "raw-only"

    def __init__(self, pricing, raw_balances=None, transactions=None):
        super().__init__(pricing)
        self._raw_balances = raw_balances or []
        self._transactions = transactions or []

    async def fetch_raw_balances(self):
        return self._raw_balances

    async def fetch_transactions(self, since=None):
        return self._transactions


@pytest.fixture
async def repo(tmp_path):
    db_path = tmp_path / "test.db"
    async with Repository(db_path) as r:
        yield r


@pytest.fixture
def pricing():
    svc = PricingService()
    svc._coins_by_symbol = {}
    return svc


async def test_collect_saves_snapshots(repo, pricing):
    snapshots = [
        Snapshot(date=date(2024, 1, 15), source="mock", asset="BTC", amount=Decimal(1), usd_value=Decimal(45000)),
    ]
    collector = MockCollector(pricing, snapshots=snapshots)
    result = await collector.collect(repo)

    assert result.source == "mock"
    assert result.snapshots_count == 1
    assert result.errors == []

    db_snapshots = await repo.get_snapshots_by_date(date(2024, 1, 15))
    assert len(db_snapshots) == 1


async def test_collect_saves_transactions(repo, pricing):
    txs = [
        Transaction(
            date=date(2024, 1, 15),
            source="mock",
            tx_type=TransactionType.DEPOSIT,
            asset="BTC",
            amount=Decimal(1),
            usd_value=Decimal(45000),
        ),
    ]
    collector = MockCollector(pricing, transactions=txs)
    result = await collector.collect(repo)

    assert result.transactions_count == 1
    assert result.errors == []


async def test_collect_graceful_degradation_balances(repo, pricing):
    collector = MockCollector(pricing, fail_balances=True)
    result = await collector.collect(repo)

    assert result.snapshots_count == 0
    assert len(result.errors) == 1
    assert "balance" in result.errors[0].lower()


async def test_collect_graceful_degradation_transactions(repo, pricing):
    collector = MockCollector(pricing, fail_transactions=True)
    result = await collector.collect(repo)

    assert result.transactions_count == 0
    assert len(result.errors) == 1
    assert "transaction" in result.errors[0].lower()


async def test_collect_both_fail(repo, pricing):
    collector = MockCollector(pricing, fail_balances=True, fail_transactions=True)
    result = await collector.collect(repo)

    assert result.snapshots_count == 0
    assert result.transactions_count == 0
    assert len(result.errors) == 2


async def test_collector_result_has_duration(repo, pricing):
    collector = MockCollector(pricing)
    result = await collector.collect(repo)
    assert result.duration_seconds >= 0


async def test_collect_formats_dns_error_with_country_access_hint(repo, pricing):
    transactions_called = False

    class NetworkBlockedCollector(MockCollector):
        async def fetch_balances(self):
            exc = httpx.ConnectError("connect error")
            exc.__cause__ = socket.gaierror(8, "nodename nor servname provided, or not known")
            raise exc

        async def fetch_transactions(self, since=None):
            nonlocal transactions_called
            transactions_called = True
            return []

    collector = NetworkBlockedCollector(pricing)
    result = await collector.collect(repo)

    assert len(result.errors) == 1
    assert (
        "service access appears restricted from your current network or region. try a vpn and retry."
        in result.errors[0]
    )
    assert transactions_called is False


async def test_collect_saves_sync_marker_for_zero_balance_source(repo, pricing):
    collector = RawOnlyCollector(pricing, raw_balances=[])

    result = await collector.collect(repo)

    assert result.snapshots_count == 0

    db_snapshots = await repo.get_snapshots_by_date(pricing.today())
    assert len(db_snapshots) == 1
    assert db_snapshots[0].asset == SYNC_MARKER_ASSET
    assert db_snapshots[0].usd_value == 0


async def test_cash_collector_does_not_save_sync_marker_for_empty_manual_source(repo, pricing):
    collector = CashCollector(pricing, fiat_currencies="USD")

    result = await collector.collect(repo)

    assert result.snapshots_count == 0
    assert await repo.get_snapshots_by_date(pricing.today()) == []


async def test_close_closes_owned_httpx_clients(pricing):
    collector = MockCollector(pricing)
    collector._client = httpx.AsyncClient()
    collector._extra_client = httpx.AsyncClient()

    await collector.close()

    assert collector._client.is_closed
    assert collector._extra_client.is_closed
