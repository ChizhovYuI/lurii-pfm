"""Integration tests for collection dispatch from DB-configured sources."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import patch

from pfm.cli import _collect_async
from pfm.collectors.base import BaseCollector
from pfm.db.models import RawBalance, Transaction, TransactionType, init_db
from pfm.db.repository import Repository
from pfm.db.source_store import SourceStore

if TYPE_CHECKING:
    from datetime import date


class _FakeWiseCollector(BaseCollector):
    source_name = "wise"

    def __init__(self, pricing, *, api_token: str) -> None:
        super().__init__(pricing)
        self._api_token = api_token

    async def fetch_raw_balances(self) -> list[RawBalance]:
        return [
            RawBalance(
                asset="USD",
                amount=Decimal(100),
                price=Decimal(1),
                raw_json=f'{{"api_token":"{self._api_token}"}}',
            )
        ]

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        return [
            Transaction(
                date=self._pricing.today(),
                source=self.source_name,
                tx_type=TransactionType.DEPOSIT,
                asset="USD",
                amount=Decimal(10),
                usd_value=Decimal(10),
                tx_id="wise-tx",
            )
        ]


class _FakeLobstrCollector(BaseCollector):
    source_name = "lobstr"

    def __init__(self, pricing, *, stellar_address: str) -> None:
        super().__init__(pricing)
        self._stellar_address = stellar_address

    async def fetch_raw_balances(self) -> list[RawBalance]:
        return [
            RawBalance(
                asset="XLM",
                amount=Decimal(50),
                price=Decimal("0.1"),
                raw_json=f'{{"address":"{self._stellar_address}"}}',
            )
        ]

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        return [
            Transaction(
                date=self._pricing.today(),
                source=self.source_name,
                tx_type=TransactionType.DEPOSIT,
                asset="XLM",
                amount=Decimal(5),
                usd_value=Decimal("0.5"),
                tx_id="lobstr-tx",
            )
        ]


async def test_collect_async_runs_all_enabled_sources(tmp_path):
    db_path = tmp_path / "collect.db"
    await init_db(db_path)
    store = SourceStore(db_path)
    await store.add("wise-main", "wise", {"api_token": "wise-token"})
    await store.add("lobstr-main", "lobstr", {"stellar_address": "GABC123"})

    settings = SimpleNamespace(database_path=db_path, coingecko_api_key="")
    registry = {"wise": _FakeWiseCollector, "lobstr": _FakeLobstrCollector}
    with patch("pfm.cli.get_settings", return_value=settings), patch("pfm.collectors.COLLECTOR_REGISTRY", registry):
        results = await _collect_async(None)

    assert len(results) == 2
    assert {r.source for r in results} == {"wise-main", "lobstr-main"}

    async with Repository(db_path) as repo:
        snapshots = await repo.get_latest_snapshots()
        transactions = await repo.get_transactions()
    assert len(snapshots) == 2
    assert {(s.source, s.source_name) for s in snapshots} == {
        ("wise", "wise-main"),
        ("lobstr", "lobstr-main"),
    }
    assert len(transactions) == 2


async def test_collect_async_source_filter_runs_one_source(tmp_path):
    db_path = tmp_path / "collect-filter.db"
    await init_db(db_path)
    store = SourceStore(db_path)
    await store.add("wise-main", "wise", {"api_token": "wise-token"})
    await store.add("lobstr-main", "lobstr", {"stellar_address": "GABC123"})

    settings = SimpleNamespace(database_path=db_path, coingecko_api_key="")
    registry = {"wise": _FakeWiseCollector, "lobstr": _FakeLobstrCollector}
    with patch("pfm.cli.get_settings", return_value=settings), patch("pfm.collectors.COLLECTOR_REGISTRY", registry):
        results = await _collect_async("wise-main")

    assert len(results) == 1
    assert results[0].source == "wise-main"

    async with Repository(db_path) as repo:
        snapshots = await repo.get_latest_snapshots()
    assert len(snapshots) == 1
    assert snapshots[0].source == "wise"
    assert snapshots[0].source_name == "wise-main"


async def test_collect_async_fetches_balances_in_parallel(tmp_path):
    db_path = tmp_path / "collect-parallel.db"
    await init_db(db_path)
    store = SourceStore(db_path)
    await store.add("wise-main", "wise", {"api_token": "wise-token"})
    await store.add("lobstr-main", "lobstr", {"stellar_address": "GABC123"})

    events: list[str] = []

    class _SlowWiseCollector(BaseCollector):
        source_name = "wise"

        def __init__(self, pricing, *, api_token: str) -> None:
            super().__init__(pricing)
            self._api_token = api_token

        async def fetch_raw_balances(self) -> list[RawBalance]:
            events.append("wise-start")
            await asyncio.sleep(0.02)
            events.append("wise-end")
            return []

        async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
            return []

    class _FastLobstrCollector(BaseCollector):
        source_name = "lobstr"

        def __init__(self, pricing, *, stellar_address: str) -> None:
            super().__init__(pricing)
            self._stellar_address = stellar_address

        async def fetch_raw_balances(self) -> list[RawBalance]:
            events.append("lobstr-start")
            return []

        async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
            return []

    settings = SimpleNamespace(database_path=db_path, coingecko_api_key="")
    registry = {"wise": _SlowWiseCollector, "lobstr": _FastLobstrCollector}
    with patch("pfm.cli.get_settings", return_value=settings), patch("pfm.collectors.COLLECTOR_REGISTRY", registry):
        results = await _collect_async(None)

    assert len(results) == 2
    # With parallel fetch, lobstr-start should appear before wise-end
    assert events == ["wise-start", "lobstr-start", "wise-end"]
