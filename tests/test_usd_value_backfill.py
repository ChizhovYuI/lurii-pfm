"""Tests for transaction usd_value backfill."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pfm.analytics.usd_value_backfill import backfill_transaction_usd_values
from pfm.db.models import Transaction, TransactionType
from pfm.db.repository import Repository

_D1 = date(2026, 4, 1)
_D2 = date(2026, 4, 2)


class _StubPricing:
    """Minimal stand-in exposing get_price_usd_on with preset answers."""

    def __init__(self, prices: dict[tuple[str, date], Decimal | None]) -> None:
        self._prices = prices
        self.calls: list[tuple[str, date]] = []

    async def peek_price_usd_on(self, ticker: str, on_date: date) -> tuple[str, Decimal | None]:
        # No persistent cache in the stub: every key needs a "network" lookup,
        # so the budget/dedup assertions exercise get_price_usd_on call counts.
        _ = (ticker, on_date)
        return "unknown", None

    async def get_price_usd_on(self, ticker: str, on_date: date) -> Decimal | None:
        self.calls.append((ticker.upper(), on_date))
        return self._prices.get((ticker.upper(), on_date))


def _tx(*, asset: str, amount: Decimal, usd_value: Decimal, d: date, tx_id: str) -> Transaction:
    return Transaction(
        date=d,
        source="okx",
        source_name="okx-main",
        tx_type=TransactionType.WITHDRAWAL,
        asset=asset,
        amount=amount,
        usd_value=usd_value,
        tx_id=tx_id,
    )


async def test_backfill_values_zero_usd_and_dedups_lookups(tmp_path):
    async with Repository(tmp_path / "x.db") as repo:
        await repo.save_transactions(
            [
                _tx(asset="BTC", amount=Decimal("0.5"), usd_value=Decimal(0), d=_D1, tx_id="b1"),
                _tx(asset="BTC", amount=Decimal(1), usd_value=Decimal(0), d=_D1, tx_id="b2"),
                _tx(asset="DOGE", amount=Decimal(100), usd_value=Decimal(0), d=_D2, tx_id="dg"),
                _tx(asset="USD", amount=Decimal(50), usd_value=Decimal(50), d=_D1, tx_id="already"),
            ]
        )
        stub = _StubPricing({("BTC", _D1): Decimal(40000)})

        summary = await backfill_transaction_usd_values(repo, stub)

        assert summary["scanned"] == 3  # the pre-valued USD row is excluded
        assert summary["updated"] == 2
        assert summary["no_price"] == 1  # DOGE has no price
        assert summary["unique_lookups"] == 2  # (BTC, D1) and (DOGE, D2)
        assert len(stub.calls) == 2  # BTC fetched once despite two rows

        by_tx = {t.tx_id: t for t in await repo.get_transactions()}
        assert by_tx["b1"].usd_value == Decimal(20000)  # 0.5 * 40000
        assert by_tx["b2"].usd_value == Decimal(40000)
        assert by_tx["dg"].usd_value == Decimal(0)  # untouched
        assert by_tx["already"].usd_value == Decimal(50)


async def test_backfill_respects_limit(tmp_path):
    async with Repository(tmp_path / "x.db") as repo:
        await repo.save_transactions(
            [
                _tx(asset="BTC", amount=Decimal(1), usd_value=Decimal(0), d=_D1, tx_id="b1"),
                _tx(asset="ETH", amount=Decimal(1), usd_value=Decimal(0), d=_D2, tx_id="e1"),
            ]
        )
        stub = _StubPricing({("BTC", _D1): Decimal(40000), ("ETH", _D2): Decimal(2000)})

        summary = await backfill_transaction_usd_values(repo, stub, limit=1)

        assert summary["scanned"] == 1
        assert summary["updated"] == 1


async def test_backfill_respects_max_lookups(tmp_path):
    async with Repository(tmp_path / "x.db") as repo:
        # Oldest-first default order: BTC@_D1 is reached before ETH@_D2.
        await repo.save_transactions(
            [
                _tx(asset="BTC", amount=Decimal(1), usd_value=Decimal(0), d=_D1, tx_id="b1"),
                _tx(asset="ETH", amount=Decimal(1), usd_value=Decimal(0), d=_D2, tx_id="e1"),
            ]
        )
        stub = _StubPricing({("BTC", _D1): Decimal(40000), ("ETH", _D2): Decimal(2000)})

        summary = await backfill_transaction_usd_values(repo, stub, max_lookups=1)

        # The budget stops the scan before ETH's distinct lookup.
        assert summary["unique_lookups"] == 1
        assert len(stub.calls) == 1
        assert summary["updated"] == 1
        by_tx = {t.tx_id: t for t in await repo.get_transactions()}
        assert by_tx["b1"].usd_value == Decimal(40000)
        assert by_tx["e1"].usd_value == Decimal(0)  # deferred to a later run


class _PeekStub:
    """Stub whose peek reports preset miss keys as free (no-network) misses."""

    def __init__(self, miss_keys: set[tuple[str, date]], prices: dict[tuple[str, date], Decimal | None]) -> None:
        self._miss = miss_keys
        self._prices = prices
        self.network_calls: list[tuple[str, date]] = []

    async def peek_price_usd_on(self, ticker: str, on_date: date) -> tuple[str, Decimal | None]:
        if (ticker.upper(), on_date) in self._miss:
            return "miss", None
        return "unknown", None

    async def get_price_usd_on(self, ticker: str, on_date: date) -> Decimal | None:
        self.network_calls.append((ticker.upper(), on_date))
        return self._prices.get((ticker.upper(), on_date))


async def test_backfill_miss_sentinel_does_not_consume_budget(tmp_path):
    """A cached-miss row is free, so it cannot starve a priceable row behind it."""
    async with Repository(tmp_path / "x.db") as repo:
        # newest_first: the DOGE@_D2 miss is scanned before BTC@_D1.
        await repo.save_transactions(
            [
                _tx(asset="BTC", amount=Decimal(1), usd_value=Decimal(0), d=_D1, tx_id="b1"),
                _tx(asset="DOGE", amount=Decimal(100), usd_value=Decimal(0), d=_D2, tx_id="dg"),
            ]
        )
        stub = _PeekStub(miss_keys={("DOGE", _D2)}, prices={("BTC", _D1): Decimal(40000)})

        summary = await backfill_transaction_usd_values(repo, stub, newest_first=True, max_lookups=1)

        # The miss did not spend the single-lookup budget, so BTC still got valued.
        assert stub.network_calls == [("BTC", _D1)]
        assert summary["unique_lookups"] == 1
        assert summary["updated"] == 1
        assert summary["no_price"] == 1
        by_tx = {t.tx_id: t for t in await repo.get_transactions()}
        assert by_tx["b1"].usd_value == Decimal(40000)
        assert by_tx["dg"].usd_value == Decimal(0)


async def test_backfill_max_lookups_does_not_charge_cache_reuse(tmp_path):
    async with Repository(tmp_path / "x.db") as repo:
        # Two BTC rows share (BTC, _D1): the second reuses the cached price and
        # must not consume the lookup budget.
        await repo.save_transactions(
            [
                _tx(asset="BTC", amount=Decimal("0.5"), usd_value=Decimal(0), d=_D1, tx_id="b1"),
                _tx(asset="BTC", amount=Decimal(1), usd_value=Decimal(0), d=_D1, tx_id="b2"),
                _tx(asset="ETH", amount=Decimal(1), usd_value=Decimal(0), d=_D2, tx_id="e1"),
            ]
        )
        stub = _StubPricing({("BTC", _D1): Decimal(40000), ("ETH", _D2): Decimal(2000)})

        summary = await backfill_transaction_usd_values(repo, stub, max_lookups=1)

        assert summary["unique_lookups"] == 1
        assert summary["updated"] == 2  # both BTC rows valued from one lookup
        by_tx = {t.tx_id: t for t in await repo.get_transactions()}
        assert by_tx["b1"].usd_value == Decimal(20000)
        assert by_tx["b2"].usd_value == Decimal(40000)
        assert by_tx["e1"].usd_value == Decimal(0)
