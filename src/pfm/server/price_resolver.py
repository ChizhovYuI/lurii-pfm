"""Resolve USD values for transactions using cached prices and stablecoin pegging."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import date

    from pfm.db.models import Transaction
    from pfm.db.repository import Repository

_STABLECOIN_TICKERS: frozenset[str] = frozenset({"USDC", "USDT", "DAI", "BUSD", "TUSD", "USDP", "FDUSD"})
_FIAT_USD: frozenset[str] = frozenset({"USD"})


def resolve_usd(tx: Transaction, prices: dict[str, Decimal]) -> Decimal:
    """Return stored usd_value, or estimate from price map when zero."""
    if tx.usd_value:
        return tx.usd_value
    ticker = tx.asset.upper()
    if ticker in _STABLECOIN_TICKERS or ticker in _FIAT_USD:
        return abs(tx.amount)
    price = prices.get(ticker)
    if price:
        return abs(tx.amount) * price
    return Decimal(0)


async def build_price_map(repo: Repository, dates: list[date]) -> dict[str, Decimal]:
    """Build asset -> USD price map from the prices table.

    Tries exact transaction dates first (most recent), then falls back
    to the latest available price date in the DB.
    """
    for d in sorted(dates, reverse=True):
        result = await _prices_for_date(repo, d)
        if result:
            return result
    # No prices on any transaction date — use latest available price date.
    latest = await _latest_price_date(repo)
    if latest:
        result = await _prices_for_date(repo, latest)
        if result:
            return result
    return {}


async def _latest_price_date(repo: Repository) -> date | None:
    """Return the most recent date that has cached prices."""
    cursor = await repo.connection.execute("SELECT MAX(date) FROM prices")
    row = await cursor.fetchone()
    if row is None or row[0] is None:
        return None
    from datetime import date as date_cls

    return date_cls.fromisoformat(row[0])


async def _prices_for_date(repo: Repository, d: date) -> dict[str, Decimal]:
    """Return asset->USD price map for a specific date, or empty dict."""
    prices = await repo.get_prices_by_date(d)
    if not prices:
        return {}
    result: dict[str, Decimal] = {}
    for p in prices:
        if p.currency == "USD" and p.asset.upper() not in result:
            result[p.asset.upper()] = p.price
    return result
