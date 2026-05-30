"""Backfill USD valuation for transactions ingested without it.

Most collectors defer historical pricing and store ``usd_value=0`` (notably all
crypto rows). This module values those rows from CoinGecko historical prices,
deduplicating lookups by ``(asset, date)`` and persisting each price into the
date-keyed cache so reruns are cheap.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import date
    from decimal import Decimal

    from pfm.db.repository import Repository
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_PROGRESS_EVERY = 50


async def backfill_transaction_usd_values(
    repo: Repository,
    pricing: PricingService,
    *,
    limit: int | None = None,
    newest_first: bool = False,
    max_lookups: int | None = None,
) -> dict[str, int]:
    """Value zero-USD transactions from historical prices.

    Args:
        repo: Repository for reading/updating transactions.
        pricing: Pricing service providing ``get_price_usd_on``.
        limit: Optional cap on the number of transactions processed — useful for
            incremental runs within a rate-limit budget.
        newest_first: Process most-recent transactions first (the bounded
            post-collect forward-fill); the full sweep uses oldest-first.
        max_lookups: Optional cap on the number of genuine network price lookups
            performed (cache hits and miss-sentinel hits are free and never count
            against it). CoinGecko is serialized at ~2s/request, so this bounds
            wall-clock: once the budget is spent the scan keeps valuing rows that
            resolve from the in-run cache but performs no further network calls.
            ``None`` = unbounded (the full manual sweep).

    Returns a summary dict: ``scanned``, ``updated``, ``no_price``,
    ``unique_lookups`` (the count of genuine network lookups performed).
    """
    txs = await repo.get_transactions_missing_usd_value(newest_first=newest_first, limit=limit)

    price_cache: dict[tuple[str, date], Decimal | None] = {}
    updates: list[tuple[int, Decimal]] = []
    no_price = 0
    scanned = 0
    network_lookups = 0

    for tx in txs:
        if tx.id is None:
            continue
        scanned += 1
        key = (tx.asset.upper(), tx.date)
        if key not in price_cache:
            # Resolve from the cache/miss-sentinel first (free); spend the lookup
            # budget only on genuine network calls. ``continue`` (not ``break``)
            # so a leading wall of cached-miss rows cannot starve priceable rows
            # behind it, and later cache-hit rows are still valued.
            status, price = await pricing.peek_price_usd_on(tx.asset, tx.date)
            if status == "unknown":
                if max_lookups is not None and network_lookups >= max_lookups:
                    continue
                price = await pricing.get_price_usd_on(tx.asset, tx.date)
                network_lookups += 1
            price_cache[key] = price
        price = price_cache[key]
        if price is None or price == 0:
            no_price += 1
            continue
        updates.append((tx.id, abs(tx.amount) * price))
        if scanned % _PROGRESS_EVERY == 0:
            logger.info("usd_value backfill: %d/%d scanned, %d valued", scanned, len(txs), len(updates))

    await repo.update_transaction_usd_values(updates)
    summary = {
        "scanned": scanned,
        "updated": len(updates),
        "no_price": no_price,
        "unique_lookups": network_lookups,
    }
    logger.info("usd_value backfill complete: %s", summary)
    return summary
