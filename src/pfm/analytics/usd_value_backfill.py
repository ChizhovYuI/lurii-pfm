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
    from collections.abc import Awaitable, Callable
    from datetime import date
    from decimal import Decimal

    from pfm.db.repository import Repository
    from pfm.pricing.coingecko import PricingService

    # (scanned, total, valued) — emitted periodically during a backfill run.
    BackfillProgress = Callable[[int, int, int], Awaitable[None]]

logger = logging.getLogger(__name__)

_PROGRESS_EVERY = 50

# Defaults for the bounded post-collect forward-fill (shared by the server's
# valuation background job and the ``pfm collect`` CLI). ``MAX_LOOKUPS`` bounds
# wall-clock: CoinGecko is serialized at ~2.1s/request, so 20 distinct
# (asset, date) network lookups cap the added latency at ~45s. The backlog
# drains across successive collects. A full manual sweep (the ``backfill_usd_values``
# MCP tool) passes ``limit=None``/``max_lookups=None`` to ignore these.
FORWARD_FILL_LIMIT = 200
FORWARD_FILL_MAX_LOOKUPS = 20


async def backfill_transaction_usd_values(  # noqa: PLR0913 - keyword-only tuning knobs
    repo: Repository,
    pricing: PricingService,
    *,
    limit: int | None = None,
    newest_first: bool = False,
    max_lookups: int | None = None,
    on_progress: BackfillProgress | None = None,
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
        on_progress: Optional async callback ``(scanned, total, valued)`` invoked
            periodically (and once at the end) so a caller can surface progress.

    Returns a summary dict: ``scanned``, ``updated``, ``no_price``,
    ``unique_lookups`` (the count of genuine network lookups performed).
    """
    txs = await repo.get_transactions_missing_usd_value(newest_first=newest_first, limit=limit)
    total = len(txs)

    price_cache: dict[tuple[str, date], Decimal | None] = {}
    updates: list[tuple[int, Decimal]] = []
    no_price = 0
    scanned = 0
    network_lookups = 0
    last_emitted = 0

    for tx in txs:
        if tx.id is None:
            continue
        scanned += 1
        # Emit progress per Nth *scanned* row — before the no_price/budget
        # ``continue`` paths, so a backlog of unpriceable rows still ticks the
        # indicator instead of looking hung. ``valued`` lags by the in-flight row;
        # the authoritative final count rides on the ``backfill_completed`` event.
        if scanned % _PROGRESS_EVERY == 0:
            logger.info("usd_value backfill: %d/%d scanned, %d valued", scanned, total, len(updates))
            if on_progress is not None:
                await on_progress(scanned, total, len(updates))
            last_emitted = scanned
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

    await repo.update_transaction_usd_values(updates)
    # Final tick — skip when the last periodic emit already covered this scanned
    # count (avoids a duplicate when ``total`` is a multiple of ``_PROGRESS_EVERY``).
    if on_progress is not None and (total == 0 or scanned != last_emitted):
        await on_progress(scanned, total, len(updates))
    summary = {
        "scanned": scanned,
        "updated": len(updates),
        "no_price": no_price,
        "unique_lookups": network_lookups,
    }
    logger.info("usd_value backfill complete: %s", summary)
    return summary


async def forward_fill_recent(
    repo: Repository,
    pricing: PricingService,
    *,
    on_progress: BackfillProgress | None = None,
) -> dict[str, int]:
    """Bounded post-collect valuation of the most-recent zero-USD rows.

    Single source of truth for the post-collect forward-fill shared by the
    server's background valuation job and the ``pfm collect`` CLI — both want the
    same ``FORWARD_FILL_LIMIT``/``FORWARD_FILL_MAX_LOOKUPS`` budget on the newest
    rows. A full historical sweep calls ``backfill_transaction_usd_values`` directly.
    """
    return await backfill_transaction_usd_values(
        repo,
        pricing,
        limit=FORWARD_FILL_LIMIT,
        newest_first=True,
        max_lookups=FORWARD_FILL_MAX_LOOKUPS,
        on_progress=on_progress,
    )
