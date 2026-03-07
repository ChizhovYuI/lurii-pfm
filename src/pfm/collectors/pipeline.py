"""Parallel collection pipeline: raw fetch → batch pricing → snapshots."""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal
from typing import TYPE_CHECKING

from pfm.collectors._retry import is_dns_resolution_error
from pfm.db.models import CollectorResult

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pfm.collectors.base import BaseCollector
    from pfm.db.models import RawBalance, Source
    from pfm.db.repository import Repository
    from pfm.pricing.coingecko import PricingService

    ProgressCallback = Callable[[float, float, str], Awaitable[None]]

logger = logging.getLogger(__name__)

# Progress weights: fetch 0-80%, prices 80-90%, save 90-100%
_FETCH_WEIGHT = 0.8
_PRICE_WEIGHT = 0.9


async def run_parallel_pipeline(
    collectors: list[tuple[Source, BaseCollector]],
    pricing: PricingService,
    repo: Repository,
    *,
    on_progress: ProgressCallback | None = None,
) -> list[CollectorResult]:
    """Execute the 4-phase parallel collection pipeline.

    1. Fetch raw balances from all collectors in parallel
    2. Collect unique tickers that need pricing
    3. Batch-fetch prices from CoinGecko
    4. Build snapshots, save, and fetch transactions per collector
    """
    total = len(collectors)
    raw_results = await _fetch_all(collectors, total, on_progress)

    # Phase 2+3: batch price lookup across all collectors
    all_tickers: set[str] = set()
    for raw in raw_results:
        if isinstance(raw, BaseException):
            continue
        for rb in raw:
            if rb.price is None:
                all_tickers.add(rb.asset)

    if on_progress:
        await on_progress(_FETCH_WEIGHT, 1, "Fetching prices...")

    prices = await pricing.get_prices_usd(list(all_tickers)) if all_tickers else {}

    if on_progress:
        await on_progress(_PRICE_WEIGHT, 1, "Calculating snapshots...")

    # Phase 4: build snapshots, save, fetch transactions
    results: list[CollectorResult] = []
    for (src, collector), raw in zip(collectors, raw_results, strict=True):
        result = await _process_single(src.name, collector, raw, prices, repo)
        results.append(result)

    return results


async def _fetch_all(
    collectors: list[tuple[Source, BaseCollector]],
    total: int,
    on_progress: ProgressCallback | None,
) -> list[list[RawBalance] | BaseException]:
    """Phase 1: fetch raw balances in parallel with per-source progress."""
    fetched_count = 0

    async def _fetch_one(collector: BaseCollector) -> list[RawBalance]:
        nonlocal fetched_count
        try:
            return await collector.fetch_raw_balances()
        finally:
            fetched_count += 1
            if on_progress:
                pct = fetched_count / total * _FETCH_WEIGHT
                await on_progress(pct, 1, f"Fetched {fetched_count}/{total}")

    raw_tasks = [_fetch_one(c) for _, c in collectors]
    return await asyncio.gather(*raw_tasks, return_exceptions=True)


async def _process_single(
    source_name: str,
    collector: BaseCollector,
    raw: list[RawBalance] | BaseException,
    prices: dict[str, Decimal],
    repo: Repository,
) -> CollectorResult:
    """Build snapshots and fetch transactions for one collector."""
    result = CollectorResult(source=source_name)
    start = time.monotonic()

    if isinstance(raw, BaseException):
        msg = f"Failed to fetch balances from {source_name}: {raw}"
        logger.exception(msg, exc_info=raw)
        result.errors.append(msg)
    else:
        try:
            snapshots = collector._build_snapshots(raw, prices)  # noqa: SLF001
            if snapshots:
                await repo.save_snapshots(snapshots)
                result.snapshots_count = len(snapshots)
                result.snapshots_usd_total = sum((s.usd_value for s in snapshots), start=Decimal(0))
        except Exception as exc:
            msg = f"Failed to save snapshots for {source_name}: {exc}"
            logger.exception(msg)
            result.errors.append(msg)

    # Skip transactions if raw balance fetch was a DNS/network error
    dns_blocked = isinstance(raw, Exception) and is_dns_resolution_error(raw)
    if not dns_blocked:
        try:
            effective_since = await collector.resolve_transactions_since(repo)
            transactions = collector.normalize_transactions(await collector.fetch_transactions(since=effective_since))
            if transactions:
                await repo.save_transactions(transactions)
                result.transactions_count = len(transactions)
        except Exception as exc:
            msg = f"Failed to fetch transactions from {source_name}: {exc}"
            logger.exception(msg)
            result.errors.append(msg)

    result.duration_seconds = time.monotonic() - start
    return result
