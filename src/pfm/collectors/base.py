"""Abstract base class for all data collectors."""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pfm.db.models import CollectorResult, RawResponse, Snapshot, Transaction

if TYPE_CHECKING:
    from datetime import date

    from pfm.db.repository import Repository
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """Base class for all source collectors.

    Subclasses must set `source_name` and implement `fetch_balances` and
    `fetch_transactions`.
    """

    source_name: str = ""

    def __init__(self, pricing: PricingService) -> None:
        self._pricing = pricing

    @abstractmethod
    async def fetch_balances(self) -> list[Snapshot]:
        """Fetch current balances from the source."""

    @abstractmethod
    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch transaction history from the source."""

    async def collect(self, repo: Repository, since: date | None = None) -> CollectorResult:
        """Run the full collection cycle: fetch, save, return summary.

        Catches exceptions so one failing method doesn't prevent the other.
        """
        result = CollectorResult(source=self.source_name)
        start = time.monotonic()

        # Fetch balances
        try:
            snapshots = await self.fetch_balances()
            if snapshots:
                await repo.save_snapshots(snapshots)
                result.snapshots_count = len(snapshots)
        except Exception as exc:
            msg = f"Failed to fetch balances from {self.source_name}: {exc}"
            logger.exception(msg)
            result.errors.append(msg)

        # Fetch transactions
        try:
            transactions = await self.fetch_transactions(since=since)
            if transactions:
                await repo.save_transactions(transactions)
                result.transactions_count = len(transactions)
        except Exception as exc:
            msg = f"Failed to fetch transactions from {self.source_name}: {exc}"
            logger.exception(msg)
            result.errors.append(msg)

        result.duration_seconds = time.monotonic() - start
        logger.info(
            "Collected source=%s snapshots=%d transactions=%d errors=%d duration=%.2fs",
            self.source_name,
            result.snapshots_count,
            result.transactions_count,
            len(result.errors),
            result.duration_seconds,
        )
        return result

    async def _save_raw(self, repo: Repository, endpoint: str, body: str) -> None:
        """Save a raw API response for auditability."""
        raw = RawResponse(
            date=datetime.now(tz=UTC).date(),
            source=self.source_name,
            endpoint=endpoint,
            response_body=body,
        )
        await repo.save_raw_response(raw)
