"""Abstract base class for all data collectors."""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import replace
from decimal import Decimal
from typing import TYPE_CHECKING

from pfm.collectors._retry import is_dns_resolution_error
from pfm.db.models import CollectorResult, Snapshot, Transaction

if TYPE_CHECKING:
    from datetime import date

    from pfm.db.repository import Repository
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)
_COUNTRY_ACCESS_HINT = "service access appears restricted from your current network or region. try a vpn and retry."


def _format_fetch_error(source_name: str, stage: str, exc: Exception) -> tuple[str, bool]:
    """Return user-facing collector error text and whether it is a DNS access issue."""
    if is_dns_resolution_error(exc):
        return (f"Failed to fetch {stage} from {source_name}: {_COUNTRY_ACCESS_HINT}", True)
    return (f"Failed to fetch {stage} from {source_name}: {exc}", False)


class BaseCollector(ABC):
    """Base class for all source collectors.

    Subclasses must set `source_name` and implement `fetch_balances` and
    `fetch_transactions`.
    """

    source_name: str = ""

    def __init__(self, pricing: PricingService) -> None:
        self._pricing = pricing
        # Configured source instance name from sources table (e.g., "blend-main").
        # Defaults to the collector type name for backwards compatibility.
        self.instance_name = self.source_name

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
        dns_access_blocked = False

        # Fetch balances
        try:
            snapshots = await self.fetch_balances()
            if snapshots:
                instance_name = self.instance_name or self.source_name
                snapshots = [
                    snap if snap.source_name else replace(snap, source_name=instance_name or snap.source)
                    for snap in snapshots
                ]
                await repo.save_snapshots(snapshots)
                result.snapshots_count = len(snapshots)
                result.snapshots_usd_total = sum((snapshot.usd_value for snapshot in snapshots), start=Decimal(0))
        except Exception as exc:
            msg, is_network_access_error = _format_fetch_error(self.source_name, "balances", exc)
            if is_network_access_error:
                dns_access_blocked = True
                logger.warning("%s (original error: %s)", msg, exc)
            else:
                logger.exception(msg)
            result.errors.append(msg)

        # Fetch transactions
        if dns_access_blocked:
            logger.info(
                "Skipping transactions fetch for %s due to DNS/network access restriction.",
                self.source_name,
            )
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

        try:
            transactions = await self.fetch_transactions(since=since)
            if transactions:
                await repo.save_transactions(transactions)
                result.transactions_count = len(transactions)
        except Exception as exc:
            msg, is_network_access_error = _format_fetch_error(self.source_name, "transactions", exc)
            if is_network_access_error:
                logger.warning("%s (original error: %s)", msg, exc)
            else:
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
