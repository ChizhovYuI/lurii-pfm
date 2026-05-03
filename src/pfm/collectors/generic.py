"""Generic catch-all source collector (no-op)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pfm.collectors import register_collector
from pfm.collectors.base import BaseCollector
from pfm.enums import SourceName

if TYPE_CHECKING:
    from datetime import date

    from pfm.db.models import RawBalance, Transaction
    from pfm.pricing.coingecko import PricingService


@register_collector
class GenericCollector(BaseCollector):
    """No-op collector for manual catch-all sources fed via add_manual_*."""

    source_name = SourceName.GENERIC
    records_empty_sync_marker = False

    def __init__(
        self,
        pricing: PricingService,
        *,
        label: str = "",
        group_hint: str = "",
    ) -> None:
        super().__init__(pricing)
        self._label = label
        self._group_hint = group_hint

    async def fetch_raw_balances(self) -> list[RawBalance]:
        return []

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:  # noqa: ARG002
        return []
