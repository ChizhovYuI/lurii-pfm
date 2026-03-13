"""Manual cash source collector."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pfm.collectors import register_collector
from pfm.collectors.base import BaseCollector

if TYPE_CHECKING:
    from datetime import date

    from pfm.db.models import RawBalance, Transaction
    from pfm.pricing.coingecko import PricingService


@register_collector
class CashCollector(BaseCollector):
    """No-op collector used for manual cash balances managed via API."""

    source_name = "cash"
    records_empty_sync_marker = False

    def __init__(self, pricing: PricingService, *, fiat_currencies: str = "") -> None:
        super().__init__(pricing)
        self._fiat_currencies = fiat_currencies

    async def fetch_raw_balances(self) -> list[RawBalance]:
        return []

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:  # noqa: ARG002
        return []
