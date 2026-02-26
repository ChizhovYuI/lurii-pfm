"""Binance TH collector — extends Binance collector with Thailand-specific base URL."""

from __future__ import annotations

from typing import TYPE_CHECKING

import httpx

from pfm.collectors import register_collector
from pfm.collectors.binance import BinanceCollector

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService


@register_collector
class BinanceThCollector(BinanceCollector):
    """Collector for Binance Thailand.

    Same API structure as Binance global, different base URL and spot-only.
    """

    source_name = "binance_th"
    _base_url = "https://api.binance.th"

    def __init__(
        self,
        pricing: PricingService,
        *,
        api_key: str,
        api_secret: str,
    ) -> None:
        super().__init__(pricing, api_key=api_key, api_secret=api_secret)
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers={"X-MBX-APIKEY": api_key},
            timeout=30.0,
        )
