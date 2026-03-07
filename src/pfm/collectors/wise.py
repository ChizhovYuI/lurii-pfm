"""Wise collector — reads multi-currency balances via REST API."""

from __future__ import annotations

import json
import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import httpx

from pfm.collectors import register_collector
from pfm.collectors._retry import retry
from pfm.collectors.base import BaseCollector
from pfm.db.models import RawBalance, Transaction

if TYPE_CHECKING:
    from datetime import date

    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.transferwise.com"


@register_collector
class WiseCollector(BaseCollector):
    """Collector for Wise multi-currency accounts."""

    source_name = "wise"

    def __init__(self, pricing: PricingService, *, api_token: str) -> None:
        super().__init__(pricing)
        self._client = httpx.AsyncClient(
            base_url=_BASE_URL,
            headers={"Authorization": f"Bearer {api_token}"},
            timeout=30.0,
        )

    @retry()
    async def _get_profile_id(self) -> int:
        """Get the personal profile ID."""
        resp = await self._client.get("/v1/profiles")
        resp.raise_for_status()
        profiles: list[dict[str, Any]] = resp.json()
        for profile in profiles:
            if profile.get("type") == "personal":
                return int(profile["id"])
        if profiles:
            return int(profiles[0]["id"])
        msg = "No Wise profiles found"
        raise ValueError(msg)

    @retry()
    async def _get_balances(self, profile_id: int) -> list[dict[str, Any]]:
        """Get all currency balances."""
        resp = await self._client.get(f"/v4/profiles/{profile_id}/balances", params={"types": "STANDARD"})
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    async def fetch_raw_balances(self) -> list[RawBalance]:
        """Fetch balances from all Wise currency accounts."""
        profile_id = await self._get_profile_id()
        balances = await self._get_balances(profile_id)
        raw: list[RawBalance] = []

        for bal in balances:
            amount_data = bal.get("amount", {})
            amount = Decimal(str(amount_data.get("value", 0)))
            currency = str(amount_data.get("currency", "")).upper()

            if amount == 0 or not currency:
                continue

            raw.append(
                RawBalance(
                    asset=currency,
                    amount=amount,
                    raw_json=json.dumps(bal),
                )
            )

        logger.info("Wise: found %d non-zero balances", len(raw))
        return raw

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:  # noqa: ARG002
        """Wise personal token does not have statement permissions."""
        return []
