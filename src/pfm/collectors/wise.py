"""Wise collector — reads multi-currency balances via REST API."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import httpx

from pfm.collectors import register_collector
from pfm.collectors._retry import retry
from pfm.collectors.base import BaseCollector
from pfm.db.models import Snapshot, Transaction, TransactionType

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)
_HTTP_UNAUTHORIZED = 401
_HTTP_FORBIDDEN = 403
_HTTP_NOT_FOUND = 404
_STATEMENT_UNAVAILABLE_STATUSES = (_HTTP_UNAUTHORIZED, _HTTP_FORBIDDEN, _HTTP_NOT_FOUND)

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

    @retry()
    async def _get_statement(
        self, profile_id: int, balance_id: int, currency: str, start: str, end: str
    ) -> dict[str, Any]:
        """Get account statement for a specific balance."""
        resp = await self._client.get(
            f"/v1/profiles/{profile_id}/balance-statements/{balance_id}/statement",
            params={
                "currency": currency,
                "intervalStart": start,
                "intervalEnd": end,
                "type": "COMPACT",
            },
        )
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    async def fetch_balances(self) -> list[Snapshot]:
        """Fetch balances from all Wise currency accounts."""
        profile_id = await self._get_profile_id()
        balances = await self._get_balances(profile_id)
        today = self._pricing.today()
        snapshots: list[Snapshot] = []

        for bal in balances:
            amount_data = bal.get("amount", {})
            amount = Decimal(str(amount_data.get("value", 0)))
            currency = str(amount_data.get("currency", "")).upper()

            if amount == 0 or not currency:
                continue

            usd_value = await self._pricing.convert_to_usd(amount, currency)

            snapshots.append(
                Snapshot(
                    date=today,
                    source=self.source_name,
                    asset=currency,
                    amount=amount,
                    usd_value=usd_value,
                    raw_json=json.dumps(bal),
                )
            )

        logger.info("Wise: found %d non-zero balances", len(snapshots))
        return snapshots

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch recent transactions from Wise statement API."""
        profile_id = await self._get_profile_id()
        balances = await self._get_balances(profile_id)
        today = self._pricing.today()
        start_date = since or date(today.year, today.month, 1)

        all_transactions: list[Transaction] = []
        statement_failures: list[str] = []

        for bal in balances:
            balance_id = bal.get("id")
            currency = str(bal.get("amount", {}).get("currency", "")).upper()
            if not balance_id or not currency:
                continue

            start_iso = datetime(start_date.year, start_date.month, start_date.day, tzinfo=UTC).isoformat()
            end_iso = datetime(today.year, today.month, today.day, 23, 59, 59, tzinfo=UTC).isoformat()

            try:
                statement = await self._get_statement(profile_id, balance_id, currency, start_iso, end_iso)
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status in _STATEMENT_UNAVAILABLE_STATUSES:
                    logger.info(
                        "Wise: statement API unavailable (HTTP %d). Skipping Wise transaction import for this run.",
                        status,
                    )
                    return all_transactions
                statement_failures.append(currency)
                continue

            for tx_data in statement.get("transactions", []):
                tx = self._parse_transaction(tx_data, currency)
                if tx:
                    all_transactions.append(tx)

        if statement_failures:
            logger.warning(
                "Wise: failed to get statements for %d balance(s): %s",
                len(statement_failures),
                ", ".join(statement_failures),
            )

        logger.info("Wise: parsed %d transactions", len(all_transactions))
        return all_transactions

    @staticmethod
    def _parse_transaction(tx_data: dict[str, Any], currency: str) -> Transaction | None:
        """Parse a Wise statement transaction."""
        amount_data = tx_data.get("amount", {})
        amount = Decimal(str(amount_data.get("value", 0)))
        if amount == 0:
            return None

        tx_date_str = tx_data.get("date", "")
        try:
            tx_date = datetime.fromisoformat(tx_date_str).date()
        except (ValueError, AttributeError):
            tx_date = datetime.now(tz=UTC).date()

        tx_type_str = tx_data.get("type", "").upper()
        if tx_type_str in ("CREDIT", "DEPOSIT"):
            tx_type = TransactionType.DEPOSIT
        elif tx_type_str in ("DEBIT", "WITHDRAWAL"):
            tx_type = TransactionType.WITHDRAWAL
        elif tx_type_str == "CONVERSION":
            tx_type = TransactionType.TRADE
        else:
            tx_type = TransactionType.TRANSFER

        return Transaction(
            date=tx_date,
            source="wise",
            tx_type=tx_type,
            asset=currency,
            amount=abs(amount),
            usd_value=Decimal(0),  # historical pricing deferred
            tx_id=str(tx_data.get("referenceNumber", "")),
            raw_json=json.dumps(tx_data),
        )
