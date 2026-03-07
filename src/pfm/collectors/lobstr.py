"""Lobstr / Stellar Horizon collector — reads on-chain balances and payments."""

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
from pfm.db.models import RawBalance, Transaction, TransactionType

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_HORIZON_URL = "https://horizon.stellar.org"


@register_collector
class LobstrCollector(BaseCollector):
    """Collector for Stellar wallets (Lobstr) via Horizon API."""

    source_name = "lobstr"

    def __init__(self, pricing: PricingService, *, stellar_address: str) -> None:
        super().__init__(pricing)
        self._address = stellar_address
        self._client = httpx.AsyncClient(base_url=_HORIZON_URL, timeout=30.0)

    @retry()
    async def _get_account(self) -> dict[str, Any]:
        resp = await self._client.get(f"/accounts/{self._address}")
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    @retry()
    async def _get_payments(self, cursor: str = "", limit: int = 100) -> dict[str, Any]:
        params: dict[str, str | int] = {"limit": limit, "order": "desc"}
        if cursor:
            params["cursor"] = cursor
        resp = await self._client.get(f"/accounts/{self._address}/payments", params=params)
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    async def fetch_raw_balances(self) -> list[RawBalance]:
        """Fetch all Stellar account balances."""
        account = await self._get_account()
        raw: list[RawBalance] = []

        for bal in account.get("balances", []):
            amount = Decimal(bal["balance"])
            if amount == 0:
                continue

            ticker = self._parse_ticker(bal)

            raw.append(
                RawBalance(
                    asset=ticker,
                    amount=amount,
                    raw_json=json.dumps(bal),
                )
            )

        logger.info("Lobstr: found %d non-zero balances", len(raw))
        return raw

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch recent payments from Stellar Horizon."""
        data = await self._get_payments()
        records: list[dict[str, Any]] = data.get("_embedded", {}).get("records", [])
        transactions: list[Transaction] = []

        for record in records:
            tx = self._parse_payment(record)
            if tx is None:
                continue
            if since and tx.date < since:
                break
            transactions.append(tx)

        logger.info("Lobstr: parsed %d transactions", len(transactions))
        return transactions

    def _parse_payment(self, record: dict[str, Any]) -> Transaction | None:
        """Parse a Horizon payment record into a Transaction."""
        op_type = record.get("type")
        if op_type not in ("payment", "create_account", "path_payment_strict_receive", "path_payment_strict_send"):
            return None

        created_at = record.get("created_at", "")
        try:
            tx_date = datetime.fromisoformat(created_at).date()
        except (ValueError, AttributeError):
            tx_date = datetime.now(tz=UTC).date()

        is_incoming = record.get("to") == self._address
        tx_type = TransactionType.DEPOSIT if is_incoming else TransactionType.WITHDRAWAL

        if op_type == "create_account":
            ticker = "XLM"
            amount = Decimal(record.get("starting_balance", "0"))
        else:
            ticker = self._parse_ticker_from_payment(record)
            amount = Decimal(record.get("amount", "0"))

        return Transaction(
            date=tx_date,
            source=self.source_name,
            tx_type=tx_type,
            asset=ticker,
            amount=amount,
            usd_value=Decimal(0),  # historical pricing deferred
            tx_id=record.get("transaction_hash", ""),
            raw_json=json.dumps(record),
        )

    @staticmethod
    def _parse_ticker(bal: dict[str, Any]) -> str:
        """Extract ticker from a Horizon balance object."""
        if bal.get("asset_type") == "native":
            return "XLM"
        return str(bal.get("asset_code", "UNKNOWN")).upper()

    @staticmethod
    def _parse_ticker_from_payment(record: dict[str, Any]) -> str:
        """Extract ticker from a Horizon payment record."""
        if record.get("asset_type") == "native":
            return "XLM"
        return str(record.get("asset_code", "UNKNOWN")).upper()
