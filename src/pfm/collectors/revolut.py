"""Revolut collector — reads multi-currency balances via GoCardless Bank Account Data API."""

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
from pfm.enums import SourceName

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_BASE_URL = "https://bankaccountdata.gocardless.com"
_TOKEN_PATH = "/api/v2/token/new/"  # noqa: S105
_ACCOUNTS_PATH = "/api/v2/accounts"
_REQUISITIONS_PATH = "/api/v2/requisitions"


@register_collector
class RevolutCollector(BaseCollector):
    """Collector for Revolut accounts via GoCardless (Nordigen) open banking."""

    source_name = SourceName.REVOLUT

    def __init__(
        self,
        pricing: PricingService,
        *,
        secret_id: str,
        secret_key: str,
        requisition_id: str,
    ) -> None:
        super().__init__(pricing)
        self._secret_id = secret_id
        self._secret_key = secret_key
        self._requisition_id = requisition_id
        self._client = httpx.AsyncClient(base_url=_BASE_URL, timeout=30.0)
        self._access_token: str | None = None

    async def _ensure_token(self) -> None:
        """Obtain an access token using client credentials."""
        if self._access_token:
            return
        resp = await self._client.post(
            _TOKEN_PATH,
            json={"secret_id": self._secret_id, "secret_key": self._secret_key},
        )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access"]

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._access_token}"}

    @retry()
    async def _get_account_ids(self) -> list[str]:
        """Get linked account IDs from the requisition."""
        await self._ensure_token()
        resp = await self._client.get(
            f"{_REQUISITIONS_PATH}/{self._requisition_id}/",
            headers=self._auth_headers(),
        )
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        accounts: list[str] = data.get("accounts", [])
        if not accounts:
            logger.warning("Revolut: requisition %s has no linked accounts", self._requisition_id)
        return accounts

    @retry()
    async def _get_balances(self, account_id: str) -> list[dict[str, Any]]:
        """Get balances for a single account."""
        await self._ensure_token()
        resp = await self._client.get(
            f"{_ACCOUNTS_PATH}/{account_id}/balances/",
            headers=self._auth_headers(),
        )
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        return data.get("balances", [])  # type: ignore[no-any-return]

    @retry()
    async def _get_transactions(
        self, account_id: str, date_from: str | None = None, date_to: str | None = None
    ) -> dict[str, Any]:
        """Get transactions for a single account."""
        await self._ensure_token()
        params: dict[str, str] = {}
        if date_from:
            params["date_from"] = date_from
        if date_to:
            params["date_to"] = date_to
        resp = await self._client.get(
            f"{_ACCOUNTS_PATH}/{account_id}/transactions/",
            headers=self._auth_headers(),
            params=params,
        )
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    async def fetch_raw_balances(self) -> list[RawBalance]:
        """Fetch balances from all linked Revolut accounts."""
        account_ids = await self._get_account_ids()
        raw: list[RawBalance] = []
        seen_currencies: set[str] = set()

        for account_id in account_ids:
            balances = await self._get_balances(account_id)
            for bal in balances:
                # Use closingAvailable or interimAvailable balance
                bal_type = bal.get("balanceType", "")
                if bal_type not in ("closingAvailable", "interimAvailable", "expected"):
                    continue

                amount_data = bal.get("balanceAmount", {})
                currency = str(amount_data.get("currency", "")).upper()
                amount_str = amount_data.get("amount", "0")
                amount = Decimal(str(amount_str))

                if amount == 0 or not currency:
                    continue

                # Skip duplicate currencies (API may return multiple balance types)
                if currency in seen_currencies:
                    continue
                seen_currencies.add(currency)

                raw.append(
                    RawBalance(
                        asset=currency,
                        amount=amount,
                        raw_json=json.dumps(bal),
                    )
                )

        logger.info("Revolut: found %d non-zero balances", len(raw))
        return raw

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch recent transactions from all linked Revolut accounts."""
        account_ids = await self._get_account_ids()
        today = self._pricing.today()
        start_date = since or date(today.year, today.month, 1)

        all_transactions: list[Transaction] = []

        for account_id in account_ids:
            data = await self._get_transactions(
                account_id,
                date_from=start_date.isoformat(),
                date_to=today.isoformat(),
            )
            booked = data.get("transactions", {}).get("booked", [])
            for tx_data in booked:
                tx = self._parse_transaction(tx_data)
                if tx:
                    all_transactions.append(tx)

        logger.info("Revolut: parsed %d transactions", len(all_transactions))
        return all_transactions

    @staticmethod
    def _parse_transaction(tx_data: dict[str, Any]) -> Transaction | None:
        """Parse a GoCardless booked transaction."""
        amount_data = tx_data.get("transactionAmount", {})
        amount_str = amount_data.get("amount", "0")
        amount = Decimal(str(amount_str))
        currency = str(amount_data.get("currency", "")).upper()

        if amount == 0:
            return None

        # Parse date
        booking_date = tx_data.get("bookingDate", "")
        try:
            tx_date = date.fromisoformat(booking_date)
        except (ValueError, AttributeError):
            tx_date = datetime.now(tz=UTC).date()

        # Determine type from amount sign
        tx_type = TransactionType.DEPOSIT if amount > 0 else TransactionType.WITHDRAWAL

        # Build transaction ID from available fields
        tx_id = tx_data.get("transactionId", "") or tx_data.get("internalTransactionId", "")

        return Transaction(
            date=tx_date,
            source="revolut",
            tx_type=tx_type,
            asset=currency,
            amount=abs(amount),
            usd_value=Decimal(0),  # historical pricing deferred
            tx_id=tx_id,
            raw_json=json.dumps(tx_data),
        )
