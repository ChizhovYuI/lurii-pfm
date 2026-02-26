"""Uphold collector — reads card balances and transactions via REST API."""

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

_BASE_URL = "https://api.uphold.com"


@register_collector
class UpholdCollector(BaseCollector):
    """Collector for Uphold accounts via Personal Access Token."""

    source_name = "uphold"

    def __init__(self, pricing: PricingService, *, pat: str) -> None:
        super().__init__(pricing)
        self._client = httpx.AsyncClient(
            base_url=_BASE_URL,
            headers={"Authorization": f"Bearer {pat}"},
            timeout=30.0,
        )

    @retry()
    async def _get_cards(self) -> list[dict[str, Any]]:
        """Fetch all cards (accounts) from Uphold."""
        resp = await self._client.get("/v0/me/cards")
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    @retry()
    async def _get_transactions(self, card_id: str) -> list[dict[str, Any]]:
        """Fetch transactions for a specific card."""
        resp = await self._client.get(f"/v0/me/cards/{card_id}/transactions")
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    async def fetch_balances(self) -> list[Snapshot]:
        """Fetch balances from all Uphold cards."""
        cards = await self._get_cards()
        today = self._pricing.today()
        snapshots: list[Snapshot] = []

        # Aggregate by currency (user may have multiple cards per currency)
        totals: dict[str, Decimal] = {}
        raw_by_currency: dict[str, str] = {}

        for card in cards:
            balance = Decimal(str(card.get("balance", "0")))
            currency = str(card.get("currency", "")).upper()
            if balance == 0 or not currency:
                continue
            totals[currency] = totals.get(currency, Decimal(0)) + balance
            raw_by_currency[currency] = json.dumps(card)

        for currency, total in totals.items():
            try:
                usd_value = await self._pricing.convert_to_usd(total, currency)
            except ValueError:
                logger.warning("Uphold: cannot price %s, skipping", currency)
                continue

            snapshots.append(
                Snapshot(
                    date=today,
                    source=self.source_name,
                    asset=currency,
                    amount=total,
                    usd_value=usd_value,
                    raw_json=raw_by_currency.get(currency, ""),
                )
            )

        logger.info("Uphold: found %d non-zero balances", len(snapshots))
        return snapshots

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch transactions from Uphold cards."""
        cards = await self._get_cards()
        all_transactions: list[Transaction] = []

        for card in cards:
            card_id = card.get("id")
            if not card_id:
                continue
            balance = Decimal(str(card.get("balance", "0")))
            if balance == 0:
                continue

            try:
                txs = await self._get_transactions(card_id)
            except httpx.HTTPStatusError:
                logger.warning("Uphold: failed to get transactions for card %s", card_id)
                continue

            for tx_data in txs:
                tx = self._parse_transaction(tx_data)
                if tx is None:
                    continue
                if since and tx.date < since:
                    continue
                all_transactions.append(tx)

        logger.info("Uphold: parsed %d transactions", len(all_transactions))
        return all_transactions

    @staticmethod
    def _parse_transaction(tx_data: dict[str, Any]) -> Transaction | None:
        """Parse an Uphold transaction."""
        origin = tx_data.get("origin", {})
        dest = tx_data.get("destination", {})
        amount = Decimal(str(dest.get("amount", "0")))
        currency = str(dest.get("currency", "")).upper()

        if not currency:
            return None

        created_at = tx_data.get("createdAt", "")
        try:
            tx_date = datetime.fromisoformat(created_at).date()
        except (ValueError, AttributeError):
            tx_date = datetime.now(tz=UTC).date()

        tx_type_str = str(tx_data.get("type", "")).lower()
        origin_currency = str(origin.get("currency", "")).upper()

        if tx_type_str == "deposit":
            tx_type = TransactionType.DEPOSIT
        elif tx_type_str == "withdrawal":
            tx_type = TransactionType.WITHDRAWAL
        elif origin_currency != currency:
            tx_type = TransactionType.TRADE
        else:
            tx_type = TransactionType.TRANSFER

        return Transaction(
            date=tx_date,
            source="uphold",
            tx_type=tx_type,
            asset=currency,
            amount=abs(amount),
            usd_value=Decimal(0),
            counterparty_asset=origin_currency if origin_currency != currency else "",
            counterparty_amount=Decimal(str(origin.get("amount", "0"))) if origin_currency != currency else Decimal(0),
            tx_id=str(tx_data.get("id", "")),
            raw_json=json.dumps(tx_data),
        )
