"""Rabby wallet collector via DeBank OpenAPI."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import httpx

from pfm.collectors import register_collector
from pfm.collectors._retry import RateLimiter, retry
from pfm.collectors.base import BaseCollector
from pfm.db.models import Snapshot, Transaction, TransactionType

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_DEBANK_BASE_URL = "https://pro-openapi.debank.com"
_RATE_LIMITER = RateLimiter(requests_per_minute=60.0)


@register_collector
class RabbyCollector(BaseCollector):
    """Collector for Rabby wallets using DeBank's account APIs."""

    source_name = "rabby"

    def __init__(
        self,
        pricing: PricingService,
        *,
        wallet_address: str,
        access_key: str,
    ) -> None:
        super().__init__(pricing)
        self._wallet_address = wallet_address.strip()
        self._client = httpx.AsyncClient(
            base_url=_DEBANK_BASE_URL,
            headers={"AccessKey": access_key.strip()},
            timeout=30.0,
        )

    @retry()
    async def _get(self, path: str, params: dict[str, str]) -> Any:  # noqa: ANN401
        await _RATE_LIMITER.acquire()
        resp = await self._client.get(path, params=params)
        resp.raise_for_status()
        return resp.json()

    async def fetch_balances(self) -> list[Snapshot]:
        """Fetch token balances from all EVM chains in Rabby wallet."""
        today = self._pricing.today()
        data = await self._get(
            "/v1/user/all_token_list",
            params={"id": self._wallet_address, "is_all": "false"},
        )
        if not isinstance(data, list):
            return []

        snapshots: list[Snapshot] = []
        for row in data:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol", "")).upper()
            if not symbol:
                continue

            amount = _to_decimal(row.get("amount", "0"))
            if amount <= 0:
                continue

            price = _to_decimal(row.get("price", "0"))
            if price <= 0:
                try:
                    price = await self._pricing.get_price_usd(symbol)
                except ValueError:
                    logger.warning("Rabby: cannot price %s, skipping", symbol)
                    continue
            usd_value = amount * price

            snapshots.append(
                Snapshot(
                    date=today,
                    source=self.source_name,
                    asset=symbol,
                    amount=amount,
                    usd_value=usd_value,
                    price=price,
                    raw_json=json.dumps(row),
                )
            )

        logger.info("Rabby: found %d non-zero balances", len(snapshots))
        return snapshots

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch recent wallet history and normalize key transaction types."""
        data = await self._get(
            "/v1/user/all_history_list",
            params={"id": self._wallet_address, "page_count": "100"},
        )
        if not isinstance(data, list):
            return []

        txs: list[Transaction] = []
        for row in data:
            tx = self._parse_history_item(row)
            if tx is None:
                continue
            if since and tx.date < since:
                continue
            txs.append(tx)

        logger.info("Rabby: parsed %d transactions", len(txs))
        return txs

    def _parse_history_item(self, row: object) -> Transaction | None:
        if not isinstance(row, dict):
            return None

        sends = _parse_token_flows(row.get("sends"))
        receives = _parse_token_flows(row.get("receives"))
        if not sends and not receives:
            return None

        cate_id = str(row.get("cate_id", "")).lower()
        tx_date = _parse_unix_date(row.get("time_at"))
        tx_id = _extract_tx_id(row)

        if receives and not sends:
            asset, amount = receives[0]
            tx_type = TransactionType.DEPOSIT
            return Transaction(
                date=tx_date,
                source=self.source_name,
                tx_type=tx_type,
                asset=asset,
                amount=amount,
                usd_value=Decimal(0),
                tx_id=tx_id,
                raw_json=json.dumps(row),
            )

        if sends and not receives:
            asset, amount = sends[0]
            tx_type = TransactionType.WITHDRAWAL
            return Transaction(
                date=tx_date,
                source=self.source_name,
                tx_type=tx_type,
                asset=asset,
                amount=amount,
                usd_value=Decimal(0),
                tx_id=tx_id,
                raw_json=json.dumps(row),
            )

        send_asset, send_amount = sends[0]
        recv_asset, recv_amount = receives[0]
        tx_type = TransactionType.TRADE if "swap" in cate_id or "trade" in cate_id else TransactionType.TRANSFER
        return Transaction(
            date=tx_date,
            source=self.source_name,
            tx_type=tx_type,
            asset=send_asset,
            amount=send_amount,
            usd_value=Decimal(0),
            counterparty_asset=recv_asset,
            counterparty_amount=recv_amount,
            tx_id=tx_id,
            raw_json=json.dumps(row),
        )


def _parse_token_flows(value: object) -> list[tuple[str, Decimal]]:
    if not isinstance(value, list):
        return []
    rows: list[tuple[str, Decimal]] = []
    for entry in value:
        if not isinstance(entry, dict):
            continue
        symbol = str(entry.get("symbol", "")).upper()
        if not symbol:
            continue
        amount = _to_decimal(entry.get("amount", "0"))
        if amount <= 0:
            continue
        rows.append((symbol, amount))
    return rows


def _extract_tx_id(row: dict[str, Any]) -> str:
    tx_field = row.get("tx")
    if isinstance(tx_field, dict):
        value = tx_field.get("id")
        if isinstance(value, str) and value.strip():
            return value.strip()
        value = tx_field.get("hash")
        if isinstance(value, str) and value.strip():
            return value.strip()
    txid = row.get("id")
    if isinstance(txid, str):
        return txid
    return ""


def _parse_unix_date(value: object) -> date:
    timestamp = _to_decimal(value)
    if timestamp <= 0:
        return datetime.now(tz=UTC).date()
    try:
        return datetime.fromtimestamp(float(timestamp), tz=UTC).date()
    except (OverflowError, OSError, ValueError):
        return datetime.now(tz=UTC).date()


def _to_decimal(value: object) -> Decimal:
    try:
        return Decimal(str(value))
    except ArithmeticError:
        return Decimal(0)
