"""Rabby wallet collector via Rabby public API."""

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
from pfm.db.models import RawBalance, Transaction, TransactionType

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_RABBY_BASE_URL = "https://api.rabby.io"
_RATE_LIMITER = RateLimiter(requests_per_minute=60.0)
_DEBANK_CLOUD_URL = "https://cloud.debank.com"
_HTTP_UNAUTHORIZED = 401
_HTTP_FORBIDDEN = 403
_HTTP_TOO_MANY_REQUESTS = 429


@register_collector
class RabbyCollector(BaseCollector):
    """Collector for Rabby wallets using public Rabby APIs."""

    source_name = "rabby"

    def __init__(
        self,
        pricing: PricingService,
        *,
        wallet_address: str,
        access_key: str = "",
    ) -> None:
        super().__init__(pricing)
        self._wallet_address = wallet_address.strip()
        self._access_key = access_key.strip()
        self._client = httpx.AsyncClient(
            base_url=_RABBY_BASE_URL,
            timeout=30.0,
        )

    @retry()
    async def _get(self, path: str, params: dict[str, str]) -> Any:  # noqa: ANN401
        await _RATE_LIMITER.acquire()
        resp = await self._client.get(path, params=params)
        if resp.status_code in {_HTTP_UNAUTHORIZED, _HTTP_FORBIDDEN, _HTTP_TOO_MANY_REQUESTS}:
            msg = _format_debank_auth_error(resp)
            raise ValueError(msg)
        resp.raise_for_status()
        return resp.json()

    async def fetch_raw_balances(self) -> list[RawBalance]:
        """Fetch token balances from all EVM chains in Rabby wallet."""
        data = await self._get(
            "/v1/user/token_list",
            params={"id": self._wallet_address, "is_all": "false"},
        )
        if not isinstance(data, list):
            return []

        raw: list[RawBalance] = []
        for row in data:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol", "")).upper()
            if not symbol:
                continue

            amount = _to_decimal(row.get("amount", "0"))
            if amount <= 0:
                continue

            api_price = _to_decimal(row.get("price", "0"))
            price = api_price if api_price > 0 else None

            raw.append(
                RawBalance(
                    asset=symbol,
                    amount=amount,
                    price=price,
                    raw_json=json.dumps(row),
                )
            )

        logger.info("Rabby: found %d non-zero balances", len(raw))
        return raw

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch recent wallet history and normalize key transaction types."""
        data = await self._get(
            "/v1/user/history_list",
            params={"id": self._wallet_address, "page_count": "100"},
        )
        if not isinstance(data, dict):
            return []
        rows = data.get("history_list")
        if not isinstance(rows, list):
            return []
        token_symbols = _extract_token_symbols(data.get("token_dict"))

        txs: list[Transaction] = []
        for row in rows:
            tx = self._parse_history_item(row, token_symbols)
            if tx is None:
                continue
            if since and tx.date < since:
                continue
            txs.append(tx)

        logger.info("Rabby: parsed %d transactions", len(txs))
        return txs

    def _parse_history_item(
        self,
        row: object,
        token_symbols: dict[str, str] | None = None,
    ) -> Transaction | None:
        if not isinstance(row, dict):
            return None

        sends = _parse_token_flows(row.get("sends"), token_symbols)
        receives = _parse_token_flows(row.get("receives"), token_symbols)
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


def _parse_token_flows(
    value: object,
    token_symbols: dict[str, str] | None = None,
) -> list[tuple[str, Decimal]]:
    if not isinstance(value, list):
        return []
    rows: list[tuple[str, Decimal]] = []
    symbols = token_symbols or {}
    for entry in value:
        if not isinstance(entry, dict):
            continue
        symbol = str(entry.get("symbol", "")).upper()
        if not symbol:
            token_id = str(entry.get("token_id", "")).strip()
            symbol = symbols.get(token_id, "").upper()
        if not symbol:
            continue
        amount = _to_decimal(entry.get("amount", "0"))
        if amount <= 0:
            continue
        rows.append((symbol, amount))
    return rows


def _extract_token_symbols(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, str] = {}
    for token_id, token_data in value.items():
        if not isinstance(token_id, str) or not isinstance(token_data, dict):
            continue
        symbol = str(token_data.get("symbol", "")).strip().upper()
        if symbol:
            out[token_id] = symbol
    return out


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


def _format_debank_auth_error(resp: httpx.Response) -> str:
    status = resp.status_code
    message = ""
    try:
        payload = resp.json()
        if isinstance(payload, dict):
            maybe_message = payload.get("message")
            if maybe_message is not None:
                message = str(maybe_message).strip()
    except (ValueError, TypeError):
        message = ""

    lowered = message.lower()
    if status == _HTTP_TOO_MANY_REQUESTS:
        return "Rabby API rate limit reached (429). Try again later."
    if status == _HTTP_UNAUTHORIZED:
        return f"DeBank AccessKey is unauthorized (401). Verify key at {_DEBANK_CLOUD_URL}."
    if "insufficient units" in lowered:
        return f"DeBank API quota is exhausted (insufficient units). Recharge at {_DEBANK_CLOUD_URL}."
    if message:
        return f"DeBank request is forbidden (403): {message}"
    return f"DeBank request is forbidden (403). Verify key permissions at {_DEBANK_CLOUD_URL}."
