"""Trading 212 collector — reads equity balances and historical operations via Invest API."""

from __future__ import annotations

import asyncio
import hashlib
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
from pfm.enums import SourceName

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pfm.pricing.coingecko import PricingService

    ItemParser = Callable[[dict[str, Any]], Awaitable[Transaction | None]]

logger = logging.getLogger(__name__)

_BASE_URL = "https://live.trading212.com/api/v0"
_SUMMARY_PATH = "/equity/account/summary"
_POSITIONS_PATH = "/equity/positions"
_HISTORY_ORDERS_PATH = "/equity/history/orders"
_HISTORY_TRANSACTIONS_PATH = "/equity/history/transactions"
_HISTORY_DIVIDENDS_PATH = "/equity/history/dividends"
_HISTORY_LIMIT = 50
_HISTORY_RATE_LIMIT_PER_MINUTE = 6.0
_MAX_429_RETRIES = 3
_HTTP_TOO_MANY_REQUESTS = 429


@register_collector
class Trading212Collector(BaseCollector):
    """Collector for Trading 212 Invest API."""

    source_name = SourceName.TRADING212
    incremental_history_overlap_days = 7

    def __init__(self, pricing: PricingService, *, api_key: str, api_secret: str) -> None:
        super().__init__(pricing)
        self._client = httpx.AsyncClient(
            base_url=_BASE_URL,
            auth=(api_key, api_secret),
            headers={"Accept": "application/json"},
            timeout=30.0,
        )
        self._history_rate_limiter = RateLimiter(_HISTORY_RATE_LIMIT_PER_MINUTE)
        self._usd_rate_cache: dict[str, Decimal] = {}

    @retry()
    async def _get_json(self, path: str, *, history: bool = False) -> object:
        """GET JSON payload from Trading 212 with lightweight 429 retry for history endpoints."""
        retry_after_seconds = 10.0
        for attempt in range(1, _MAX_429_RETRIES + 1):
            if history:
                await self._history_rate_limiter.acquire()

            resp = await self._client.get(path)
            if resp.status_code == _HTTP_TOO_MANY_REQUESTS and attempt < _MAX_429_RETRIES:
                retry_after_header = resp.headers.get("Retry-After")
                if retry_after_header and retry_after_header.isdigit():
                    retry_after_seconds = float(retry_after_header)
                await asyncio.sleep(retry_after_seconds)
                retry_after_seconds *= 2
                continue

            resp.raise_for_status()
            return resp.json()

        msg = f"Trading 212 request failed after {_MAX_429_RETRIES} attempts: {path}"
        raise RuntimeError(msg)

    async def _currency_to_usd(self, currency: str) -> Decimal:
        normalized = currency.upper()
        cached = self._usd_rate_cache.get(normalized)
        if cached is not None:
            return cached
        price = await self._pricing.get_price_usd(normalized)
        self._usd_rate_cache[normalized] = price
        return price

    async def fetch_raw_balances(self) -> list[RawBalance]:
        """Fetch cash summary and open positions."""
        summary = await self._get_json(_SUMMARY_PATH)
        positions = await self._get_json(_POSITIONS_PATH)

        if not isinstance(summary, dict):
            msg = f"Trading 212 summary response must be an object, got {type(summary).__name__}"
            raise TypeError(msg)
        if not isinstance(positions, list):
            msg = f"Trading 212 positions response must be a list, got {type(positions).__name__}"
            raise TypeError(msg)

        account_currency = str(summary.get("currency") or summary.get("currencyCode") or "").upper()
        if not account_currency:
            msg = "Trading 212 summary missing account currency"
            raise ValueError(msg)

        account_fx = await self._currency_to_usd(account_currency)
        raw: list[RawBalance] = []

        cash = summary.get("cash", {})
        cash_total = (
            _to_decimal(_get_path(cash, "availableToTrade"))
            + _to_decimal(_get_path(cash, "reservedForOrders"))
            + _to_decimal(_get_path(cash, "inPies"))
        )
        if cash_total != 0:
            raw.append(
                RawBalance(
                    asset=account_currency,
                    amount=cash_total,
                    price=account_fx,
                    raw_json=json.dumps(summary),
                )
            )

        for position in positions:
            ticker = str(_get_path(position, "instrument", "ticker") or "")
            quantity = _to_decimal(position.get("quantity"))
            if not ticker or quantity == 0:
                continue
            price_usd = await self._position_price_usd(position, account_currency, account_fx)
            raw.append(
                RawBalance(
                    asset=ticker,
                    amount=quantity,
                    price=price_usd,
                    raw_json=json.dumps(position),
                )
            )

        logger.info("Trading 212: found %d non-zero balances", len(raw))
        return raw

    async def validate_connection(self) -> None:
        """Validate credentials with read-only account endpoints only."""
        summary = await self._get_json(_SUMMARY_PATH)
        positions = await self._get_json(_POSITIONS_PATH)
        if not isinstance(summary, dict):
            msg = f"Trading 212 summary response must be an object, got {type(summary).__name__}"
            raise TypeError(msg)
        if not isinstance(positions, list):
            msg = f"Trading 212 positions response must be a list, got {type(positions).__name__}"
            raise TypeError(msg)

    async def _position_price_usd(
        self,
        position: dict[str, Any],
        account_currency: str,
        account_fx: Decimal,
    ) -> Decimal:
        quantity = _to_decimal(position.get("quantity"))
        if quantity == 0:
            return Decimal(0)

        wallet_impact = position.get("walletImpact", {})
        current_value = _to_decimal(_get_path(wallet_impact, "currentValue"))
        wallet_currency = str(_get_path(wallet_impact, "currency") or account_currency).upper()
        if current_value != 0 and wallet_currency:
            wallet_fx = (
                account_fx if wallet_currency == account_currency else await self._currency_to_usd(wallet_currency)
            )
            return (current_value * wallet_fx) / quantity

        instrument_currency = str(_get_path(position, "instrument", "currency") or account_currency).upper()
        current_price = _to_decimal(position.get("currentPrice"))
        if current_price != 0 and instrument_currency:
            instrument_fx = (
                account_fx
                if instrument_currency == account_currency
                else await self._currency_to_usd(instrument_currency)
            )
            return current_price * instrument_fx

        ticker = str(_get_path(position, "instrument", "ticker") or "UNKNOWN")
        msg = f"Trading 212 position {ticker!r} is missing valuation data"
        raise ValueError(msg)

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch orders, cash transactions, and dividends."""
        transactions: list[Transaction] = []
        transactions.extend(await self._fetch_history(_HISTORY_ORDERS_PATH, self._parse_order_item, since=since))
        transactions.extend(await self._fetch_history(_HISTORY_TRANSACTIONS_PATH, self._parse_cash_item, since=since))
        transactions.extend(await self._fetch_history(_HISTORY_DIVIDENDS_PATH, self._parse_dividend_item, since=since))
        transactions.sort(key=lambda tx: (tx.date, tx.tx_id), reverse=True)
        logger.info("Trading 212: parsed %d historical transactions", len(transactions))
        return transactions

    async def _fetch_history(
        self,
        base_path: str,
        parser: ItemParser,
        *,
        since: date | None = None,
    ) -> list[Transaction]:
        """Page through a Trading 212 history endpoint until exhaustion or lower bound is crossed."""
        next_query: str | None = f"limit={_HISTORY_LIMIT}"
        transactions: list[Transaction] = []

        while next_query is not None:
            path = f"{base_path}?{next_query}"
            payload = await self._get_json(path, history=True)
            if not isinstance(payload, dict):
                msg = f"Trading 212 history response for {base_path} must be an object"
                raise TypeError(msg)

            items = payload.get("items", [])
            if not isinstance(items, list):
                msg = f"Trading 212 history items for {base_path} must be a list"
                raise TypeError(msg)

            oldest_item_date: date | None = None
            for item in items:
                if not isinstance(item, dict):
                    continue
                item_date = _extract_item_date(item)
                if item_date is not None and (oldest_item_date is None or item_date < oldest_item_date):
                    oldest_item_date = item_date

                tx = await parser(item)
                if tx is None:
                    continue
                if since is not None and tx.date < since:
                    continue
                transactions.append(tx)

            if since is not None and oldest_item_date is not None and oldest_item_date < since:
                break

            next_page_path = payload.get("nextPagePath")
            next_query = str(next_page_path) if next_page_path else None

        return transactions

    async def _parse_order_item(self, item: dict[str, Any]) -> Transaction | None:
        """Parse a filled order history item into a normalized trade transaction."""
        order = item.get("order", {})
        fill = item.get("fill", {})
        if not isinstance(order, dict) or not isinstance(fill, dict):
            return None
        if str(order.get("status", "")).upper() != "FILLED":
            return None
        if str(fill.get("type", "")).upper() != "TRADE":
            return None

        asset = str(order.get("ticker") or _get_path(order, "instrument", "ticker") or "")
        quantity = abs(_to_decimal(fill.get("quantity")))
        if not asset or quantity == 0:
            return None

        cash_currency = str(_get_path(fill, "walletImpact", "currency") or order.get("currency") or "").upper()
        if not cash_currency:
            return None
        net_value = _to_decimal(_get_path(fill, "walletImpact", "netValue"))
        if net_value == 0:
            net_value = _to_decimal(order.get("filledValue") or order.get("value"))

        trade_side = str(order.get("side", "")).lower()
        tx_id = str(fill.get("id") or order.get("id") or "")
        if not tx_id:
            tx_id = _synthetic_tx_id("trading212-order", item)

        tx_date = _extract_item_date(item) or self._pricing.today()
        usd_value = abs(net_value) * await self._currency_to_usd(cash_currency)
        item_with_endpoint = {**item, "_endpoint": "orders"}
        return Transaction(
            date=tx_date,
            source=self.source_name,
            tx_type=TransactionType.UNKNOWN,
            asset=asset,
            amount=quantity,
            usd_value=usd_value,
            counterparty_asset=cash_currency,
            counterparty_amount=abs(net_value),
            tx_id=tx_id,
            raw_json=json.dumps(item_with_endpoint),
            trade_side=trade_side,
        )

    async def _parse_cash_item(self, item: dict[str, Any]) -> Transaction | None:
        """Parse a cash transaction item."""
        currency = str(item.get("currency") or "").upper()
        amount = abs(_to_decimal(item.get("amount")))
        if not currency or amount == 0:
            return None

        tx_date = _extract_item_date(item) or self._pricing.today()
        tx_id = str(item.get("reference") or "")
        if not tx_id:
            tx_id = _synthetic_tx_id("trading212-cash", item)

        item_with_endpoint = {**item, "_endpoint": "cash"}
        return Transaction(
            date=tx_date,
            source=self.source_name,
            tx_type=TransactionType.UNKNOWN,
            asset=currency,
            amount=amount,
            usd_value=amount * await self._currency_to_usd(currency),
            tx_id=tx_id,
            raw_json=json.dumps(item_with_endpoint),
        )

    async def _parse_dividend_item(self, item: dict[str, Any]) -> Transaction | None:
        """Parse a dividend payout item."""
        amount, currency = _extract_amount_and_currency(item)
        if amount == 0 or not currency:
            return None

        tx_date = _extract_item_date(item) or self._pricing.today()
        tx_id = str(item.get("reference") or item.get("id") or "")
        if not tx_id:
            tx_id = _synthetic_tx_id("trading212-dividend", item)

        amount = abs(amount)
        currency = currency.upper()
        item_with_endpoint = {**item, "_endpoint": "dividends"}
        return Transaction(
            date=tx_date,
            source=self.source_name,
            tx_type=TransactionType.UNKNOWN,
            asset=currency,
            amount=amount,
            usd_value=amount * await self._currency_to_usd(currency),
            tx_id=tx_id,
            raw_json=json.dumps(item_with_endpoint),
        )


def _get_path(data: dict[str, Any], *path: str) -> object | None:
    current: object = data
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _to_decimal(value: object) -> Decimal:
    if value in (None, ""):
        return Decimal(0)
    try:
        return Decimal(str(value))
    except ArithmeticError:
        return Decimal(0)


def _parse_date(value: object) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).astimezone(UTC).date()
    except ValueError:
        return None


def _extract_item_date(item: dict[str, Any]) -> date | None:
    """Extract the most relevant date across Trading 212 history payload variants."""
    candidates = (
        _get_path(item, "fill", "filledAt"),
        item.get("dateTime"),
        item.get("paidAt"),
        item.get("paymentDate"),
        item.get("createdAt"),
        _get_path(item, "order", "createdAt"),
    )
    for candidate in candidates:
        parsed = _parse_date(candidate)
        if parsed is not None:
            return parsed
    return None


def _extract_amount_and_currency(item: dict[str, Any]) -> tuple[Decimal, str]:
    """Extract amount/currency from flexible dividend payload shapes."""
    for field in ("paidAmount", "amount", "value"):
        value = item.get(field)
        if isinstance(value, dict):
            amount = _to_decimal(value.get("amount") or value.get("value"))
            currency = str(value.get("currency") or value.get("currencyCode") or "")
            if amount != 0 and currency:
                return amount, currency

    amount = _to_decimal(item.get("paidAmount") or item.get("amount") or item.get("value"))
    currency = str(item.get("currency") or item.get("currencyCode") or "")
    return amount, currency


def _synthetic_tx_id(prefix: str, payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]
    return f"{prefix}-{digest}"
