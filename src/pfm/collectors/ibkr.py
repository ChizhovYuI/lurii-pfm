"""IBKR collector — reads portfolio via Flex Query automated retrieval."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING

import httpx

from pfm.collectors import register_collector
from pfm.collectors._retry import retry
from pfm.collectors.base import BaseCollector
from pfm.db.models import RawBalance, Transaction, TransactionType
from pfm.enums import SourceName

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_FLEX_BASE = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService"
_MAX_POLL_ATTEMPTS = 10
_POLL_DELAY_SECONDS = 5
_SEND_REQUEST_MIN_INTERVAL_SECONDS = 15.0
_STATEMENT_CACHE_TTL_SECONDS = 60.0
_CASH_TAGS: tuple[str, ...] = ("CashReport", "CashReportCurrency")
_CASH_AMOUNT_FIELDS: tuple[str, ...] = (
    "endingCash",
    "endingSettledCash",
    "settledCash",
    "totalCashValue",
)


@register_collector
class IbkrCollector(BaseCollector):
    """Collector for Interactive Brokers via Flex Query Web Service."""

    source_name = SourceName.IBKR

    def __init__(
        self,
        pricing: PricingService,
        *,
        flex_token: str,
        flex_query_id: str,
    ) -> None:
        super().__init__(pricing)
        self._flex_token = flex_token
        self._flex_query_id = flex_query_id
        self._client = httpx.AsyncClient(timeout=60.0)
        self._last_send_request_at: float = 0.0
        self._statement_cache: tuple[str, float] | None = None

    async def _throttle_send_request(self) -> None:
        """Ensure enough delay between Flex SendRequest calls."""
        now = time.monotonic()
        elapsed = now - self._last_send_request_at
        if elapsed < _SEND_REQUEST_MIN_INTERVAL_SECONDS:
            await asyncio.sleep(_SEND_REQUEST_MIN_INTERVAL_SECONDS - elapsed)

    @retry(max_attempts=2)
    async def _request_statement(self) -> str:
        """Step 1: Request Flex statement generation, returns reference code."""
        await self._throttle_send_request()
        resp = await self._client.get(
            f"{_FLEX_BASE}/SendRequest",
            params={"t": self._flex_token, "q": self._flex_query_id, "v": "3"},
        )
        self._last_send_request_at = time.monotonic()
        resp.raise_for_status()
        # Response is XML: <FlexStatementResponse><Status>Success</Status><ReferenceCode>xxx</ReferenceCode>...
        text: str = resp.text
        if "<Status>Success</Status>" not in text:
            msg = f"IBKR Flex request failed: {text}"
            raise ValueError(msg)

        start = text.find("<ReferenceCode>") + len("<ReferenceCode>")
        end = text.find("</ReferenceCode>")
        if start == -1 or end == -1:
            msg = f"Cannot parse reference code from: {text}"
            raise ValueError(msg)

        return text[start:end]

    async def _get_statement_xml(self) -> str:
        """Fetch Flex XML with a short-lived cache to avoid duplicate SendRequest calls."""
        if self._statement_cache is not None:
            cached_xml, cached_at = self._statement_cache
            if time.monotonic() - cached_at < _STATEMENT_CACHE_TTL_SECONDS:
                return cached_xml

        reference_code = await self._request_statement()
        xml_text = await self._fetch_statement(reference_code)
        self._statement_cache = (xml_text, time.monotonic())
        return xml_text

    async def _fetch_statement(self, reference_code: str) -> str:
        """Step 2: Poll until the statement is ready, return XML content."""
        for attempt in range(1, _MAX_POLL_ATTEMPTS + 1):
            resp = await self._client.get(
                f"{_FLEX_BASE}/GetStatement",
                params={"t": self._flex_token, "q": reference_code, "v": "3"},
            )
            resp.raise_for_status()
            text: str = resp.text

            if "<FlexQueryResponse" in text or "<FlexStatements" in text:
                return text

            if "Statement generation in progress" in text:
                logger.debug("IBKR: statement not ready, attempt %d/%d", attempt, _MAX_POLL_ATTEMPTS)
                await asyncio.sleep(_POLL_DELAY_SECONDS)
                continue

            msg = f"IBKR unexpected response: {text[:200]}"
            raise ValueError(msg)

        msg = "IBKR: statement generation timed out"
        raise TimeoutError(msg)

    def _parse_positions_from_xml(self, xml_text: str) -> list[dict[str, str]]:
        """Parse OpenPosition elements from Flex XML without a full XML parser."""
        positions: list[dict[str, str]] = []
        for match in re.finditer(r"<OpenPosition\s+(.*?)/>", xml_text, re.DOTALL):
            attrs_str = match.group(1)
            attrs: dict[str, str] = {}
            for attr_match in re.finditer(r'(\w+)="([^"]*)"', attrs_str):
                attrs[attr_match.group(1)] = attr_match.group(2)
            if attrs:
                positions.append(attrs)
        return positions

    def _parse_cash_from_xml(self, xml_text: str) -> list[dict[str, str]]:
        """Parse CashReport elements from Flex XML."""
        cash_items: list[dict[str, str]] = []
        for tag in _CASH_TAGS:
            for match in re.finditer(rf"<{tag}\s+([^>]*?)\s*/?>", xml_text, re.DOTALL):
                attrs_str = match.group(1)
                attrs: dict[str, str] = {}
                for attr_match in re.finditer(r'(\w+)="([^"]*)"', attrs_str):
                    attrs[attr_match.group(1)] = attr_match.group(2)
                if attrs:
                    cash_items.append(attrs)
        return cash_items

    def _parse_trades_from_xml(self, xml_text: str) -> list[dict[str, str]]:
        """Parse Trade elements from Flex XML."""
        trades: list[dict[str, str]] = []
        for match in re.finditer(r"<Trade\s+(.*?)/>", xml_text, re.DOTALL):
            attrs_str = match.group(1)
            attrs: dict[str, str] = {}
            for attr_match in re.finditer(r'(\w+)="([^"]*)"', attrs_str):
                attrs[attr_match.group(1)] = attr_match.group(2)
            if attrs:
                trades.append(attrs)
        return trades

    async def fetch_raw_balances(self) -> list[RawBalance]:
        """Fetch positions and cash balances via Flex Query."""
        xml_text = await self._get_statement_xml()
        raw: list[RawBalance] = []

        # Open positions (stocks, ETFs) — prefer SUMMARY level to avoid lot duplicates.
        # Some Flex payloads omit levelOfDetail entirely; treat those as acceptable.
        positions = self._parse_positions_from_xml(xml_text)
        for pos in positions:
            level_of_detail = pos.get("levelOfDetail", "").upper()
            if level_of_detail and level_of_detail != "SUMMARY":
                continue

            symbol = pos.get("symbol", "").upper()
            quantity = Decimal(pos.get("position", "0"))
            market_value_str = pos.get("positionValue", pos.get("markMarketValue", "0"))
            market_value = Decimal(market_value_str)

            if quantity == 0 or not symbol:
                continue

            price = market_value / quantity if quantity else Decimal(0)
            raw.append(
                RawBalance(
                    asset=symbol,
                    amount=quantity,
                    price=price,  # IBKR provides USD value directly
                    raw_json=json.dumps(pos),
                )
            )

        # Cash balances, grouped by currency to avoid duplicate rows from multiple cash tags.
        cash_items = self._parse_cash_from_xml(xml_text)
        cash_by_currency: dict[str, Decimal] = {}
        for cash in cash_items:
            currency = cash.get("currency", "").upper()
            ending_cash = _parse_cash_amount(cash)

            if ending_cash == 0 or not currency or currency == "BASE_SUMMARY":
                continue

            cash_by_currency[currency] = cash_by_currency.get(currency, Decimal(0)) + ending_cash

        for currency, amount in cash_by_currency.items():
            raw.append(
                RawBalance(
                    asset=currency,
                    amount=amount,
                    raw_json=json.dumps({"currency": currency, "endingCash": str(amount)}),
                )
            )

        logger.info("IBKR: found %d positions + cash balances", len(raw))
        return raw

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch trades from the Flex Query statement."""
        xml_text = await self._get_statement_xml()
        trades = self._parse_trades_from_xml(xml_text)
        transactions: list[Transaction] = []

        for trade in trades:
            tx = self._parse_trade(trade)
            if tx is None:
                continue
            if since and tx.date < since:
                continue
            transactions.append(tx)

        logger.info("IBKR: parsed %d trades", len(transactions))
        return transactions

    @staticmethod
    def _parse_trade(trade: dict[str, str]) -> Transaction | None:
        """Parse a Flex Trade element into a Transaction."""
        symbol = trade.get("symbol", "").upper()
        quantity = Decimal(trade.get("quantity", "0"))
        proceeds = Decimal(trade.get("proceeds", "0"))
        trade_date_str = trade.get("tradeDate", "")

        if not symbol:
            return None

        try:
            tx_date = date.fromisoformat(trade_date_str)
        except ValueError:
            return None

        return Transaction(
            date=tx_date,
            source="ibkr",
            tx_type=TransactionType.UNKNOWN,
            asset=symbol,
            amount=abs(quantity),
            usd_value=abs(proceeds),
            tx_id=str(trade.get("tradeID", "")),
            raw_json=json.dumps(trade),
        )


def _parse_cash_amount(cash: dict[str, str]) -> Decimal:
    """Parse the first available IBKR cash amount field."""
    for field in _CASH_AMOUNT_FIELDS:
        value = cash.get(field, "")
        if not value:
            continue
        amount = _to_decimal(value)
        if amount != 0:
            return amount
    return Decimal(0)


def _to_decimal(value: str) -> Decimal:
    """Parse IBKR numeric strings (commas, parentheses negatives)."""
    normalized = value.strip().replace(",", "")
    if normalized.startswith("(") and normalized.endswith(")"):
        normalized = f"-{normalized[1:-1]}"
    try:
        return Decimal(normalized)
    except ArithmeticError:
        return Decimal(0)
