"""Bybit collector — reads wallet balances and transaction log via V5 API."""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import httpx

from pfm.collectors import register_collector
from pfm.collectors._auth import sign_bybit
from pfm.collectors._math import apr_to_apy
from pfm.collectors._retry import RateLimiter, retry
from pfm.collectors.base import BaseCollector
from pfm.db.models import RawBalance, Transaction, TransactionType

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.bybit.com"
_RECV_WINDOW = "20000"
_RATE_LIMITER = RateLimiter(requests_per_minute=600.0)


@register_collector
class BybitCollector(BaseCollector):
    """Collector for Bybit exchange via V5 API."""

    source_name = "bybit"

    def __init__(
        self,
        pricing: PricingService,
        *,
        api_key: str,
        api_secret: str,
    ) -> None:
        super().__init__(pricing)
        self._api_key = api_key
        self._api_secret = api_secret
        self._client = httpx.AsyncClient(base_url=_BASE_URL, timeout=30.0)

    def _signed_headers(self, params_str: str) -> dict[str, str]:
        """Generate signed headers for Bybit V5 API."""
        timestamp = str(int(time.time() * 1000))
        signature = sign_bybit(timestamp, self._api_key, _RECV_WINDOW, params_str, self._api_secret)
        return {
            "X-BAPI-API-KEY": self._api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": _RECV_WINDOW,
            "Content-Type": "application/json",
        }

    @retry()
    async def _get(self, path: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        """Make a signed GET request to Bybit V5 API."""
        await _RATE_LIMITER.acquire()
        query = "&".join(f"{k}={v}" for k, v in (params or {}).items())
        headers = self._signed_headers(query)
        resp = await self._client.get(path, params=params, headers=headers)
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        if data.get("retCode") != 0:
            msg = f"Bybit API error: {data.get('retMsg', 'unknown')}"
            raise ValueError(msg)
        return data

    @staticmethod
    def _accumulate(totals: dict[str, Decimal], ticker: str, amount: Decimal) -> None:
        """Add amount to running totals for a ticker."""
        if amount != 0 and ticker:
            totals[ticker] = totals.get(ticker, Decimal(0)) + amount

    @staticmethod
    def _to_decimal(value: object) -> Decimal:
        """Parse decimal-like values from API payloads, falling back to 0."""
        try:
            return Decimal(str(value))
        except (ArithmeticError, TypeError, ValueError):
            return Decimal(0)

    @classmethod
    def _to_apr(cls, value: object) -> Decimal:
        """Parse APR-like fields that may be fraction (0.06) or percent-like (6, 6%)."""
        apr_raw = str(value).strip().rstrip("%")
        apr = cls._to_decimal(apr_raw)
        return apr / 100 if apr > 1 else apr

    async def _fetch_unified(self, totals: dict[str, Decimal]) -> None:
        """Fetch unified trading account balances."""
        try:
            data = await self._get(
                "/v5/account/wallet-balance",
                params={"accountType": "UNIFIED"},
            )
        except (httpx.HTTPStatusError, ValueError):
            logger.debug("Bybit: UNIFIED account not available")
            return

        for account in data.get("result", {}).get("list", []):
            for coin in account.get("coin", []):
                ticker = str(coin.get("coin", "")).upper()
                self._accumulate(totals, ticker, self._to_decimal(coin.get("walletBalance", "0")))

    async def _fetch_funding(self, totals: dict[str, Decimal]) -> None:
        """Fetch funding account balances."""
        try:
            data = await self._get(
                "/v5/asset/transfer/query-account-coins-balance",
                params={"accountType": "FUND"},
            )
        except (httpx.HTTPStatusError, ValueError):
            logger.debug("Bybit: FUND account not available")
            return

        for item in data.get("result", {}).get("balance", []):
            ticker = str(item.get("coin", "")).upper()
            self._accumulate(totals, ticker, self._to_decimal(item.get("walletBalance", "0")))

    async def _fetch_earn_raw(self) -> list[RawBalance]:
        """Fetch Bybit Earn positions with APY as separate raw balances."""
        raw: list[RawBalance] = []
        for category in ("FlexibleSaving", "OnChain"):
            apr_by_product_id, apr_by_coin = await self._fetch_earn_product_apr_maps(category)
            try:
                data = await self._get(
                    "/v5/earn/position",
                    params={"category": category},
                )
            except (httpx.HTTPStatusError, ValueError):
                logger.warning("Bybit: failed to fetch %s earn positions", category)
                continue

            for item in data.get("result", {}).get("list", []):
                ticker = str(item.get("coin", "")).upper()
                amount = self._to_decimal(item.get("amount", "0"))
                if not ticker or amount == 0:
                    continue

                # Primary: yesterdayYield → APR
                yesterday_yield = self._to_decimal(item.get("yesterdayYield", "0"))
                if yesterday_yield > 0 and amount > 0:
                    apr = yesterday_yield * 365 / amount
                else:
                    # Fallback: estimateApr string from position
                    apr = self._to_apr(item.get("estimateApr", "0"))
                    if apr <= 0:
                        product_id = str(item.get("productId", "")).strip()
                        if product_id:
                            apr = apr_by_product_id.get(product_id, Decimal(0))
                    if apr <= 0:
                        apr = apr_by_coin.get(ticker, Decimal(0))

                apy = apr_to_apy(apr)
                raw.append(
                    RawBalance(
                        asset=ticker,
                        amount=amount,
                        apy=apy,
                        raw_json=json.dumps({"account_type": "earn", "category": category, "row": item}),
                    )
                )

        return raw

    async def _fetch_earn_product_apr_maps(self, category: str) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
        """Fetch Earn product APR maps keyed by productId and coin symbol."""
        try:
            data = await self._get(
                "/v5/earn/product",
                params={"category": category},
            )
        except (httpx.HTTPStatusError, ValueError):
            logger.warning("Bybit: failed to fetch %s earn products", category)
            return {}, {}

        apr_by_product_id: dict[str, Decimal] = {}
        apr_by_coin: dict[str, Decimal] = {}
        for item in data.get("result", {}).get("list", []):
            product_id = str(item.get("productId", "")).strip()
            apr = self._to_apr(item.get("estimateApr", "0"))
            if apr <= 0:
                continue
            if product_id:
                apr_by_product_id[product_id] = apr
            coin = str(item.get("coin", "")).upper().strip()
            if coin:
                apr_by_coin[coin] = apr
        return apr_by_product_id, apr_by_coin

    async def fetch_raw_balances(self) -> list[RawBalance]:
        """Fetch unified + funding + earn account balances."""
        totals: dict[str, Decimal] = {}
        await self._fetch_unified(totals)
        await self._fetch_funding(totals)

        raw: list[RawBalance] = []
        for ticker, amount in totals.items():
            if amount == 0:
                continue
            raw.append(
                RawBalance(
                    asset=ticker,
                    amount=amount,
                )
            )

        # Earn accounts are separate — append as distinct raw balances with APY
        earn_raw = await self._fetch_earn_raw()
        raw.extend(earn_raw)

        logger.info("Bybit: found %d non-zero balances", len(raw))
        return raw

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch transaction log from Bybit."""
        data = await self._get("/v5/account/transaction-log", params={"limit": "50"})
        transactions: list[Transaction] = []

        for item in data.get("result", {}).get("list", []):
            tx = self._parse_transaction(item)
            if tx is None:
                continue
            if since and tx.date < since:
                continue
            transactions.append(tx)

        logger.info("Bybit: parsed %d transactions", len(transactions))
        return transactions

    @staticmethod
    def _parse_transaction(item: dict[str, Any]) -> Transaction | None:
        """Parse a Bybit transaction log entry."""
        ticker = str(item.get("currency", "")).upper()
        change = BybitCollector._to_decimal(item.get("cashFlow", "0"))
        if not ticker:
            return None

        ts_ms = item.get("transactionTime", "0")
        try:
            tx_date = datetime.fromtimestamp(int(ts_ms) / 1000, tz=UTC).date()
        except (ValueError, OSError):
            tx_date = datetime.now(tz=UTC).date()

        tx_type_str = str(item.get("type", "")).upper()
        if tx_type_str == "TRADE":
            tx_type = TransactionType.TRADE
        elif tx_type_str == "DEPOSIT":
            tx_type = TransactionType.DEPOSIT
        elif tx_type_str == "WITHDRAWAL":
            tx_type = TransactionType.WITHDRAWAL
        elif "INTEREST" in tx_type_str:
            tx_type = TransactionType.INTEREST
        else:
            tx_type = TransactionType.TRANSFER

        return Transaction(
            date=tx_date,
            source="bybit",
            tx_type=tx_type,
            asset=ticker,
            amount=abs(change),
            usd_value=Decimal(0),
            tx_id=str(item.get("id", "")),
            raw_json=json.dumps(item),
        )
