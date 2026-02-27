"""OKX collector — reads trading, funding, and earn balances via REST API v5."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import httpx

from pfm.collectors import register_collector
from pfm.collectors._auth import sign_okx
from pfm.collectors._retry import RateLimiter, retry
from pfm.collectors.base import BaseCollector
from pfm.db.models import Snapshot, Transaction, TransactionType

if TYPE_CHECKING:
    from datetime import date

    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.okx.com"
_RATE_LIMITER = RateLimiter(requests_per_minute=300.0)  # 10 req/2s


@register_collector
class OkxCollector(BaseCollector):
    """Collector for OKX exchange."""

    source_name = "okx"

    def __init__(
        self,
        pricing: PricingService,
        *,
        api_key: str,
        api_secret: str,
        passphrase: str,
    ) -> None:
        super().__init__(pricing)
        self._api_key = api_key
        self._api_secret = api_secret
        self._passphrase = passphrase
        self._client = httpx.AsyncClient(base_url=_BASE_URL, timeout=30.0)

    def _sign_request(self, method: str, path: str, body: str = "") -> dict[str, str]:
        """Generate signed headers for OKX API."""
        timestamp = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        signature = sign_okx(timestamp, method, path, body, self._api_secret)
        return {
            "OK-ACCESS-KEY": self._api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self._passphrase,
            "Content-Type": "application/json",
        }

    @retry()
    async def _get(self, path: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        """Make a signed GET request to OKX API."""
        await _RATE_LIMITER.acquire()
        query = ""
        if params:
            query = "?" + "&".join(f"{k}={v}" for k, v in params.items())
        headers = self._sign_request("GET", path + query)
        resp = await self._client.get(path, params=params, headers=headers)
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    @staticmethod
    def _accumulate(totals: dict[str, Decimal], ticker: str, amount: Decimal) -> None:
        """Add amount to running totals for a ticker."""
        if amount != 0 and ticker:
            totals[ticker] = totals.get(ticker, Decimal(0)) + amount

    async def _fetch_trading(self, totals: dict[str, Decimal]) -> None:
        trading_data = await self._get("/api/v5/account/balance")
        for account in trading_data.get("data", []):
            for detail in account.get("details", []):
                ticker = str(detail.get("ccy", "")).upper()
                self._accumulate(totals, ticker, Decimal(str(detail.get("eq", "0"))))

    async def _fetch_funding(self, totals: dict[str, Decimal]) -> None:
        funding_data = await self._get("/api/v5/asset/balances")
        for item in funding_data.get("data", []):
            ticker = str(item.get("ccy", "")).upper()
            total = Decimal(str(item.get("availBal", "0"))) + Decimal(str(item.get("frozenBal", "0")))
            self._accumulate(totals, ticker, total)

    async def _fetch_earn(self, totals: dict[str, Decimal]) -> None:
        try:
            savings_data = await self._get("/api/v5/finance/savings/balance")
            for item in savings_data.get("data", []):
                ticker = str(item.get("ccy", "")).upper()
                self._accumulate(totals, ticker, Decimal(str(item.get("amt", "0"))))
        except httpx.HTTPStatusError:
            logger.warning("OKX: failed to fetch savings balances")

        try:
            staking_data = await self._get("/api/v5/finance/staking-defi/orders-active")
            for item in staking_data.get("data", []):
                ticker = str(item.get("ccy", "")).upper()
                self._accumulate(totals, ticker, Decimal(str(item.get("investAmt", "0"))))
        except httpx.HTTPStatusError:
            logger.warning("OKX: failed to fetch staking balances")

    async def fetch_balances(self) -> list[Snapshot]:
        """Fetch trading + funding + earn account balances."""
        totals: dict[str, Decimal] = {}
        await self._fetch_trading(totals)
        await self._fetch_funding(totals)
        await self._fetch_earn(totals)

        today = self._pricing.today()
        snapshots: list[Snapshot] = []
        for ticker, amount in totals.items():
            usd_value = await self._pricing.convert_to_usd(amount, ticker)
            snapshots.append(
                Snapshot(
                    date=today,
                    source=self.source_name,
                    asset=ticker,
                    amount=amount,
                    usd_value=usd_value,
                )
            )

        logger.info("OKX: found %d non-zero balances", len(snapshots))
        return snapshots

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch recent bills/transactions from OKX."""
        bills_data = await self._get("/api/v5/account/bills", params={"limit": "100"})
        transactions: list[Transaction] = []

        for bill in bills_data.get("data", []):
            tx = self._parse_bill(bill)
            if tx is None:
                continue
            if since and tx.date < since:
                continue
            transactions.append(tx)

        logger.info("OKX: parsed %d transactions", len(transactions))
        return transactions

    @staticmethod
    def _parse_bill(bill: dict[str, Any]) -> Transaction | None:
        """Parse an OKX bill record."""
        ticker = str(bill.get("ccy", "")).upper()
        amount = Decimal(str(bill.get("balChg", "0")))
        if not ticker:
            return None

        ts_ms = bill.get("ts", "0")
        try:
            tx_date = datetime.fromtimestamp(int(ts_ms) / 1000, tz=UTC).date()
        except (ValueError, OSError):
            tx_date = datetime.now(tz=UTC).date()

        sub_type = str(bill.get("subType", ""))
        if sub_type in ("1", "2"):  # buy/sell
            tx_type = TransactionType.TRADE
        elif sub_type in ("13", "14"):  # deposit/withdrawal
            tx_type = TransactionType.DEPOSIT if amount > 0 else TransactionType.WITHDRAWAL
        else:
            tx_type = TransactionType.TRANSFER

        return Transaction(
            date=tx_date,
            source="okx",
            tx_type=tx_type,
            asset=ticker,
            amount=abs(amount),
            usd_value=Decimal(0),
            tx_id=str(bill.get("billId", "")),
            raw_json=json.dumps(bill),
        )
