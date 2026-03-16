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
from pfm.collectors._math import apr_to_apy
from pfm.collectors._retry import RateLimiter, retry
from pfm.collectors.base import BaseCollector
from pfm.db.models import RawBalance, Transaction, TransactionType
from pfm.enums import SourceName

if TYPE_CHECKING:
    from datetime import date

    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_OKX_DOMAINS = ("https://www.okx.com", "https://my.okx.com")
_HTTP_UNAUTHORIZED = 401
_RATE_LIMITER = RateLimiter(requests_per_minute=300.0)  # 10 req/2s


@register_collector
class OkxCollector(BaseCollector):
    """Collector for OKX exchange."""

    source_name = SourceName.OKX

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
        self._client = httpx.AsyncClient(base_url=_OKX_DOMAINS[0], timeout=30.0)
        self._domain_resolved = False

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
        """Make a signed GET request to OKX API.

        On first call, if the primary domain returns 401, the method
        automatically retries with the fallback domain and locks the
        working domain for all subsequent requests.
        """
        await _RATE_LIMITER.acquire()
        query = ""
        if params:
            query = "?" + "&".join(f"{k}={v}" for k, v in params.items())
        headers = self._sign_request("GET", path + query)
        resp = await self._client.get(path, params=params, headers=headers)

        if resp.status_code == _HTTP_UNAUTHORIZED and not self._domain_resolved:
            fallback = _OKX_DOMAINS[1]
            logger.info("OKX: 401 on %s, trying %s", self._client.base_url, fallback)
            self._client = httpx.AsyncClient(base_url=fallback, timeout=30.0)
            await _RATE_LIMITER.acquire()
            headers = self._sign_request("GET", path + query)
            resp = await self._client.get(path, params=params, headers=headers)

        resp.raise_for_status()
        self._domain_resolved = True
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

    async def _fetch_savings_apr(self, ccy: str, balance: Decimal) -> Decimal:
        """Get effective APR for a savings currency (includes bonus rate).

        Primary: ``lending-history`` → sum(earnings) * 8760 / balance.
        Fallback: ``lending-rate-summary`` → ``estRate``.
        """
        try:
            data = await self._get("/api/v5/finance/savings/lending-history", params={"ccy": ccy})
            entries = data.get("data", [])
            if entries:
                latest_ts = entries[0]["ts"]
                latest = [e for e in entries if e["ts"] == latest_ts]
                total_earnings = sum(Decimal(str(e["earnings"])) for e in latest)
                if balance > 0:
                    return total_earnings * 8760 / balance
        except (httpx.HTTPStatusError, KeyError, ValueError):
            logger.debug("OKX: lending-history unavailable for %s", ccy)

        try:
            summary = await self._get("/api/v5/finance/savings/lending-rate-summary", params={"ccy": ccy})
            items = summary.get("data", [])
            if items:
                return Decimal(str(items[0].get("estRate", "0")))
        except (httpx.HTTPStatusError, KeyError, ValueError):
            logger.debug("OKX: lending-rate-summary unavailable for %s", ccy)

        return Decimal(0)

    async def _fetch_earn_raw(self) -> list[RawBalance]:
        """Fetch earn account balances with APY as separate raw balances."""
        raw: list[RawBalance] = []

        # Savings
        try:
            savings_data = await self._get("/api/v5/finance/savings/balance")
            for item in savings_data.get("data", []):
                ticker = str(item.get("ccy", "")).upper()
                amount = Decimal(str(item.get("amt", "0")))
                if not ticker or amount == 0:
                    continue
                apr = await self._fetch_savings_apr(ticker, amount)
                apy = apr_to_apy(apr)
                raw.append(
                    RawBalance(
                        asset=ticker,
                        amount=amount,
                        apy=apy,
                    )
                )
        except httpx.HTTPStatusError:
            logger.warning("OKX: failed to fetch savings balances")

        # Staking
        try:
            staking_data = await self._get("/api/v5/finance/staking-defi/orders-active")
            for item in staking_data.get("data", []):
                ticker = str(item.get("ccy", "")).upper()
                amount = Decimal(str(item.get("investAmt", "0")))
                if not ticker or amount == 0:
                    continue
                est_apr = Decimal(str(item.get("estApr", "0")))
                apy = apr_to_apy(est_apr)
                raw.append(
                    RawBalance(
                        asset=ticker,
                        amount=amount,
                        apy=apy,
                    )
                )
        except httpx.HTTPStatusError:
            logger.warning("OKX: failed to fetch staking balances")

        return raw

    async def fetch_raw_balances(self) -> list[RawBalance]:
        """Fetch trading + funding + earn account balances."""
        totals: dict[str, Decimal] = {}
        await self._fetch_trading(totals)
        await self._fetch_funding(totals)

        raw: list[RawBalance] = []
        for ticker, amount in totals.items():
            raw.append(
                RawBalance(
                    asset=ticker,
                    amount=amount,
                )
            )

        # Earn accounts are separate — append as distinct raw balances with APY
        earn_raw = await self._fetch_earn_raw()
        raw.extend(earn_raw)

        logger.info("OKX: found %d non-zero balances", len(raw))
        return raw

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
