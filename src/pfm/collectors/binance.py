"""Binance collector — reads spot balances and transaction history."""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import httpx

from pfm.collectors import register_collector
from pfm.collectors._auth import sign_binance
from pfm.collectors._retry import RateLimiter, retry
from pfm.collectors.base import BaseCollector
from pfm.db.models import Snapshot, Transaction, TransactionType

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_RATE_LIMITER = RateLimiter(requests_per_minute=600.0)


@register_collector
class BinanceCollector(BaseCollector):
    """Collector for Binance (global) exchange."""

    source_name = "binance"
    _base_url = "https://api.binance.com"

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
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers={"X-MBX-APIKEY": api_key},
            timeout=30.0,
        )

    def _signed_params(self, params: dict[str, str] | None = None) -> dict[str, str]:
        """Add timestamp and signature to request params."""
        p = dict(params or {})
        p["timestamp"] = str(int(time.time() * 1000))
        query = "&".join(f"{k}={v}" for k, v in p.items())
        p["signature"] = sign_binance(query, self._api_secret)
        return p

    @retry()
    async def _get(self, path: str, params: dict[str, str] | None = None) -> Any:  # noqa: ANN401
        """Make a signed GET request."""
        await _RATE_LIMITER.acquire()
        signed = self._signed_params(params)
        resp = await self._client.get(path, params=signed)
        resp.raise_for_status()
        return resp.json()

    async def fetch_balances(self) -> list[Snapshot]:
        """Fetch spot account balances."""
        today = self._pricing.today()
        data = await self._get("/api/v3/account")
        snapshots: list[Snapshot] = []

        for bal in data.get("balances", []):
            free = Decimal(str(bal.get("free", "0")))
            locked = Decimal(str(bal.get("locked", "0")))
            total = free + locked
            ticker = str(bal.get("asset", "")).upper()

            if total == 0 or not ticker:
                continue

            try:
                price = await self._pricing.get_price_usd(ticker)
            except ValueError:
                logger.warning("Binance: cannot price %s, skipping", ticker)
                continue

            usd_value = total * price
            snapshots.append(
                Snapshot(
                    date=today,
                    source=self.source_name,
                    asset=ticker,
                    amount=total,
                    usd_value=usd_value,
                    price=price,
                    raw_json=json.dumps(bal),
                )
            )

        logger.info("Binance: found %d non-zero balances", len(snapshots))
        return snapshots

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch deposit and withdrawal history."""
        transactions: list[Transaction] = []

        # Deposits
        params: dict[str, str] = {}
        if since:
            since_dt = datetime(since.year, since.month, since.day, tzinfo=UTC)
            params["startTime"] = str(int(since_dt.timestamp() * 1000))

        try:
            deposits = await self._get("/sapi/v1/capital/deposit/hisrec", params)
            for dep in deposits:
                tx = self._parse_deposit(dep)
                if tx:
                    transactions.append(tx)
        except httpx.HTTPStatusError as exc:
            logger.warning("Binance: failed to fetch deposits: %s", exc)

        # Withdrawals
        try:
            withdrawals = await self._get("/sapi/v1/capital/withdraw/history", params)
            for wd in withdrawals:
                tx = self._parse_withdrawal(wd)
                if tx:
                    transactions.append(tx)
        except httpx.HTTPStatusError as exc:
            logger.warning("Binance: failed to fetch withdrawals: %s", exc)

        logger.info("Binance: parsed %d transactions", len(transactions))
        return transactions

    @staticmethod
    def _parse_deposit(dep: dict[str, Any]) -> Transaction | None:
        ticker = str(dep.get("coin", "")).upper()
        amount = Decimal(str(dep.get("amount", "0")))
        if not ticker or amount == 0:
            return None

        ts_ms = dep.get("insertTime", 0)
        try:
            tx_date = datetime.fromtimestamp(int(ts_ms) / 1000, tz=UTC).date()
        except (ValueError, OSError):
            tx_date = datetime.now(tz=UTC).date()

        return Transaction(
            date=tx_date,
            source="binance",
            tx_type=TransactionType.DEPOSIT,
            asset=ticker,
            amount=amount,
            usd_value=Decimal(0),
            tx_id=str(dep.get("txId", "")),
            raw_json=json.dumps(dep),
        )

    @staticmethod
    def _parse_withdrawal(wd: dict[str, Any]) -> Transaction | None:
        ticker = str(wd.get("coin", "")).upper()
        amount = Decimal(str(wd.get("amount", "0")))
        if not ticker or amount == 0:
            return None

        apply_time = wd.get("applyTime", "")
        try:
            tx_date = datetime.fromisoformat(apply_time).date()
        except (ValueError, AttributeError):
            tx_date = datetime.now(tz=UTC).date()

        return Transaction(
            date=tx_date,
            source="binance",
            tx_type=TransactionType.WITHDRAWAL,
            asset=ticker,
            amount=amount,
            usd_value=Decimal(0),
            tx_id=str(wd.get("id", "")),
            raw_json=json.dumps(wd),
        )
