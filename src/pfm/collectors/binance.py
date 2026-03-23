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
from pfm.db.models import RawBalance, Transaction, TransactionType
from pfm.enums import SourceName

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_RATE_LIMITER = RateLimiter(requests_per_minute=600.0)
_CLOCK_DRIFT_LOG_THRESHOLD_MS = 500


@register_collector
class BinanceCollector(BaseCollector):
    """Collector for Binance (global) exchange."""

    source_name = SourceName.BINANCE
    _base_url = "https://api.binance.com"
    _server_time_path = "/api/v3/time"

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
        self._time_offset_ms = 0
        self._time_synced = False

    async def _sync_server_time(self) -> None:
        """Fetch exchange server time and compute clock offset to prevent 400 errors."""
        try:
            resp = await self._client.get(self._server_time_path)
            resp.raise_for_status()
            server_time = resp.json()["serverTime"]
            local_time = int(time.time() * 1000)
            self._time_offset_ms = server_time - local_time
            if abs(self._time_offset_ms) > _CLOCK_DRIFT_LOG_THRESHOLD_MS:
                logger.info("%s: server clock offset: %dms", self.source_name, self._time_offset_ms)
        except (httpx.HTTPError, KeyError):
            logger.warning("%s: failed to sync server time, using local clock", self.source_name)
        self._time_synced = True

    def _signed_params(self, params: dict[str, str] | None = None) -> dict[str, str]:
        """Add timestamp and signature to request params."""
        p = dict(params or {})
        p["timestamp"] = str(int(time.time() * 1000) + self._time_offset_ms)
        query = "&".join(f"{k}={v}" for k, v in p.items())
        p["signature"] = sign_binance(query, self._api_secret)
        return p

    @retry()
    async def _get(self, path: str, params: dict[str, str] | None = None) -> Any:  # noqa: ANN401
        """Make a signed GET request."""
        if not self._time_synced:
            await self._sync_server_time()
        await _RATE_LIMITER.acquire()
        signed = self._signed_params(params)
        resp = await self._client.get(path, params=signed)
        resp.raise_for_status()
        return resp.json()

    async def fetch_raw_balances(self) -> list[RawBalance]:
        """Fetch spot account balances."""
        data = await self._get("/api/v3/account")
        raw: list[RawBalance] = []

        for bal in data.get("balances", []):
            free = Decimal(str(bal.get("free", "0")))
            locked = Decimal(str(bal.get("locked", "0")))
            total = free + locked
            ticker = str(bal.get("asset", "")).upper()

            if total == 0 or not ticker:
                continue

            raw.append(
                RawBalance(
                    asset=ticker,
                    amount=total,
                    raw_json=json.dumps(bal),
                )
            )

        logger.info("Binance: found %d non-zero balances", len(raw))
        return raw

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

        dep_with_direction = {**dep, "_direction": "deposit"}
        return Transaction(
            date=tx_date,
            source="binance",
            tx_type=TransactionType.UNKNOWN,
            asset=ticker,
            amount=amount,
            usd_value=Decimal(0),
            tx_id=str(dep.get("txId", "")),
            raw_json=json.dumps(dep_with_direction),
        )

    @staticmethod
    def _parse_withdrawal(wd: dict[str, Any]) -> Transaction | None:
        ticker = str(wd.get("coin", "")).upper()
        amount = Decimal(str(wd.get("amount", "0")))
        if not ticker or amount == 0:
            return None

        apply_time = wd.get("applyTime", "")
        try:
            tx_date = datetime.fromisoformat(apply_time).replace(tzinfo=UTC).date()
        except (ValueError, AttributeError):
            tx_date = datetime.now(tz=UTC).date()

        wd_with_direction = {**wd, "_direction": "withdrawal"}
        return Transaction(
            date=tx_date,
            source="binance",
            tx_type=TransactionType.UNKNOWN,
            asset=ticker,
            amount=amount,
            usd_value=Decimal(0),
            tx_id=str(wd.get("id", "")),
            raw_json=json.dumps(wd_with_direction),
        )
