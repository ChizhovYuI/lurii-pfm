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
from pfm.collectors._retry import RateLimiter, retry
from pfm.collectors.base import BaseCollector
from pfm.db.models import Snapshot, Transaction, TransactionType

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.bybit.com"
_RECV_WINDOW = "20000"
_RATE_LIMITER = RateLimiter(requests_per_second=10.0)


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

    async def fetch_balances(self) -> list[Snapshot]:
        """Fetch wallet balances from Bybit unified account."""
        today = self._pricing.today()
        snapshots: list[Snapshot] = []

        for account_type in ("UNIFIED", "SPOT", "FUND"):
            try:
                data = await self._get(
                    "/v5/account/wallet-balance",
                    params={"accountType": account_type},
                )
            except (httpx.HTTPStatusError, ValueError):
                logger.debug("Bybit: account type %s not available", account_type)
                continue

            for account in data.get("result", {}).get("list", []):
                for coin in account.get("coin", []):
                    ticker = str(coin.get("coin", "")).upper()
                    wallet_balance = Decimal(str(coin.get("walletBalance", "0")))

                    if wallet_balance == 0 or not ticker:
                        continue

                    # Check if already counted from another account type
                    existing_assets = {s.asset for s in snapshots}
                    if ticker in existing_assets:
                        continue

                    try:
                        usd_value = await self._pricing.convert_to_usd(wallet_balance, ticker)
                    except ValueError:
                        logger.warning("Bybit: cannot price %s, skipping", ticker)
                        continue

                    snapshots.append(
                        Snapshot(
                            date=today,
                            source=self.source_name,
                            asset=ticker,
                            amount=wallet_balance,
                            usd_value=usd_value,
                            raw_json=json.dumps(coin),
                        )
                    )

        logger.info("Bybit: found %d non-zero balances", len(snapshots))
        return snapshots

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
        change = Decimal(str(item.get("cashFlow", "0")))
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
