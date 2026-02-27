"""Binance TH collector — Thailand-specific API (v1 endpoints, THB pairs)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import httpx

from pfm.collectors import register_collector
from pfm.collectors.binance import BinanceCollector
from pfm.db.models import Snapshot, Transaction, TransactionType

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)
_HTTP_NOT_FOUND = 404


@register_collector
class BinanceThCollector(BinanceCollector):
    """Collector for Binance Thailand.

    Uses /api/v1 endpoints instead of /api/v3 (Binance Global).
    Spot-only exchange with THB trading pairs.
    """

    source_name = "binance_th"
    _base_url = "https://api.binance.th"
    _DEPOSIT_ENDPOINTS = ("/api/v1/capital/deposit/hisrec", "/sapi/v1/capital/deposit/hisrec")
    _WITHDRAW_ENDPOINTS = ("/api/v1/capital/withdraw/history", "/sapi/v1/capital/withdraw/history")

    def __init__(
        self,
        pricing: PricingService,
        *,
        api_key: str,
        api_secret: str,
    ) -> None:
        super().__init__(pricing, api_key=api_key, api_secret=api_secret)
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers={"X-MBX-APIKEY": api_key},
            timeout=30.0,
        )

    async def fetch_balances(self) -> list[Snapshot]:
        """Fetch spot account balances via /api/v1/accountV2."""
        today = self._pricing.today()
        data = await self._get("/api/v1/accountV2")
        snapshots: list[Snapshot] = []

        for bal in data.get("balances", []):
            free = Decimal(str(bal.get("free", "0")))
            locked = Decimal(str(bal.get("locked", "0")))
            total = free + locked
            ticker = str(bal.get("asset", "")).upper()

            if total == 0 or not ticker:
                continue

            try:
                usd_value = await self._pricing.convert_to_usd(total, ticker)
            except ValueError:
                logger.warning("Binance TH: cannot price %s, skipping", ticker)
                continue

            snapshots.append(
                Snapshot(
                    date=today,
                    source=self.source_name,
                    asset=ticker,
                    amount=total,
                    usd_value=usd_value,
                    raw_json=json.dumps(bal),
                )
            )

        logger.info("Binance TH: found %d non-zero balances", len(snapshots))
        return snapshots

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch deposit and withdrawal history via /api/v1 endpoints."""
        transactions: list[Transaction] = []
        params: dict[str, str] = {}
        if since:
            since_dt = datetime(since.year, since.month, since.day, tzinfo=UTC)
            params["startTime"] = str(int(since_dt.timestamp() * 1000))

        deposits = await self._fetch_with_fallback(self._DEPOSIT_ENDPOINTS, params, "deposits")
        for dep in deposits:
            tx = self._parse_deposit_th(dep)
            if tx:
                transactions.append(tx)

        withdrawals = await self._fetch_with_fallback(self._WITHDRAW_ENDPOINTS, params, "withdrawals")
        for wd in withdrawals:
            tx = self._parse_withdrawal_th(wd)
            if tx:
                transactions.append(tx)

        logger.info("Binance TH: parsed %d transactions", len(transactions))
        return transactions

    async def _fetch_with_fallback(
        self,
        paths: tuple[str, ...],
        params: dict[str, str],
        label: str,
    ) -> list[dict[str, Any]]:
        """Fetch an endpoint with fallback from /api/v1 to /sapi/v1 on 404."""
        for i, path in enumerate(paths):
            try:
                payload = await self._get(path, params)
                return payload if isinstance(payload, list) else []
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                is_last = i == len(paths) - 1
                if status == _HTTP_NOT_FOUND and not is_last:
                    logger.info("Binance TH: endpoint %s returned 404, trying fallback path", path)
                    continue
                if status == _HTTP_NOT_FOUND and is_last:
                    logger.info(
                        "Binance TH: %s endpoint is unavailable (404) on known paths, skipping.",
                        label,
                    )
                    return []
                logger.warning(
                    "Binance TH: failed to fetch %s from %s (HTTP %d)",
                    label,
                    path,
                    status,
                )
                return []
        return []

    @staticmethod
    def _parse_deposit_th(dep: dict[str, Any]) -> Transaction | None:
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
            source="binance_th",
            tx_type=TransactionType.DEPOSIT,
            asset=ticker,
            amount=amount,
            usd_value=Decimal(0),
            tx_id=str(dep.get("txId", "")),
            raw_json=json.dumps(dep),
        )

    @staticmethod
    def _parse_withdrawal_th(wd: dict[str, Any]) -> Transaction | None:
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
            source="binance_th",
            tx_type=TransactionType.WITHDRAWAL,
            asset=ticker,
            amount=amount,
            usd_value=Decimal(0),
            tx_id=str(wd.get("id", "")),
            raw_json=json.dumps(wd),
        )
