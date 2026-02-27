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


@register_collector
class BinanceThCollector(BinanceCollector):
    """Collector for Binance Thailand.

    Uses /api/v1 endpoints instead of /api/v3 (Binance Global).
    Spot-only exchange with THB trading pairs.
    """

    source_name = "binance_th"
    _base_url = "https://api.binance.th"

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

        try:
            deposits = await self._get("/api/v1/capital/deposit/hisrec", params)
            for dep in deposits:
                tx = self._parse_deposit_th(dep)
                if tx:
                    transactions.append(tx)
        except httpx.HTTPStatusError as exc:
            logger.warning("Binance TH: failed to fetch deposits: %s", exc)

        try:
            withdrawals = await self._get("/api/v1/capital/withdraw/history", params)
            for wd in withdrawals:
                tx = self._parse_withdrawal_th(wd)
                if tx:
                    transactions.append(tx)
        except httpx.HTTPStatusError as exc:
            logger.warning("Binance TH: failed to fetch withdrawals: %s", exc)

        logger.info("Binance TH: parsed %d transactions", len(transactions))
        return transactions

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
