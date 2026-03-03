"""MEXC collector — reads spot/contract balances and transfer history."""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import httpx

from pfm.collectors import register_collector
from pfm.collectors._auth import sign_binance
from pfm.collectors._retry import RateLimiter, retry
from pfm.collectors.base import BaseCollector
from pfm.db.models import Snapshot, Transaction, TransactionType

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_SPOT_BASE_URL = "https://api.mexc.com"
_CONTRACT_BASE_URL = "https://contract.mexc.com"
_CONTRACT_ASSETS_PATH = "/api/v1/private/account/assets"
_RATE_LIMITER = RateLimiter(requests_per_minute=600.0)
_EARN_POSITION_PATHS: tuple[str, ...] = (
    "/api/v3/asset/earn/position",
    "/api/v3/earn/position",
    "/api/v3/savings/position",
    "/api/v3/staking/position",
)


@register_collector
class MexcCollector(BaseCollector):
    """Collector for MEXC Spot exchange (signed V3 endpoints)."""

    source_name = "mexc"

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
            base_url=_SPOT_BASE_URL,
            headers={"X-MEXC-APIKEY": api_key},
            timeout=30.0,
        )
        self._contract_client = httpx.AsyncClient(
            base_url=_CONTRACT_BASE_URL,
            timeout=30.0,
        )

    def _signed_params(self, params: dict[str, str] | None = None) -> dict[str, str]:
        """Add required timestamp/signature params for signed MEXC endpoints."""
        payload = dict(params or {})
        payload["timestamp"] = str(int(time.time() * 1000))
        query = "&".join(f"{k}={v}" for k, v in payload.items())
        payload["signature"] = sign_binance(query, self._api_secret)
        return payload

    @retry()
    async def _get(self, path: str, params: dict[str, str] | None = None) -> Any:  # noqa: ANN401
        await _RATE_LIMITER.acquire()
        signed = self._signed_params(params)
        resp = await self._client.get(path, params=signed)
        resp.raise_for_status()
        return resp.json()

    def _openapi_headers(self, params: dict[str, str] | None = None) -> dict[str, str]:
        """Create OPEN-API auth headers (ApiKey, Request-Time, Signature)."""
        request_time = str(int(time.time() * 1000))
        param_string = _build_openapi_param_string(params)
        signature_payload = f"{self._api_key}{request_time}{param_string}"
        return {
            "ApiKey": self._api_key,
            "Request-Time": request_time,
            "Signature": sign_binance(signature_payload, self._api_secret),
        }

    @retry()
    async def _get_openapi(self, path: str, params: dict[str, str] | None = None) -> Any:  # noqa: ANN401
        await _RATE_LIMITER.acquire()
        payload = dict(params or {})
        headers = self._openapi_headers(payload)
        resp = await self._contract_client.get(path, params=payload, headers=headers)
        resp.raise_for_status()
        return resp.json()

    async def fetch_balances(self) -> list[Snapshot]:
        """Fetch spot balances from account endpoint."""
        today = self._pricing.today()
        snapshots: list[Snapshot] = []

        # Spot wallet balances.
        data = await self._get("/api/v3/account")
        for bal in data.get("balances", []):
            free = Decimal(str(bal.get("free", "0")))
            locked = Decimal(str(bal.get("locked", "0")))
            amount = free + locked
            ticker = str(bal.get("asset", "")).upper()

            if not ticker or amount == 0:
                continue

            try:
                price = await self._pricing.get_price_usd(ticker)
            except ValueError:
                logger.warning("MEXC: cannot price %s, skipping", ticker)
                continue

            snapshots.append(
                Snapshot(
                    date=today,
                    source=self.source_name,
                    asset=ticker,
                    amount=amount,
                    usd_value=amount * price,
                    price=price,
                    raw_json=json.dumps(bal),
                )
            )

        # Contract account balances via OPEN-API auth.
        snapshots.extend(await self._fetch_contract_balances(today))

        # Earn positions are separate APY-bearing snapshots when available.
        earn_snapshots = await self._fetch_earn(today)
        snapshots.extend(earn_snapshots)

        logger.info("MEXC: found %d non-zero balances", len(snapshots))
        return snapshots

    async def _fetch_contract_balances(self, today: date) -> list[Snapshot]:
        try:
            data = await self._get_openapi(_CONTRACT_ASSETS_PATH)
        except (httpx.HTTPStatusError, json.JSONDecodeError, ValueError) as exc:
            logger.warning("MEXC: failed to fetch contract assets: %s", exc)
            return []

        rows: list[dict[str, Any]] = []
        if isinstance(data, list):
            rows = _as_dict_rows(data)
        elif isinstance(data, dict):
            if data.get("success") is False:
                message = data.get("message", "unknown error")
                logger.warning("MEXC: contract assets request failed: %s", message)
                return []
            rows = _as_dict_rows(data.get("data"))

        snapshots: list[Snapshot] = []
        for row in rows:
            ticker = str(row.get("currency", "")).upper().strip()
            if not ticker:
                continue

            equity = _to_decimal(row.get("equity"))
            if equity > 0:
                amount = equity
            else:
                amount = (
                    _to_decimal(row.get("availableBalance"))
                    + _to_decimal(row.get("frozenBalance"))
                    + _to_decimal(row.get("positionMargin"))
                )

            if amount <= 0:
                continue

            try:
                price = await self._pricing.get_price_usd(ticker)
            except ValueError:
                logger.warning("MEXC: cannot price contract asset %s, skipping", ticker)
                continue

            snapshots.append(
                Snapshot(
                    date=today,
                    source=self.source_name,
                    asset=ticker,
                    amount=amount,
                    usd_value=amount * price,
                    price=price,
                    raw_json=json.dumps({"accountType": "contract", "row": row}),
                )
            )
        return snapshots

    async def _fetch_earn(self, today: date) -> list[Snapshot]:
        snapshots: list[Snapshot] = []
        for path in _EARN_POSITION_PATHS:
            rows = await self._fetch_earn_rows(path)
            if not rows:
                continue
            for row in rows:
                parsed = self._parse_earn_row(row)
                if parsed is None:
                    continue
                symbol, amount, apy = parsed

                try:
                    price = await self._pricing.get_price_usd(symbol)
                except ValueError:
                    logger.warning("MEXC: cannot price earn asset %s, skipping", symbol)
                    continue

                snapshots.append(
                    Snapshot(
                        date=today,
                        source=self.source_name,
                        asset=symbol,
                        amount=amount,
                        usd_value=amount * price,
                        price=price,
                        apy=apy,
                        raw_json=json.dumps({"path": path, "row": row}),
                    )
                )
            if snapshots:
                logger.info("MEXC: found %d earn positions via %s", len(snapshots), path)
                return snapshots
        return snapshots

    async def _fetch_earn_rows(self, path: str) -> list[dict[str, Any]]:
        try:
            data = await self._get(path)
        except (httpx.HTTPStatusError, json.JSONDecodeError, ValueError):
            return []

        rows = _as_dict_rows(data)
        if rows:
            return rows
        if isinstance(data, dict):
            for key in ("data", "rows", "result", "list", "positions"):
                rows = _as_dict_rows(data.get(key))
                if rows:
                    return rows
        return []

    @staticmethod
    def _parse_earn_row(row: dict[str, Any]) -> tuple[str, Decimal, Decimal] | None:
        symbol = str(row.get("coin", row.get("asset", row.get("symbol", "")))).upper().strip()
        if not symbol:
            return None

        amount = _to_decimal(
            row.get(
                "amount",
                row.get(
                    "holdAmount",
                    row.get("positionAmount", row.get("principal", row.get("investAmount", "0"))),
                ),
            )
        )
        if amount <= 0:
            return None

        apy_raw = _to_decimal(
            row.get(
                "apy",
                row.get(
                    "apr",
                    row.get(
                        "interestRate",
                        row.get("annualRate", row.get("rate", row.get("estimateApr", "0"))),
                    ),
                ),
            )
        )
        if apy_raw <= 0:
            return None

        apy = apy_raw / Decimal(100) if apy_raw > 1 else apy_raw
        return symbol, amount, apy

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch deposits/withdrawals from MEXC capital endpoints."""
        params: dict[str, str] = {}
        if since:
            since_dt = datetime(since.year, since.month, since.day, tzinfo=UTC)
            params["startTime"] = str(int(since_dt.timestamp() * 1000))

        transactions: list[Transaction] = []

        try:
            deposits = await self._get("/api/v3/capital/deposit/hisrec", params)
            if isinstance(deposits, list):
                for dep in deposits:
                    tx = self._parse_deposit(dep)
                    if tx is not None:
                        transactions.append(tx)
        except httpx.HTTPStatusError as exc:
            logger.warning("MEXC: failed to fetch deposits: %s", exc)

        try:
            withdrawals = await self._get("/api/v3/capital/withdraw/history", params)
            if isinstance(withdrawals, list):
                for wd in withdrawals:
                    tx = self._parse_withdrawal(wd)
                    if tx is not None:
                        transactions.append(tx)
        except httpx.HTTPStatusError as exc:
            logger.warning("MEXC: failed to fetch withdrawals: %s", exc)

        logger.info("MEXC: parsed %d transactions", len(transactions))
        return transactions

    @staticmethod
    def _parse_deposit(dep: object) -> Transaction | None:
        if not isinstance(dep, dict):
            return None
        ticker = str(dep.get("coin", "")).upper()
        amount = Decimal(str(dep.get("amount", "0")))
        if not ticker or amount == 0:
            return None

        ts = dep.get("insertTime", dep.get("successTime", 0))
        try:
            tx_date = datetime.fromtimestamp(int(str(ts)) / 1000, tz=UTC).date()
        except (TypeError, ValueError, OSError):
            tx_date = datetime.now(tz=UTC).date()

        return Transaction(
            date=tx_date,
            source="mexc",
            tx_type=TransactionType.DEPOSIT,
            asset=ticker,
            amount=amount,
            usd_value=Decimal(0),
            tx_id=str(dep.get("txId", dep.get("id", ""))),
            raw_json=json.dumps(dep),
        )

    @staticmethod
    def _parse_withdrawal(wd: object) -> Transaction | None:
        if not isinstance(wd, dict):
            return None
        ticker = str(wd.get("coin", "")).upper()
        amount = Decimal(str(wd.get("amount", "0")))
        if not ticker or amount == 0:
            return None

        apply_time = wd.get("applyTime", wd.get("createTime", ""))
        tx_date: date
        if isinstance(apply_time, int | float | Decimal) or str(apply_time).isdigit():
            try:
                tx_date = datetime.fromtimestamp(int(str(apply_time)) / 1000, tz=UTC).date()
            except (TypeError, ValueError, OSError):
                tx_date = datetime.now(tz=UTC).date()
        else:
            try:
                tx_date = datetime.fromisoformat(str(apply_time)).date()
            except (TypeError, ValueError):
                tx_date = datetime.now(tz=UTC).date()

        return Transaction(
            date=tx_date,
            source="mexc",
            tx_type=TransactionType.WITHDRAWAL,
            asset=ticker,
            amount=amount,
            usd_value=Decimal(0),
            tx_id=str(wd.get("id", wd.get("txId", ""))),
            raw_json=json.dumps(wd),
        )


def _as_dict_rows(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [row for row in value if isinstance(row, dict)]


def _to_decimal(value: object) -> Decimal:
    try:
        return Decimal(str(value))
    except ArithmeticError:
        return Decimal(0)


def _build_openapi_param_string(params: dict[str, str] | None) -> str:
    if not params:
        return ""
    items: list[str] = []
    for key in sorted(params):
        value = params[key]
        items.append(f"{key}={quote(str(value), safe='')}")
    return "&".join(items)
