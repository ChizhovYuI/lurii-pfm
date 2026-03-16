"""CoinEx collector — reads spot/futures/financial balances and spot account history."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

import httpx

from pfm.collectors import register_collector
from pfm.collectors._auth import sign_coinex
from pfm.collectors._retry import RateLimiter, retry
from pfm.collectors.base import BaseCollector
from pfm.db.models import RawBalance, Transaction, TransactionType
from pfm.enums import SourceName

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.coinex.com"
_RATE_LIMITER = RateLimiter(requests_per_minute=600.0)
_HISTORY_PAGE_SIZE = 100
_MAX_HISTORY_PAGES = 200
_MS_IN_DAY = 86_400_000

_SPOT_BALANCE_PATH = "/v2/assets/spot/balance"
_FUTURES_BALANCE_PATH = "/v2/assets/futures/balance"
_FINANCIAL_BALANCE_PATH = "/v2/assets/financial/balance"
_SPOT_HISTORY_PATH = "/v2/assets/spot/transcation-history"
_PUBLIC_INVEST_SUMMARY_URL = "https://www.coinex.com/res/invest/summary/new"
_PUBLIC_IP_URL = "https://api.ipify.org"
_HISTORY_TYPES: tuple[str, ...] = (
    "deposit",
    "withdraw",
    "trade",
    "maker_cash_back",
    "investment_interest",
    "exchange_order_transfer",
)


@register_collector
class CoinexCollector(BaseCollector):
    """Collector for CoinEx exchange API V2."""

    source_name = SourceName.COINEX

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
        self._public_client = httpx.AsyncClient(timeout=30.0)
        self._public_ip: str | None = None

    @staticmethod
    def _request_path(path: str, params: dict[str, str] | None = None) -> str:
        if not params:
            return path
        return f"{path}?{urlencode(params)}"

    def _headers(self, method: str, path: str, params: dict[str, str] | None = None, body: str = "") -> dict[str, str]:
        timestamp = str(int(datetime.now(tz=UTC).timestamp() * 1000))
        request_path = self._request_path(path, params)
        signature = sign_coinex(method.upper(), request_path, body, timestamp, self._api_secret)
        return {
            "X-COINEX-KEY": self._api_key,
            "X-COINEX-SIGN": signature,
            "X-COINEX-TIMESTAMP": timestamp,
        }

    @retry()
    async def _get(self, path: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        await _RATE_LIMITER.acquire()
        headers = self._headers("GET", path, params)
        resp = await self._client.get(path, params=params, headers=headers)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, dict):
            msg = f"CoinEx payload for {path} must be an object"
            raise TypeError(msg)
        code = payload.get("code")
        if code != 0:
            message = str(payload.get("message") or "unknown error")
            msg = f"CoinEx API error ({code}) on {path}: {message}"
            if _is_ip_prohibited_error(code, message):
                public_ip = await self._get_public_ip()
                if public_ip:
                    msg = f"{msg} (current public IP: {public_ip})"
            raise ValueError(msg)
        return payload

    @retry()
    async def _get_public_invest_summary(self) -> list[dict[str, Any]]:
        resp = await self._public_client.get(_PUBLIC_INVEST_SUMMARY_URL)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, dict):
            msg = "CoinEx invest summary payload must be an object"
            raise TypeError(msg)
        code = payload.get("code")
        if code != 0:
            message = str(payload.get("message") or "unknown error")
            msg = f"CoinEx invest summary API error ({code}): {message}"
            raise ValueError(msg)
        return _as_dict_rows(payload.get("data"))

    async def _get_public_ip(self) -> str | None:
        if self._public_ip is not None:
            return self._public_ip

        try:
            resp = await self._public_client.get(_PUBLIC_IP_URL, params={"format": "text"})
            resp.raise_for_status()
        except (httpx.HTTPStatusError, httpx.TransportError, httpx.TimeoutException) as exc:
            logger.warning("CoinEx: failed to resolve current public IP: %s", exc)
            return None

        ip_text = resp.text.strip()
        if not ip_text:
            return None

        self._public_ip = ip_text
        return self._public_ip

    async def fetch_raw_balances(self) -> list[RawBalance]:
        """Fetch spot + futures + financial balances."""
        spot_rows = _as_dict_rows((await self._get(_SPOT_BALANCE_PATH)).get("data"))

        futures_rows: list[dict[str, Any]]
        try:
            futures_rows = _as_dict_rows((await self._get(_FUTURES_BALANCE_PATH)).get("data"))
        except (httpx.HTTPStatusError, TypeError, ValueError) as exc:
            logger.warning("CoinEx: failed to fetch futures balances: %s", exc)
            futures_rows = []

        financial_rows: list[dict[str, Any]]
        try:
            financial_rows = _as_dict_rows((await self._get(_FINANCIAL_BALANCE_PATH)).get("data"))
        except (httpx.HTTPStatusError, TypeError, ValueError) as exc:
            logger.warning("CoinEx: failed to fetch financial balances: %s", exc)
            financial_rows = []

        financial_totals = _balances_by_asset(financial_rows)
        financial_apy = await self._financial_apy(financial_totals)

        raw: list[RawBalance] = []
        raw.extend(_build_raw_balances(spot_rows, account_type="spot"))
        raw.extend(_build_raw_balances(futures_rows, account_type="futures"))
        raw.extend(_build_raw_balances(financial_rows, account_type="financial", apy_by_asset=financial_apy))
        logger.info("CoinEx: found %d non-zero balances", len(raw))
        return raw

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch spot account transaction history for supported types."""
        txs: list[Transaction] = []
        for history_type in _HISTORY_TYPES:
            rows = await self._fetch_history_rows(history_type, since=since)
            for row in rows:
                tx = _parse_history_transaction(row)
                if tx is None:
                    continue
                if since is not None and tx.date < since:
                    continue
                txs.append(tx)
        txs.sort(key=lambda tx: (tx.date, tx.tx_id), reverse=True)
        logger.info("CoinEx: parsed %d transactions", len(txs))
        return txs

    async def _financial_apy(self, balances: dict[str, Decimal]) -> dict[str, Decimal]:
        if not balances:
            return {}

        apy_by_asset = await self._financial_apy_from_public_summary(balances)
        missing_balances = {asset: amount for asset, amount in balances.items() if asset not in apy_by_asset}
        if not missing_balances:
            return apy_by_asset

        fallback_apy = await self._financial_apy_from_interest(missing_balances)
        apy_by_asset.update(fallback_apy)
        return apy_by_asset

    async def _financial_apy_from_public_summary(self, balances: dict[str, Decimal]) -> dict[str, Decimal]:
        try:
            rows = await self._get_public_invest_summary()
        except (httpx.HTTPStatusError, httpx.TransportError, httpx.TimeoutException, TypeError, ValueError) as exc:
            logger.warning("CoinEx: failed to fetch public invest APY summary: %s", exc)
            return {}

        apy_by_asset: dict[str, Decimal] = {}
        for row in rows:
            asset = str(row.get("asset", "")).upper().strip()
            if not asset:
                continue
            balance = balances.get(asset)
            if balance is None or balance <= 0:
                continue
            apy = _effective_public_apy(row, amount=balance)
            if apy is None:
                continue
            apy_by_asset[asset] = apy
        return apy_by_asset

    async def _financial_apy_from_interest(self, balances: dict[str, Decimal]) -> dict[str, Decimal]:
        end_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
        start_ms = end_ms - _MS_IN_DAY

        try:
            rows = await self._fetch_history_rows(
                "investment_interest",
                start_time_ms=start_ms,
                end_time_ms=end_ms,
            )
        except (httpx.HTTPStatusError, TypeError, ValueError) as exc:
            logger.warning("CoinEx: failed to estimate financial APY from interest history: %s", exc)
            return {}

        interest_by_asset: dict[str, Decimal] = {}
        for row in rows:
            asset = str(row.get("ccy", "")).upper().strip()
            if not asset:
                continue
            change = _to_decimal(row.get("change"))
            if change <= 0:
                continue
            interest_by_asset[asset] = interest_by_asset.get(asset, Decimal(0)) + change

        apy_by_asset: dict[str, Decimal] = {}
        for asset, balance in balances.items():
            if balance <= 0:
                continue
            interest = interest_by_asset.get(asset, Decimal(0))
            if interest <= 0:
                continue
            apy_by_asset[asset] = (interest / balance) * Decimal(365)
        return apy_by_asset

    async def _fetch_history_rows(
        self,
        history_type: str,
        *,
        since: date | None = None,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, str] = {
            "type": history_type,
            "page": "1",
            "limit": str(_HISTORY_PAGE_SIZE),
        }
        if since is not None and start_time_ms is None:
            start = datetime(since.year, since.month, since.day, tzinfo=UTC)
            start_time_ms = int(start.timestamp() * 1000)
        if start_time_ms is not None:
            params["start_time"] = str(start_time_ms)
        if end_time_ms is not None:
            params["end_time"] = str(end_time_ms)

        rows: list[dict[str, Any]] = []
        page = 1
        while page <= _MAX_HISTORY_PAGES:
            params["page"] = str(page)
            payload = await self._get(_SPOT_HISTORY_PATH, params=params)
            page_rows = _as_dict_rows(payload.get("data"))
            rows.extend(page_rows)
            pagination = payload.get("pagination")
            if not _has_next_page(page_rows, pagination, page=page, page_size=_HISTORY_PAGE_SIZE):
                break
            page += 1
        return rows


def _build_raw_balances(
    rows: list[dict[str, Any]],
    *,
    account_type: str,
    apy_by_asset: dict[str, Decimal] | None = None,
) -> list[RawBalance]:
    apy_by_asset = apy_by_asset or {}
    raw: list[RawBalance] = []
    for row in rows:
        asset = str(row.get("ccy", "")).upper().strip()
        if not asset:
            continue
        available = _to_decimal(row.get("available"))
        frozen = _to_decimal(row.get("frozen"))
        amount = available + frozen
        if amount == 0:
            continue
        raw.append(
            RawBalance(
                asset=asset,
                amount=amount,
                apy=apy_by_asset.get(asset, Decimal(0)),
                raw_json=json.dumps({"account_type": account_type, "row": row}),
            )
        )
    return raw


def _is_ip_prohibited_error(code: object, message: str) -> bool:
    return str(code) == "23" and "ip prohibited" in message.lower()


def _balances_by_asset(rows: list[dict[str, Any]]) -> dict[str, Decimal]:
    totals: dict[str, Decimal] = {}
    for row in rows:
        asset = str(row.get("ccy", "")).upper().strip()
        if not asset:
            continue
        available = _to_decimal(row.get("available"))
        frozen = _to_decimal(row.get("frozen"))
        amount = available + frozen
        if amount <= 0:
            continue
        totals[asset] = totals.get(asset, Decimal(0)) + amount
    return totals


def _parse_history_transaction(row: dict[str, Any]) -> Transaction | None:
    raw_type = str(row.get("type", "")).lower().strip()
    tx_type = _map_history_type(raw_type)
    if tx_type is None:
        return None

    asset = str(row.get("ccy", "")).upper().strip()
    if not asset:
        return None

    change = _to_decimal(row.get("change"))
    if change == 0:
        return None

    created_at_ms = _to_int(row.get("created_at"))
    tx_date = _ms_to_date(created_at_ms)
    tx_id = str(row.get("id") or "").strip() or _synthetic_tx_id(raw_type, asset, change, created_at_ms)

    return Transaction(
        date=tx_date,
        source="coinex",
        tx_type=tx_type,
        asset=asset,
        amount=abs(change),
        usd_value=Decimal(0),
        tx_id=tx_id,
        raw_json=json.dumps(row),
    )


def _map_history_type(raw_type: str) -> TransactionType | None:
    if raw_type == "deposit":
        return TransactionType.DEPOSIT
    if raw_type == "withdraw":
        return TransactionType.WITHDRAWAL
    if raw_type == "trade":
        return TransactionType.TRADE
    if raw_type == "investment_interest":
        return TransactionType.INTEREST
    if raw_type in {"maker_cash_back", "exchange_order_transfer"}:
        return TransactionType.TRANSFER
    return None


def _synthetic_tx_id(raw_type: str, asset: str, change: Decimal, created_at_ms: int) -> str:
    return f"coinex:{raw_type}:{asset}:{format(change.normalize(), 'f')}:{created_at_ms}"


def _has_next_page(
    rows: list[dict[str, Any]],
    pagination: object,
    *,
    page: int,
    page_size: int,
) -> bool:
    if isinstance(pagination, dict):
        for key in ("has_next", "has_more", "has_next_page", "more"):
            raw = pagination.get(key)
            if isinstance(raw, bool):
                return raw
            if isinstance(raw, int):
                return raw != 0

        next_page = _to_int(pagination.get("next_page"))
        if next_page > page:
            return True

        total = _to_int(pagination.get("total"))
        if total > 0:
            return page * page_size < total

    return len(rows) >= page_size


def _as_dict_rows(value: object) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    if isinstance(value, dict):
        for key in ("items", "records", "list"):
            nested = value.get(key)
            if isinstance(nested, list):
                return [row for row in nested if isinstance(row, dict)]
        return [value]
    return []


def _to_decimal(value: object) -> Decimal:
    try:
        return Decimal(str(value))
    except (ArithmeticError, TypeError, ValueError):
        return Decimal(0)


def _parse_decimal(value: object) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (ArithmeticError, TypeError, ValueError):
        return None


def _effective_public_apy(row: dict[str, Any], *, amount: Decimal) -> Decimal | None:
    if amount <= 0:
        return None

    base_rate = _parse_decimal(row.get("rate"))
    if base_rate is None or base_rate < 0:
        return None

    apy = base_rate
    ladder = row.get("ladder_rule")
    if not isinstance(ladder, dict):
        return apy

    ladder_rate = _parse_decimal(ladder.get("rate"))
    ladder_limit = _parse_decimal(ladder.get("limit"))
    if ladder_rate is None or ladder_limit is None:
        return apy
    if ladder_rate <= 0 or ladder_limit <= 0:
        return apy

    bonus_portion = min(amount, ladder_limit) / amount
    return apy + (ladder_rate * bonus_portion)


def _to_int(value: object) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return 0


def _ms_to_date(value: int) -> date:
    try:
        return datetime.fromtimestamp(value / 1000, tz=UTC).date()
    except (OSError, OverflowError, ValueError):
        return datetime.now(tz=UTC).date()
