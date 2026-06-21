"""Bybit collector — reads wallet balances and transaction log via V5 API."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote

import httpx

from pfm.collectors import register_collector
from pfm.collectors._auth import sign_bybit
from pfm.collectors._math import apr_to_apy
from pfm.collectors._retry import RateLimiter, retry
from pfm.collectors.base import BaseCollector
from pfm.db.models import RawBalance, Transaction, TransactionType
from pfm.enums import SourceName

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.bybit.com"
_RECV_WINDOW = "20000"
_RATE_LIMITER = RateLimiter(requests_per_minute=600.0)

# Earn sub-categories already covered by _fetch_earn_raw() via /v5/earn/position.
_KNOWN_EARN_CATEGORIES: frozenset[str] = frozenset({"Easy Earn"})

# Transaction-log windowing. The endpoint returns only the last 24h when no time
# range is given, caps the query interval at 7 days, and retains 2 years of data.
_TX_LOG_PATH = "/v5/account/transaction-log"
_TX_WINDOW_DAYS = 7
_TX_PAGE_LIMIT = "50"
# Bybit rejects startTime older than ~2 years ("Can't query order earlier than 2
# years"). Stay a margin inside that bound so the oldest window is never refused.
_MAX_BACKFILL_DAYS = 720
_MAX_TX_PAGES_PER_WINDOW = 50

# Bybit Card asset records — a separate POST endpoint, body-signed and rate-limited
# far tighter than the account APIs. SIDE_QUERY_AUTH returns settled card purchases.
_CARD_TX_PATH = "/v5/card/transaction/query-asset-records"
_CARD_PAGE_LIMIT = 50
_CARD_MAX_PAGES = 100
_CARD_RATE_LIMITER = RateLimiter(requests_per_minute=20.0)
_CARD_RATE_LIMIT_RETCODE = 10006
_CARD_MAX_RETRIES = 4
_CARD_RETRY_BACKOFF_S = 3.0


def _synthetic_tx_id(raw_type: str, asset: str, change: Decimal, ts_ms: str) -> str:
    """Stable id for transaction-log rows that lack Bybit's own ``id``.

    The transactions unique index ignores empty ``tx_id``, so a blank id would
    re-insert a fresh duplicate on every overlapping run. Mirror CoinEx's
    synthetic-id approach to keep such rows idempotent.
    """
    return f"bybit:{raw_type}:{asset}:{format(change.normalize(), 'f')}:{ts_ms}"


@register_collector
class BybitCollector(BaseCollector):
    """Collector for Bybit exchange via V5 API."""

    source_name = SourceName.BYBIT
    incremental_history_overlap_days = 2

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
        # Sign the exact query string httpx puts on the wire. Building it via
        # QueryParams (rather than a hand-rolled join) keeps the signed bytes and
        # the sent bytes identical even when a value needs percent-encoding (e.g.
        # the nextPageCursor token), which a manual join would not encode.
        query = str(httpx.QueryParams(params)) if params else ""
        headers = self._signed_headers(query)
        resp = await self._client.get(path, params=params, headers=headers)
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        if data.get("retCode") != 0:
            msg = f"Bybit API error: {data.get('retMsg', 'unknown')}"
            raise ValueError(msg)
        return data

    @retry()
    async def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Make a signed POST request (body-signed) to Bybit V5 API.

        Used for the card endpoint, which is throttled hard; retries on the
        rate-limit retCode with a short backoff before giving up.
        """
        # Sign the exact body bytes we send (content=body), so the signature
        # matches regardless of how a dict would otherwise be serialized.
        body = json.dumps(payload)
        for attempt in range(_CARD_MAX_RETRIES):
            await _CARD_RATE_LIMITER.acquire()
            headers = self._signed_headers(body)
            resp = await self._client.post(path, content=body, headers=headers)
            resp.raise_for_status()
            data: dict[str, Any] = resp.json()
            ret_code = data.get("retCode")
            if ret_code == _CARD_RATE_LIMIT_RETCODE and attempt < _CARD_MAX_RETRIES - 1:
                await asyncio.sleep(_CARD_RETRY_BACKOFF_S * (attempt + 1))
                continue
            if ret_code != 0:
                msg = f"Bybit card API error: {data.get('retMsg', 'unknown')}"
                raise ValueError(msg)
            return data
        msg = "Bybit card API error: rate limited after retries"
        raise ValueError(msg)

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

    def _find_earn_override(self, category: str, coin: str) -> dict[str, str] | None:
        """Look up a user-supplied earn override for (category, coin)."""
        overrides: list[dict[str, str]] = getattr(self, "earn_overrides", [])
        for ov in overrides:
            if ov.get("category") == category and ov.get("coin", "").upper() == coin.upper():
                return ov
        return None

    def _apply_earn_override(self, cat_name: str, ticker: str, meta: dict[str, object]) -> Decimal:
        """Apply user-supplied earn override (APR, settlement) and return APR as-is."""
        override = self._find_earn_override(cat_name, ticker)
        if not override:
            return Decimal(0)
        apy = self._to_apr(override.get("apr", "0"))
        settlement = override.get("settlement_at", "")
        if settlement:
            meta["settlement_at"] = settlement
        return apy

    async def _fetch_earn_extra_raw(self) -> list[RawBalance]:
        """Fetch Earn sub-categories not covered by /v5/earn/position (e.g. Dual Asset)."""
        try:
            data = await self._get("/v5/asset/asset-overview")
        except (httpx.HTTPStatusError, ValueError):
            logger.warning("Bybit: failed to fetch asset-overview for extra earn categories")
            return []

        raw: list[RawBalance] = []
        for account in data.get("result", {}).get("list", []):
            if account.get("accountType") != "Earn":
                continue
            for cat in account.get("categories", []):
                cat_name = str(cat.get("category", ""))
                if cat_name in _KNOWN_EARN_CATEGORIES:
                    continue
                for coin in cat.get("coinDetail", []):
                    ticker = str(coin.get("coin", "")).upper()
                    amount = self._to_decimal(coin.get("equity", "0"))
                    if not ticker or amount == 0:
                        continue

                    meta: dict[str, object] = {
                        "account_type": "earn",
                        "category": cat_name,
                        "row": coin,
                    }
                    apy = self._apply_earn_override(cat_name, ticker, meta)

                    raw.append(
                        RawBalance(
                            asset=ticker,
                            amount=amount,
                            apy=apy,
                            raw_json=json.dumps(meta),
                        )
                    )
        return raw

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

        # Extra earn sub-categories (Dual Asset, Double-Win, etc.)
        earn_extra = await self._fetch_earn_extra_raw()
        raw.extend(earn_extra)

        logger.info("Bybit: found %d non-zero balances", len(raw))
        return raw

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Fetch the account transaction log plus card spend from Bybit.

        The log endpoint returns only the last 24h without an explicit range,
        caps the query interval at 7 days, and retains 2 years — so it is walked
        in <=7-day windows, each paged via nextPageCursor. Card spend lives in a
        separate, optional endpoint and is appended non-fatally.
        """
        now_dt = datetime.now(tz=UTC)
        floor = now_dt.date() - timedelta(days=_MAX_BACKFILL_DAYS)
        start = since if since is not None and since > floor else floor

        transactions: list[Transaction] = []
        seen_ids: set[str] = set()
        self._append_deduped(transactions, await self._fetch_log_transactions(start, now_dt), seen_ids, since=since)

        # Bybit Card spend is a separate, optional ledger. Keep it strictly
        # non-fatal: a key without card permission (10005) or a card-side outage
        # must not break the primary account-log collection.
        try:
            card = await self._fetch_card_transactions(start)
        except (httpx.HTTPError, ValueError) as exc:
            logger.warning("Bybit: card transactions unavailable (%s)", exc)
        else:
            self._append_deduped(transactions, card, seen_ids, since=since)

        logger.info("Bybit: parsed %d transactions", len(transactions))
        return transactions

    @staticmethod
    def _append_deduped(
        dest: list[Transaction],
        src: list[Transaction],
        seen_ids: set[str],
        *,
        since: date | None,
    ) -> None:
        """Append ``src`` to ``dest``, dropping rows before ``since`` or already seen.

        Windows touch at their boundaries (Bybit's endTime is inclusive) and the
        incremental overlap re-fetches recent rows, so dedup on the entry id.
        """
        for tx in src:
            if since is not None and tx.date < since:
                continue
            if tx.tx_id and tx.tx_id in seen_ids:
                continue
            if tx.tx_id:
                seen_ids.add(tx.tx_id)
            dest.append(tx)

    async def _fetch_log_transactions(self, start: date, now_dt: datetime) -> list[Transaction]:
        """Walk the account transaction log in <=7-day windows from ``start``."""
        window_start = datetime(start.year, start.month, start.day, tzinfo=UTC)
        step = timedelta(days=_TX_WINDOW_DAYS)

        out: list[Transaction] = []
        while window_start < now_dt:
            window_end = min(window_start + step, now_dt)
            for item in await self._fetch_transaction_window(window_start, window_end):
                tx = self._parse_transaction(item)
                if tx is not None:
                    out.append(tx)
            window_start = window_end
        return out

    async def _fetch_card_transactions(self, start: date) -> list[Transaction]:
        """Fetch settled Bybit Card purchases (SIDE_QUERY_AUTH) since ``start``."""
        begin_dt = datetime(start.year, start.month, start.day, tzinfo=UTC)
        begin_ms = int(begin_dt.timestamp() * 1000)
        end_ms = int(datetime.now(tz=UTC).timestamp() * 1000)

        transactions: list[Transaction] = []
        for page in range(1, _CARD_MAX_PAGES + 1):
            payload = {
                "type": "SIDE_QUERY_AUTH",
                "createBeginTime": begin_ms,
                "createEndTime": end_ms,
                "limit": _CARD_PAGE_LIMIT,
                "page": page,
            }
            data = await self._post(_CARD_TX_PATH, payload)
            result = data.get("result") or {}
            rows = result.get("data") or []
            for record in rows:
                tx = self._parse_card_transaction(record)
                if tx is not None:
                    transactions.append(tx)
            total = int(result.get("totalCount") or 0)
            if not rows or page * _CARD_PAGE_LIMIT >= total:
                break

        logger.info("Bybit: parsed %d card transactions", len(transactions))
        return transactions

    @staticmethod
    def _parse_card_transaction(record: dict[str, Any]) -> Transaction | None:
        """Parse a settled Bybit Card AUTH record into a SPEND transaction."""
        # Only settled, successful purchases represent real outflow. Declined,
        # pending, and reversal rows are skipped (refunds are a future refinement).
        if str(record.get("tradeStatus", "")) != "1" or str(record.get("status", "")) != "1":
            return None

        asset = str(record.get("paidCurrency") or record.get("basicCurrency") or "").upper()
        amount = BybitCollector._to_decimal(record.get("paidAmount") or record.get("basicAmount") or "0")
        tx_id = str(record.get("txnId") or "").strip()
        if not asset or amount == 0 or not tx_id:
            return None

        ts_ms = str(record.get("txnCreate", "0"))
        try:
            tx_date = datetime.fromtimestamp(int(ts_ms) / 1000, tz=UTC).date()
        except (ValueError, OSError):
            tx_date = datetime.now(tz=UTC).date()

        return Transaction(
            date=tx_date,
            source="bybit",
            tx_type=TransactionType.SPEND,
            asset=asset,
            amount=abs(amount),
            usd_value=Decimal(0),
            tx_id=tx_id,
            raw_json=json.dumps({"account_type": "card", "row": record}),
        )

    async def _fetch_transaction_window(
        self,
        start_dt: datetime,
        end_dt: datetime,
    ) -> list[dict[str, Any]]:
        """Fetch all transaction-log rows in a <=7-day window via cursor paging."""
        rows: list[dict[str, Any]] = []
        cursor = ""
        seen_cursors: set[str] = set()
        for _ in range(_MAX_TX_PAGES_PER_WINDOW):
            params = {
                "limit": _TX_PAGE_LIMIT,
                "startTime": str(int(start_dt.timestamp() * 1000)),
                "endTime": str(int(end_dt.timestamp() * 1000)),
            }
            if cursor:
                params["cursor"] = cursor
            # Let _get errors (HTTP status, retCode) propagate. A swallowed window
            # would drop data silently, and the incremental overlap then anchors
            # past the gap so it is never re-fetched; failing loudly lets collect()
            # record the error and retry the run.
            data = await self._get(_TX_LOG_PATH, params=params)
            # result is null (-> None) on some edge responses; guard before .get().
            result = data.get("result") or {}
            page_rows = result.get("list") or []
            rows.extend(page_rows)
            # nextPageCursor may be JSON null (-> None), "", or a percent-encoded
            # token. Decode it once so httpx re-encodes back to the same token
            # instead of double-encoding the '%'. Bybit returns a trailing cursor
            # that points at an empty page and then repeats the first page, so stop
            # on an empty page or a cursor we have already followed.
            next_cursor = unquote((result.get("nextPageCursor") or "").strip())
            if not page_rows or not next_cursor or next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        else:
            # Page cap reached with a live cursor: more rows exist than we read.
            # Raise rather than return a truncated window that the incremental
            # overlap would then permanently skip.
            msg = (
                f"Bybit: transaction-log window {start_dt.date()}..{end_dt.date()} "
                f"exceeded {_MAX_TX_PAGES_PER_WINDOW} pages; refusing to truncate history"
            )
            logger.error(msg)
            raise ValueError(msg)
        return rows

    @staticmethod
    def _parse_transaction(item: dict[str, Any]) -> Transaction | None:
        """Parse a Bybit transaction log entry."""
        ticker = str(item.get("currency", "")).upper()
        change = BybitCollector._to_decimal(item.get("cashFlow", "0"))
        if not ticker:
            return None

        ts_ms = str(item.get("transactionTime", "0"))
        try:
            tx_date = datetime.fromtimestamp(int(ts_ms) / 1000, tz=UTC).date()
        except (ValueError, OSError):
            tx_date = datetime.now(tz=UTC).date()

        # Fall back to a deterministic id so blank-id rows still dedup (the unique
        # index ignores empty tx_id).
        tx_id = str(item.get("id") or "").strip() or _synthetic_tx_id(str(item.get("type", "")), ticker, change, ts_ms)

        return Transaction(
            date=tx_date,
            source="bybit",
            tx_type=TransactionType.UNKNOWN,
            asset=ticker,
            amount=abs(change),
            usd_value=Decimal(0),
            tx_id=tx_id,
            raw_json=json.dumps(item),
        )
