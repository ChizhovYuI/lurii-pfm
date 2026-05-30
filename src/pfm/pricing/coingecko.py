"""CoinGecko pricing service for crypto and fiat rate conversion."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import aiosqlite
import httpx

from pfm.pricing.constants import (
    HISTORICAL_PRICE_SOURCE as _HISTORICAL_SOURCE,
)
from pfm.pricing.constants import (
    MISS_PRICE_SOURCE as _MISS_SOURCE,
)
from pfm.pricing.constants import (
    REAL_PRICE_SOURCE as _REAL_SOURCE,
)

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

# Stablecoins pegged to USD — skip API call
STABLECOINS: frozenset[str] = frozenset({"USDC", "USDT", "DAI", "BUSD", "TUSD", "USDP", "FDUSD"})

# Fiat currencies — use /exchange_rates or /simple/price with vs_currencies
FIAT_TICKERS: frozenset[str] = frozenset(
    {"USD", "GBP", "EUR", "THB", "JPY", "CHF", "CAD", "AUD", "NZD", "SGD", "HKD", "AMD"}
)

_BASE_URL = "https://api.coingecko.com/api/v3"
_RATE_LIMIT_DELAY = 2.1  # seconds between requests (30 req/min = 1 per 2s)
_MAX_429_RETRIES = 3
_RETRY_BACKOFF_BASE_SECONDS = 2.0
_HTTP_STATUS_TOO_MANY_REQUESTS = 429

# ``prices.source`` tags (see ``pfm.pricing.constants``). ``_REAL_SOURCE`` is a
# live spot price (date=today); ``_HISTORICAL_SOURCE`` is a back-dated backfill
# price kept distinct so it can never be served by the live cache read even when
# its ``on_date`` is today; a miss sentinel stops the backfill from re-hitting
# CoinGecko for an unpriceable (asset, date) and is time-limited so a transient
# outage self-heals.
_MISS_RETRY_WINDOW = "-7 days"


class PricingService:
    """Fetches crypto prices and fiat rates from CoinGecko."""

    def __init__(self, api_key: str = "", cache_db_path: str | Path | None = None) -> None:
        headers: dict[str, str] = {"Accept": "application/json"}
        if api_key:
            headers["x-cg-demo-api-key"] = api_key
        self._client = httpx.AsyncClient(
            base_url=_BASE_URL,
            headers=headers,
            timeout=30.0,
        )
        self._cache_db_path = str(cache_db_path) if cache_db_path is not None else None
        self._last_request_time: float = 0.0
        self._request_lock = asyncio.Lock()
        self._cache: dict[str, tuple[Decimal, datetime]] = {}
        self._cache_ttl_seconds: float = 3600.0  # 1 hour
        self._resolved_symbol_ids: dict[str, str] = {}
        self._coins_by_symbol: dict[str, list[str]] | None = None

    async def close(self) -> None:
        """Close the HTTP client."""
        await self._client.aclose()

    def set_test_price(self, ticker: str, price: Decimal) -> None:
        """Test-only: prime the in-memory cache with a fixed USD price."""
        self._set_cache(ticker.upper(), price)

    async def get_price_usd(self, ticker: str) -> Decimal:
        """Get the USD price for a single asset ticker (e.g. 'BTC', 'GBP')."""
        ticker = ticker.upper()

        # USD is 1:1
        if ticker == "USD":
            return Decimal(1)

        # Stablecoins
        if ticker in STABLECOINS:
            return Decimal(1)

        # Check cache
        cached = self._get_cached(ticker)
        if cached is not None:
            return cached
        persisted = await self._get_persisted_cache(ticker)
        if persisted is not None:
            self._set_cache(ticker, persisted)
            return persisted

        # Fiat
        if ticker in FIAT_TICKERS:
            return await self._fetch_fiat_rate(ticker)

        # Crypto
        return await self._fetch_crypto_price(ticker)

    async def get_prices_usd(self, tickers: list[str]) -> dict[str, Decimal]:
        """Get USD prices for multiple tickers."""
        results: dict[str, Decimal] = {}
        crypto_to_fetch: list[str] = []
        fiat_to_fetch: list[str] = []

        for ticker in tickers:
            t = ticker.upper()
            if t == "USD" or t in STABLECOINS:
                results[t] = Decimal(1)
            else:
                cached = self._get_cached(t)
                if cached is not None:
                    results[t] = cached
                    continue

                persisted = await self._get_persisted_cache(t)
                if persisted is not None:
                    self._set_cache(t, persisted)
                    results[t] = persisted
                    continue

                if t in FIAT_TICKERS:
                    fiat_to_fetch.append(t)
                else:
                    crypto_to_fetch.append(t)

        # Batch fetch crypto
        if crypto_to_fetch:
            prices = await self._fetch_crypto_prices_batch(crypto_to_fetch)
            results.update(prices)

        # Batch fetch fiat
        if fiat_to_fetch:
            rates = await self._fetch_fiat_rates_batch(fiat_to_fetch)
            results.update(rates)

        return results

    async def convert_to_usd(self, amount: Decimal, ticker: str) -> Decimal:
        """Convert an amount to USD."""
        price = await self.get_price_usd(ticker)
        return amount * price

    async def get_price_usd_on(self, ticker: str, on_date: date) -> Decimal | None:
        """USD price for ``ticker`` on a historical ``on_date``.

        Returns ``None`` (rather than raising) when no price is available — the
        caller decides whether to skip. Results are persisted to the date-keyed
        ``prices`` cache so a backfill never re-fetches the same (asset, date);
        a definitive miss is recorded as a time-limited sentinel so unpriceable
        rows are not re-fetched on every collect.

        Note: ``on_date`` is treated as a UTC calendar day (CoinGecko's
        ``/history`` daily snapshots are UTC). A transaction whose local
        timestamp falls near a UTC day boundary may be valued against the
        adjacent day's price — an accepted limitation, since stored transaction
        dates carry no intraday time.
        """
        ticker = ticker.upper()
        if ticker == "USD" or ticker in STABLECOINS:
            return Decimal(1)

        cached = await self._get_persisted_cache_on(ticker, on_date)
        if cached is not None:
            return cached
        # A recent miss sentinel means we already tried and CoinGecko had no
        # price; skip the network until the retry window lapses.
        if await self._recent_miss_on(ticker, on_date):
            return None

        try:
            if ticker in FIAT_TICKERS:
                price = await self._fetch_fiat_rate_on(ticker, on_date)
            else:
                price = await self._fetch_crypto_price_on(ticker, on_date)
        except (httpx.HTTPStatusError, RuntimeError, ValueError):
            # Transient error — do NOT record a sentinel; let it retry next run.
            logger.warning("CoinGecko: no historical price for %s on %s", ticker, on_date)
            return None

        if price is not None:
            await self._save_persisted_cache_on(ticker, price, on_date)
            return price
        # Definitive "no price for this date" — record a time-limited sentinel so
        # the per-collect forward-fill stops re-fetching this (asset, date).
        await self._save_miss_on(ticker, on_date)
        return None

    async def peek_price_usd_on(self, ticker: str, on_date: date) -> tuple[str, Decimal | None]:
        """Resolve a historical price from the cache only — never the network.

        Returns ``(status, price)`` where status is ``"hit"`` (price known, no
        network needed), ``"miss"`` (a recent "no price" sentinel — also free),
        or ``"unknown"`` (a network lookup via :meth:`get_price_usd_on` is
        required). Lets a bounded backfill spend its lookup budget only on
        genuine network calls instead of free cache/sentinel hits.
        """
        ticker = ticker.upper()
        if ticker == "USD" or ticker in STABLECOINS:
            return "hit", Decimal(1)
        cached = await self._get_persisted_cache_on(ticker, on_date)
        if cached is not None:
            return "hit", cached
        if await self._recent_miss_on(ticker, on_date):
            return "miss", None
        return "unknown", None

    def _get_cached(self, ticker: str) -> Decimal | None:
        if ticker in self._cache:
            price, cached_at = self._cache[ticker]
            age = (datetime.now(tz=UTC) - cached_at).total_seconds()
            if age < self._cache_ttl_seconds:
                return price
        return None

    def _set_cache(self, ticker: str, price: Decimal) -> None:
        self._cache[ticker] = (price, datetime.now(tz=UTC))

    async def _rate_limit(self) -> None:
        """Ensure we don't exceed CoinGecko rate limits."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < _RATE_LIMIT_DELAY:
            await asyncio.sleep(_RATE_LIMIT_DELAY - elapsed)
        self._last_request_time = time.monotonic()

    async def _request(self, path: str, params: dict[str, str] | None = None) -> httpx.Response:
        """Rate-limited HTTP GET with 429 retry."""
        effective_params = params or {}
        for attempt in range(_MAX_429_RETRIES + 1):
            async with self._request_lock:
                await self._rate_limit()
                resp = await self._client.get(path, params=effective_params)

            if resp.status_code == _HTTP_STATUS_TOO_MANY_REQUESTS and attempt < _MAX_429_RETRIES:
                retry_after_header = resp.headers.get("Retry-After")
                retry_after = float(retry_after_header) if retry_after_header and retry_after_header.isdigit() else 0.0
                delay = max(retry_after, _RETRY_BACKOFF_BASE_SECONDS ** (attempt + 1))
                logger.warning("CoinGecko rate limited (429). Retrying in %.1fs", delay)
                await asyncio.sleep(delay)
                continue

            resp.raise_for_status()
            return resp

        msg = "CoinGecko request failed after retries"
        raise RuntimeError(msg)

    async def _request_json(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        """Request JSON dict from CoinGecko."""
        resp = await self._request(path, params)
        data: dict[str, Any] = resp.json()
        return data

    async def _ensure_coins_map(self) -> dict[str, list[str]]:
        """Fetch /coins/list once and build symbol → [coin_ids] map."""
        if self._coins_by_symbol is not None:
            return self._coins_by_symbol

        try:
            resp = await self._request("/coins/list")
            raw: list[dict[str, str]] = resp.json()
            symbol_map: dict[str, list[str]] = {}
            for coin in raw:
                symbol = coin.get("symbol", "").upper()
                coin_id = coin.get("id", "")
                if symbol and coin_id:
                    symbol_map.setdefault(symbol, []).append(coin_id)
            self._coins_by_symbol = symbol_map
            logger.info("CoinGecko: loaded %d coins, %d unique symbols", len(raw), len(symbol_map))
        except (httpx.HTTPStatusError, RuntimeError):
            logger.warning("CoinGecko: failed to fetch coins list, falling back to /search")
            self._coins_by_symbol = {}

        return self._coins_by_symbol

    async def _get_persisted_cache(self, ticker: str) -> Decimal | None:
        """Read recent cached price from SQLite, if configured."""
        if self._cache_db_path is None:
            return None

        ttl_window = f"-{int(self._cache_ttl_seconds)} seconds"
        # Pin to today's row only. The same ``prices`` table also holds
        # historical rows written by the usd_value backfill (old ``date``,
        # fresh ``created_at``); without the date pin a backfill would let an
        # old price win the ``created_at DESC`` ordering and be served as the
        # current price for the whole TTL window.
        sql = (
            "SELECT price FROM prices "
            "WHERE asset = ? AND currency = 'USD' AND date = ? AND source = ? "
            "AND created_at >= datetime('now', ?) "
            "ORDER BY created_at DESC LIMIT 1"
        )
        try:
            async with aiosqlite.connect(self._cache_db_path) as db:
                row = await (await db.execute(sql, (ticker, str(self.today()), _REAL_SOURCE, ttl_window))).fetchone()
        except aiosqlite.Error:
            logger.exception("Failed to read price cache from SQLite")
            return None

        if row is None:
            return None
        return Decimal(str(row[0]))

    async def _write_price_row(self, ticker: str, price: str, on_date: date, source: str) -> None:
        """Append a price row to the SQLite cache, if configured.

        Single writer behind the live cache, the historical cache, and the miss
        sentinel so the INSERT lives in one place.
        """
        if self._cache_db_path is None:
            return
        sql = "INSERT INTO prices (date, asset, currency, price, source) VALUES (?, ?, 'USD', ?, ?)"
        try:
            async with aiosqlite.connect(self._cache_db_path) as db:
                await db.execute(sql, (str(on_date), ticker, price, source))
                await db.commit()
        except aiosqlite.Error:
            logger.exception("Failed to write price row into SQLite")

    async def _save_persisted_cache(self, ticker: str, price: Decimal) -> None:
        """Write a fetched current price into SQLite cache, if configured."""
        await self._write_price_row(ticker, str(price), self.today(), _REAL_SOURCE)

    async def _get_persisted_cache_on(self, ticker: str, on_date: date) -> Decimal | None:
        """Read a real price for an exact historical date from SQLite, if configured.

        Reads both the historical-backfill source and a same-day live row, but
        never the miss sentinel, so a "no price" marker is not served as a price.
        """
        if self._cache_db_path is None:
            return None
        sql = (
            "SELECT price FROM prices WHERE asset = ? AND currency = 'USD' AND date = ? "
            "AND source IN (?, ?) "
            "ORDER BY created_at DESC LIMIT 1"
        )
        try:
            async with aiosqlite.connect(self._cache_db_path) as db:
                row = await (await db.execute(sql, (ticker, str(on_date), _REAL_SOURCE, _HISTORICAL_SOURCE))).fetchone()
        except aiosqlite.Error:
            logger.exception("Failed to read historical price cache from SQLite")
            return None
        return Decimal(str(row[0])) if row is not None else None

    async def _save_persisted_cache_on(self, ticker: str, price: Decimal, on_date: date) -> None:
        """Write a fetched historical price into SQLite under the historical source.

        Tagged ``_HISTORICAL_SOURCE`` (not ``_REAL_SOURCE``) so the live cache read
        — which pins ``source = _REAL_SOURCE`` — can never serve a back-dated
        backfill price as the current spot price, even when ``on_date`` is today.
        """
        await self._write_price_row(ticker, str(price), on_date, _HISTORICAL_SOURCE)

    async def _recent_miss_on(self, ticker: str, on_date: date) -> bool:
        """True if a non-expired "no price" sentinel exists for (ticker, date)."""
        if self._cache_db_path is None:
            return False
        sql = (
            "SELECT 1 FROM prices WHERE asset = ? AND currency = 'USD' AND date = ? AND source = ? "
            "AND created_at >= datetime('now', ?) LIMIT 1"
        )
        try:
            async with aiosqlite.connect(self._cache_db_path) as db:
                row = await (await db.execute(sql, (ticker, str(on_date), _MISS_SOURCE, _MISS_RETRY_WINDOW))).fetchone()
        except aiosqlite.Error:
            logger.exception("Failed to read price miss sentinel from SQLite")
            return False
        return row is not None

    async def _save_miss_on(self, ticker: str, on_date: date) -> None:
        """Record a time-limited "no price available" sentinel for (ticker, date)."""
        await self._write_price_row(ticker, "0", on_date, _MISS_SOURCE)

    async def _history_market_data(self, coin_id: str, on_date: date) -> dict[str, Any]:
        """Fetch /coins/{id}/history market_data for a date (dd-mm-yyyy)."""
        data = await self._request_json(
            f"/coins/{coin_id}/history",
            {"date": on_date.strftime("%d-%m-%Y"), "localization": "false"},
        )
        market_data = data.get("market_data")
        return market_data if isinstance(market_data, dict) else {}

    async def _fetch_crypto_price_on(self, ticker: str, on_date: date) -> Decimal | None:
        """Fetch a single crypto's USD price on a historical date."""
        coingecko_id = await self._resolve_coingecko_id(ticker)
        if not coingecko_id:
            return None
        market_data = await self._history_market_data(coingecko_id, on_date)
        price_val = market_data.get("current_price", {}).get("usd")
        return Decimal(str(price_val)) if price_val is not None else None

    async def _fetch_fiat_rate_on(self, ticker: str, on_date: date) -> Decimal | None:
        """Fetch a fiat-to-USD rate on a historical date via the BTC bridge."""
        market_data = await self._history_market_data("bitcoin", on_date)
        prices = market_data.get("current_price", {})
        btc_usd = prices.get("usd")
        btc_fiat = prices.get(ticker.lower())
        if btc_usd is None or not btc_fiat:
            return None
        return Decimal(str(btc_usd)) / Decimal(str(btc_fiat))

    async def _fetch_crypto_price(self, ticker: str) -> Decimal:
        """Fetch a single crypto price from CoinGecko."""
        coingecko_id = await self._resolve_coingecko_id(ticker)
        if not coingecko_id:
            msg = f"Unknown crypto ticker: {ticker}"
            raise ValueError(msg)

        data = await self._request_json(
            "/simple/price",
            {"ids": coingecko_id, "vs_currencies": "usd"},
        )

        price_val = data.get(coingecko_id, {}).get("usd")
        if price_val is None:
            msg = f"No price data for {ticker} (CoinGecko ID: {coingecko_id})"
            raise ValueError(msg)

        price = Decimal(str(price_val))
        self._set_cache(ticker, price)
        await self._save_persisted_cache(ticker, price)
        logger.debug("Fetched price %s = $%s", ticker, price)
        return price

    async def _fetch_crypto_prices_batch(self, tickers: list[str]) -> dict[str, Decimal]:
        """Fetch multiple crypto prices in a single API call."""
        id_to_ticker: dict[str, str] = {}
        for t in tickers:
            cg_id = await self._resolve_coingecko_id(t)
            if cg_id:
                id_to_ticker[cg_id] = t
            else:
                logger.warning("Unknown crypto ticker: %s — skipping", t)

        if not id_to_ticker:
            return {}

        data = await self._request_json(
            "/simple/price",
            {"ids": ",".join(id_to_ticker.keys()), "vs_currencies": "usd"},
        )

        results: dict[str, Decimal] = {}
        for cg_id, ticker in id_to_ticker.items():
            price_val = data.get(cg_id, {}).get("usd")
            if price_val is not None:
                price = Decimal(str(price_val))
                self._set_cache(ticker, price)
                await self._save_persisted_cache(ticker, price)
                results[ticker] = price
            else:
                logger.warning("No price data for %s", ticker)

        return results

    async def _fetch_fiat_rate(self, ticker: str) -> Decimal:
        """Fetch fiat-to-USD rate. Returns how much 1 unit of `ticker` is worth in USD."""
        # CoinGecko /simple/price can accept vs_currencies for fiat.
        # We use BTC as a bridge: get BTC price in both USD and target fiat.
        data = await self._request_json(
            "/simple/price",
            {"ids": "bitcoin", "vs_currencies": f"usd,{ticker.lower()}"},
        )

        btc_usd = data.get("bitcoin", {}).get("usd")
        btc_fiat = data.get("bitcoin", {}).get(ticker.lower())

        if btc_usd is None or btc_fiat is None:
            msg = f"Cannot determine {ticker}/USD rate from CoinGecko"
            raise ValueError(msg)

        # 1 BTC = X USD, 1 BTC = Y FIAT => 1 FIAT = X/Y USD
        rate = Decimal(str(btc_usd)) / Decimal(str(btc_fiat))
        self._set_cache(ticker, rate)
        await self._save_persisted_cache(ticker, rate)
        logger.debug("Fetched fiat rate 1 %s = $%s", ticker, rate)
        return rate

    async def _fetch_fiat_rates_batch(self, tickers: list[str]) -> dict[str, Decimal]:
        """Fetch multiple fiat-to-USD rates in one call via the BTC bridge."""
        currencies = ",".join(["usd"] + [t.lower() for t in tickers])
        data = await self._request_json(
            "/simple/price",
            {"ids": "bitcoin", "vs_currencies": currencies},
        )
        btc_usd_raw = data.get("bitcoin", {}).get("usd")
        if btc_usd_raw is None:
            logger.warning("CoinGecko: BTC/USD not available for fiat batch")
            return {}

        btc_usd = Decimal(str(btc_usd_raw))
        results: dict[str, Decimal] = {}
        for t in tickers:
            btc_fiat = data.get("bitcoin", {}).get(t.lower())
            if btc_fiat is None or Decimal(str(btc_fiat)) == 0:
                logger.warning("CoinGecko: no fiat rate for %s", t)
                continue
            rate = btc_usd / Decimal(str(btc_fiat))
            self._set_cache(t, rate)
            await self._save_persisted_cache(t, rate)
            results[t] = rate
        return results

    async def _resolve_coingecko_id(self, ticker: str) -> str | None:
        """Resolve ticker symbol to CoinGecko coin ID.

        Uses the cached /coins/list for fast lookup.  Falls back to
        /search only when a symbol maps to multiple coin IDs (rare).
        """
        if ticker in self._resolved_symbol_ids:
            return self._resolved_symbol_ids[ticker]

        coins_map = await self._ensure_coins_map()
        ids = coins_map.get(ticker, [])

        if len(ids) == 1:
            self._resolved_symbol_ids[ticker] = ids[0]
            return ids[0]

        if ids:
            # Multiple matches — use /search for market-cap ranking
            coin_id = await self._search_coingecko_id(ticker)
            if coin_id:
                return coin_id

        return None

    async def _search_coingecko_id(self, ticker: str) -> str | None:
        """Disambiguate a ticker via /search using market-cap rank."""
        data = await self._request_json("/search", {"query": ticker})
        coins = data.get("coins", [])
        if not isinstance(coins, list):
            return None

        symbol_matches: list[dict[str, Any]] = []
        for coin in coins:
            if not isinstance(coin, dict):
                continue
            symbol = str(coin.get("symbol", "")).upper()
            if symbol == ticker:
                symbol_matches.append(coin)

        if not symbol_matches:
            return None

        def _rank_key(coin: dict[str, Any]) -> tuple[int, int]:
            rank_raw = coin.get("market_cap_rank")
            if isinstance(rank_raw, int):
                rank = rank_raw
            elif isinstance(rank_raw, str) and rank_raw.isdigit():
                rank = int(rank_raw)
            else:
                rank = 10**9
            return (rank, 0 if coin.get("id") else 1)

        best = sorted(symbol_matches, key=_rank_key)[0]
        coin_id = str(best.get("id", ""))
        if not coin_id:
            return None

        self._resolved_symbol_ids[ticker] = coin_id
        return coin_id

    def today(self) -> date:
        """Return today's date in UTC."""
        return datetime.now(tz=UTC).date()
