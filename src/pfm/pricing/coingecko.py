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

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

# Map common ticker symbols to CoinGecko IDs
TICKER_TO_COINGECKO: dict[str, str] = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "XLM": "stellar",
    "XRP": "ripple",
    "SOL": "solana",
    "USDC": "usd-coin",
    "USDT": "tether",
    "DAI": "dai",
    "BNB": "binancecoin",
    "DOGE": "dogecoin",
    "ADA": "cardano",
    "DOT": "polkadot",
    "AVAX": "avalanche-2",
    "MATIC": "matic-network",
    "LINK": "chainlink",
    "UNI": "uniswap",
    "AAVE": "aave",
    "ATOM": "cosmos",
    "NEAR": "near",
    "ARB": "arbitrum",
    "OP": "optimism",
}

# Stablecoins pegged to USD — skip API call
STABLECOINS: frozenset[str] = frozenset({"USDC", "USDT", "DAI", "BUSD", "TUSD", "USDP", "FDUSD"})

# Fiat currencies — use /exchange_rates or /simple/price with vs_currencies
FIAT_TICKERS: frozenset[str] = frozenset({"USD", "GBP", "EUR", "THB", "JPY", "CHF", "CAD", "AUD"})

_BASE_URL = "https://api.coingecko.com/api/v3"
_RATE_LIMIT_DELAY = 2.1  # seconds between requests (30 req/min = 1 per 2s)
_MAX_429_RETRIES = 3
_RETRY_BACKOFF_BASE_SECONDS = 2.0
_HTTP_STATUS_TOO_MANY_REQUESTS = 429


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

    async def close(self) -> None:
        """Close the HTTP client."""
        await self._client.aclose()

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

    async def _request_json(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        """Request JSON from CoinGecko with serialized rate limiting and 429 backoff."""
        for attempt in range(_MAX_429_RETRIES + 1):
            async with self._request_lock:
                await self._rate_limit()
                resp = await self._client.get(path, params=params)

            if resp.status_code == _HTTP_STATUS_TOO_MANY_REQUESTS and attempt < _MAX_429_RETRIES:
                retry_after_header = resp.headers.get("Retry-After")
                retry_after = float(retry_after_header) if retry_after_header and retry_after_header.isdigit() else 0.0
                delay = max(retry_after, _RETRY_BACKOFF_BASE_SECONDS ** (attempt + 1))
                logger.warning("CoinGecko rate limited (429). Retrying in %.1fs", delay)
                await asyncio.sleep(delay)
                continue

            resp.raise_for_status()
            data: dict[str, Any] = resp.json()
            return data

        msg = "CoinGecko request failed after retries"
        raise RuntimeError(msg)

    async def _get_persisted_cache(self, ticker: str) -> Decimal | None:
        """Read recent cached price from SQLite, if configured."""
        if self._cache_db_path is None:
            return None

        ttl_window = f"-{int(self._cache_ttl_seconds)} seconds"
        sql = (
            "SELECT price FROM prices "
            "WHERE asset = ? AND currency = 'USD' AND created_at >= datetime('now', ?) "
            "ORDER BY created_at DESC LIMIT 1"
        )
        try:
            async with aiosqlite.connect(self._cache_db_path) as db:
                row = await (await db.execute(sql, (ticker, ttl_window))).fetchone()
        except aiosqlite.Error:
            logger.exception("Failed to read price cache from SQLite")
            return None

        if row is None:
            return None
        return Decimal(str(row[0]))

    async def _save_persisted_cache(self, ticker: str, price: Decimal) -> None:
        """Write fetched price into SQLite cache, if configured."""
        if self._cache_db_path is None:
            return

        sql = "INSERT INTO prices (date, asset, currency, price, source) VALUES (?, ?, 'USD', ?, 'coingecko')"
        try:
            async with aiosqlite.connect(self._cache_db_path) as db:
                await db.execute(sql, (str(self.today()), ticker, str(price)))
                await db.commit()
        except aiosqlite.Error:
            logger.exception("Failed to write price cache into SQLite")

    async def _fetch_crypto_price(self, ticker: str) -> Decimal:
        """Fetch a single crypto price from CoinGecko."""
        coingecko_id = TICKER_TO_COINGECKO.get(ticker) or await self._resolve_coingecko_id(ticker)
        if not coingecko_id:
            msg = f"Unknown crypto ticker: {ticker}. Add it to TICKER_TO_COINGECKO mapping."
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
            cg_id = TICKER_TO_COINGECKO.get(t) or await self._resolve_coingecko_id(t)
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

    async def _resolve_coingecko_id(self, ticker: str) -> str | None:  # noqa: C901
        """Best-effort resolve a CoinGecko coin id from a ticker symbol."""
        if ticker in self._resolved_symbol_ids:
            return self._resolved_symbol_ids[ticker]

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
