"""CoinGecko pricing service for crypto and fiat rate conversion."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import httpx

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
STABLECOINS: frozenset[str] = frozenset({"USDC", "USDT", "DAI", "BUSD", "TUSD", "USDP"})

# Fiat currencies — use /exchange_rates or /simple/price with vs_currencies
FIAT_TICKERS: frozenset[str] = frozenset({"USD", "GBP", "EUR", "THB", "JPY", "CHF", "CAD", "AUD"})

_BASE_URL = "https://api.coingecko.com/api/v3"
_RATE_LIMIT_DELAY = 2.1  # seconds between requests (30 req/min = 1 per 2s)


class PricingService:
    """Fetches crypto prices and fiat rates from CoinGecko."""

    def __init__(self, api_key: str = "") -> None:
        headers: dict[str, str] = {"Accept": "application/json"}
        if api_key:
            headers["x-cg-demo-api-key"] = api_key
        self._client = httpx.AsyncClient(
            base_url=_BASE_URL,
            headers=headers,
            timeout=30.0,
        )
        self._last_request_time: float = 0.0
        self._cache: dict[str, tuple[Decimal, datetime]] = {}
        self._cache_ttl_seconds: float = 3600.0  # 1 hour

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
            elif cached := self._get_cached(t):
                results[t] = cached
            elif t in FIAT_TICKERS:
                fiat_to_fetch.append(t)
            else:
                crypto_to_fetch.append(t)

        # Batch fetch crypto
        if crypto_to_fetch:
            prices = await self._fetch_crypto_prices_batch(crypto_to_fetch)
            results.update(prices)

        # Fetch fiat one by one (different endpoint)
        for fiat in fiat_to_fetch:
            results[fiat] = await self._fetch_fiat_rate(fiat)

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

    async def _fetch_crypto_price(self, ticker: str) -> Decimal:
        """Fetch a single crypto price from CoinGecko."""
        coingecko_id = TICKER_TO_COINGECKO.get(ticker)
        if not coingecko_id:
            msg = f"Unknown crypto ticker: {ticker}. Add it to TICKER_TO_COINGECKO mapping."
            raise ValueError(msg)

        await self._rate_limit()
        resp = await self._client.get(
            "/simple/price",
            params={"ids": coingecko_id, "vs_currencies": "usd"},
        )
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()

        price_val = data.get(coingecko_id, {}).get("usd")
        if price_val is None:
            msg = f"No price data for {ticker} (CoinGecko ID: {coingecko_id})"
            raise ValueError(msg)

        price = Decimal(str(price_val))
        self._set_cache(ticker, price)
        logger.debug("Fetched price %s = $%s", ticker, price)
        return price

    async def _fetch_crypto_prices_batch(self, tickers: list[str]) -> dict[str, Decimal]:
        """Fetch multiple crypto prices in a single API call."""
        id_to_ticker: dict[str, str] = {}
        for t in tickers:
            cg_id = TICKER_TO_COINGECKO.get(t)
            if cg_id:
                id_to_ticker[cg_id] = t
            else:
                logger.warning("Unknown crypto ticker: %s — skipping", t)

        if not id_to_ticker:
            return {}

        await self._rate_limit()
        resp = await self._client.get(
            "/simple/price",
            params={"ids": ",".join(id_to_ticker.keys()), "vs_currencies": "usd"},
        )
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()

        results: dict[str, Decimal] = {}
        for cg_id, ticker in id_to_ticker.items():
            price_val = data.get(cg_id, {}).get("usd")
            if price_val is not None:
                price = Decimal(str(price_val))
                self._set_cache(ticker, price)
                results[ticker] = price
            else:
                logger.warning("No price data for %s", ticker)

        return results

    async def _fetch_fiat_rate(self, ticker: str) -> Decimal:
        """Fetch fiat-to-USD rate. Returns how much 1 unit of `ticker` is worth in USD."""
        await self._rate_limit()
        # CoinGecko /simple/price can accept vs_currencies for fiat
        # We use BTC as a bridge: get BTC price in both USD and target fiat
        resp = await self._client.get(
            "/simple/price",
            params={"ids": "bitcoin", "vs_currencies": f"usd,{ticker.lower()}"},
        )
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()

        btc_usd = data.get("bitcoin", {}).get("usd")
        btc_fiat = data.get("bitcoin", {}).get(ticker.lower())

        if btc_usd is None or btc_fiat is None:
            msg = f"Cannot determine {ticker}/USD rate from CoinGecko"
            raise ValueError(msg)

        # 1 BTC = X USD, 1 BTC = Y FIAT => 1 FIAT = X/Y USD
        rate = Decimal(str(btc_usd)) / Decimal(str(btc_fiat))
        self._set_cache(ticker, rate)
        logger.debug("Fetched fiat rate 1 %s = $%s", ticker, rate)
        return rate

    def today(self) -> date:
        """Return today's date in UTC."""
        return datetime.now(tz=UTC).date()
