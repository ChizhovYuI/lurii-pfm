"""Tests for CoinGecko pricing service."""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from pfm.pricing.coingecko import STABLECOINS, TICKER_TO_COINGECKO, PricingService


@pytest.fixture
def pricing():
    return PricingService()


async def test_usd_returns_one(pricing):
    assert await pricing.get_price_usd("USD") == Decimal(1)


async def test_stablecoins_return_one(pricing):
    for coin in STABLECOINS:
        assert await pricing.get_price_usd(coin) == Decimal(1)


async def test_cache_hit(pricing):
    pricing._set_cache("BTC", Decimal(50000))
    price = await pricing.get_price_usd("BTC")
    assert price == Decimal(50000)


async def test_unknown_ticker_raises(pricing):
    with pytest.raises(ValueError, match="Unknown crypto ticker"):
        await pricing.get_price_usd("FAKECOIN123XYZ")


async def test_convert_to_usd_stablecoin(pricing):
    result = await pricing.convert_to_usd(Decimal(100), "USDC")
    assert result == Decimal(100)


async def test_convert_to_usd_with_cached_price(pricing):
    pricing._set_cache("BTC", Decimal(50000))
    result = await pricing.convert_to_usd(Decimal(2), "BTC")
    assert result == Decimal(100000)


def test_ticker_mapping_has_common_assets():
    assert "BTC" in TICKER_TO_COINGECKO
    assert "ETH" in TICKER_TO_COINGECKO
    assert "XLM" in TICKER_TO_COINGECKO
    assert "USDC" in TICKER_TO_COINGECKO  # mapped but short-circuited for stablecoins


async def test_get_prices_usd_batch(pricing):
    pricing._set_cache("BTC", Decimal(50000))
    pricing._set_cache("ETH", Decimal(3000))
    results = await pricing.get_prices_usd(["USD", "USDC", "BTC", "ETH"])
    assert results["USD"] == Decimal(1)
    assert results["USDC"] == Decimal(1)
    assert results["BTC"] == Decimal(50000)
    assert results["ETH"] == Decimal(3000)


async def test_retries_on_rate_limit_429(pricing):
    rate_limited = MagicMock(spec=httpx.Response)
    rate_limited.status_code = 429
    rate_limited.headers = {}

    ok_resp = MagicMock(spec=httpx.Response)
    ok_resp.status_code = 200
    ok_resp.headers = {}
    ok_resp.json.return_value = {"bitcoin": {"usd": 60000}}
    ok_resp.raise_for_status = MagicMock()

    pricing._client.get = AsyncMock(side_effect=[rate_limited, ok_resp])

    with patch("pfm.pricing.coingecko.asyncio.sleep", new=AsyncMock()):
        price = await pricing.get_price_usd("BTC")

    assert price == Decimal(60000)
    assert pricing._client.get.await_count == 2
