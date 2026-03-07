"""Tests for CoinGecko pricing service."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import aiosqlite
import httpx
import pytest

from pfm.db.models import init_db
from pfm.pricing.coingecko import STABLECOINS, PricingService


@pytest.fixture
def pricing():
    svc = PricingService()
    # Pre-populate coins map so tests don't trigger /coins/list HTTP call
    svc._coins_by_symbol = {
        "BTC": ["bitcoin"],
        "ETH": ["ethereum"],
        "XLM": ["stellar"],
    }
    return svc


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


async def test_unique_symbol_resolves_from_coins_list(pricing):
    """Single match in coins list resolves without /search."""
    pricing._coins_by_symbol["ETC"] = ["ethereum-classic"]

    price_resp = MagicMock(spec=httpx.Response)
    price_resp.status_code = 200
    price_resp.headers = {}
    price_resp.json.return_value = {"ethereum-classic": {"usd": 30}}
    price_resp.raise_for_status = MagicMock()

    pricing._client.get = AsyncMock(return_value=price_resp)

    price = await pricing.get_price_usd("ETC")
    assert price == Decimal(30)
    assert pricing._client.get.await_count == 1  # only /simple/price, no /search


async def test_ambiguous_symbol_falls_back_to_search(pricing):
    """Multiple matches in coins list triggers /search for disambiguation."""
    pricing._coins_by_symbol["ETC"] = ["ethereum-classic", "eternal-token"]

    search_resp = MagicMock(spec=httpx.Response)
    search_resp.status_code = 200
    search_resp.headers = {}
    search_resp.json.return_value = {
        "coins": [
            {"id": "ethereum-classic", "symbol": "etc", "market_cap_rank": 40},
            {"id": "eternal-token", "symbol": "etc", "market_cap_rank": 9999},
        ]
    }
    search_resp.raise_for_status = MagicMock()

    price_resp = MagicMock(spec=httpx.Response)
    price_resp.status_code = 200
    price_resp.headers = {}
    price_resp.json.return_value = {"ethereum-classic": {"usd": 30}}
    price_resp.raise_for_status = MagicMock()

    pricing._client.get = AsyncMock(side_effect=[search_resp, price_resp])

    price = await pricing.get_price_usd("ETC")
    assert price == Decimal(30)


async def test_convert_to_usd_stablecoin(pricing):
    result = await pricing.convert_to_usd(Decimal(100), "USDC")
    assert result == Decimal(100)


async def test_convert_to_usd_with_cached_price(pricing):
    pricing._set_cache("BTC", Decimal(50000))
    result = await pricing.convert_to_usd(Decimal(2), "BTC")
    assert result == Decimal(100000)


async def test_coins_list_loaded_lazily(pricing):
    """Coins map is fetched from /coins/list on first resolution."""
    pricing._coins_by_symbol = None  # reset to trigger fetch

    coins_list_resp = MagicMock(spec=httpx.Response)
    coins_list_resp.status_code = 200
    coins_list_resp.headers = {}
    coins_list_resp.json.return_value = [
        {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"},
        {"id": "ethereum", "symbol": "eth", "name": "Ethereum"},
    ]
    coins_list_resp.raise_for_status = MagicMock()

    price_resp = MagicMock(spec=httpx.Response)
    price_resp.status_code = 200
    price_resp.headers = {}
    price_resp.json.return_value = {"bitcoin": {"usd": 60000}}
    price_resp.raise_for_status = MagicMock()

    pricing._client.get = AsyncMock(side_effect=[coins_list_resp, price_resp])

    price = await pricing.get_price_usd("BTC")
    assert price == Decimal(60000)
    assert pricing._coins_by_symbol is not None
    assert "BTC" in pricing._coins_by_symbol


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


async def test_persistent_cache_hit_avoids_http(tmp_path):
    db_path = tmp_path / "pricing-cache.db"
    await init_db(db_path)

    async with aiosqlite.connect(str(db_path)) as db:
        await db.execute(
            "INSERT INTO prices (date, asset, currency, price, source) VALUES (?, ?, ?, ?, ?)",
            (str(datetime.now(tz=UTC).date()), "BTC", "USD", "12345.67", "coingecko"),
        )
        await db.commit()

    pricing = PricingService(cache_db_path=db_path)
    pricing._client.get = AsyncMock(side_effect=AssertionError("HTTP should not be called"))  # type: ignore[assignment]

    price = await pricing.get_price_usd("BTC")
    await pricing.close()
    assert price == Decimal("12345.67")


async def test_persistent_cache_write_through(tmp_path):
    db_path = tmp_path / "pricing-cache-write.db"
    await init_db(db_path)

    ok_resp = MagicMock(spec=httpx.Response)
    ok_resp.status_code = 200
    ok_resp.headers = {}
    ok_resp.json.return_value = {"bitcoin": {"usd": 50000}}
    ok_resp.raise_for_status = MagicMock()

    pricing1 = PricingService(cache_db_path=db_path)
    pricing1._coins_by_symbol = {"BTC": ["bitcoin"]}
    pricing1._client.get = AsyncMock(return_value=ok_resp)  # type: ignore[assignment]
    fetched = await pricing1.get_price_usd("BTC")
    await pricing1.close()
    assert fetched == Decimal(50000)

    pricing2 = PricingService(cache_db_path=db_path)
    pricing2._client.get = AsyncMock(side_effect=AssertionError("HTTP should not be called"))  # type: ignore[assignment]
    cached = await pricing2.get_price_usd("BTC")
    await pricing2.close()
    assert cached == Decimal(50000)
