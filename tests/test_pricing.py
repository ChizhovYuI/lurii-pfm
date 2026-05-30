"""Tests for CoinGecko pricing service."""

from datetime import UTC, date, datetime
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


async def test_get_price_usd_on_usd_and_stablecoin(pricing):
    assert await pricing.get_price_usd_on("USD", date(2026, 4, 1)) == Decimal(1)
    assert await pricing.get_price_usd_on("USDC", date(2026, 4, 1)) == Decimal(1)


async def test_get_price_usd_on_crypto_uses_history_endpoint(pricing):
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 200
    resp.headers = {}
    resp.json.return_value = {"market_data": {"current_price": {"usd": 42000}}}
    resp.raise_for_status = MagicMock()
    pricing._client.get = AsyncMock(return_value=resp)

    price = await pricing.get_price_usd_on("BTC", date(2026, 4, 1))
    assert price == Decimal(42000)
    # /coins/{id}/history?date=dd-mm-yyyy
    call = pricing._client.get.await_args
    assert call is not None
    assert call.args[0] == "/coins/bitcoin/history"
    assert call.kwargs["params"]["date"] == "01-04-2026"


async def test_get_price_usd_on_fiat_via_btc_bridge(pricing):
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 200
    resp.headers = {}
    # 1 BTC = 40000 USD = 1,400,000 THB → 1 THB = 40000/1400000 USD
    resp.json.return_value = {"market_data": {"current_price": {"usd": 40000, "thb": 1400000}}}
    resp.raise_for_status = MagicMock()
    pricing._client.get = AsyncMock(return_value=resp)

    price = await pricing.get_price_usd_on("THB", date(2026, 4, 1))
    assert price is not None
    assert price == Decimal(40000) / Decimal(1400000)


async def test_get_price_usd_on_returns_none_when_missing(pricing):
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 200
    resp.headers = {}
    resp.json.return_value = {}  # no market_data
    resp.raise_for_status = MagicMock()
    pricing._client.get = AsyncMock(return_value=resp)

    assert await pricing.get_price_usd_on("BTC", date(2026, 4, 1)) is None


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


async def test_additional_fiat_tickers_use_fiat_path(pricing):
    pricing._fetch_fiat_rate = AsyncMock(return_value=Decimal("0.62"))  # type: ignore[method-assign]

    price = await pricing.get_price_usd("NZD")

    assert price == Decimal("0.62")
    pricing._fetch_fiat_rate.assert_awaited_once_with("NZD")


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


async def test_historical_cache_row_does_not_poison_live_price(tmp_path):
    db_path = tmp_path / "pricing-cache-poison.db"
    await init_db(db_path)

    # A historical backfill row: old ``date`` but fresh ``created_at``. The live
    # lookup must ignore it (date pin) instead of serving it as the current price.
    async with aiosqlite.connect(str(db_path)) as db:
        await db.execute(
            "INSERT INTO prices (date, asset, currency, price, source) VALUES (?, ?, ?, ?, ?)",
            ("2021-01-01", "BTC", "USD", "29000", "coingecko"),
        )
        await db.commit()

    ok_resp = MagicMock(spec=httpx.Response)
    ok_resp.status_code = 200
    ok_resp.headers = {}
    ok_resp.json.return_value = {"bitcoin": {"usd": 60000}}
    ok_resp.raise_for_status = MagicMock()

    pricing = PricingService(cache_db_path=db_path)
    pricing._coins_by_symbol = {"BTC": ["bitcoin"]}
    pricing._client.get = AsyncMock(return_value=ok_resp)  # type: ignore[assignment]

    price = await pricing.get_price_usd("BTC")
    await pricing.close()

    # Stale historical row ignored; the live fetch wins.
    assert price == Decimal(60000)


async def test_today_backfill_does_not_poison_live_price(tmp_path):
    db_path = tmp_path / "pricing-today-poison.db"
    await init_db(db_path)

    pricing = PricingService(cache_db_path=db_path)
    pricing._coins_by_symbol = {"BTC": ["bitcoin"]}

    # Backfill a TODAY-dated row: this writes a 'coingecko-history' row via the
    # /history endpoint (a daily-open snapshot), NOT the live spot.
    hist_resp = MagicMock(spec=httpx.Response)
    hist_resp.status_code = 200
    hist_resp.headers = {}
    hist_resp.json.return_value = {"market_data": {"current_price": {"usd": 50000}}}
    hist_resp.raise_for_status = MagicMock()
    pricing._client.get = AsyncMock(return_value=hist_resp)  # type: ignore[assignment]
    today = pricing.today()
    assert await pricing.get_price_usd_on("BTC", today) == Decimal(50000)

    # A subsequent LIVE lookup must NOT serve that historical row as the current
    # spot price — it must hit the network for the real live value.
    pricing._cache.clear()  # drop the in-memory layer so the persisted path is exercised
    live_resp = MagicMock(spec=httpx.Response)
    live_resp.status_code = 200
    live_resp.headers = {}
    live_resp.json.return_value = {"bitcoin": {"usd": 61000}}
    live_resp.raise_for_status = MagicMock()
    pricing._client.get = AsyncMock(return_value=live_resp)  # type: ignore[assignment]

    live = await pricing.get_price_usd("BTC")
    await pricing.close()
    assert live == Decimal(61000)  # live fetch, not the 50000 daily-open


async def test_historical_miss_is_cached_and_not_refetched(tmp_path):
    db_path = tmp_path / "pricing-miss.db"
    await init_db(db_path)

    pricing = PricingService(cache_db_path=db_path)
    pricing._coins_by_symbol = {}  # unknown ticker → resolver returns None (definitive miss)
    call_count = 0

    async def _fetch(ticker: str, on_date: date) -> Decimal | None:
        nonlocal call_count
        call_count += 1
        return None

    pricing._fetch_crypto_price_on = _fetch  # type: ignore[assignment, method-assign]

    on = date(2026, 4, 1)
    first = await pricing.get_price_usd_on("WAT", on)
    second = await pricing.get_price_usd_on("WAT", on)
    await pricing.close()

    assert first is None
    assert second is None
    # The miss sentinel short-circuits the second call: no second fetch attempt.
    assert call_count == 1


async def test_historical_miss_sentinel_not_served_as_real_price(tmp_path):
    db_path = tmp_path / "pricing-miss-isolation.db"
    await init_db(db_path)

    pricing = PricingService(cache_db_path=db_path)
    pricing._coins_by_symbol = {}

    async def _miss(ticker: str, on_date: date) -> Decimal | None:
        return None

    pricing._fetch_crypto_price_on = _miss  # type: ignore[assignment, method-assign]

    on = date(2026, 4, 1)
    await pricing.get_price_usd_on("WAT", on)  # writes a '0' sentinel
    # A subsequent real read for the same (asset, date) must not return the
    # sentinel as a price.
    assert await pricing._get_persisted_cache_on("WAT", on) is None
    await pricing.close()


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
