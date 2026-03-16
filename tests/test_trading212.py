"""Tests for the Trading 212 collector."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from pfm.collectors.trading212 import Trading212Collector
from pfm.db.models import TransactionType
from pfm.pricing.coingecko import PricingService


@pytest.fixture
def pricing():
    svc = PricingService()
    svc._coins_by_symbol = {}
    svc._set_cache("EUR", Decimal("1.1"))
    svc._set_cache("USD", Decimal(1))
    svc.today = lambda: date(2026, 3, 7)  # type: ignore[assignment]
    return svc


async def test_get_json_retries_rate_limit_for_history_requests(pricing):
    collector = Trading212Collector(pricing, api_key="key", api_secret="secret")
    collector._history_rate_limiter.acquire = AsyncMock()  # type: ignore[method-assign]

    rate_limited = MagicMock(spec=httpx.Response)
    rate_limited.status_code = 429
    rate_limited.headers = {"Retry-After": "1"}

    ok_resp = MagicMock(spec=httpx.Response)
    ok_resp.status_code = 200
    ok_resp.headers = {}
    ok_resp.json.return_value = {"ok": True}
    ok_resp.raise_for_status = MagicMock()

    collector._client.get = AsyncMock(side_effect=[rate_limited, ok_resp])  # type: ignore[method-assign]

    with patch("pfm.collectors.trading212.asyncio.sleep", new=AsyncMock()):
        payload = await collector._get_json("/equity/history/orders?limit=1", history=True)

    assert payload == {"ok": True}
    assert collector._client.get.await_count == 2  # type: ignore[attr-defined]


async def test_fetch_raw_balances_requires_account_currency(pricing):
    collector = Trading212Collector(pricing, api_key="key", api_secret="secret")
    collector._get_json = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            {"cash": {"availableToTrade": 10, "reservedForOrders": 0, "inPies": 0}},
            [],
        ]
    )

    with pytest.raises(ValueError, match="missing account currency"):
        await collector.fetch_raw_balances()


async def test_fetch_raw_balances_prices_cash_and_position_in_usd(pricing):
    collector = Trading212Collector(pricing, api_key="key", api_secret="secret")
    collector._get_json = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            {
                "currency": "EUR",
                "cash": {"availableToTrade": 100, "reservedForOrders": 5, "inPies": 2},
            },
            [
                {
                    "instrument": {"ticker": "LHAd_EQ", "currency": "EUR"},
                    "quantity": "1.46537059",
                    "walletImpact": {"currency": "EUR", "currentValue": "11.90"},
                }
            ],
        ]
    )

    balances = await collector.fetch_raw_balances()

    assert len(balances) == 2
    cash = next(balance for balance in balances if balance.asset == "EUR")
    assert cash.amount == Decimal(107)
    assert cash.price == Decimal("1.1")

    position = next(balance for balance in balances if balance.asset == "LHAd_EQ")
    expected_price = (Decimal("11.90") * Decimal("1.1")) / Decimal("1.46537059")
    assert position.price == expected_price


async def test_fetch_raw_balances_skips_zero_quantity_positions(pricing):
    collector = Trading212Collector(pricing, api_key="key", api_secret="secret")
    collector._get_json = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            {"currency": "EUR", "cash": {"availableToTrade": 0, "reservedForOrders": 0, "inPies": 0}},
            [
                {
                    "instrument": {"ticker": "ZERO_EQ", "currency": "EUR"},
                    "quantity": "0",
                    "walletImpact": {"currency": "EUR", "currentValue": "0"},
                }
            ],
        ]
    )

    balances = await collector.fetch_raw_balances()
    assert balances == []


async def test_fetch_raw_balances_falls_back_to_current_price(pricing):
    collector = Trading212Collector(pricing, api_key="key", api_secret="secret")
    collector._get_json = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            {"currency": "EUR", "cash": {"availableToTrade": 0, "reservedForOrders": 0, "inPies": 0}},
            [
                {
                    "instrument": {"ticker": "BMW_EQ", "currency": "EUR"},
                    "quantity": "2",
                    "currentPrice": "10.5",
                    "walletImpact": {"currency": "EUR", "currentValue": "0"},
                }
            ],
        ]
    )

    balances = await collector.fetch_raw_balances()
    assert balances[0].price == Decimal("11.55")


async def test_parse_order_item_maps_buy_trade(pricing):
    collector = Trading212Collector(pricing, api_key="key", api_secret="secret")
    item = {
        "order": {
            "id": 100,
            "ticker": "LHAd_EQ",
            "status": "FILLED",
            "side": "BUY",
            "currency": "EUR",
            "createdAt": "2026-03-01T13:12:02.000Z",
        },
        "fill": {
            "id": 200,
            "quantity": "1.5",
            "type": "TRADE",
            "filledAt": "2026-03-02T08:03:05.000Z",
            "walletImpact": {"currency": "EUR", "netValue": "12.06"},
        },
    }

    tx = await collector._parse_order_item(item)

    assert tx is not None
    assert tx.tx_type == TransactionType.UNKNOWN
    assert tx.trade_side == "buy"
    assert tx.counterparty_asset == "EUR"
    assert tx.counterparty_amount == Decimal("12.06")
    assert tx.usd_value == Decimal("13.266")
    assert tx.tx_id == "200"


async def test_parse_order_item_maps_sell_trade(pricing):
    collector = Trading212Collector(pricing, api_key="key", api_secret="secret")
    item = {
        "order": {
            "id": 101,
            "ticker": "LHAd_EQ",
            "status": "FILLED",
            "side": "SELL",
            "currency": "EUR",
            "createdAt": "2026-03-03T13:12:02.000Z",
        },
        "fill": {
            "id": 201,
            "quantity": "0.5",
            "type": "TRADE",
            "filledAt": "2026-03-03T13:15:00.000Z",
            "walletImpact": {"currency": "EUR", "netValue": "5.25"},
        },
    }

    tx = await collector._parse_order_item(item)

    assert tx is not None
    assert tx.trade_side == "sell"
    assert tx.amount == Decimal("0.5")
    assert tx.usd_value == Decimal("5.775")


async def test_parse_cash_item_maps_deposit(pricing):
    collector = Trading212Collector(pricing, api_key="key", api_secret="secret")
    item = {
        "type": "DEPOSIT",
        "amount": 5000,
        "currency": "EUR",
        "reference": "cash-1",
        "dateTime": "2026-03-07T11:32:08.545Z",
    }

    tx = await collector._parse_cash_item(item)

    assert tx is not None
    assert tx.tx_type == TransactionType.UNKNOWN
    assert tx.asset == "EUR"
    assert tx.amount == Decimal(5000)
    assert tx.usd_value == Decimal(5500)
    assert tx.tx_id == "cash-1"


async def test_parse_cash_item_maps_interest(pricing):
    collector = Trading212Collector(pricing, api_key="key", api_secret="secret")
    item = {
        "type": "DAILY_INTEREST",
        "amount": "1.25",
        "currency": "EUR",
        "reference": "interest-1",
        "dateTime": "2026-03-07T11:32:08.545Z",
    }

    tx = await collector._parse_cash_item(item)

    assert tx is not None
    assert tx.tx_type == TransactionType.UNKNOWN


async def test_parse_dividend_item_generates_stable_id_when_reference_missing(pricing):
    collector = Trading212Collector(pricing, api_key="key", api_secret="secret")
    item = {
        "amount": {"amount": "3.25", "currency": "EUR"},
        "dateTime": "2026-03-06T09:00:00.000Z",
        "ticker": "LHAd_EQ",
    }

    tx = await collector._parse_dividend_item(item)

    assert tx is not None
    assert tx.tx_type == TransactionType.UNKNOWN
    assert tx.asset == "EUR"
    assert tx.amount == Decimal("3.25")
    assert tx.usd_value == Decimal("3.575")
    assert tx.tx_id.startswith("trading212-dividend-")


async def test_fetch_history_follows_next_page_and_stops_on_since(pricing):
    collector = Trading212Collector(pricing, api_key="key", api_secret="secret")
    collector._get_json = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            {
                "items": [
                    {
                        "type": "DEPOSIT",
                        "amount": 10,
                        "currency": "EUR",
                        "reference": "newer",
                        "dateTime": "2026-03-07T11:32:08.545Z",
                    }
                ],
                "nextPagePath": "limit=50&cursor=older",
            },
            {
                "items": [
                    {
                        "type": "DEPOSIT",
                        "amount": 20,
                        "currency": "EUR",
                        "reference": "older",
                        "dateTime": "2026-02-01T11:32:08.545Z",
                    }
                ],
                "nextPagePath": "limit=50&cursor=ignored",
            },
        ]
    )

    txs = await collector._fetch_history(
        "/equity/history/transactions",
        collector._parse_cash_item,
        since=date(2026, 3, 1),
    )

    assert len(txs) == 1
    assert txs[0].tx_id == "newer"
    assert collector._get_json.await_count == 2  # type: ignore[attr-defined]
