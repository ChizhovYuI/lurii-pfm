"""Edge-case tests for Bybit collector payload parsing."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import httpx

from pfm.collectors.bybit import BybitCollector


def _mock_response(json_data: dict) -> httpx.Response:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 200
    resp.json.return_value = json_data
    resp.text = json.dumps(json_data)
    resp.raise_for_status = MagicMock()
    return resp


async def test_bybit_fetch_earn_invalid_yesterday_yield_uses_estimate_apr(pricing):
    pricing._set_cache("USDT", Decimal(1))
    pricing.today = lambda: date(2024, 1, 15)  # type: ignore[assignment]
    collector = BybitCollector(pricing, api_key="key", api_secret="secret")

    async def mock_get(path, **kwargs):
        params = kwargs.get("params") or {}
        category = params.get("category", "")
        acct_type = params.get("accountType", "")
        if acct_type == "UNIFIED":
            return _mock_response({"retCode": 0, "result": {"list": []}})
        if acct_type == "FUND":
            return _mock_response({"retCode": 0, "result": {"balance": []}})
        if "earn/position" in path and category == "FlexibleSaving":
            return _mock_response(
                {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {
                                "coin": "USDT",
                                "amount": "200",
                                # Real-world bug: empty/non-decimal yesterdayYield
                                "yesterdayYield": "",
                                "estimateApr": "0.6%",
                            }
                        ]
                    },
                }
            )
        return _mock_response({"retCode": 0, "result": {"list": []}})

    collector._client.get = AsyncMock(side_effect=mock_get)

    snapshots = await collector.fetch_balances()
    usdt = next(s for s in snapshots if s.asset == "USDT")
    assert usdt.amount == Decimal(200)
    # 0.6% APR should map to non-zero APY via fallback.
    assert usdt.apy > Decimal(0)


async def test_bybit_fetch_onchain_earn_uses_product_apr_fallback(pricing):
    pricing._set_cache("USDT", Decimal(1))
    collector = BybitCollector(pricing, api_key="key", api_secret="secret")

    async def mock_get(path, **kwargs):
        params = kwargs.get("params") or {}
        category = params.get("category", "")
        acct_type = params.get("accountType", "")
        if acct_type == "UNIFIED":
            return _mock_response({"retCode": 0, "result": {"list": []}})
        if acct_type == "FUND":
            return _mock_response({"retCode": 0, "result": {"balance": []}})
        if "earn/product" in path and category == "OnChain":
            return _mock_response(
                {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {
                                "productId": "onchain-usdt-1",
                                "coin": "USDT",
                                "estimateApr": "10.0%",
                            }
                        ]
                    },
                }
            )
        if "earn/position" in path and category == "OnChain":
            return _mock_response(
                {
                    "retCode": 0,
                    "result": {
                        "list": [
                            {
                                "productId": "onchain-usdt-1",
                                "coin": "USDT",
                                "amount": "1000",
                                # Some OnChain rows can miss these fields.
                                "yesterdayYield": "",
                                "estimateApr": "",
                            }
                        ]
                    },
                }
            )
        return _mock_response({"retCode": 0, "result": {"list": []}})

    collector._client.get = AsyncMock(side_effect=mock_get)

    snapshots = await collector.fetch_balances()
    usdt = next(s for s in snapshots if s.asset == "USDT")
    assert usdt.amount == Decimal(1000)
    # 10% APR converted to APY.
    assert usdt.apy > Decimal("0.10")


def _empty_response() -> httpx.Response:
    return _mock_response({"retCode": 0, "result": {"list": []}})


def _asset_overview_response(categories: list[dict]) -> httpx.Response:
    return _mock_response(
        {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "accountType": "Earn",
                        "totalEquity": "10000",
                        "categories": categories,
                    }
                ]
            },
        }
    )


async def test_bybit_dual_asset_captured(pricing):
    """Dual Asset coins from asset-overview are captured; Easy Earn is skipped."""
    pricing._set_cache("USDT", Decimal(1))
    pricing._set_cache("BTC", Decimal(60000))
    collector = BybitCollector(pricing, api_key="key", api_secret="secret")

    async def mock_get(path, **kwargs):
        params = kwargs.get("params") or {}
        acct_type = params.get("accountType", "")
        if acct_type == "UNIFIED":
            return _mock_response({"retCode": 0, "result": {"list": []}})
        if acct_type == "FUND":
            return _mock_response({"retCode": 0, "result": {"balance": []}})
        if "asset-overview" in path:
            return _asset_overview_response(
                [
                    {
                        "category": "Easy Earn",
                        "equity": "5000",
                        "coinDetail": [{"coin": "USDT", "equity": "5000"}],
                    },
                    {
                        "category": "Dual Asset",
                        "equity": "0.1",
                        "coinDetail": [{"coin": "BTC", "equity": "0.1"}],
                    },
                ]
            )
        return _empty_response()

    collector._client.get = AsyncMock(side_effect=mock_get)

    snapshots = await collector.fetch_balances()
    btc = [s for s in snapshots if s.asset == "BTC"]
    assert len(btc) == 1
    assert btc[0].amount == Decimal("0.1")
    assert btc[0].apy == Decimal(0)
    raw = json.loads(btc[0].raw_json)
    assert raw["category"] == "Dual Asset"

    # Easy Earn USDT should NOT appear (no earn/position mock returned it).
    usdt = [s for s in snapshots if s.asset == "USDT"]
    assert len(usdt) == 0


async def test_bybit_earn_extra_api_failure_non_fatal(pricing):
    """Asset-overview failure does not break the rest of the collection."""
    pricing._set_cache("USDT", Decimal(1))
    collector = BybitCollector(pricing, api_key="key", api_secret="secret")

    async def mock_get(path, **kwargs):
        params = kwargs.get("params") or {}
        acct_type = params.get("accountType", "")
        if acct_type == "UNIFIED":
            return _mock_response(
                {
                    "retCode": 0,
                    "result": {"list": [{"coin": [{"coin": "USDT", "walletBalance": "100"}]}]},
                }
            )
        if acct_type == "FUND":
            return _mock_response({"retCode": 0, "result": {"balance": []}})
        if "asset-overview" in path:
            return _mock_response({"retCode": 10000, "retMsg": "server error"})
        return _empty_response()

    collector._client.get = AsyncMock(side_effect=mock_get)

    snapshots = await collector.fetch_balances()
    usdt = next(s for s in snapshots if s.asset == "USDT")
    assert usdt.amount == Decimal(100)


async def test_bybit_dual_asset_with_earn_override(pricing):
    """Earn overrides inject APR and settlement_at into Dual Asset positions."""
    pricing._set_cache("USDT", Decimal(1))
    collector = BybitCollector(pricing, api_key="key", api_secret="secret")
    collector.earn_overrides = [  # type: ignore[attr-defined]
        {
            "category": "Dual Asset",
            "coin": "USDT",
            "apr": "102.75",
            "settlement_at": "2026-03-28T07:59:00Z",
        }
    ]

    async def mock_get(path, **kwargs):
        params = kwargs.get("params") or {}
        acct_type = params.get("accountType", "")
        if acct_type == "UNIFIED":
            return _mock_response({"retCode": 0, "result": {"list": []}})
        if acct_type == "FUND":
            return _mock_response({"retCode": 0, "result": {"balance": []}})
        if "asset-overview" in path:
            return _asset_overview_response(
                [
                    {
                        "category": "Dual Asset",
                        "equity": "1053.6253",
                        "coinDetail": [{"coin": "USDT", "equity": "1053.6253"}],
                    }
                ]
            )
        return _empty_response()

    collector._client.get = AsyncMock(side_effect=mock_get)

    snapshots = await collector.fetch_balances()
    usdt = next(s for s in snapshots if s.asset == "USDT")
    assert usdt.amount == Decimal("1053.6253")
    # 102.75% APR → APY > 1.0
    # 102.75% APR stored as fraction 1.0275 (no compounding for structured products).
    assert usdt.apy == Decimal("1.0275")
    raw = json.loads(usdt.raw_json)
    assert raw["settlement_at"] == "2026-03-28T07:59:00Z"
    assert raw["category"] == "Dual Asset"


async def test_bybit_earn_extra_zero_equity_skipped(pricing):
    """Coins with zero equity in asset-overview are not emitted."""
    collector = BybitCollector(pricing, api_key="key", api_secret="secret")

    async def mock_get(path, **kwargs):
        params = kwargs.get("params") or {}
        acct_type = params.get("accountType", "")
        if acct_type == "UNIFIED":
            return _mock_response({"retCode": 0, "result": {"list": []}})
        if acct_type == "FUND":
            return _mock_response({"retCode": 0, "result": {"balance": []}})
        if "asset-overview" in path:
            return _asset_overview_response(
                [
                    {
                        "category": "Dual Asset",
                        "equity": "0",
                        "coinDetail": [{"coin": "USDT", "equity": "0"}],
                    }
                ]
            )
        return _empty_response()

    collector._client.get = AsyncMock(side_effect=mock_get)

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 0
