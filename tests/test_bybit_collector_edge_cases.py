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
        params = kwargs.get("params", {})
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
