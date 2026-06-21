"""Edge-case tests for Bybit collector payload parsing."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from pfm.collectors._auth import sign_bybit
from pfm.collectors.bybit import _MAX_TX_PAGES_PER_WINDOW, _RECV_WINDOW, BybitCollector


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


def _ms_ago(days: int) -> str:
    return str(int((datetime.now(tz=UTC) - timedelta(days=days)).timestamp() * 1000))


def _tx_log_response(rows: list[dict], next_cursor: str = "") -> httpx.Response:
    return _mock_response({"retCode": 0, "result": {"list": rows, "nextPageCursor": next_cursor}})


async def test_bybit_fetch_transactions_paginates_cursor(pricing):
    """A window with a nextPageCursor is paged until the cursor is empty; the
    returned token is decoded once before being sent back (no double-encoding)."""
    collector = BybitCollector(pricing, api_key="key", api_secret="secret")
    calls: list[dict] = []

    async def mock_get(path, **kwargs):
        params = kwargs.get("params") or {}
        if "transaction-log" in path:
            calls.append(params)
            if params.get("cursor"):
                return _tx_log_response(
                    [{"currency": "USDT", "cashFlow": "-5", "transactionTime": _ms_ago(0), "id": "tx2"}]
                )
            return _tx_log_response(
                [{"currency": "USDT", "cashFlow": "10", "transactionTime": _ms_ago(0), "id": "tx1"}],
                next_cursor="cur%3Dabc%2Cdef",
            )
        return _empty_response()

    collector._client.get = AsyncMock(side_effect=mock_get)

    since = datetime.now(tz=UTC).date() - timedelta(days=1)
    txs = await collector.fetch_transactions(since=since)

    assert {t.tx_id for t in txs} == {"tx1", "tx2"}
    # The percent-encoded nextPageCursor is decoded once before the next request.
    assert any(c.get("cursor") == "cur=abc,def" for c in calls)


async def test_bybit_fetch_transactions_walks_windows(pricing):
    """A lookback wider than 7 days is split into multiple <=7-day windows."""
    collector = BybitCollector(pricing, api_key="key", api_secret="secret")
    windows: set[tuple[str, str]] = set()

    async def mock_get(path, **kwargs):
        params = kwargs.get("params") or {}
        if "transaction-log" in path:
            windows.add((params["startTime"], params["endTime"]))
            return _tx_log_response([])
        return _empty_response()

    collector._client.get = AsyncMock(side_effect=mock_get)

    since = datetime.now(tz=UTC).date() - timedelta(days=20)
    await collector.fetch_transactions(since=since)

    # 20 days at a 7-day step spans 3 windows.
    assert len(windows) >= 3


async def test_bybit_fetch_transactions_filters_before_since(pricing):
    """Rows older than `since` are dropped even if the window returns them."""
    collector = BybitCollector(pricing, api_key="key", api_secret="secret")

    async def mock_get(path, **kwargs):
        if "transaction-log" in path:
            return _tx_log_response(
                [
                    {"currency": "USDT", "cashFlow": "1", "transactionTime": _ms_ago(400), "id": "old"},
                    {"currency": "USDT", "cashFlow": "2", "transactionTime": _ms_ago(0), "id": "new"},
                ]
            )
        return _empty_response()

    collector._client.get = AsyncMock(side_effect=mock_get)

    since = datetime.now(tz=UTC).date() - timedelta(days=2)
    txs = await collector.fetch_transactions(since=since)

    ids = {t.tx_id for t in txs}
    assert "new" in ids
    assert "old" not in ids


async def test_bybit_get_signs_exact_wire_query_with_cursor(pricing):
    """The signature is computed over the same percent-encoded query httpx sends,
    so a cursor containing reserved characters does not break the signature."""
    collector = BybitCollector(pricing, api_key="key", api_secret="secret")
    captured: dict = {}

    async def mock_get(path, **kwargs):
        captured["params"] = kwargs.get("params")
        captured["headers"] = kwargs.get("headers")
        return _tx_log_response([])

    collector._client.get = AsyncMock(side_effect=mock_get)

    params = {"limit": "50", "cursor": "cur=abc,def/ghi+jkl"}
    await collector._get("/v5/account/transaction-log", params=params)

    sent_query = str(httpx.QueryParams(captured["params"]))
    headers = captured["headers"]
    expected = sign_bybit(headers["X-BAPI-TIMESTAMP"], "key", _RECV_WINDOW, sent_query, "secret")
    assert headers["X-BAPI-SIGN"] == expected


def test_bybit_parse_synthetic_tx_id_for_missing_id():
    """Rows without Bybit's own id get a deterministic synthetic id so they dedup."""
    item = {"currency": "usdt", "cashFlow": "5.50", "transactionTime": "1700000000000", "type": "TRANSFER_IN"}
    tx_a = BybitCollector._parse_transaction(item)
    tx_b = BybitCollector._parse_transaction(dict(item))
    assert tx_a is not None
    assert tx_b is not None
    assert tx_a.tx_id == "bybit:TRANSFER_IN:USDT:5.5:1700000000000"
    assert tx_a.tx_id == tx_b.tx_id  # deterministic -> INSERT OR IGNORE dedups
    # A different cash flow yields a different id.
    other = BybitCollector._parse_transaction({**item, "cashFlow": "6"})
    assert other is not None
    assert other.tx_id != tx_a.tx_id
    # A present id always wins over the synthetic fallback.
    with_id = BybitCollector._parse_transaction({**item, "id": "real-123"})
    assert with_id is not None
    assert with_id.tx_id == "real-123"


async def test_bybit_fetch_transactions_handles_null_result(pricing):
    """A `result: null` body is tolerated, not crashed on (no AttributeError)."""
    collector = BybitCollector(pricing, api_key="key", api_secret="secret")

    async def mock_get(path, **kwargs):
        if "transaction-log" in path:
            return _mock_response({"retCode": 0, "result": None})
        return _empty_response()

    collector._client.get = AsyncMock(side_effect=mock_get)

    since = datetime.now(tz=UTC).date() - timedelta(days=1)
    assert await collector.fetch_transactions(since=since) == []


async def test_bybit_window_propagates_get_errors(pricing):
    """A window fetch error is no longer swallowed; it propagates to the caller."""
    collector = BybitCollector(pricing, api_key="key", api_secret="secret")

    async def mock_get(path, **kwargs):
        raise ValueError("Bybit API error: signature invalid")

    collector._client.get = AsyncMock(side_effect=mock_get)

    with pytest.raises(ValueError, match="signature invalid"):
        await collector._fetch_transaction_window(datetime(2026, 6, 7, tzinfo=UTC), datetime(2026, 6, 14, tzinfo=UTC))


async def test_bybit_window_raises_on_page_cap(pricing):
    """Exhausting the per-window page cap with a live cursor raises, not truncates."""
    collector = BybitCollector(pricing, api_key="key", api_secret="secret")
    counter = {"n": 0}

    async def mock_get(path, **kwargs):
        counter["n"] += 1
        # Always one row plus a fresh, never-before-seen cursor -> never terminates.
        return _tx_log_response(
            [{"currency": "USDT", "cashFlow": "1", "transactionTime": _ms_ago(0), "id": f"tx{counter['n']}"}],
            next_cursor=f"cursor-{counter['n']}",
        )

    collector._client.get = AsyncMock(side_effect=mock_get)

    with pytest.raises(ValueError, match=r"exceeded \d+ pages"):
        await collector._fetch_transaction_window(datetime(2026, 6, 7, tzinfo=UTC), datetime(2026, 6, 14, tzinfo=UTC))
    assert counter["n"] == _MAX_TX_PAGES_PER_WINDOW
