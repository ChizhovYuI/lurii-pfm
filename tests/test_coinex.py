"""Tests for CoinEx collector."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from pfm.collectors._auth import sign_coinex
from pfm.collectors.coinex import (
    _FINANCIAL_BALANCE_PATH,
    _FUTURES_BALANCE_PATH,
    _PUBLIC_INVEST_SUMMARY_URL,
    _SPOT_BALANCE_PATH,
    _SPOT_HISTORY_PATH,
    CoinexCollector,
    _effective_public_apy,
    _parse_history_transaction,
)
from pfm.db.models import TransactionType


def _mock_response(payload: object, status_code: int = 200) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = payload
    resp.raise_for_status = MagicMock()
    return resp


async def test_coinex_signed_headers_include_query_string(monkeypatch, pricing):
    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls.fromtimestamp(1700490703.564, tz=tz or UTC)

    monkeypatch.setattr("pfm.collectors.coinex.datetime", _FixedDatetime)

    collector = CoinexCollector(pricing, api_key="key-1", api_secret="secret-1")
    collector._client.get = AsyncMock(return_value=_mock_response({"code": 0, "data": [], "message": "OK"}))

    params = {"type": "trade", "page": "1", "limit": "10"}
    await collector._get(_SPOT_HISTORY_PATH, params=params)
    kwargs = collector._client.get.await_args.kwargs
    headers = kwargs["headers"]

    assert headers["X-COINEX-KEY"] == "key-1"
    timestamp = "1700490703564"
    expected = sign_coinex(
        "GET",
        "/v2/assets/spot/transcation-history?type=trade&page=1&limit=10",
        "",
        timestamp,
        "secret-1",
    )
    assert headers["X-COINEX-TIMESTAMP"] == timestamp
    assert headers["X-COINEX-SIGN"] == expected


async def test_fetch_raw_balances_uses_public_summary_for_financial_apy(pricing):
    collector = CoinexCollector(pricing, api_key="k", api_secret="s")

    async def fake_get(path: str, params=None):
        if path == _SPOT_BALANCE_PATH:
            return {
                "code": 0,
                "data": [
                    {"ccy": "ETH", "available": "2", "frozen": "0"},
                ],
                "message": "OK",
            }
        if path == _FUTURES_BALANCE_PATH:
            return {"code": 0, "data": None, "message": "OK"}
        if path == _FINANCIAL_BALANCE_PATH:
            return {
                "code": 0,
                "data": [
                    {"ccy": "USDC", "available": "500", "frozen": "0"},
                    {"ccy": "USDT", "available": "2000", "frozen": "0"},
                ],
                "message": "OK",
            }
        raise AssertionError(f"Unexpected path={path} params={params}")

    collector._get = fake_get  # type: ignore[method-assign]
    collector._get_public_invest_summary = AsyncMock(  # type: ignore[method-assign]
        return_value=[
            {"asset": "USDC", "rate": "0.05", "ladder_rule": {"rate": "0.1", "limit": "1000"}},
            {"asset": "USDT", "rate": "0.04", "ladder_rule": {"rate": "0.1", "limit": "1000"}},
        ]
    )
    raw = await collector.fetch_raw_balances()

    financial_rows = {}
    for row in raw:
        if '"account_type": "financial"' in row.raw_json:
            financial_rows[row.asset] = row

    assert financial_rows["USDC"].amount == Decimal(500)
    assert financial_rows["USDT"].amount == Decimal(2000)
    assert financial_rows["USDC"].apy == Decimal("0.15")
    assert financial_rows["USDT"].apy == Decimal("0.09")


async def test_fetch_raw_balances_public_summary_missing_asset_falls_back_to_interest(pricing):
    collector = CoinexCollector(pricing, api_key="k", api_secret="s")

    async def fake_get(path: str, params=None):
        if path == _SPOT_BALANCE_PATH:
            return {"code": 0, "data": [], "message": "OK"}
        if path == _FUTURES_BALANCE_PATH:
            return {"code": 0, "data": None, "message": "OK"}
        if path == _FINANCIAL_BALANCE_PATH:
            return {
                "code": 0,
                "data": [
                    {"ccy": "USDC", "available": "100", "frozen": "0"},
                    {"ccy": "USDT", "available": "200", "frozen": "0"},
                ],
                "message": "OK",
            }
        if path == _SPOT_HISTORY_PATH and params and params.get("type") == "investment_interest":
            return {
                "code": 0,
                "data": [
                    {"type": "investment_interest", "ccy": "USDT", "change": "0.2", "created_at": 1700000000000},
                ],
                "pagination": {"has_next": False},
                "message": "OK",
            }
        raise AssertionError(f"Unexpected path={path} params={params}")

    collector._get = fake_get  # type: ignore[method-assign]
    collector._get_public_invest_summary = AsyncMock(  # type: ignore[method-assign]
        return_value=[{"asset": "USDC", "rate": "0.05", "ladder_rule": None}]
    )
    raw = await collector.fetch_raw_balances()

    financial_rows = {}
    for row in raw:
        if '"account_type": "financial"' in row.raw_json:
            financial_rows[row.asset] = row

    assert financial_rows["USDC"].apy == Decimal("0.05")
    assert financial_rows["USDT"].apy == Decimal("0.365")


async def test_fetch_raw_balances_public_summary_failure_falls_back_to_interest(pricing):
    collector = CoinexCollector(pricing, api_key="k", api_secret="s")

    async def fake_get(path: str, params=None):
        if path == _SPOT_BALANCE_PATH:
            return {"code": 0, "data": [], "message": "OK"}
        if path == _FUTURES_BALANCE_PATH:
            return {"code": 0, "data": None, "message": "OK"}
        if path == _FINANCIAL_BALANCE_PATH:
            return {"code": 0, "data": [{"ccy": "USDC", "available": "100", "frozen": "0"}], "message": "OK"}
        if path == _SPOT_HISTORY_PATH and params and params.get("type") == "investment_interest":
            return {
                "code": 0,
                "data": [
                    {"type": "investment_interest", "ccy": "USDC", "change": "0.5", "created_at": 1700000000000},
                ],
                "pagination": {"has_next": False},
                "message": "OK",
            }
        raise AssertionError(f"Unexpected path={path} params={params}")

    collector._get = fake_get  # type: ignore[method-assign]
    collector._get_public_invest_summary = AsyncMock(side_effect=httpx.ConnectError("connection failed"))  # type: ignore[method-assign]
    raw = await collector.fetch_raw_balances()

    financial = next(row for row in raw if row.asset == "USDC")
    assert financial.apy == Decimal("1.825")


def test_public_summary_effective_apy_ladder_and_min_amount_handling():
    row = {
        "asset": "USDT",
        "rate": "0.04151901",
        "min_amount": "10",  # ignored by design
        "ladder_rule": {"rate": "0.1", "limit": "1000"},
    }

    assert _effective_public_apy(row, amount=Decimal(5)) == Decimal("0.14151901")
    assert _effective_public_apy(row, amount=Decimal(1000)) == Decimal("0.14151901")
    assert _effective_public_apy(row, amount=Decimal(2000)) == Decimal("0.09151901")


async def test_get_public_invest_summary_parses_payload(pricing):
    collector = CoinexCollector(pricing, api_key="k", api_secret="s")
    response = _mock_response(
        {
            "code": 0,
            "data": [{"asset": "USDT", "rate": "0.04", "ladder_rule": {"rate": "0.1", "limit": "1000"}}],
            "message": "OK",
        }
    )
    collector._public_client.get = AsyncMock(return_value=response)

    rows = await collector._get_public_invest_summary()

    assert rows == [{"asset": "USDT", "rate": "0.04", "ladder_rule": {"rate": "0.1", "limit": "1000"}}]
    collector._public_client.get.assert_awaited_once_with(_PUBLIC_INVEST_SUMMARY_URL)


async def test_get_appends_current_public_ip_for_ip_prohibited_error(pricing):
    collector = CoinexCollector(pricing, api_key="k", api_secret="s")
    collector._client.get = AsyncMock(
        return_value=_mock_response({"code": 23, "data": None, "message": "IP Prohibited"})
    )

    ip_response = MagicMock(spec=httpx.Response)
    ip_response.raise_for_status = MagicMock()
    ip_response.text = "203.0.113.42"
    collector._public_client.get = AsyncMock(return_value=ip_response)

    with pytest.raises(ValueError, match=r"current public IP: 203\.0\.113\.42") as exc_info:
        await collector._get(_SPOT_BALANCE_PATH)
    assert str(exc_info.value) == (
        "CoinEx API error (23) on /v2/assets/spot/balance: IP Prohibited " "(current public IP: 203.0.113.42)"
    )


async def test_fetch_history_rows_handles_null_data(pricing):
    collector = CoinexCollector(pricing, api_key="k", api_secret="s")

    async def fake_get(path: str, params=None):
        assert path == _SPOT_HISTORY_PATH
        return {"code": 0, "data": None, "pagination": {"has_next": False}, "message": "OK"}

    collector._get = fake_get  # type: ignore[method-assign]
    rows = await collector._fetch_history_rows("deposit")
    assert rows == []


async def test_fetch_history_rows_paginates_with_has_next(pricing):
    collector = CoinexCollector(pricing, api_key="k", api_secret="s")
    seen_pages: list[str] = []

    async def fake_get(path: str, params=None):
        assert path == _SPOT_HISTORY_PATH
        seen_pages.append(str(params["page"]))
        if params["page"] == "1":
            return {
                "code": 0,
                "data": [{"type": "trade", "ccy": "USDT", "change": "-1", "created_at": 1700000000000}],
                "pagination": {"has_next": True},
                "message": "OK",
            }
        return {
            "code": 0,
            "data": [{"type": "trade", "ccy": "USDT", "change": "-2", "created_at": 1700000001000}],
            "pagination": {"has_next": False},
            "message": "OK",
        }

    collector._get = fake_get  # type: ignore[method-assign]
    rows = await collector._fetch_history_rows("trade")
    assert len(rows) == 2
    assert seen_pages == ["1", "2"]


def test_parse_history_transaction_uses_synthetic_tx_id_without_id():
    row = {
        "type": "trade",
        "ccy": "USDT",
        "change": "-0.500000",
        "created_at": 1773152771540,
    }
    tx = _parse_history_transaction(row)
    assert tx is not None
    assert tx.tx_type == TransactionType.UNKNOWN
    assert tx.amount == Decimal("0.5")
    assert tx.date == date(2026, 3, 10)
    assert tx.tx_id == "coinex:trade:USDT:-0.5:1773152771540"


async def test_fetch_transactions_maps_types_and_filters_since(pricing):
    collector = CoinexCollector(pricing, api_key="k", api_secret="s")

    ts_old = int(datetime(2026, 3, 9, 0, 0, tzinfo=UTC).timestamp() * 1000)
    ts_new = int(datetime(2026, 3, 10, 0, 0, tzinfo=UTC).timestamp() * 1000)

    payloads = {
        "deposit": [{"type": "deposit", "ccy": "USDC", "change": "5", "created_at": ts_new, "id": "dep-1"}],
        "withdraw": [{"type": "withdraw", "ccy": "USDC", "change": "-2", "created_at": ts_new, "id": "wd-1"}],
        "trade": [{"type": "trade", "ccy": "USDT", "change": "-1", "created_at": ts_old}],
        "maker_cash_back": [{"type": "maker_cash_back", "ccy": "USDT", "change": "0.1", "created_at": ts_new}],
        "investment_interest": [{"type": "investment_interest", "ccy": "USDC", "change": "0.2", "created_at": ts_new}],
        "exchange_order_transfer": [
            {"type": "exchange_order_transfer", "ccy": "USDC", "change": "-3", "created_at": ts_new},
        ],
    }

    async def fake_get(path: str, params=None):
        assert path == _SPOT_HISTORY_PATH
        tx_type = str(params["type"])
        return {"code": 0, "data": payloads[tx_type], "pagination": {"has_next": False}, "message": "OK"}

    collector._get = fake_get  # type: ignore[method-assign]
    txs = await collector.fetch_transactions(since=date(2026, 3, 10))

    # one old trade row is filtered out by since
    assert len(txs) == 5
    # All types are UNKNOWN now (type resolution happens post-import)
    assert all(tx.tx_type == TransactionType.UNKNOWN for tx in txs)
