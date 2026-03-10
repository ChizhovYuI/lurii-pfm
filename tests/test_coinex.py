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
    _SPOT_BALANCE_PATH,
    _SPOT_HISTORY_PATH,
    CoinexCollector,
    _map_history_type,
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


async def test_fetch_raw_balances_parses_rows_and_estimates_financial_apy(pricing):
    collector = CoinexCollector(pricing, api_key="k", api_secret="s")

    async def fake_get(path: str, params=None):
        if path == _SPOT_BALANCE_PATH:
            return {
                "code": 0,
                "data": [
                    {"ccy": "USDT", "available": "10", "frozen": "1"},
                    {"ccy": "BTC", "available": "0", "frozen": "0"},
                ],
                "message": "OK",
            }
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
    raw = await collector.fetch_raw_balances()

    by_asset = {row.asset: row for row in raw}
    assert set(by_asset) == {"USDT", "USDC"}
    assert by_asset["USDT"].amount == Decimal(11)
    assert by_asset["USDT"].apy == Decimal(0)
    assert by_asset["USDC"].amount == Decimal(100)
    assert by_asset["USDC"].apy == Decimal("1.825")


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


@pytest.mark.parametrize(
    ("raw_type", "expected"),
    [
        ("deposit", TransactionType.DEPOSIT),
        ("withdraw", TransactionType.WITHDRAWAL),
        ("trade", TransactionType.TRADE),
        ("investment_interest", TransactionType.INTEREST),
        ("maker_cash_back", TransactionType.TRANSFER),
        ("exchange_order_transfer", TransactionType.TRANSFER),
    ],
)
def test_map_history_type(raw_type: str, expected: TransactionType):
    assert _map_history_type(raw_type) == expected


def test_parse_history_transaction_uses_synthetic_tx_id_without_id():
    row = {
        "type": "trade",
        "ccy": "USDT",
        "change": "-0.500000",
        "created_at": 1773152771540,
    }
    tx = _parse_history_transaction(row)
    assert tx is not None
    assert tx.tx_type == TransactionType.TRADE
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
    by_type = {tx.tx_type for tx in txs}
    assert by_type == {
        TransactionType.DEPOSIT,
        TransactionType.WITHDRAWAL,
        TransactionType.INTEREST,
        TransactionType.TRANSFER,
    }
    transfer_count = sum(1 for tx in txs if tx.tx_type == TransactionType.TRANSFER)
    assert transfer_count == 2
