"""Tests for yo.xyz collector."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import httpx

from pfm.collectors.yo import (
    YoCollector,
    _extract_holdings,
    _first_symbol_amount,
    _parse_history_row,
    _parse_timestamp,
    _read_amount,
    _to_decimal,
)


def _mock_response(payload: object) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.json.return_value = payload
    resp.raise_for_status = MagicMock()
    return resp


async def test_yo_collect_balances_and_transactions(repo, pricing):
    pricing._set_cache("WETH", Decimal(3000))
    pricing.today = lambda: date(2024, 6, 15)  # type: ignore[assignment]

    collector = YoCollector(
        pricing,  # type: ignore[arg-type]
        network="base",
        vault_address="0xvault",
        user_address="0xuser",
    )

    async def mock_get(path: str, **kwargs: object) -> MagicMock:
        if path == "/api/v1/vault/base/0xvault":
            return _mock_response(
                {
                    "statusCode": 200,
                    "message": "SUCCESS",
                    "data": {
                        "asset": {"symbol": "WETH"},
                        "shareAsset": {"symbol": "yoETH"},
                        "stats": {"sharePrice": {"formatted": "1.05"}},
                        "inputTokenBalance": {"formatted": "2.5"},
                    },
                }
            )
        if path == "/api/v1/history/user/base/0xvault/0xuser":
            return _mock_response(
                {
                    "statusCode": 200,
                    "message": "SUCCESS",
                    "data": [
                        {
                            "type": "deposit",
                            "timestamp": 1718400000,
                            "transactionHash": "0xdep",
                            "assets": [{"symbol": "WETH", "formatted": "1.0"}],
                            "shares": [{"symbol": "yoETH", "formatted": "1.0"}],
                        },
                        {
                            "type": "redeem",
                            "timestamp": 1718313600,
                            "transactionHash": "0xred",
                            "assets": [{"symbol": "WETH", "formatted": "0.25"}],
                            "shares": [{"symbol": "yoETH", "formatted": "0.25"}],
                        },
                    ],
                }
            )
        return _mock_response({})

    collector._client.get = mock_get  # type: ignore[assignment]

    result = await collector.collect(repo)
    assert result.snapshots_count == 1
    assert result.transactions_count == 2
    assert result.errors == []

    snapshots = await repo.get_latest_snapshots()
    assert len(snapshots) == 1
    assert snapshots[0].asset == "WETH"
    assert snapshots[0].amount == Decimal("2.5")
    assert snapshots[0].usd_value == Decimal("7500.0")

    txs = await repo.get_transactions(source="yo")
    assert len(txs) == 2
    assert {tx.tx_type.value for tx in txs} == {"deposit", "withdrawal"}


async def test_yo_returns_empty_balances_for_missing_user_position(pricing):
    collector = YoCollector(
        pricing,  # type: ignore[arg-type]
        network="base",
        vault_address="0xvault",
        user_address="0xuser",
    )

    async def mock_get(path: str, **kwargs: object) -> MagicMock:
        if path == "/api/v1/vault/base/0xvault":
            return _mock_response(
                {
                    "statusCode": 200,
                    "message": "SUCCESS",
                    "data": {"asset": {"symbol": "WETH"}, "stats": {}},
                }
            )
        return _mock_response({"statusCode": 200, "data": []})

    collector._client.get = mock_get  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    assert snapshots == []


async def test_yo_history_since_filter(pricing):
    collector = YoCollector(
        pricing,  # type: ignore[arg-type]
        network="base",
        vault_address="0xvault",
        user_address="0xuser",
    )

    async def mock_get(path: str, **kwargs: object) -> MagicMock:
        if path == "/api/v1/history/user/base/0xvault/0xuser":
            return _mock_response(
                {
                    "statusCode": 200,
                    "data": [
                        {
                            "type": "deposit",
                            "timestamp": 1718400000,  # 2024-06-15 UTC
                            "transactionHash": "0xnew",
                            "assets": [{"symbol": "WETH", "formatted": "1"}],
                            "shares": [{"symbol": "yoETH", "formatted": "1"}],
                        },
                        {
                            "type": "redeem",
                            "timestamp": 1717459200,  # 2024-06-04 UTC
                            "transactionHash": "0xold",
                            "assets": [{"symbol": "WETH", "formatted": "0.5"}],
                            "shares": [{"symbol": "yoETH", "formatted": "0.5"}],
                        },
                    ],
                }
            )
        return _mock_response({"statusCode": 200, "data": []})

    collector._client.get = mock_get  # type: ignore[assignment]

    txs = await collector.fetch_transactions(since=date(2024, 6, 10))
    assert len(txs) == 1
    assert txs[0].tx_id == "0xnew"


async def test_yo_get_payload_shapes(pricing):
    collector = YoCollector(
        pricing,  # type: ignore[arg-type]
        network="base",
        vault_address="0xvault",
        user_address="0xuser",
    )

    async def mock_get(path: str, **kwargs: object) -> MagicMock:
        if path == "/api/v1/vault/base/0xvault":
            return _mock_response({"statusCode": 200, "data": []})
        if path == "/api/v1/history/user/base/0xvault/0xuser":
            return _mock_response({"statusCode": 200, "data": [1, {"ok": True}, "x"]})
        return _mock_response(["not-dict"])

    collector._client.get = mock_get  # type: ignore[assignment]

    assert await collector._get("/x") == {}
    assert await collector._get_vault() == {}
    assert await collector._get_history() == [{"ok": True}]


def test_yo_helper_paths_and_parsers():
    holdings = _extract_holdings(
        {
            "asset": {"symbol": "WETH"},
            "shareAsset": {"symbol": "yoETH"},
            "outputTokenBalances": [
                {"symbol": "WETH", "formatted": "0.2", "priceUsd": "3000"},
                {"token": {"symbol": "USDC"}, "amount": "50", "price": "1"},
                {"symbol": "", "formatted": "1"},
            ],
            "shareBalance": {"formatted": "1.0"},
        }
    )
    assert [h.symbol for h in holdings] == ["WETH", "USDC", "YOETH"]

    fallback_holdings = _extract_holdings({"asset": {"symbol": "ETH"}, "balance": {"formatted": "2"}})
    assert len(fallback_holdings) == 1
    assert fallback_holdings[0].symbol == "ETH"

    dep = _parse_history_row(
        {
            "type": "deposit",
            "timestamp": 1718400000,
            "transactionHash": "0x1",
            "assets": [{"symbol": "WETH", "formatted": "1"}],
            "shares": [{"symbol": "yoETH", "formatted": "1"}],
        }
    )
    assert dep is not None
    assert dep.tx_type.value == "deposit"

    claim = _parse_history_row(
        {
            "type": "claim",
            "timestamp": 1718400000,
            "transactionHash": "0x2",
            "assets": [{"symbol": "WETH", "formatted": "0.1"}],
            "shares": [],
        }
    )
    assert claim is not None
    assert claim.tx_type.value == "yield"

    transfer = _parse_history_row(
        {
            "type": "unknown",
            "timestamp": 1718400000,
            "transactionHash": "0x3",
            "assets": [{"symbol": "WETH", "formatted": "0.1"}],
            "shares": [],
        }
    )
    assert transfer is not None
    assert transfer.tx_type.value == "transfer"

    assert _parse_history_row({"type": "deposit", "assets": [], "shares": []}) is None
    assert _first_symbol_amount("bad") == ("", Decimal(0))
    assert _first_symbol_amount([{"symbol": "WETH", "formatted": "0.5"}]) == ("WETH", Decimal("0.5"))
    assert isinstance(_parse_timestamp("bad"), date)
    assert _read_amount({}) == Decimal(0)
    assert _read_amount({"raw": "12"}) == Decimal(12)
    assert _to_decimal("bad") == Decimal(0)
