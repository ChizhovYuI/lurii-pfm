"""Tests for Rabby collector via DeBank OpenAPI."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import httpx

from pfm.collectors.rabby import (
    RabbyCollector,
    _extract_tx_id,
    _parse_token_flows,
    _parse_unix_date,
    _to_decimal,
)


def _mock_response(payload: object) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.json.return_value = payload
    resp.raise_for_status = MagicMock()
    return resp


async def test_rabby_collect_balances_and_transactions(repo, pricing):
    pricing._set_cache("ARB", Decimal("0.75"))
    pricing.today = lambda: date(2024, 6, 15)  # type: ignore[assignment]

    collector = RabbyCollector(
        pricing,  # type: ignore[arg-type]
        wallet_address="0xabc",
        access_key="key",
    )

    async def mock_get(path: str, **kwargs: object) -> MagicMock:
        if path == "/v1/user/all_token_list":
            return _mock_response(
                [
                    {"symbol": "ETH", "amount": 1.25, "price": 3000},
                    {"symbol": "ARB", "amount": "100", "price": 0},
                    {"symbol": "USDC", "amount": "0", "price": 1},
                    {"symbol": "", "amount": "5", "price": 1},
                ]
            )
        if path == "/v1/user/all_history_list":
            return _mock_response(
                [
                    {
                        "cate_id": "receive",
                        "time_at": 1718400000,
                        "tx": {"id": "tx-dep"},
                        "receives": [{"symbol": "ETH", "amount": "0.1"}],
                        "sends": [],
                    },
                    {
                        "cate_id": "send",
                        "time_at": 1718313600,
                        "tx": {"id": "tx-wd"},
                        "receives": [],
                        "sends": [{"symbol": "USDC", "amount": "25"}],
                    },
                    {
                        "cate_id": "swap",
                        "time_at": 1718227200,
                        "tx": {"id": "tx-trade"},
                        "receives": [{"symbol": "ETH", "amount": "0.01"}],
                        "sends": [{"symbol": "USDC", "amount": "35"}],
                    },
                ]
            )
        return _mock_response([])

    collector._client.get = mock_get  # type: ignore[assignment]

    result = await collector.collect(repo)
    assert result.snapshots_count == 2
    assert result.transactions_count == 3
    assert result.errors == []

    snapshots = await repo.get_latest_snapshots()
    assert len(snapshots) == 2
    assert any(s.asset == "ETH" and s.usd_value == Decimal("3750.00") for s in snapshots)
    assert any(s.asset == "ARB" and s.usd_value == Decimal("75.00") for s in snapshots)

    txs = await repo.get_transactions(source="rabby")
    assert len(txs) == 3
    assert {tx.tx_type.value for tx in txs} == {"deposit", "withdrawal", "trade"}


async def test_rabby_handles_non_list_payloads(pricing):
    collector = RabbyCollector(
        pricing,  # type: ignore[arg-type]
        wallet_address="0xabc",
        access_key="key",
    )

    async def mock_get(path: str, **kwargs: object) -> MagicMock:
        return _mock_response({"unexpected": True})

    collector._client.get = mock_get  # type: ignore[assignment]

    assert await collector.fetch_balances() == []
    assert await collector.fetch_transactions() == []


def test_rabby_helper_parsers_cover_edge_cases():
    assert _parse_token_flows("bad") == []
    assert _parse_token_flows([{"symbol": "", "amount": "1"}]) == []
    assert _parse_token_flows([{"symbol": "ETH", "amount": "0"}]) == []
    assert _parse_token_flows([{"symbol": "ETH", "amount": "1.5"}]) == [("ETH", Decimal("1.5"))]

    assert _extract_tx_id({"tx": {"id": "0xid"}}) == "0xid"
    assert _extract_tx_id({"tx": {"hash": "0xhash"}}) == "0xhash"
    assert _extract_tx_id({"id": "fallback"}) == "fallback"
    assert _extract_tx_id({}) == ""

    assert isinstance(_parse_unix_date("not-a-ts"), date)
    assert _to_decimal("bad") == Decimal(0)
