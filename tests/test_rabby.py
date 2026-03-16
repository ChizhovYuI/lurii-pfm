"""Tests for Rabby collector via DeBank OpenAPI."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import httpx
import pytest

from pfm.collectors.rabby import (
    RabbyCollector,
    _extract_token_symbols,
    _extract_tx_id,
    _format_debank_auth_error,
    _parse_token_flows,
    _parse_unix_date,
    _to_decimal,
)


def _mock_response(payload: object, status_code: int = 200) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
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
        if path == "/v1/user/token_list":
            return _mock_response(
                [
                    {"symbol": "ETH", "amount": 1.25, "price": 3000},
                    {"symbol": "ARB", "amount": "100", "price": 0},
                    {"symbol": "USDC", "amount": "0", "price": 1},
                    {"symbol": "", "amount": "5", "price": 1},
                ]
            )
        if path == "/v1/user/history_list":
            return _mock_response(
                {
                    "history_list": [
                        {
                            "cate_id": "receive",
                            "time_at": 1718400000,
                            "tx": {"id": "tx-dep"},
                            "receives": [{"token_id": "eth", "amount": "0.1"}],
                            "sends": [],
                        },
                        {
                            "cate_id": "send",
                            "time_at": 1718313600,
                            "tx": {"id": "tx-wd"},
                            "receives": [],
                            "sends": [{"token_id": "usdc", "amount": "25"}],
                        },
                        {
                            "cate_id": "swap",
                            "time_at": 1718227200,
                            "tx": {"id": "tx-trade"},
                            "receives": [{"token_id": "eth", "amount": "0.01"}],
                            "sends": [{"token_id": "usdc", "amount": "35"}],
                        },
                    ],
                    "token_dict": {
                        "eth": {"symbol": "ETH"},
                        "usdc": {"symbol": "USDC"},
                    },
                }
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


async def test_rabby_skips_scam_transactions(pricing):
    collector = RabbyCollector(
        pricing,  # type: ignore[arg-type]
        wallet_address="0xabc",
        access_key="key",
    )

    async def mock_get(path: str, **kwargs: object) -> MagicMock:
        if path == "/v1/user/history_list":
            return _mock_response(
                {
                    "history_list": [
                        {
                            "cate_id": "receive",
                            "time_at": 1718400000,
                            "is_scam": False,
                            "tx": {"id": "tx-legit"},
                            "receives": [{"symbol": "USDC", "amount": "100"}],
                            "sends": [],
                        },
                        {
                            "cate_id": None,
                            "time_at": 1718400000,
                            "is_scam": True,
                            "tx": {"id": "tx-scam"},
                            "receives": [{"symbol": "WWW.2BASE.CFD", "amount": "999"}],
                            "sends": [],
                        },
                    ],
                    "token_dict": {},
                }
            )
        return _mock_response([])

    collector._client.get = mock_get  # type: ignore[assignment]

    txs = await collector.fetch_transactions()
    assert len(txs) == 1
    assert txs[0].tx_id == "tx-legit"


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
    assert _parse_token_flows([{"token_id": "eth", "amount": "2"}], {"eth": "ETH"}) == [("ETH", Decimal(2))]

    assert _extract_tx_id({"tx": {"id": "0xid"}}) == "0xid"
    assert _extract_tx_id({"tx": {"hash": "0xhash"}}) == "0xhash"
    assert _extract_tx_id({"id": "fallback"}) == "fallback"
    assert _extract_tx_id({}) == ""

    assert isinstance(_parse_unix_date("not-a-ts"), date)
    assert _to_decimal("bad") == Decimal(0)
    assert _extract_token_symbols({"eth": {"symbol": "ETH"}, "bad": "x"}) == {"eth": "ETH"}


def test_rabby_format_debank_auth_errors():
    unauthorized = httpx.Response(
        401,
        json={"message": "You are not authorized to access the URL"},
        request=httpx.Request("GET", "https://pro-openapi.debank.com/v1/user/all_token_list"),
    )
    forbidden_quota = httpx.Response(
        403,
        json={"message": "Requests are limited, because of insufficient units"},
        request=httpx.Request("GET", "https://pro-openapi.debank.com/v1/user/all_token_list"),
    )
    forbidden_generic = httpx.Response(
        403,
        json={"message": "forbidden"},
        request=httpx.Request("GET", "https://pro-openapi.debank.com/v1/user/all_token_list"),
    )

    assert "unauthorized (401)" in _format_debank_auth_error(unauthorized)
    assert "insufficient units" in _format_debank_auth_error(forbidden_quota)
    assert "forbidden (403): forbidden" in _format_debank_auth_error(forbidden_generic)


async def test_rabby_get_converts_403_to_readable_error(pricing):
    collector = RabbyCollector(
        pricing,  # type: ignore[arg-type]
        wallet_address="0xabc",
        access_key="key",
    )

    async def mock_get(path: str, **kwargs: object) -> httpx.Response:
        return httpx.Response(
            403,
            json={"message": "Requests are limited, because of insufficient units"},
            request=httpx.Request("GET", f"https://api.rabby.io{path}"),
        )

    collector._client.get = mock_get  # type: ignore[assignment]

    with pytest.raises(ValueError, match="insufficient units"):
        await collector._get("/v1/user/token_list", {"id": "0xabc", "is_all": "false"})


async def test_rabby_get_converts_429_to_readable_error(pricing):
    collector = RabbyCollector(
        pricing,  # type: ignore[arg-type]
        wallet_address="0xabc",
        access_key="key",
    )

    async def mock_get(path: str, **kwargs: object) -> httpx.Response:
        return httpx.Response(
            429,
            json={"error_msg": "Request too fast"},
            request=httpx.Request("GET", f"https://api.rabby.io{path}"),
        )

    collector._client.get = mock_get  # type: ignore[assignment]

    with pytest.raises(ValueError, match="rate limit reached"):
        await collector._get("/v1/user/token_list", {"id": "0xabc", "is_all": "false"})
