"""Tests for Bitget Wallet collector (Aave V3 Base integration)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import httpx

from pfm.collectors.bitget_wallet import BitgetWalletCollector
from pfm.pricing.coingecko import PricingService


def _mock_response(json_data: object) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 200
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    return resp


def _reserve_data_hex(*, liquidity_rate_ray: int) -> str:
    words = [0] * 12
    words[5] = liquidity_rate_ray
    return "0x" + "".join(f"{word:064x}" for word in words)


def _pricing() -> PricingService:
    pricing = PricingService()
    pricing._set_cache("USDC", Decimal(1))
    pricing.today = lambda: date(2024, 1, 15)  # type: ignore[assignment]
    return pricing


async def test_bitget_wallet_fetch_balances_with_onchain_apy_and_bonus():
    pricing = _pricing()
    collector = BitgetWalletCollector(
        pricing,
        wallet_address="0x771e4E594855e95eE1280940F69D2b0F0C0a1417",
        base_apy_override="",
        bonus_apy="18.8",
    )

    async def mock_post(_url: str, *, json: dict[str, object]) -> MagicMock:
        method = json.get("method")
        params = json.get("params", [])
        if method == "eth_call":
            first = params[0]
            if isinstance(first, dict):
                to = str(first.get("to", "")).lower()
                data = str(first.get("data", "")).lower()
                if to == collector._a_token_address and data.startswith("0x70a08231"):
                    return _mock_response(
                        {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "result": "0x000000000000000000000000000000000000000000000000000000000ee6b280",
                        }
                    )
                if to == collector._pool_data_provider_address and data.startswith("0x35ea6a75"):
                    # 10% APR in ray units.
                    reserve_data = _reserve_data_hex(liquidity_rate_ray=100000000000000000000000000)
                    return _mock_response({"jsonrpc": "2.0", "id": 1, "result": reserve_data})
        raise AssertionError(f"Unexpected RPC call: {json}")

    collector._client.post = AsyncMock(side_effect=mock_post)  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 1
    snap = snapshots[0]
    assert snap.asset == "USDC"
    assert snap.amount == Decimal(250)
    assert snap.usd_value == Decimal(250)
    # APY = on-chain 10% APR -> APY (~10.52%) + bonus 18.8% = > 29%.
    assert snap.apy > Decimal("0.29")


async def test_bitget_wallet_fetch_balances_with_user_override():
    pricing = _pricing()
    collector = BitgetWalletCollector(
        pricing,
        wallet_address="0x771e4E594855e95eE1280940F69D2b0F0C0a1417",
        base_apy_override="10",
        bonus_apy="18.8",
    )

    async def mock_post(_url: str, *, json: dict[str, object]) -> MagicMock:
        method = json.get("method")
        params = json.get("params", [])
        if method == "eth_call":
            first = params[0]
            if isinstance(first, dict) and str(first.get("to", "")).lower() == collector._a_token_address:
                return _mock_response(
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": "0x00000000000000000000000000000000000000000000000000000000000f4240",
                    }
                )
        raise AssertionError(f"Unexpected RPC call: {json}")

    collector._client.post = AsyncMock(side_effect=mock_post)  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 1
    snap = snapshots[0]
    assert snap.amount == Decimal(1)
    assert snap.apy == Decimal("0.288")


async def test_bitget_wallet_fetch_transactions_from_transfer_logs():
    pricing = _pricing()
    collector = BitgetWalletCollector(
        pricing,
        wallet_address="0x771e4E594855e95eE1280940F69D2b0F0C0a1417",
        lookback_blocks="100",
    )

    wallet_topic = "0x000000000000000000000000771e4e594855e95ee1280940f69d2b0f0c0a1417"
    zero_topic = "0x" + ("0" * 64)

    async def mock_post(_url: str, *, json: dict[str, object]) -> MagicMock:
        method = json.get("method")
        params = json.get("params", [])
        if method == "eth_blockNumber":
            return _mock_response({"jsonrpc": "2.0", "id": 1, "result": "0x64"})
        if method == "eth_getLogs":
            first = params[0]
            if isinstance(first, dict):
                topics = first.get("topics")
                if topics == [
                    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                    zero_topic,
                    wallet_topic,
                ]:
                    return _mock_response(
                        {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "result": [
                                {
                                    "blockNumber": "0x60",
                                    "transactionHash": "0xabc",
                                    "data": "0x00000000000000000000000000000000000000000000000000000000000f4240",
                                }
                            ],
                        }
                    )
                if topics == [
                    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                    wallet_topic,
                    zero_topic,
                ]:
                    return _mock_response(
                        {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "result": [
                                {
                                    "blockNumber": "0x61",
                                    "transactionHash": "0xdef",
                                    "data": "0x000000000000000000000000000000000000000000000000000000000007a120",
                                }
                            ],
                        }
                    )
        if method == "eth_getBlockByNumber":
            if params and params[0] == "0x60":
                return _mock_response(
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": {"timestamp": "0x65a4f800"},
                    }
                )
            if params and params[0] == "0x61":
                return _mock_response(
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": {"timestamp": "0x65a64a80"},
                    }
                )
        raise AssertionError(f"Unexpected RPC call: {json}")

    collector._client.post = AsyncMock(side_effect=mock_post)  # type: ignore[assignment]

    txs = await collector.fetch_transactions()
    assert len(txs) == 2
    assert txs[0].amount == Decimal(1)
    assert txs[1].amount == Decimal("0.5")
