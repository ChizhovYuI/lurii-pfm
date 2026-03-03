"""Tests for Bitget Wallet collector (Aave API integration)."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import httpx

from pfm.collectors.bitget_wallet import BitgetWalletCollector
from pfm.db.models import TransactionType
from pfm.pricing.coingecko import PricingService


def _mock_response(json_data: object) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 200
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    return resp


def _pricing() -> PricingService:
    pricing = PricingService()
    pricing._set_cache("USDC", Decimal(1))
    pricing.today = lambda: date(2024, 1, 15)  # type: ignore[assignment]
    return pricing


async def test_bitget_wallet_fetch_balances_from_aave_api():
    pricing = _pricing()
    collector = BitgetWalletCollector(
        pricing,
        wallet_address="0x771e4E594855e95eE1280940F69D2b0F0C0a1417",
    )

    async def mock_post(_url: str, *, json: dict[str, object]) -> MagicMock:
        query = str(json.get("query", ""))
        variables = json.get("variables", {})
        if "markets(request" in query:
            return _mock_response(
                {
                    "data": {
                        "value": [
                            {
                                "address": "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5",
                                "name": "AaveV3Base",
                                "chain": {"chainId": 8453, "name": "Base"},
                            }
                        ]
                    }
                }
            )
        if "userSupplies(request" in query:
            assert isinstance(variables, dict)
            request = variables.get("request")
            assert isinstance(request, dict)
            assert request.get("user") == collector._wallet_address
            return _mock_response(
                {
                    "data": {
                        "value": [
                            {
                                "market": {
                                    "address": "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5",
                                    "name": "AaveV3Base",
                                    "chain": {"chainId": 8453, "name": "Base"},
                                },
                                "currency": {
                                    "symbol": "USDC",
                                    "address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                                    "decimals": 6,
                                    "chainId": 8453,
                                },
                                "balance": {"amount": {"value": "250"}, "usd": "250"},
                                "apy": {"value": "0.031", "formatted": "3.10"},
                            },
                            {
                                "market": {
                                    "address": "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5",
                                    "name": "AaveV3Base",
                                    "chain": {"chainId": 8453, "name": "Base"},
                                },
                                "currency": {
                                    "symbol": "USDC",
                                    "address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                                    "decimals": 6,
                                    "chainId": 8453,
                                },
                                "balance": {"amount": {"value": "0"}, "usd": "0"},
                                "apy": {"value": "0.01", "formatted": "1.00"},
                            },
                        ]
                    }
                }
            )
        raise AssertionError(f"Unexpected RPC call: {json}")

    collector._client.post = AsyncMock(side_effect=mock_post)  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 1
    snap = snapshots[0]
    assert snap.asset == "USDC"
    assert snap.amount == Decimal(250)
    assert snap.usd_value == Decimal(250)
    assert snap.apy == Decimal("0.031")
    assert snap.price == Decimal(1)
    raw = json.loads(snap.raw_json)
    assert raw["wallet_address"] == collector._wallet_address


async def test_bitget_wallet_fetch_transactions_from_aave_api():
    pricing = _pricing()
    collector = BitgetWalletCollector(
        pricing,
        wallet_address="0x771e4E594855e95eE1280940F69D2b0F0C0a1417",
        bonus_apy="18.8",
        lookback_blocks="100",
    )

    async def mock_post(_url: str, *, json: dict[str, object]) -> MagicMock:
        query = str(json.get("query", ""))
        variables = json.get("variables", {})
        if "markets(request" in query:
            return _mock_response(
                {
                    "data": {
                        "value": [
                            {
                                "address": "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5",
                                "name": "AaveV3Base",
                                "chain": {"chainId": 8453, "name": "Base"},
                            }
                        ]
                    }
                }
            )
        if "userTransactionHistory(request" in query:
            assert isinstance(variables, dict)
            request = variables.get("request")
            assert isinstance(request, dict)
            cursor = request.get("cursor")
            if not cursor:
                return _mock_response(
                    {
                        "data": {
                            "value": {
                                "items": [
                                    {
                                        "__typename": "UserSupplyTransaction",
                                        "txHash": "0xabc",
                                        "timestamp": "2024-01-15T00:00:00+00:00",
                                        "amount": {"amount": {"value": "1"}, "usd": "1"},
                                        "reserve": {"underlyingToken": {"symbol": "USDC"}},
                                    },
                                    {
                                        "__typename": "UserWithdrawTransaction",
                                        "txHash": "0xdef",
                                        "timestamp": "2024-01-16T00:00:00+00:00",
                                        "amount": {"amount": {"value": "0.5"}, "usd": "0.5"},
                                        "reserve": {"underlyingToken": {"symbol": "USDC"}},
                                    },
                                ],
                                "pageInfo": {"next": "cursor-2"},
                            }
                        }
                    }
                )
            return _mock_response(
                {
                    "data": {
                        "value": {
                            "items": [
                                {
                                    "__typename": "UserSupplyTransaction",
                                    "txHash": "0xold",
                                    "timestamp": "2023-12-01T00:00:00+00:00",
                                    "amount": {"amount": {"value": "7"}, "usd": "7"},
                                    "reserve": {"underlyingToken": {"symbol": "USDC"}},
                                }
                            ],
                            "pageInfo": {"next": None},
                        }
                    }
                }
            )
        raise AssertionError(f"Unexpected RPC call: {json}")

    collector._client.post = AsyncMock(side_effect=mock_post)  # type: ignore[assignment]

    txs = await collector.fetch_transactions(since=date(2024, 1, 1))
    assert len(txs) == 2
    assert txs[0].tx_type == TransactionType.DEPOSIT
    assert txs[1].tx_type == TransactionType.WITHDRAWAL
    assert txs[0].amount == Decimal(1)
    assert txs[1].amount == Decimal("0.5")
