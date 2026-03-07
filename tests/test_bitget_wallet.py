"""Tests for Bitget Wallet collector (Aave + SOL staking)."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from pfm.collectors.bitget_wallet import BitgetWalletCollector
from pfm.db.models import TransactionType
from pfm.pricing.coingecko import PricingService

_EVM_ADDR = "0x744DA4BF067e0Fa5Fc9CcE0b6Cec7EFF0BbAb10D"
_SOL_ADDR = "BcbaVrK3VQyKPNSN9W3sWBN8b5cLriPe7GqyzwKTMxdk"

# Real stake account data (single delegation, ~7.89 SOL)
_STAKE_ACCOUNT_PUBKEY = "8xj6caougAMWgM7Fx5gHnGkxEi6g9Hzjsv9k27vsDHkj"
_STAKE_LAMPORTS = 7_892_121_621
_STAKE_SOL = Decimal("7.892121621")
_STAKE_VOTER = "7tKWFaaLi2FJSqukHxUrnXph8M3ynrqn3kEkKPpgcNHZ"

_AAVE_MARKET_ADDR = "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5"
_USDC_TOKEN_ADDR = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"


def _aave_market() -> dict[str, object]:
    return {
        "address": _AAVE_MARKET_ADDR,
        "name": "AaveV3Base",
        "chain": {"chainId": 8453, "name": "Base"},
    }


def _aave_markets_response() -> dict[str, object]:
    return {"data": {"value": [_aave_market()]}}


def _usdc_supply(
    amount: str = "250",
    usd: str = "250",
    apy: str = "0.031",
) -> dict[str, object]:
    return {
        "market": _aave_market(),
        "currency": {
            "symbol": "USDC",
            "address": _USDC_TOKEN_ADDR,
            "decimals": 6,
            "chainId": 8453,
        },
        "balance": {"amount": {"value": amount}, "usd": usd},
        "apy": {"value": apy, "formatted": f"{float(apy) * 100:.2f}"},
    }


def _mock_response(json_data: object) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 200
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    return resp


def _pricing() -> PricingService:
    pricing = PricingService()
    pricing._coins_by_symbol = {}
    pricing._set_cache("USDC", Decimal(1))
    pricing._set_cache("SOL", Decimal(130))
    pricing.today = lambda: date(2024, 1, 15)  # type: ignore[assignment]
    return pricing


_STAKEWIZ_APY = Decimal("0.0613")


def _solana_rpc_response() -> dict[str, object]:
    """Real Solana getProgramAccounts response shape for a single stake account."""
    return {
        "jsonrpc": "2.0",
        "result": [
            {
                "account": {
                    "data": {
                        "parsed": {
                            "info": {
                                "meta": {
                                    "authorized": {
                                        "staker": _SOL_ADDR,
                                        "withdrawer": _SOL_ADDR,
                                    },
                                    "lockup": {
                                        "custodian": "11111111111111111111111111111111",
                                        "epoch": 0,
                                        "unixTimestamp": 0,
                                    },
                                    "rentExemptReserve": "2282880",
                                },
                                "stake": {
                                    "creditsObserved": 527319728,
                                    "delegation": {
                                        "activationEpoch": "935",
                                        "deactivationEpoch": "18446744073709551615",
                                        "stake": "7889838740",
                                        "voter": _STAKE_VOTER,
                                        "warmupCooldownRate": 0.25,
                                    },
                                },
                            },
                            "type": "delegated",
                        },
                        "program": "stake",
                        "space": 200,
                    },
                    "executable": False,
                    "lamports": _STAKE_LAMPORTS,
                    "owner": "Stake11111111111111111111111111111111111111",
                    "rentEpoch": 18446744073709551615,
                    "space": 200,
                },
                "pubkey": _STAKE_ACCOUNT_PUBKEY,
            }
        ],
        "id": 1,
    }


def _stakewiz_response() -> dict[str, object]:
    return {
        "name": "Bitget Wallet",
        "vote_identity": _STAKE_VOTER,
        "apy_estimate": 6.13,
        "commission": 0,
    }


async def test_bitget_wallet_fetch_balances_from_aave_api():
    pricing = _pricing()
    collector = BitgetWalletCollector(
        pricing,
        wallet_address="0x771e4E594855e95eE1280940F69D2b0F0C0a1417",
    )

    # 251.5 USDC = 251_500_000 in 6 decimals = 0xEFE1A0 hex
    onchain_hex = hex(251_500_000)

    async def mock_post(_url: str, *, json: dict[str, object]) -> MagicMock:
        # On-chain RPC call (eth_call to Base)
        if json.get("method") == "eth_call":
            return _mock_response({"jsonrpc": "2.0", "id": 1, "result": onchain_hex})
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
    # On-chain balance (251.5) overrides GraphQL amount (250)
    assert snap.amount == Decimal("251.5")
    assert snap.usd_value == Decimal("251.5")
    assert snap.apy == Decimal("0.031")
    assert snap.price == Decimal(1)
    raw = json.loads(snap.raw_json)
    assert raw["wallet_address"] == collector._wallet_address
    assert raw["onchain_amount"] == "251.5"


async def test_bitget_wallet_fetch_transactions_from_aave_api():
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


async def test_sol_staking_fetch_balances():
    """SOL staking snapshot based on real stake account data."""
    pricing = _pricing()
    collector = BitgetWalletCollector(
        pricing,
        wallet_address=_EVM_ADDR,
        solana_address=_SOL_ADDR,
    )

    aave_markets_resp = _mock_response(
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
    aave_supplies_resp = _mock_response({"data": {"value": []}})
    solana_resp = _mock_response(_solana_rpc_response())
    stakewiz_resp = _mock_response(_stakewiz_response())

    async def mock_post(url: str, *, json: dict[str, object]) -> MagicMock:
        if json.get("method") == "getProgramAccounts":
            return solana_resp
        query = str(json.get("query", ""))
        if "markets(request" in query:
            return aave_markets_resp
        if "userSupplies(request" in query:
            return aave_supplies_resp
        raise AssertionError(f"Unexpected call: {url} {json}")

    async def mock_get(url: str, **_kwargs: object) -> MagicMock:
        if "stakewiz.com" in url:
            return stakewiz_resp
        raise AssertionError(f"Unexpected GET: {url}")

    collector._client.post = AsyncMock(side_effect=mock_post)  # type: ignore[assignment]
    collector._client.get = AsyncMock(side_effect=mock_get)  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 1
    snap = snapshots[0]
    assert snap.asset == "SOL"
    assert snap.amount == _STAKE_SOL
    assert snap.price == Decimal(130)
    assert snap.usd_value == _STAKE_SOL * Decimal(130)
    assert snap.apy == _STAKEWIZ_APY
    raw = json.loads(snap.raw_json)
    assert raw["solana_address"] == _SOL_ADDR
    assert raw["total_lamports"] == _STAKE_LAMPORTS
    assert raw["voter"] == _STAKE_VOTER
    assert len(raw["stake_accounts"]) == 1
    assert raw["stake_accounts"][0]["pubkey"] == _STAKE_ACCOUNT_PUBKEY


async def test_sol_staking_combined_with_aave():
    """Both Aave USDC and SOL staking returned together."""
    pricing = _pricing()
    collector = BitgetWalletCollector(
        pricing,
        wallet_address=_EVM_ADDR,
        solana_address=_SOL_ADDR,
    )

    onchain_hex = hex(251_500_000)

    async def mock_post(url: str, *, json: dict[str, object]) -> MagicMock:
        if json.get("method") == "eth_call":
            return _mock_response({"jsonrpc": "2.0", "id": 1, "result": onchain_hex})
        if json.get("method") == "getProgramAccounts":
            return _mock_response(_solana_rpc_response())
        query = str(json.get("query", ""))
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
                            }
                        ]
                    }
                }
            )
        raise AssertionError(f"Unexpected call: {url} {json}")

    async def mock_get(url: str, **_kwargs: object) -> MagicMock:
        if "stakewiz.com" in url:
            return _mock_response(_stakewiz_response())
        raise AssertionError(f"Unexpected GET: {url}")

    collector._client.post = AsyncMock(side_effect=mock_post)  # type: ignore[assignment]
    collector._client.get = AsyncMock(side_effect=mock_get)  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 2
    assets = {s.asset for s in snapshots}
    assert assets == {"USDC", "SOL"}


async def test_sol_staking_no_solana_address_skips():
    """No SOL staking when solana_address is not provided."""
    pricing = _pricing()
    collector = BitgetWalletCollector(pricing, wallet_address=_EVM_ADDR)

    async def mock_post(_url: str, *, json: dict[str, object]) -> MagicMock:
        if json.get("method") == "eth_call":
            return _mock_response({"jsonrpc": "2.0", "id": 1, "result": hex(100_000_000)})
        query = str(json.get("query", ""))
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
                                "balance": {"amount": {"value": "100"}, "usd": "100"},
                                "apy": {"value": "0.03", "formatted": "3.00"},
                            }
                        ]
                    }
                }
            )
        raise AssertionError(f"Unexpected call: {json}")

    collector._client.post = AsyncMock(side_effect=mock_post)  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 1
    assert snapshots[0].asset == "USDC"


async def test_sol_staking_empty_result():
    """No snapshots when Solana RPC returns no stake accounts."""
    pricing = _pricing()
    collector = BitgetWalletCollector(
        pricing,
        wallet_address=_EVM_ADDR,
        solana_address=_SOL_ADDR,
    )

    async def mock_post(url: str, *, json: dict[str, object]) -> MagicMock:
        if json.get("method") == "getProgramAccounts":
            return _mock_response({"jsonrpc": "2.0", "result": [], "id": 1})
        query = str(json.get("query", ""))
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
            return _mock_response({"data": {"value": []}})
        raise AssertionError(f"Unexpected call: {url} {json}")

    collector._client.post = AsyncMock(side_effect=mock_post)  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 0


async def test_apy_rules_override():
    """APY rules override the protocol APY during collection."""
    from pfm.db.apy_rules_store import ApyRule, RuleLimit

    pricing = _pricing()
    collector = BitgetWalletCollector(
        pricing,
        wallet_address="0x771e4E594855e95eE1280940F69D2b0F0C0a1417",
    )

    # Set up base rule: 10% for first 5000 USDC, rest falls back to protocol APY
    collector.apy_rules = [
        ApyRule(
            id="rule-1",
            protocol="aave",
            coin="usdc",
            type="base",
            limits=(RuleLimit(from_amount=Decimal(0), to_amount=Decimal(5000), apy=Decimal("0.10")),),
            started_at=date(2024, 1, 1),
            finished_at=date(2025, 12, 31),
        ),
    ]

    onchain_hex = hex(251_500_000)  # 251.5 USDC

    async def mock_post(_url: str, *, json: dict[str, object]) -> MagicMock:
        if json.get("method") == "eth_call":
            return _mock_response({"jsonrpc": "2.0", "id": 1, "result": onchain_hex})
        query = str(json.get("query", ""))
        if "markets(request" in query:
            return _mock_response(_aave_markets_response())
        if "userSupplies(request" in query:
            return _mock_response({"data": {"value": [_usdc_supply()]}})
        raise AssertionError(f"Unexpected RPC call: {json}")

    collector._client.post = AsyncMock(side_effect=mock_post)  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 1
    snap = snapshots[0]
    # Protocol APY was 0.031, but base rule overrides to 0.10 (amount 251.5 is in 0-5000 bracket)
    assert snap.apy == Decimal("0.10")


def test_invalid_solana_address():
    """Reject invalid Solana addresses."""
    pricing = _pricing()
    with pytest.raises(ValueError, match="solana_address must be a base58"):
        BitgetWalletCollector(
            pricing,
            wallet_address=_EVM_ADDR,
            solana_address="0xNotASolanaAddress",
        )
