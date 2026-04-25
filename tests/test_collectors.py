"""Tests for all collector implementations with mocked HTTP responses."""

from __future__ import annotations

import imaplib
import json
import time
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from pfm.collectors import COLLECTOR_REGISTRY
from pfm.collectors.binance import BinanceCollector
from pfm.collectors.binance_th import BinanceThCollector
from pfm.collectors.blend import BlendCollector
from pfm.collectors.bybit import BybitCollector
from pfm.collectors.ibkr import IbkrCollector
from pfm.collectors.kbank import KbankCollector
from pfm.collectors.lobstr import LobstrCollector
from pfm.collectors.mexc import MexcCollector
from pfm.collectors.okx import OkxCollector
from pfm.collectors.wise import WiseCollector
from pfm.db.models import RawBalance, Transaction, TransactionType
from pfm.pricing.coingecko import PricingService


@pytest.fixture
def pricing():
    p = PricingService()
    p._coins_by_symbol = {}
    p._set_cache("BTC", Decimal(50000))
    p._set_cache("ETH", Decimal(3000))
    p._set_cache("XLM", Decimal("0.10"))
    p._set_cache("USDC", Decimal(1))
    p.today = lambda: date(2024, 1, 15)  # type: ignore[assignment]
    return p


def _mock_response(json_data, status_code=200):
    """Create a mock httpx.Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.text = json.dumps(json_data) if isinstance(json_data, dict | list) else str(json_data)
    resp.raise_for_status = MagicMock()
    return resp


# ── Lobstr / Stellar ──────────────────────────────────────────────────


async def test_lobstr_fetch_balances(pricing):
    collector = LobstrCollector(pricing, stellar_address="GABC123")
    account_resp = _mock_response(
        {
            "balances": [
                {"balance": "100.0000000", "asset_type": "native"},
                {"balance": "500.0000000", "asset_type": "credit_alphanum4", "asset_code": "USDC"},
                {"balance": "0.0000000", "asset_type": "credit_alphanum4", "asset_code": "BTC"},
            ]
        }
    )
    collector._client.get = AsyncMock(return_value=account_resp)

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 2  # zero balances excluded
    assert snapshots[0].asset == "XLM"
    assert snapshots[0].amount == Decimal("100.0000000")
    assert snapshots[1].asset == "USDC"


async def test_lobstr_fetch_transactions(pricing):
    collector = LobstrCollector(pricing, stellar_address="GABC123")
    payments_resp = _mock_response(
        {
            "_embedded": {
                "records": [
                    {
                        "type": "payment",
                        "created_at": "2024-01-15T10:00:00Z",
                        "to": "GABC123",
                        "asset_type": "credit_alphanum4",
                        "asset_code": "USDC",
                        "amount": "100.0",
                        "transaction_hash": "abc123",
                    },
                    {
                        "type": "manage_offer",  # should be skipped
                        "created_at": "2024-01-15T09:00:00Z",
                    },
                ]
            }
        }
    )
    collector._client.get = AsyncMock(return_value=payments_resp)

    txs = await collector.fetch_transactions()
    assert len(txs) == 1
    assert txs[0].tx_type == TransactionType.UNKNOWN
    assert txs[0].asset == "USDC"
    assert txs[0].amount == Decimal("100.0")


async def test_lobstr_parse_create_account(pricing):
    collector = LobstrCollector(pricing, stellar_address="GABC123")
    record = {
        "type": "create_account",
        "created_at": "2024-01-15T10:00:00Z",
        "to": "GABC123",
        "starting_balance": "50.0",
        "transaction_hash": "hash1",
    }
    tx = collector._parse_payment(record)
    assert tx is not None
    assert tx.asset == "XLM"
    assert tx.amount == Decimal("50.0")


async def test_lobstr_outgoing_payment(pricing):
    collector = LobstrCollector(pricing, stellar_address="GABC123")
    record = {
        "type": "payment",
        "created_at": "2024-01-15T10:00:00Z",
        "to": "GOTHER",
        "from": "GABC123",
        "asset_type": "native",
        "amount": "25.0",
        "transaction_hash": "hash2",
    }
    tx = collector._parse_payment(record)
    assert tx is not None
    assert tx.tx_type == TransactionType.UNKNOWN


async def test_lobstr_transactions_since_filter(pricing):
    collector = LobstrCollector(pricing, stellar_address="GABC123")
    payments_resp = _mock_response(
        {
            "_embedded": {
                "records": [
                    {
                        "type": "payment",
                        "created_at": "2024-01-15T10:00:00Z",
                        "to": "GABC123",
                        "asset_type": "native",
                        "amount": "10",
                        "transaction_hash": "h1",
                    },
                    {
                        "type": "payment",
                        "created_at": "2024-01-10T10:00:00Z",
                        "to": "GABC123",
                        "asset_type": "native",
                        "amount": "5",
                        "transaction_hash": "h2",
                    },
                ]
            }
        }
    )
    collector._client.get = AsyncMock(return_value=payments_resp)

    txs = await collector.fetch_transactions(since=date(2024, 1, 12))
    assert len(txs) == 1


# ── Binance ───────────────────────────────────────────────────────────


async def test_binance_fetch_balances(pricing):
    collector = BinanceCollector(pricing, api_key="key", api_secret="secret")
    collector._time_synced = True
    account_resp = _mock_response(
        {
            "balances": [
                {"asset": "BTC", "free": "1.0", "locked": "0.5"},
                {"asset": "ETH", "free": "10.0", "locked": "0.0"},
                {"asset": "DOGE", "free": "0.0", "locked": "0.0"},
            ]
        }
    )
    collector._client.get = AsyncMock(return_value=account_resp)

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 2  # DOGE excluded (zero)
    btc = next(s for s in snapshots if s.asset == "BTC")
    assert btc.amount == Decimal("1.5")  # free + locked
    assert btc.usd_value == Decimal(75000)


async def test_binance_fetch_balances_unknown_ticker(pricing):
    collector = BinanceCollector(pricing, api_key="key", api_secret="secret")
    collector._time_synced = True
    account_resp = _mock_response(
        {
            "balances": [
                {"asset": "UNKNOWNCOIN", "free": "100", "locked": "0"},
            ]
        }
    )
    collector._client.get = AsyncMock(return_value=account_resp)

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 1  # included with zero USD price
    assert snapshots[0].asset == "UNKNOWNCOIN"
    assert snapshots[0].usd_value == Decimal(0)


async def test_binance_fetch_transactions(pricing):
    collector = BinanceCollector(pricing, api_key="key", api_secret="secret")
    collector._time_synced = True
    deposits_resp = _mock_response(
        [
            {"coin": "BTC", "amount": "1.0", "insertTime": 1705276800000, "txId": "dep1"},
        ]
    )
    withdrawals_resp = _mock_response(
        [
            {"coin": "ETH", "amount": "5.0", "applyTime": "2024-01-15T00:00:00+00:00", "id": "wd1"},
        ]
    )

    call_count = 0

    async def mock_get(path, **kwargs):
        nonlocal call_count
        call_count += 1
        if "deposit" in path:
            return deposits_resp
        return withdrawals_resp

    collector._client.get = mock_get  # type: ignore[assignment]

    txs = await collector.fetch_transactions()
    assert len(txs) == 2
    dep = txs[0]
    assert dep.asset == "BTC"
    assert dep.amount == Decimal("1.0")
    wd = txs[1]
    assert wd.asset == "ETH"


async def test_binance_fetch_transactions_with_since(pricing):
    collector = BinanceCollector(pricing, api_key="key", api_secret="secret")
    collector._time_synced = True
    collector._client.get = AsyncMock(return_value=_mock_response([]))

    await collector.fetch_transactions(since=date(2024, 1, 1))
    # Just verify it doesn't crash with since parameter


async def test_binance_deposit_http_error(pricing):
    collector = BinanceCollector(pricing, api_key="key", api_secret="secret")
    collector._time_synced = True

    call_count = 0

    async def mock_get(path, **kwargs):
        nonlocal call_count
        call_count += 1
        if "deposit" in path:
            resp = _mock_response([])
            resp.raise_for_status.side_effect = httpx.HTTPStatusError("403", request=MagicMock(), response=MagicMock())
            return resp
        return _mock_response([])

    collector._client.get = mock_get  # type: ignore[assignment]

    txs = await collector.fetch_transactions()
    # Should gracefully handle the error
    assert isinstance(txs, list)


async def test_binance_signed_params(pricing):
    collector = BinanceCollector(pricing, api_key="key", api_secret="secret")
    params = collector._signed_params({"symbol": "BTCUSDT"})
    assert "timestamp" in params
    assert "signature" in params
    assert params["symbol"] == "BTCUSDT"


async def test_binance_parse_deposit_empty():
    tx = BinanceCollector._parse_deposit({"coin": "", "amount": "0"})
    assert tx is None


async def test_binance_parse_withdrawal_empty():
    tx = BinanceCollector._parse_withdrawal({"coin": "", "amount": "0"})
    assert tx is None


async def test_binance_server_time_sync(pricing):
    collector = BinanceCollector(pricing, api_key="key", api_secret="secret")
    server_time_resp = _mock_response({"serverTime": int(time.time() * 1000) + 2000})
    account_resp = _mock_response({"balances": [{"asset": "BTC", "free": "1.0", "locked": "0.0"}]})

    async def mock_get(path, **kwargs):
        if "time" in path:
            return server_time_resp
        return account_resp

    collector._client.get = mock_get  # type: ignore[assignment]
    raw = await collector.fetch_raw_balances()
    assert collector._time_synced is True
    assert abs(collector._time_offset_ms - 2000) < 200
    assert len(raw) == 1


async def test_binance_server_time_sync_failure_falls_back(pricing):
    collector = BinanceCollector(pricing, api_key="key", api_secret="secret")
    account_resp = _mock_response({"balances": [{"asset": "BTC", "free": "1.0", "locked": "0.0"}]})

    async def mock_get(path, **kwargs):
        if "time" in path:
            raise httpx.ConnectError("timeout")
        return account_resp

    collector._client.get = mock_get  # type: ignore[assignment]
    raw = await collector.fetch_raw_balances()
    assert collector._time_synced is True
    assert collector._time_offset_ms == 0
    assert len(raw) == 1


# ── Binance TH ────────────────────────────────────────────────────────


async def test_binance_th_has_different_base_url(pricing):
    collector = BinanceThCollector(pricing, api_key="key", api_secret="secret")
    assert collector.source_name == "binance_th"
    assert collector._base_url == "https://api.binance.th"


async def test_binance_th_transactions_fallback_to_sapi(pricing):
    collector = BinanceThCollector(pricing, api_key="key", api_secret="secret")
    called_paths: list[str] = []

    async def mock_get(path, params=None):
        called_paths.append(path)
        if path == "/api/v1/capital/withdraw/history":
            response = MagicMock()
            response.status_code = 404
            raise httpx.HTTPStatusError("404", request=MagicMock(), response=response)
        if path == "/sapi/v1/capital/withdraw/history":
            return [
                {
                    "coin": "USDC",
                    "amount": "10",
                    "applyTime": "2024-01-15T00:00:00",
                    "id": "wd-1",
                }
            ]
        raise AssertionError(f"Unexpected path: {path}")

    collector._get = mock_get  # type: ignore[assignment]

    txs = await collector.fetch_transactions()
    assert len(txs) == 1
    assert txs[0].tx_type == TransactionType.UNKNOWN
    assert "/api/v1/capital/withdraw/history" in called_paths
    assert "/sapi/v1/capital/withdraw/history" in called_paths


async def test_binance_th_transactions_404_on_all_paths_is_clean_skip(pricing):
    collector = BinanceThCollector(pricing, api_key="key", api_secret="secret")

    async def mock_get(path, params=None):
        response = MagicMock()
        response.status_code = 404
        raise httpx.HTTPStatusError("404", request=MagicMock(), response=response)

    collector._get = mock_get  # type: ignore[assignment]
    with patch("pfm.collectors.binance_th.logger.info") as log_info:
        txs = await collector.fetch_transactions()

    assert txs == []
    assert any(
        "endpoint is unavailable (404) on known paths, skipping." in str(call.args[0])
        for call in log_info.call_args_list
    )


# ── MEXC ──────────────────────────────────────────────────────────────


async def test_mexc_fetch_balances(pricing):
    collector = MexcCollector(pricing, api_key="key", api_secret="secret")
    collector._time_synced = True
    collector._get_openapi = AsyncMock(return_value={"success": True, "data": []})  # type: ignore[method-assign]
    account_resp = _mock_response(
        {
            "balances": [
                {"asset": "BTC", "free": "0.1", "locked": "0.2"},
                {"asset": "USDC", "free": "500", "locked": "0"},
                {"asset": "DOGE", "free": "0", "locked": "0"},
            ]
        }
    )
    collector._client.get = AsyncMock(return_value=account_resp)

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 2
    btc = next(s for s in snapshots if s.asset == "BTC")
    assert btc.amount == Decimal("0.3")


async def test_mexc_fetch_transactions(pricing):
    collector = MexcCollector(pricing, api_key="key", api_secret="secret")
    collector._time_synced = True
    deposit_resp = _mock_response(
        [
            {
                "coin": "USDC",
                "amount": "100",
                "insertTime": 1705276800000,
                "txId": "dep-1",
            }
        ]
    )
    withdrawal_resp = _mock_response(
        [
            {
                "coin": "USDC",
                "amount": "50",
                "applyTime": "2024-01-15T00:00:00+00:00",
                "id": "wd-1",
            }
        ]
    )

    async def mock_get(path, **kwargs):
        if "deposit/hisrec" in path:
            return deposit_resp
        if "withdraw/history" in path:
            return withdrawal_resp
        raise AssertionError(f"Unexpected path: {path}")

    collector._client.get = mock_get  # type: ignore[assignment]

    txs = await collector.fetch_transactions()
    assert len(txs) == 2
    dep = txs[0]
    assert dep.asset == "USDC"
    wd = txs[1]
    assert wd.asset == "USDC"


async def test_mexc_signed_params(pricing):
    collector = MexcCollector(pricing, api_key="key", api_secret="secret")
    params = collector._signed_params({"symbol": "BTCUSDT"})
    assert "timestamp" in params
    assert "signature" in params
    assert params["symbol"] == "BTCUSDT"


async def test_mexc_openapi_headers(pricing):
    collector = MexcCollector(pricing, api_key="key", api_secret="secret")
    with patch("pfm.collectors.mexc.time.time", return_value=1700000000.0):
        headers = collector._openapi_headers({"recv_window": "5000", "symbol": "BTC_USDT"})
    assert headers["ApiKey"] == "key"
    assert headers["Request-Time"] == "1700000000000"
    assert headers["Signature"] == "93c3bb21b72cc9f5d779a973b8f4f862bc52d6567033a5c1c4dc9d0bfa7c4aca"


async def test_mexc_sync_server_time(pricing):
    collector = MexcCollector(pricing, api_key="key", api_secret="secret")
    time_resp = _mock_response({"serverTime": 1700000005000})
    collector._client.get = AsyncMock(return_value=time_resp)

    with patch("pfm.collectors.mexc.time.time", return_value=1700000000.0):
        await collector._sync_server_time()

    assert collector._time_synced is True
    assert collector._time_offset_ms == 5000


async def test_mexc_sync_server_time_failure_uses_local_clock(pricing):
    collector = MexcCollector(pricing, api_key="key", api_secret="secret")
    error_resp = _mock_response({})
    collector._client.get = AsyncMock(return_value=error_resp)

    await collector._sync_server_time()

    assert collector._time_synced is True
    assert collector._time_offset_ms == 0


async def test_mexc_get_raises_on_api_error_code(pricing):
    collector = MexcCollector(pricing, api_key="key", api_secret="secret")
    collector._time_synced = True
    error_resp = _mock_response({"code": 700002, "msg": "Signature for this request is not valid."})
    collector._client.get = AsyncMock(return_value=error_resp)

    with pytest.raises(httpx.HTTPStatusError, match="Signature for this request is not valid."):
        await collector._get("/api/v3/account")


async def test_mexc_fetch_contract_balances(pricing):
    collector = MexcCollector(pricing, api_key="key", api_secret="secret")
    collector._get_openapi = AsyncMock(  # type: ignore[method-assign]
        return_value={
            "success": True,
            "data": [
                {"currency": "USDC", "equity": "42.5", "availableBalance": "42.5", "frozenBalance": "0"},
                {"currency": "DOGE", "equity": "0", "availableBalance": "0", "frozenBalance": "0"},
            ],
        }
    )
    raw = await collector._fetch_contract_raw()
    assert len(raw) == 1
    usdc = raw[0]
    assert usdc.asset == "USDC"
    assert usdc.amount == Decimal("42.5")


async def test_mexc_fetch_earn_with_apy(pricing):
    pricing._set_cache("USDC", Decimal(1))
    collector = MexcCollector(pricing, api_key="key", api_secret="secret")
    collector._time_synced = True
    collector._get_openapi = AsyncMock(return_value={"success": True, "data": []})  # type: ignore[method-assign]

    async def mock_get(path, **kwargs):
        if path == "/api/v3/account":
            return _mock_response({"balances": []})
        if path == "/api/v3/asset/earn/position":
            return _mock_response(
                [
                    {
                        "asset": "USDC",
                        "amount": "250",
                        "apy": "12.5",
                    }
                ]
            )
        return _mock_response([])

    collector._client.get = mock_get  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    earn_snaps = [s for s in snapshots if s.apy > 0]
    assert len(earn_snaps) == 1
    usdc = earn_snaps[0]
    assert usdc.asset == "USDC"
    assert usdc.amount == Decimal(250)
    assert usdc.apy == Decimal("0.125")


async def test_mexc_fetch_fixed_earn_uses_realized_rate_over_show_apr(pricing):
    pricing._set_cache("USDT", Decimal(1))
    collector = MexcCollector(pricing, api_key="key", api_secret="secret")
    collector._time_synced = True
    collector._get_openapi = AsyncMock(return_value={"success": True, "data": []})  # type: ignore[method-assign]

    async def mock_get(path, **kwargs):
        if path == "/api/v3/account":
            return _mock_response({"balances": []})
        if path == "/api/v3/asset/earn/position":
            return _mock_response(
                [
                    {
                        "pledgeCurrency": "USDT",
                        "positionQuantity": "300.86335046",
                        "positionUsdtQuantity": "300.86335046",
                        "totalGrantedProfitQuantity": "0.86335046",
                        "totalGrantedProfitUsdtQuantity": "0.86335046",
                        "yesterdayProfitQuantity": "0.12337596",
                        "yesterdayProfitUsdtQuantity": "0.12337596",
                        "financialType": "FIXED",
                        "showApr": "25",
                    }
                ]
            )
        return _mock_response([])

    collector._client.get = mock_get  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    earn_snaps = [s for s in snapshots if s.apy > 0]
    assert len(earn_snaps) == 1
    usdt = earn_snaps[0]
    assert usdt.asset == "USDT"
    assert usdt.amount == Decimal("300.86335046")
    assert usdt.apy == Decimal("0.150107418")


async def test_mexc_fetch_earn_nets_out_spot_balance(pricing):
    pricing._set_cache("USDC", Decimal(1))
    collector = MexcCollector(pricing, api_key="key", api_secret="secret")
    collector._time_synced = True
    collector._get_openapi = AsyncMock(return_value={"success": True, "data": []})  # type: ignore[method-assign]

    async def mock_get(path, **kwargs):
        if path == "/api/v3/account":
            return _mock_response({"balances": [{"asset": "USDC", "free": "500", "locked": "0"}]})
        if path == "/api/v3/asset/earn/position":
            return _mock_response([{"asset": "USDC", "amount": "250", "apy": "12.5"}])
        return _mock_response([])

    collector._client.get = mock_get  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()

    assert len(snapshots) == 2
    assert sum((snapshot.amount for snapshot in snapshots), Decimal(0)) == Decimal(500)
    earn_snap = next(snapshot for snapshot in snapshots if snapshot.apy > 0)
    spot_snap = next(snapshot for snapshot in snapshots if snapshot.apy == 0)
    assert earn_snap.asset == "USDC"
    assert earn_snap.amount == Decimal(250)
    assert spot_snap.asset == "USDC"
    assert spot_snap.amount == Decimal(250)


# ── OKX ───────────────────────────────────────────────────────────────


async def test_okx_fetch_balances(pricing):
    collector = OkxCollector(pricing, api_key="key", api_secret="secret", passphrase="pass")

    async def mock_get(path, **kwargs):
        if "account/balance" in path:
            return _mock_response(
                {
                    "data": [
                        {
                            "details": [
                                {"ccy": "BTC", "eq": "2.0"},
                                {"ccy": "ETH", "eq": "0"},  # zero, excluded
                            ]
                        }
                    ]
                }
            )
        if "asset/balances" in path:
            return _mock_response(
                {
                    "data": [
                        {"ccy": "USDC", "availBal": "1000", "frozenBal": "0"},
                        {"ccy": "BTC", "availBal": "0.5", "frozenBal": "0"},  # merge with trading
                    ]
                }
            )
        # earn endpoints return empty data
        return _mock_response({"data": []})

    collector._client.get = mock_get  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 2  # BTC (merged), USDC
    btc = next(s for s in snapshots if s.asset == "BTC")
    assert btc.amount == Decimal("2.5")  # 2.0 trading + 0.5 funding
    assert btc.apy == Decimal(0)


async def test_okx_fetch_earn_with_apy(pricing):
    """Test OKX earn snapshots include APY from lending-history."""
    pricing._set_cache("USDT", Decimal(1))
    collector = OkxCollector(pricing, api_key="key", api_secret="secret", passphrase="pass")

    async def mock_get(path, **kwargs):
        if "account/balance" in path:
            return _mock_response({"data": [{"details": []}]})
        if "asset/balances" in path:
            return _mock_response({"data": []})
        if "savings/balance" in path:
            return _mock_response({"data": [{"ccy": "USDT", "amt": "500"}]})
        if "lending-history" in path:
            return _mock_response(
                {
                    "data": [
                        {"amt": "500", "earnings": "0.005", "rate": "0.09", "ts": "1000"},
                        {"amt": "500", "earnings": "0.0005", "rate": "0.01", "ts": "1000"},
                    ]
                }
            )
        if "staking-defi" in path:
            return _mock_response({"data": []})
        return _mock_response({"data": []})

    collector._client.get = mock_get  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    earn_snaps = [s for s in snapshots if s.apy > 0]
    assert len(earn_snaps) == 1
    usdt = earn_snaps[0]
    assert usdt.asset == "USDT"
    assert usdt.amount == Decimal(500)
    # APR = (0.005 + 0.0005) * 8760 / 500 ≈ 0.0964
    assert usdt.apy > Decimal("0.09")


async def test_okx_fetch_transactions(pricing):
    collector = OkxCollector(pricing, api_key="key", api_secret="secret", passphrase="pass")
    bills_resp = _mock_response(
        {
            "data": [
                {"ccy": "BTC", "balChg": "1.0", "ts": "1705276800000", "subType": "1", "billId": "b1"},
                {"ccy": "ETH", "balChg": "-0.5", "ts": "1705276800000", "subType": "13", "billId": "b2"},
            ]
        }
    )
    collector._client.get = AsyncMock(return_value=bills_resp)

    txs = await collector.fetch_transactions()
    assert len(txs) == 2
    trade_tx = txs[0]
    assert trade_tx.asset == "BTC"


async def test_okx_parse_bill_empty_ccy():
    tx = OkxCollector._parse_bill({"ccy": "", "balChg": "1.0"})
    assert tx is None


async def test_okx_parse_bill_deposit():
    bill = {"ccy": "BTC", "balChg": "1.0", "ts": "1705276800000", "subType": "13", "billId": "b"}
    tx = OkxCollector._parse_bill(bill)
    assert tx is not None
    assert tx.tx_type == TransactionType.UNKNOWN


async def test_okx_parse_bill_withdrawal():
    bill = {"ccy": "BTC", "balChg": "-1.0", "ts": "1705276800000", "subType": "14", "billId": "b"}
    tx = OkxCollector._parse_bill(bill)
    assert tx is not None
    assert tx.tx_type == TransactionType.UNKNOWN


async def test_okx_parse_bill_transfer():
    bill = {"ccy": "BTC", "balChg": "1.0", "ts": "1705276800000", "subType": "99", "billId": "b"}
    tx = OkxCollector._parse_bill(bill)
    assert tx is not None
    assert tx.tx_type == TransactionType.UNKNOWN


async def test_okx_sign_request(pricing):
    collector = OkxCollector(pricing, api_key="key", api_secret="secret", passphrase="pass")
    headers = collector._sign_request("GET", "/api/v5/account/balance")
    assert "OK-ACCESS-KEY" in headers
    assert "OK-ACCESS-SIGN" in headers
    assert "OK-ACCESS-TIMESTAMP" in headers
    assert "OK-ACCESS-PASSPHRASE" in headers


async def test_okx_transactions_since_filter(pricing):
    collector = OkxCollector(pricing, api_key="key", api_secret="secret", passphrase="pass")
    bills_resp = _mock_response(
        {
            "data": [
                {"ccy": "BTC", "balChg": "1.0", "ts": "1705276800000", "subType": "1", "billId": "b1"},
                {"ccy": "BTC", "balChg": "0.5", "ts": "1704067200000", "subType": "1", "billId": "b2"},
            ]
        }
    )
    collector._client.get = AsyncMock(return_value=bills_resp)

    txs = await collector.fetch_transactions(since=date(2024, 1, 10))
    assert len(txs) == 1


async def test_okx_domain_fallback(pricing):
    """On 401 from www.okx.com, collector switches to my.okx.com."""
    collector = OkxCollector(pricing, api_key="key", api_secret="secret", passphrase="pass")
    assert not collector._domain_resolved

    ok_resp = _mock_response({"data": [{"details": [{"ccy": "BTC", "eq": "1.0"}]}]})
    fail_resp = httpx.Response(401, request=httpx.Request("GET", "https://www.okx.com/api/v5/account/balance"))

    collector._client.get = AsyncMock(return_value=fail_resp)

    fallback_client = MagicMock(spec=httpx.AsyncClient)
    fallback_client.get = AsyncMock(return_value=ok_resp)
    fallback_client.base_url = httpx.URL("https://my.okx.com")

    with patch("pfm.collectors.okx.httpx.AsyncClient", return_value=fallback_client):
        data = await collector._get("/api/v5/account/balance")

    assert data["data"][0]["details"][0]["ccy"] == "BTC"
    assert collector._domain_resolved
    assert "my.okx.com" in str(collector._client.base_url)


# ── Bybit ─────────────────────────────────────────────────────────────


async def test_bybit_fetch_balances(pricing):
    collector = BybitCollector(pricing, api_key="key", api_secret="secret")

    async def mock_get(path, **kwargs):
        return _mock_response(
            {
                "retCode": 0,
                "result": {
                    "list": [
                        {
                            "coin": [
                                {"coin": "BTC", "walletBalance": "1.0"},
                                {"coin": "USDC", "walletBalance": "500"},
                                {"coin": "DOGE", "walletBalance": "0"},  # zero excluded
                            ]
                        }
                    ]
                },
            }
        )

    collector._client.get = mock_get  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 2


async def test_bybit_fetch_balances_account_error(pricing):
    collector = BybitCollector(pricing, api_key="key", api_secret="secret")

    call_count = 0

    async def mock_get(path, **kwargs):
        nonlocal call_count
        call_count += 1
        return _mock_response(
            {
                "retCode": 10001,
                "retMsg": "account not found",
            }
        )

    collector._client.get = mock_get  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    assert snapshots == []


async def test_bybit_fetch_transactions(pricing):
    collector = BybitCollector(pricing, api_key="key", api_secret="secret")
    tx_resp = _mock_response(
        {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "currency": "BTC",
                        "cashFlow": "1.0",
                        "transactionTime": "1705276800000",
                        "type": "TRADE",
                        "id": "t1",
                    },
                    {
                        "currency": "ETH",
                        "cashFlow": "-0.5",
                        "transactionTime": "1705276800000",
                        "type": "WITHDRAWAL",
                        "id": "t2",
                    },
                ]
            },
        }
    )
    collector._client.get = AsyncMock(return_value=tx_resp)

    txs = await collector.fetch_transactions()
    assert len(txs) == 2


async def test_bybit_parse_transaction_types():
    ts = "1705276800000"

    tx = BybitCollector._parse_transaction(
        {"currency": "BTC", "cashFlow": "1", "transactionTime": ts, "type": "TRADE", "id": "1"},
    )
    assert tx is not None
    assert tx.tx_type == TransactionType.UNKNOWN

    tx = BybitCollector._parse_transaction(
        {"currency": "BTC", "cashFlow": "1", "transactionTime": ts, "type": "DEPOSIT", "id": "2"},
    )
    assert tx is not None
    assert tx.tx_type == TransactionType.UNKNOWN

    tx = BybitCollector._parse_transaction(
        {"currency": "BTC", "cashFlow": "0.01", "transactionTime": ts, "type": "INTEREST", "id": "3"},
    )
    assert tx is not None
    assert tx.tx_type == TransactionType.UNKNOWN

    tx = BybitCollector._parse_transaction(
        {"currency": "BTC", "cashFlow": "1", "transactionTime": ts, "type": "OTHER", "id": "4"},
    )
    assert tx is not None
    assert tx.tx_type == TransactionType.UNKNOWN


async def test_bybit_parse_transaction_empty_currency():
    tx = BybitCollector._parse_transaction({"currency": "", "cashFlow": "1", "transactionTime": "0", "type": "TRADE"})
    assert tx is None


async def test_bybit_signed_headers(pricing):
    collector = BybitCollector(pricing, api_key="key", api_secret="secret")
    headers = collector._signed_headers("accountType=UNIFIED")
    assert "X-BAPI-API-KEY" in headers
    assert "X-BAPI-SIGN" in headers
    assert "X-BAPI-TIMESTAMP" in headers


async def test_bybit_dedup_across_account_types(pricing):
    collector = BybitCollector(pricing, api_key="key", api_secret="secret")

    calls = 0

    async def mock_get(path, **kwargs):
        nonlocal calls
        calls += 1
        params = kwargs.get("params") or {}
        acct_type = params.get("accountType", "")
        if acct_type == "UNIFIED":
            return _mock_response(
                {
                    "retCode": 0,
                    "result": {"list": [{"coin": [{"coin": "BTC", "walletBalance": "1.0"}]}]},
                }
            )
        if acct_type == "SPOT":
            return _mock_response(
                {
                    "retCode": 0,
                    "result": {"list": [{"coin": [{"coin": "BTC", "walletBalance": "0.5"}]}]},
                }
            )
        return _mock_response({"retCode": 0, "result": {"list": []}})

    collector._client.get = mock_get  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    # BTC should appear only once (from UNIFIED, SPOT duplicate skipped)
    btc_snapshots = [s for s in snapshots if s.asset == "BTC"]
    assert len(btc_snapshots) == 1


async def test_bybit_fetch_earn_with_apy(pricing):
    """Test Bybit earn snapshots include APY from yesterdayYield."""
    pricing._set_cache("USDT", Decimal(1))
    collector = BybitCollector(pricing, api_key="key", api_secret="secret")

    async def mock_get(path, **kwargs):
        params = kwargs.get("params") or {}
        acct_type = params.get("accountType", "")
        category = params.get("category", "")
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
                                "yesterdayYield": "0.030692",
                                "estimateApr": "0.6%",
                            }
                        ]
                    },
                }
            )
        return _mock_response({"retCode": 0, "result": {"list": []}})

    collector._client.get = mock_get  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    earn_snaps = [s for s in snapshots if s.apy > 0]
    assert len(earn_snaps) == 1
    usdt = earn_snaps[0]
    assert usdt.asset == "USDT"
    assert usdt.amount == Decimal(200)
    # APR = 0.030692 * 365 / 200 ≈ 0.0560
    assert usdt.apy > Decimal("0.05")


# ── Wise ──────────────────────────────────────────────────────────────


async def test_wise_fetch_balances(pricing):
    collector = WiseCollector(pricing, api_token="token")

    async def mock_get(path, **kwargs):
        if "/v1/profiles" in path:
            return _mock_response([{"id": 123, "type": "personal"}])
        if "/v4/profiles" in path:
            return _mock_response(
                [
                    {"amount": {"value": 1000, "currency": "GBP"}, "id": 1},
                    {"amount": {"value": 0, "currency": "EUR"}, "id": 2},
                ]
            )
        return _mock_response([])

    collector._client.get = mock_get  # type: ignore[assignment]

    # GBP is fiat; need to cache it
    pricing._set_cache("GBP", Decimal("1.25"))

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 1
    assert snapshots[0].asset == "GBP"
    assert snapshots[0].amount == Decimal(1000)


async def test_wise_get_profile_fallback(pricing):
    collector = WiseCollector(pricing, api_token="token")
    # No personal profile, should fall back to first
    resp = _mock_response([{"id": 456, "type": "business"}])
    collector._client.get = AsyncMock(return_value=resp)

    profile_id = await collector._get_profile_id()
    assert profile_id == 456


async def test_wise_no_profiles_raises(pricing):
    collector = WiseCollector(pricing, api_token="token")
    resp = _mock_response([])
    collector._client.get = AsyncMock(return_value=resp)

    with pytest.raises(ValueError, match="No Wise profiles found"):
        await collector._get_profile_id()


async def test_wise_fetch_transactions_returns_empty(pricing):
    """Wise personal token lacks statement permissions — always empty."""
    collector = WiseCollector(pricing, api_token="token")
    txs = await collector.fetch_transactions()
    assert txs == []


# ── IBKR ──────────────────────────────────────────────────────────────


async def test_ibkr_parse_positions_from_xml():
    collector = IbkrCollector(PricingService(), flex_token="tok", flex_query_id="qid")
    xml = """<OpenPosition symbol="AAPL" position="10" markMarketValue="1500.00"/>
<OpenPosition symbol="MSFT" position="5" markMarketValue="1750.00"/>"""
    positions = collector._parse_positions_from_xml(xml)
    assert len(positions) == 2
    assert positions[0]["symbol"] == "AAPL"
    assert positions[1]["markMarketValue"] == "1750.00"


async def test_ibkr_parse_cash_from_xml():
    collector = IbkrCollector(PricingService(), flex_token="tok", flex_query_id="qid")
    xml = (
        '<CashReport currency="USD" endingCash="5000.00"/>'
        '<CashReportCurrency currency="EUR" endingSettledCash="1200.25"/>'
    )
    cash = collector._parse_cash_from_xml(xml)
    assert len(cash) == 2
    assert cash[0]["currency"] == "USD"
    assert cash[0]["endingCash"] == "5000.00"
    assert cash[1]["currency"] == "EUR"
    assert cash[1]["endingSettledCash"] == "1200.25"


async def test_ibkr_parse_trades_from_xml():
    collector = IbkrCollector(PricingService(), flex_token="tok", flex_query_id="qid")
    xml = '<Trade symbol="AAPL" quantity="10" proceeds="1500.00" tradeDate="2024-01-15" tradeID="123"/>'
    trades = collector._parse_trades_from_xml(xml)
    assert len(trades) == 1
    assert trades[0]["symbol"] == "AAPL"


async def test_ibkr_parse_trade_valid():
    trade = {"symbol": "AAPL", "quantity": "10", "proceeds": "1500.00", "tradeDate": "2024-01-15", "tradeID": "123"}
    tx = IbkrCollector._parse_trade(trade)
    assert tx is not None
    assert tx.asset == "AAPL"
    assert tx.amount == Decimal(10)
    assert tx.usd_value == Decimal("1500.00")


async def test_ibkr_parse_trade_no_symbol():
    tx = IbkrCollector._parse_trade({"symbol": "", "quantity": "10", "proceeds": "1500", "tradeDate": "2024-01-15"})
    assert tx is None


async def test_ibkr_parse_trade_bad_date():
    tx = IbkrCollector._parse_trade({"symbol": "AAPL", "quantity": "10", "proceeds": "1500", "tradeDate": "invalid"})
    assert tx is None


async def test_ibkr_request_statement(pricing):
    collector = IbkrCollector(pricing, flex_token="tok", flex_query_id="qid")
    resp = MagicMock(spec=httpx.Response)
    resp.text = (
        "<FlexStatementResponse><Status>Success</Status><ReferenceCode>REF123</ReferenceCode></FlexStatementResponse>"
    )
    resp.raise_for_status = MagicMock()
    collector._client.get = AsyncMock(return_value=resp)

    ref_code = await collector._request_statement()
    assert ref_code == "REF123"


async def test_ibkr_request_statement_failure(pricing):
    collector = IbkrCollector(pricing, flex_token="tok", flex_query_id="qid")
    resp = MagicMock(spec=httpx.Response)
    resp.text = (
        "<FlexStatementResponse><Status>Fail</Status><ErrorMessage>Invalid token</ErrorMessage></FlexStatementResponse>"
    )
    resp.raise_for_status = MagicMock()
    collector._client.get = AsyncMock(return_value=resp)

    with pytest.raises(ValueError, match="IBKR Flex request failed"):
        await collector._request_statement()


async def test_ibkr_fetch_statement_ready(pricing):
    collector = IbkrCollector(pricing, flex_token="tok", flex_query_id="qid")
    resp = MagicMock(spec=httpx.Response)
    resp.text = '<FlexQueryResponse queryName="Test"><FlexStatements count="1"></FlexStatements></FlexQueryResponse>'
    resp.raise_for_status = MagicMock()
    collector._client.get = AsyncMock(return_value=resp)

    result = await collector._fetch_statement("REF123")
    assert "<FlexQueryResponse" in result


async def test_ibkr_fetch_statement_timeout(pricing):
    collector = IbkrCollector(pricing, flex_token="tok", flex_query_id="qid")
    resp = MagicMock(spec=httpx.Response)
    resp.text = "Statement generation in progress. Please try again shortly."
    resp.raise_for_status = MagicMock()
    collector._client.get = AsyncMock(return_value=resp)

    with (
        patch("pfm.collectors.ibkr._MAX_POLL_ATTEMPTS", 2),
        patch("pfm.collectors.ibkr._POLL_DELAY_SECONDS", 0.01),
        pytest.raises(TimeoutError, match="timed out"),
    ):
        await collector._fetch_statement("REF123")


async def test_ibkr_fetch_statement_unexpected_response(pricing):
    collector = IbkrCollector(pricing, flex_token="tok", flex_query_id="qid")
    resp = MagicMock(spec=httpx.Response)
    resp.text = "<SomeOtherResponse>Unexpected</SomeOtherResponse>"
    resp.raise_for_status = MagicMock()
    collector._client.get = AsyncMock(return_value=resp)

    with pytest.raises(ValueError, match="IBKR unexpected response"):
        await collector._fetch_statement("REF123")


async def test_ibkr_fetch_balances(pricing):
    collector = IbkrCollector(pricing, flex_token="tok", flex_query_id="qid")
    pricing._set_cache("USD", Decimal(1))
    pricing._set_cache("EUR", Decimal("1.10"))

    request_resp = MagicMock(spec=httpx.Response)
    request_resp.text = (
        "<FlexStatementResponse><Status>Success</Status><ReferenceCode>REF1</ReferenceCode></FlexStatementResponse>"
    )
    request_resp.raise_for_status = MagicMock()

    statement_xml = """<FlexQueryResponse>
<FlexStatements>
<OpenPosition symbol="AAPL" position="10" markMarketValue="1500.00"/>
<CashReport currency="USD" endingCash="5000.00"/>
<CashReportCurrency currency="EUR" endingSettledCash="1000.00"/>
<CashReport currency="BASE_SUMMARY" endingCash="5000.00"/>
</FlexStatements>
</FlexQueryResponse>"""

    statement_resp = MagicMock(spec=httpx.Response)
    statement_resp.text = statement_xml
    statement_resp.raise_for_status = MagicMock()

    call_count = 0

    async def mock_get(url, **kwargs):
        nonlocal call_count
        call_count += 1
        if "SendRequest" in str(url):
            return request_resp
        return statement_resp

    collector._client.get = mock_get  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 3  # AAPL + USD cash + EUR cash (BASE_SUMMARY excluded)


async def test_ibkr_fetch_balances_aggregates_duplicate_cash_rows(pricing):
    collector = IbkrCollector(pricing, flex_token="tok", flex_query_id="qid")
    pricing._set_cache("USD", Decimal(1))

    request_resp = MagicMock(spec=httpx.Response)
    request_resp.text = (
        "<FlexStatementResponse><Status>Success</Status><ReferenceCode>REF1</ReferenceCode></FlexStatementResponse>"
    )
    request_resp.raise_for_status = MagicMock()

    statement_xml = """<FlexQueryResponse>
<FlexStatements>
<CashReport currency="USD" endingCash="1,500.00"/>
<CashReportCurrency currency="USD" endingSettledCash="250.00"/>
</FlexStatements>
</FlexQueryResponse>"""
    statement_resp = MagicMock(spec=httpx.Response)
    statement_resp.text = statement_xml
    statement_resp.raise_for_status = MagicMock()

    async def mock_get(url, **kwargs):
        if "SendRequest" in str(url):
            return request_resp
        return statement_resp

    collector._client.get = mock_get  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 1
    assert snapshots[0].asset == "USD"
    assert snapshots[0].amount == Decimal("1750.00")
    assert snapshots[0].usd_value == Decimal("1750.00")


async def test_ibkr_reuses_statement_between_balances_and_transactions(pricing):
    collector = IbkrCollector(pricing, flex_token="tok", flex_query_id="qid")
    pricing._set_cache("USD", Decimal(1))

    request_resp = MagicMock(spec=httpx.Response)
    request_resp.text = (
        "<FlexStatementResponse><Status>Success</Status><ReferenceCode>REF1</ReferenceCode></FlexStatementResponse>"
    )
    request_resp.raise_for_status = MagicMock()

    statement_xml = """<FlexQueryResponse>
<FlexStatements>
<OpenPosition symbol="AAPL" position="10" markMarketValue="1500.00"/>
<CashReport currency="USD" endingCash="5000.00"/>
<Trade symbol="AAPL" quantity="1" proceeds="100.00" tradeDate="2024-01-15" tradeID="tx1"/>
</FlexStatements>
</FlexQueryResponse>"""

    statement_resp = MagicMock(spec=httpx.Response)
    statement_resp.text = statement_xml
    statement_resp.raise_for_status = MagicMock()

    send_request_calls = 0

    async def mock_get(url, **kwargs):
        nonlocal send_request_calls
        if "SendRequest" in str(url):
            send_request_calls += 1
            return request_resp
        return statement_resp

    collector._client.get = mock_get  # type: ignore[assignment]

    snapshots = await collector.fetch_balances()
    txs = await collector.fetch_transactions()
    assert len(snapshots) == 2
    assert len(txs) == 1
    assert send_request_calls == 1


# ── Blend ─────────────────────────────────────────────────────────────


async def test_blend_fetch_balances_no_contract(pricing):
    collector = BlendCollector(pricing, stellar_address="GABC", pool_contract_id="", soroban_rpc_url="http://rpc")
    snapshots = await collector.fetch_balances()
    assert snapshots == []


async def test_blend_fetch_transactions(pricing):
    collector = BlendCollector(
        pricing, stellar_address="GABC", pool_contract_id="contract", soroban_rpc_url="http://rpc"
    )
    txs = await collector.fetch_transactions()
    assert txs == []


async def test_blend_fetch_balances_with_positions(pricing):
    collector = BlendCollector(
        pricing, stellar_address="GABC", pool_contract_id="contract", soroban_rpc_url="http://rpc"
    )

    mock_addr = MagicMock()
    mock_addr.address = "CCW67TSZV3SSS2HXMBQ5JFGCKJNXKZM7UQUWUZPUTHXSTZLEO7SJMI75"
    mock_addr.to_xdr_sc_val.return_value = MagicMock()

    collector._get_positions = MagicMock(return_value={"collateral": {1: 10000000000}, "supply": {}, "liabilities": {}})
    collector._get_reserve_list = MagicMock(return_value=[MagicMock(), mock_addr])
    collector._get_reserve = MagicMock(
        return_value={
            "data": {"b_rate": 1_000_000_000_000, "b_supply": 0, "d_supply": 0, "d_rate": 0, "ir_mod": 10_000_000},
            "config": {"r_base": 0, "r_one": 0, "r_two": 0, "r_three": 0, "util": 8_000_000},
            "scalar": 10_000_000,
        }
    )
    collector._get_pool_config = MagicMock(return_value={"bstop_rate": 2_000_000})

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 1
    assert snapshots[0].asset == "USDC"
    assert snapshots[0].amount == Decimal(1000)
    # With zero supply/borrow, APY should be 0
    assert snapshots[0].apy == Decimal(0)


async def test_blend_fetch_balances_empty_positions(pricing):
    collector = BlendCollector(
        pricing, stellar_address="GABC", pool_contract_id="contract", soroban_rpc_url="http://rpc"
    )
    collector._get_positions = MagicMock(return_value={"collateral": {}, "supply": {}, "liabilities": {}})
    collector._get_reserve_list = MagicMock(return_value=[])

    snapshots = await collector.fetch_balances()
    assert snapshots == []


async def test_blend_fetch_balances_rpc_error(pricing):
    collector = BlendCollector(
        pricing, stellar_address="GABC", pool_contract_id="contract", soroban_rpc_url="http://rpc"
    )
    collector._get_positions = MagicMock(side_effect=ValueError("RPC error"))

    snapshots = await collector.fetch_balances()
    assert snapshots == []


async def test_blend_resolve_ticker_known(pricing):
    collector = BlendCollector(
        pricing, stellar_address="GABC", pool_contract_id="contract", soroban_rpc_url="http://rpc"
    )
    mock_addr = MagicMock()
    mock_addr.address = "CCW67TSZV3SSS2HXMBQ5JFGCKJNXKZM7UQUWUZPUTHXSTZLEO7SJMI75"
    assert collector._resolve_ticker(mock_addr) == "USDC"

    mock_addr.address = "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA"
    assert collector._resolve_ticker(mock_addr) == "XLM"


async def test_blend_supply_and_collateral_merged(pricing):
    collector = BlendCollector(
        pricing, stellar_address="GABC", pool_contract_id="contract", soroban_rpc_url="http://rpc"
    )

    mock_addr = MagicMock()
    mock_addr.address = "CCW67TSZV3SSS2HXMBQ5JFGCKJNXKZM7UQUWUZPUTHXSTZLEO7SJMI75"
    mock_addr.to_xdr_sc_val.return_value = MagicMock()

    # Both supply and collateral at index 0
    collector._get_positions = MagicMock(
        return_value={
            "collateral": {0: 5000000000},
            "supply": {0: 5000000000},
            "liabilities": {},
        }
    )
    collector._get_reserve_list = MagicMock(return_value=[mock_addr])
    collector._get_reserve = MagicMock(
        return_value={
            "data": {"b_rate": 1_000_000_000_000, "b_supply": 0, "d_supply": 0, "d_rate": 0, "ir_mod": 10_000_000},
            "config": {"r_base": 0, "r_one": 0, "r_two": 0, "r_three": 0, "util": 8_000_000},
            "scalar": 10_000_000,
        }
    )
    collector._get_pool_config = MagicMock(return_value={"bstop_rate": 2_000_000})

    snapshots = await collector.fetch_balances()
    assert len(snapshots) == 1
    assert snapshots[0].amount == Decimal(1000)


async def test_blend_compute_supply_apy(pricing):
    """Test Blend supply APY calculation with realistic reserve data."""
    # USDC reserve data matching ~8.57% supply APR from research doc
    reserve = {
        "data": {
            "b_supply": 100_000_0000000,  # 100k USDC in bTokens (7-decimal scalar)
            "d_supply": 70_000_0000000,  # 70k USDC debt
            "b_rate": 1_050_000_000_000,  # b_rate (12-decimal)
            "d_rate": 1_100_000_000_000,  # d_rate (12-decimal)
            "ir_mod": 23_300_000,  # 2.33x multiplier (7-decimal)
        },
        "config": {
            "r_base": 300_000,
            "r_one": 400_000,
            "r_two": 1_200_000,
            "r_three": 50_000_000,
            "util": 8_000_000,  # 80% target utilization
        },
        "scalar": 10_000_000,
    }
    backstop_rate = Decimal("0.20")
    apy = BlendCollector._compute_supply_apy(reserve, backstop_rate)
    # Should be a positive APY
    assert apy > Decimal("0.01")
    # Should be in a reasonable range for DeFi lending
    assert apy < Decimal("0.50")


# ── KBank ─────────────────────────────────────────────────────────────


async def test_kbank_no_pdf_path(pricing):
    collector = KbankCollector(pricing)
    snapshots = await collector.fetch_balances()
    assert snapshots == []
    txs = await collector.fetch_transactions()
    assert txs == []


async def test_kbank_no_pdf_no_gmail_skips(pricing):
    """No pdf_path and no Gmail creds → skip without error."""
    collector = KbankCollector(pricing, gmail_address="", gmail_app_password="")
    snapshots = await collector.fetch_balances()
    assert snapshots == []


async def test_kbank_converts_thb_to_usd(pricing):
    collector = KbankCollector(pricing)
    fake_raw = RawBalance(
        asset="THB",
        amount=Decimal(1000),
        raw_json="{}",
        date=date(2024, 1, 15),
    )
    with patch.object(collector, "_parse_pdf", return_value=([fake_raw], [])):
        collector._pdf_path = Path("/tmp/fake.pdf")
        pricing._set_cache("THB", Decimal("0.028"))
        snapshots = await collector.fetch_balances()

    assert len(snapshots) == 1
    assert snapshots[0].usd_value == Decimal("28.000")


async def test_kbank_set_pdf_path(pricing):
    collector = KbankCollector(pricing)
    collector.set_pdf_path(Path("/tmp/test.pdf"))
    assert collector._pdf_path == Path("/tmp/test.pdf")


async def test_kbank_gmail_configured_property(pricing):
    collector = KbankCollector(pricing, gmail_address="a@b.com", gmail_app_password="pass")
    assert collector._gmail_configured is True

    collector2 = KbankCollector(pricing, gmail_address="", gmail_app_password="pass")
    assert collector2._gmail_configured is False

    collector3 = KbankCollector(pricing, gmail_address="a@b.com", gmail_app_password="")
    assert collector3._gmail_configured is False


async def test_kbank_fetch_pdf_from_gmail(pricing, tmp_path):
    """Gmail IMAP fetch returns a saved PDF path."""
    collector = KbankCollector(
        pricing,
        gmail_address="test@gmail.com",
        gmail_app_password="apppass",
        kbank_sender_email="kbank@test.com",
    )

    pdf_bytes = b"%PDF-1.4 fake content"

    # Build a fake email with a PDF attachment
    from email.mime.application import MIMEApplication
    from email.mime.multipart import MIMEMultipart

    msg = MIMEMultipart()
    msg["From"] = "kbank@test.com"
    msg["Subject"] = "Your Statement"
    attachment = MIMEApplication(pdf_bytes, _subtype="pdf")
    attachment.add_header("Content-Disposition", "attachment", filename="statement.pdf")
    msg.attach(attachment)
    raw_email = msg.as_bytes()

    mock_conn = MagicMock()
    mock_conn.login.return_value = ("OK", [b"Logged in"])
    mock_conn.select.return_value = ("OK", [b"1"])
    mock_conn.search.return_value = ("OK", [b"1 2 3"])
    mock_conn.fetch.return_value = ("OK", [(b"1 (RFC822 {100})", raw_email)])
    mock_conn.logout.return_value = ("BYE", [b"Logging out"])

    with (
        patch("pfm.collectors.kbank.imaplib.IMAP4_SSL", return_value=mock_conn),
        patch("pfm.collectors.kbank._KBANK_PDF_DIR", tmp_path),
    ):
        result = collector._fetch_pdf_from_gmail()

    assert result is not None
    assert result.name == "statement.pdf"
    assert result.read_bytes() == pdf_bytes
    mock_conn.login.assert_called_once_with("test@gmail.com", "apppass")
    mock_conn.search.assert_called_once_with(None, "FROM", '"kbank@test.com"')
    # Fetches the latest email (id "3")
    mock_conn.fetch.assert_called_once_with(b"3", "(RFC822)")


async def test_kbank_fetch_pdf_from_gmail_login_failure(pricing):
    """Gmail login failure returns None gracefully."""
    collector = KbankCollector(
        pricing,
        gmail_address="test@gmail.com",
        gmail_app_password="badpass",
    )

    mock_conn = MagicMock()
    mock_conn.login.side_effect = imaplib.IMAP4.error("auth failed")

    with patch("pfm.collectors.kbank.imaplib.IMAP4_SSL", return_value=mock_conn):
        result = collector._fetch_pdf_from_gmail()

    assert result is None


async def test_kbank_fetch_pdf_from_gmail_no_emails(pricing):
    """No matching emails returns None."""
    collector = KbankCollector(
        pricing,
        gmail_address="test@gmail.com",
        gmail_app_password="pass",
    )

    mock_conn = MagicMock()
    mock_conn.login.return_value = ("OK", [b"Logged in"])
    mock_conn.select.return_value = ("OK", [b"0"])
    mock_conn.search.return_value = ("OK", [b""])
    mock_conn.logout.return_value = ("BYE", [b"Logging out"])

    with patch("pfm.collectors.kbank.imaplib.IMAP4_SSL", return_value=mock_conn):
        result = collector._fetch_pdf_from_gmail()

    assert result is None


async def test_kbank_fetch_pdf_from_gmail_no_attachment(pricing):
    """Email without PDF attachment returns None."""
    collector = KbankCollector(
        pricing,
        gmail_address="test@gmail.com",
        gmail_app_password="pass",
    )

    # Plain text email, no PDF
    from email.mime.text import MIMEText

    msg = MIMEText("Your statement is ready")
    msg["From"] = "kbank@test.com"
    raw_email = msg.as_bytes()

    mock_conn = MagicMock()
    mock_conn.login.return_value = ("OK", [b"Logged in"])
    mock_conn.select.return_value = ("OK", [b"1"])
    mock_conn.search.return_value = ("OK", [b"1"])
    mock_conn.fetch.return_value = ("OK", [(b"1 (RFC822 {100})", raw_email)])
    mock_conn.logout.return_value = ("BYE", [b"Logging out"])

    with patch("pfm.collectors.kbank.imaplib.IMAP4_SSL", return_value=mock_conn):
        result = collector._fetch_pdf_from_gmail()

    assert result is None


async def test_kbank_auto_fetch_from_gmail(pricing, tmp_path):
    """fetch_balances() auto-fetches from Gmail when no pdf_path is set."""
    collector = KbankCollector(
        pricing,
        gmail_address="test@gmail.com",
        gmail_app_password="pass",
    )

    fake_pdf = tmp_path / "auto.pdf"
    fake_pdf.write_bytes(b"not a real pdf")

    with (
        patch.object(collector, "_fetch_pdf_from_gmail", return_value=fake_pdf) as mock_fetch,
        patch.object(collector, "_parse_pdf", return_value=([], [])) as mock_parse,
    ):
        await collector.fetch_balances()

    mock_fetch.assert_called_once()
    mock_parse.assert_called_once_with(fake_pdf)


async def test_kbank_auto_fetch_skipped_when_pdf_path_set(pricing, tmp_path):
    """fetch_balances() does NOT call Gmail when pdf_path is already set."""
    fake_pdf = tmp_path / "manual.pdf"
    fake_pdf.write_bytes(b"not a real pdf")

    collector = KbankCollector(
        pricing,
        pdf_path=fake_pdf,
        gmail_address="test@gmail.com",
        gmail_app_password="pass",
    )

    with (
        patch.object(collector, "_fetch_pdf_from_gmail") as mock_fetch,
        patch.object(collector, "_parse_pdf", return_value=([], [])),
    ):
        await collector.fetch_balances()

    mock_fetch.assert_not_called()


def test_kbank_parse_tx_date():
    assert KbankCollector._parse_tx_date("01-02-26 12:09 Pa") == date(2026, 2, 1)
    assert KbankCollector._parse_tx_date("15-01-24 Be") == date(2024, 1, 15)
    assert KbankCollector._parse_tx_date("invalid") is None
    assert KbankCollector._parse_tx_date("") is None
    assert KbankCollector._parse_tx_date("short") is None


def test_kbank_parse_amount():
    assert KbankCollector._parse_amount("1,234.56") == Decimal("1234.56")
    assert KbankCollector._parse_amount("100.00") == Decimal("100.00")
    assert KbankCollector._parse_amount("-50.00") == Decimal("-50.00")
    assert KbankCollector._parse_amount("") is None
    assert KbankCollector._parse_amount("-") is None
    assert KbankCollector._parse_amount("abc") is None
    assert KbankCollector._parse_amount("  ") is None


def test_kbank_parse_header_balance(pricing):
    collector = KbankCollector(pricing)
    table = [
        ["Reference Code", "26022716366931630111"],
        ["Account Number", "163-8-08872-6"],
        ["Ending Balance 42,327.80", None],
    ]
    assert collector._parse_header_balance(table) == Decimal("42327.80")


def test_kbank_parse_header_balance_missing(pricing):
    collector = KbankCollector(pricing)
    table = [["Reference Code", "123"], ["Account Number", "456"]]
    assert collector._parse_header_balance(table) == Decimal(0)


def test_kbank_assign_words_to_columns():
    """Words are assigned to the correct column by X position."""
    col_bounds = [(66.0, 129.1), (129.1, 202.0), (202.0, 270.0), (270.0, 332.0), (332.0, 403.0), (403.0, 535.0)]
    words = [
        {"x0": 67.9, "text": "01-10-25"},
        {"x0": 101.2, "text": "02:16"},
        {"x0": 123.0, "text": "Debit Card Spending"},  # near col 0/1 boundary → col 1
        {"x0": 229.4, "text": "1,800.00"},
        {"x0": 303.1, "text": "19,991.20"},
        {"x0": 333.0, "text": "EDC/E-Commerce"},
        {"x0": 404.0, "text": "Ref Code EDC50445"},
    ]
    result = KbankCollector._assign_words_to_columns(words, col_bounds)
    assert result[0] == "01-10-25 02:16"
    assert result[1] == "Debit Card Spending"
    assert result[2] == "1,800.00"
    assert result[3] == "19,991.20"
    assert result[4] == "EDC/E-Commerce"
    assert result[5] == "Ref Code EDC50445"


def test_kbank_group_words_into_rows():
    """Words at similar Y positions are grouped into rows."""
    words = [
        {"top": 200.0, "x0": 68, "text": "01-10-25"},
        {"top": 200.0, "x0": 404, "text": "Ref Code EDC50445"},
        {"top": 212.0, "x0": 68, "text": "01-10-25"},
        {"top": 212.0, "x0": 404, "text": "Paid for Ref X123"},
        {"top": 223.5, "x0": 404, "text": "Ksher_FITNESS"},  # continuation (no date)
    ]
    rows = KbankCollector._group_words_into_rows(words)
    assert len(rows) == 3
    assert len(rows[0]) == 2  # first tx + detail
    assert len(rows[1]) == 2  # second tx + detail
    assert len(rows[2]) == 1  # continuation (detail only)


async def test_kbank_fetch_transactions_with_cache(pricing):
    collector = KbankCollector(pricing)
    collector._cached_transactions = [
        Transaction(
            date=date(2024, 1, 15),
            source="kbank",
            tx_type=TransactionType.DEPOSIT,
            asset="THB",
            amount=Decimal(100),
            usd_value=Decimal(0),
        ),
        Transaction(
            date=date(2024, 1, 10),
            source="kbank",
            tx_type=TransactionType.DEPOSIT,
            asset="THB",
            amount=Decimal(50),
            usd_value=Decimal(0),
        ),
    ]
    txs = await collector.fetch_transactions(since=date(2024, 1, 12))
    assert len(txs) == 1


async def test_kbank_tracks_last_statement_date(pricing):
    collector = KbankCollector(pricing)
    fake_raw = RawBalance(
        asset="THB",
        amount=Decimal(1000),
        raw_json="{}",
        date=date(2026, 2, 27),
    )
    fake_txs = [
        Transaction(
            date=date(2026, 2, 24),
            source="kbank",
            tx_type=TransactionType.DEPOSIT,
            asset="THB",
            amount=Decimal(100),
            usd_value=Decimal(0),
        ),
        Transaction(
            date=date(2026, 2, 25),
            source="kbank",
            tx_type=TransactionType.WITHDRAWAL,
            asset="THB",
            amount=Decimal(50),
            usd_value=Decimal(0),
        ),
    ]
    with patch.object(collector, "_parse_pdf", return_value=([fake_raw], fake_txs)):
        collector._pdf_path = Path("/tmp/fake.pdf")
        pricing._set_cache("THB", Decimal("0.028"))
        await collector.fetch_balances()

    # last_statement_date comes from the RawBalance.date (Period end date)
    assert collector.last_statement_date == date(2026, 2, 27)


# ── Collector Registry ────────────────────────────────────────────────


def test_collector_registry_populated():
    assert "lobstr" in COLLECTOR_REGISTRY
    assert "binance" in COLLECTOR_REGISTRY
    assert "binance_th" in COLLECTOR_REGISTRY
    assert "mexc" in COLLECTOR_REGISTRY
    assert "cash" in COLLECTOR_REGISTRY
    assert "coinex" in COLLECTOR_REGISTRY
    assert "okx" in COLLECTOR_REGISTRY
    assert "bybit" in COLLECTOR_REGISTRY
    assert "wise" in COLLECTOR_REGISTRY
    assert "ibkr" in COLLECTOR_REGISTRY
    assert "blend" in COLLECTOR_REGISTRY
    assert "kbank" in COLLECTOR_REGISTRY
    assert "rabby" in COLLECTOR_REGISTRY
    assert "bitget_wallet" in COLLECTOR_REGISTRY
    assert "revolut" in COLLECTOR_REGISTRY
    assert "yo" in COLLECTOR_REGISTRY
