"""Tests for transaction grouping logic."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

from pfm.analytics.transaction_grouper import group_transactions
from pfm.db.models import Transaction, TransactionMetadata, TransactionType

W = TransactionType.WITHDRAWAL
D = TransactionType.DEPOSIT
T = TransactionType.TRADE


def _tx(
    *,
    tx_id: int,
    source_name: str = "coinex",
    tx_type: TransactionType = T,
    asset: str = "USDT",
    amount: Decimal = Decimal(100),
    usd_value: Decimal = Decimal(100),
    tx_date: date = date(2026, 3, 10),
    trade_side: str = "",
    raw_json: str = "",
) -> Transaction:
    return Transaction(
        id=tx_id,
        date=tx_date,
        source=source_name,
        source_name=source_name,
        tx_type=tx_type,
        asset=asset,
        amount=amount,
        usd_value=usd_value,
        trade_side=trade_side,
        raw_json=raw_json,
    )


def _meta(
    tx_id: int,
    *,
    is_internal_transfer: bool = False,
    transfer_pair_id: int | None = None,
) -> TransactionMetadata:
    return TransactionMetadata(
        transaction_id=tx_id,
        is_internal_transfer=is_internal_transfer,
        transfer_pair_id=transfer_pair_id,
    )


def _rj(**kwargs: object) -> str:
    return json.dumps(kwargs)


class TestInternalTransfers:
    def test_linked_pair_grouped(self) -> None:
        items = [
            (
                _tx(tx_id=1, source_name="okx", tx_type=W, asset="USDC"),
                _meta(1, is_internal_transfer=True, transfer_pair_id=2),
            ),
            (
                _tx(tx_id=2, source_name="binance", tx_type=D, asset="USDC"),
                _meta(2, is_internal_transfer=True, transfer_pair_id=1),
            ),
        ]
        result = group_transactions(items)
        assert len(result.groups) == 1
        g = result.groups[0]
        assert g.group_type == "internal_transfer"
        assert g.from_source == "okx"
        assert g.to_source == "binance"
        assert g.child_ids == [1, 2]
        assert len(result.ungrouped) == 0

    def test_missing_pair_not_grouped(self) -> None:
        items = [
            (
                _tx(tx_id=1, source_name="okx", tx_type=W),
                _meta(1, is_internal_transfer=True, transfer_pair_id=99),
            ),
        ]
        result = group_transactions(items)
        assert len(result.groups) == 0
        assert len(result.ungrouped) == 1


class TestTradePairs:
    def test_coinex_swap_grouped(self) -> None:
        """CoinEx 6-row swap (USDT -> USDC) grouped into 1."""
        ts = 1710000000000
        items = [
            (
                _tx(
                    tx_id=1,
                    asset="USDT",
                    amount=Decimal("994.11"),
                    usd_value=Decimal("994.11"),
                    trade_side="sell",
                    raw_json=_rj(created_at=ts),
                ),
                None,
            ),
            (
                _tx(
                    tx_id=2,
                    asset="USDT",
                    amount=Decimal("5.89"),
                    usd_value=Decimal("5.89"),
                    trade_side="sell",
                    raw_json=_rj(created_at=ts + 1),
                ),
                None,
            ),
            (
                _tx(
                    tx_id=3,
                    asset="USDT",
                    amount=Decimal("0.99"),
                    usd_value=Decimal(0),
                    trade_side="sell",
                    raw_json=_rj(created_at=ts + 1),
                ),
                None,
            ),
            (
                _tx(
                    tx_id=4,
                    asset="USDT",
                    amount=Decimal("0.006"),
                    usd_value=Decimal(0),
                    trade_side="sell",
                    raw_json=_rj(created_at=ts + 2),
                ),
                None,
            ),
            (
                _tx(
                    tx_id=5,
                    asset="USDC",
                    amount=Decimal("994.11"),
                    usd_value=Decimal("994.11"),
                    trade_side="buy",
                    raw_json=_rj(created_at=ts + 1),
                ),
                None,
            ),
            (
                _tx(
                    tx_id=6,
                    asset="USDC",
                    amount=Decimal("5.89"),
                    usd_value=Decimal("5.89"),
                    trade_side="buy",
                    raw_json=_rj(created_at=ts + 2),
                ),
                None,
            ),
        ]
        result = group_transactions(items)
        assert len(result.groups) == 1
        g = result.groups[0]
        assert g.group_type == "trade_pair"
        assert g.from_asset == "USDT"
        assert g.to_asset == "USDC"
        assert len(g.child_ids) == 6
        assert len(result.ungrouped) == 0

    def test_ibkr_multi_leg_grouped(self) -> None:
        """IBKR 4-row: 3x GBP.USD sells + 1x VXUS buy."""
        ts = "2026-03-10T14:30:00"
        ts1 = "2026-03-10T14:30:01"
        items = [
            (
                _tx(
                    tx_id=10,
                    source_name="ibkr",
                    asset="GBP.USD",
                    amount=Decimal("0.31"),
                    usd_value=Decimal("0.31"),
                    trade_side="sell",
                    raw_json=_rj(dateTime=ts),
                ),
                None,
            ),
            (
                _tx(
                    tx_id=11,
                    source_name="ibkr",
                    asset="GBP.USD",
                    amount=Decimal("0.12"),
                    usd_value=Decimal("0.12"),
                    trade_side="sell",
                    raw_json=_rj(dateTime=ts),
                ),
                None,
            ),
            (
                _tx(
                    tx_id=12,
                    source_name="ibkr",
                    asset="GBP.USD",
                    amount=Decimal("3489.62"),
                    usd_value=Decimal("3489.62"),
                    trade_side="sell",
                    raw_json=_rj(dateTime=ts1),
                ),
                None,
            ),
            (
                _tx(
                    tx_id=13,
                    source_name="ibkr",
                    asset="VXUS",
                    amount=Decimal(50),
                    usd_value=Decimal("3489.75"),
                    trade_side="buy",
                    raw_json=_rj(dateTime=ts1),
                ),
                None,
            ),
        ]
        result = group_transactions(items)
        assert len(result.groups) == 1
        g = result.groups[0]
        assert g.group_type == "trade_pair"
        assert g.from_asset == "GBP.USD"
        assert g.to_asset == "VXUS"
        assert len(g.child_ids) == 4

    def test_different_dates_not_grouped(self) -> None:
        """Trades on different dates stay separate."""
        items = [
            (_tx(tx_id=1, asset="USDT", tx_date=date(2026, 3, 10)), None),
            (_tx(tx_id=2, asset="USDC", tx_date=date(2026, 3, 11)), None),
        ]
        result = group_transactions(items)
        assert len(result.groups) == 0
        assert len(result.ungrouped) == 2


class TestPartialFills:
    def test_same_asset_fills_merged(self) -> None:
        """Multiple small trades of same asset on same day merge."""
        items = [
            (
                _tx(
                    tx_id=1,
                    asset="BTC",
                    amount=Decimal("0.01"),
                    usd_value=Decimal(500),
                    tx_type=D,
                ),
                None,
            ),
            (
                _tx(
                    tx_id=2,
                    asset="BTC",
                    amount=Decimal("0.02"),
                    usd_value=Decimal(1000),
                    tx_type=D,
                ),
                None,
            ),
            (
                _tx(
                    tx_id=3,
                    asset="BTC",
                    amount=Decimal("0.005"),
                    usd_value=Decimal(250),
                    tx_type=D,
                ),
                None,
            ),
        ]
        result = group_transactions(items)
        assert len(result.groups) == 1
        g = result.groups[0]
        assert g.group_type == "partial_fill"
        assert g.from_amount == Decimal("0.035")
        assert g.display_usd_value == Decimal(1750)

    def test_single_tx_not_grouped(self) -> None:
        items = [(_tx(tx_id=1), None)]
        result = group_transactions(items)
        assert len(result.groups) == 0
        assert len(result.ungrouped) == 1


class TestPassOrdering:
    def test_transfers_consume_before_trade_pairs(self) -> None:
        """Internal transfers consumed first, not as trade pairs."""
        items = [
            (
                _tx(tx_id=1, source_name="okx", tx_type=W, asset="USDC"),
                _meta(1, is_internal_transfer=True, transfer_pair_id=2),
            ),
            (
                _tx(tx_id=2, source_name="binance", tx_type=D, asset="USDC"),
                _meta(2, is_internal_transfer=True, transfer_pair_id=1),
            ),
        ]
        result = group_transactions(items)
        assert len(result.groups) == 1
        assert result.groups[0].group_type == "internal_transfer"


class TestEmptyInput:
    def test_empty_list(self) -> None:
        result = group_transactions([])
        assert len(result.groups) == 0
        assert len(result.ungrouped) == 0
        assert result.total_ungrouped == 0
