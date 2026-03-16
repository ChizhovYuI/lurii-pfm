"""Tests for internal transfer detection."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pfm.analytics.transfer_detector import detect_transfer_pairs
from pfm.db.models import Transaction, TransactionType


def _tx(
    *,
    tx_id: int,
    source_name: str,
    tx_type: TransactionType,
    asset: str = "USDC",
    amount: Decimal = Decimal(2000),
    tx_date: date = date(2026, 3, 1),
) -> Transaction:
    return Transaction(
        id=tx_id,
        date=tx_date,
        source=source_name,
        source_name=source_name,
        tx_type=tx_type,
        asset=asset,
        amount=amount,
        usd_value=amount,
    )


class TestDetectTransferPairs:
    def test_perfect_match_same_day(self) -> None:
        txs = [
            _tx(tx_id=1, source_name="bitget_wallet", tx_type=TransactionType.WITHDRAWAL),
            _tx(tx_id=2, source_name="coinex", tx_type=TransactionType.DEPOSIT),
        ]
        pairs = detect_transfer_pairs(txs)
        assert len(pairs) == 1
        assert pairs[0].tx_id_a == 1  # outflow
        assert pairs[0].tx_id_b == 2  # inflow
        assert pairs[0].score > 0.9

    def test_no_match_same_source(self) -> None:
        txs = [
            _tx(tx_id=1, source_name="okx", tx_type=TransactionType.WITHDRAWAL),
            _tx(tx_id=2, source_name="okx", tx_type=TransactionType.DEPOSIT),
        ]
        pairs = detect_transfer_pairs(txs)
        assert len(pairs) == 0

    def test_no_match_different_asset(self) -> None:
        txs = [
            _tx(tx_id=1, source_name="okx", tx_type=TransactionType.WITHDRAWAL, asset="BTC"),
            _tx(tx_id=2, source_name="binance", tx_type=TransactionType.DEPOSIT, asset="ETH"),
        ]
        pairs = detect_transfer_pairs(txs)
        assert len(pairs) == 0

    def test_stablecoin_equivalence(self) -> None:
        txs = [
            _tx(tx_id=1, source_name="okx", tx_type=TransactionType.WITHDRAWAL, asset="USDC"),
            _tx(tx_id=2, source_name="binance", tx_type=TransactionType.DEPOSIT, asset="USDC-BASE"),
        ]
        pairs = detect_transfer_pairs(txs)
        assert len(pairs) == 1

    def test_amount_within_tolerance(self) -> None:
        txs = [
            _tx(
                tx_id=1,
                source_name="okx",
                tx_type=TransactionType.WITHDRAWAL,
                amount=Decimal(2000),
            ),
            _tx(
                tx_id=2,
                source_name="binance",
                tx_type=TransactionType.DEPOSIT,
                amount=Decimal(1950),  # 2.5% less (within 5% tolerance)
            ),
        ]
        pairs = detect_transfer_pairs(txs)
        assert len(pairs) == 1

    def test_amount_outside_tolerance(self) -> None:
        txs = [
            _tx(
                tx_id=1,
                source_name="okx",
                tx_type=TransactionType.WITHDRAWAL,
                amount=Decimal(2000),
            ),
            _tx(
                tx_id=2,
                source_name="binance",
                tx_type=TransactionType.DEPOSIT,
                amount=Decimal(1800),  # 10% less (outside 5% tolerance)
            ),
        ]
        pairs = detect_transfer_pairs(txs)
        assert len(pairs) == 0

    def test_date_within_window(self) -> None:
        txs = [
            _tx(
                tx_id=1,
                source_name="okx",
                tx_type=TransactionType.WITHDRAWAL,
                tx_date=date(2026, 3, 1),
            ),
            _tx(
                tx_id=2,
                source_name="binance",
                tx_type=TransactionType.DEPOSIT,
                tx_date=date(2026, 3, 3),  # 2 days later
            ),
        ]
        pairs = detect_transfer_pairs(txs)
        assert len(pairs) == 1

    def test_date_outside_window(self) -> None:
        txs = [
            _tx(
                tx_id=1,
                source_name="okx",
                tx_type=TransactionType.WITHDRAWAL,
                tx_date=date(2026, 3, 1),
            ),
            _tx(
                tx_id=2,
                source_name="binance",
                tx_type=TransactionType.DEPOSIT,
                tx_date=date(2026, 3, 10),  # 9 days later
            ),
        ]
        pairs = detect_transfer_pairs(txs)
        assert len(pairs) == 0

    def test_greedy_matching_no_reuse(self) -> None:
        """Each transaction should only appear in one pair."""
        txs = [
            _tx(tx_id=1, source_name="okx", tx_type=TransactionType.WITHDRAWAL),
            _tx(tx_id=2, source_name="binance", tx_type=TransactionType.DEPOSIT),
            _tx(tx_id=3, source_name="bybit", tx_type=TransactionType.DEPOSIT),
        ]
        pairs = detect_transfer_pairs(txs)
        assert len(pairs) == 1
        used_ids = {pairs[0].tx_id_a, pairs[0].tx_id_b}
        assert len(used_ids) == 2

    def test_transfer_type_matching(self) -> None:
        txs = [
            _tx(tx_id=1, source_name="okx", tx_type=TransactionType.TRANSFER),
            _tx(tx_id=2, source_name="binance", tx_type=TransactionType.TRANSFER),
        ]
        pairs = detect_transfer_pairs(txs)
        # Both are TRANSFER type, one can be outflow and the other inflow.
        assert len(pairs) == 1

    def test_empty_list(self) -> None:
        pairs = detect_transfer_pairs([])
        assert len(pairs) == 0

    def test_no_id_skipped(self) -> None:
        txs = [
            Transaction(
                id=None,
                date=date(2026, 3, 1),
                source="okx",
                source_name="okx",
                tx_type=TransactionType.WITHDRAWAL,
                asset="USDC",
                amount=Decimal(2000),
                usd_value=Decimal(2000),
            ),
            _tx(tx_id=2, source_name="binance", tx_type=TransactionType.DEPOSIT),
        ]
        pairs = detect_transfer_pairs(txs)
        assert len(pairs) == 0
