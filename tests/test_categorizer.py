"""Tests for compound category rule categorizer."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pfm.analytics.categorizer import categorize_batch, categorize_transaction
from pfm.db.models import CategoryRule, Transaction, TransactionMetadata, TransactionType, effective_type


def _tx(
    *,
    source_name: str = "okx",
    tx_type: TransactionType = TransactionType.TRADE,
    asset: str = "BTC",
    amount: Decimal = Decimal("0.1"),
    usd_value: Decimal = Decimal(5000),
    tx_id: int | None = 1,
    raw_json: str = "",
) -> Transaction:
    return Transaction(
        id=tx_id,
        date=date(2026, 3, 1),
        source=source_name.split("-", maxsplit=1)[0] if "-" in source_name else source_name,
        source_name=source_name,
        tx_type=tx_type,
        asset=asset,
        amount=amount,
        usd_value=usd_value,
        raw_json=raw_json,
    )


def _rule(
    *,
    type_match: str = "trade",
    result_category: str = "spot_trade",
    type_operator: str = "eq",
    field_name: str = "",
    field_operator: str = "",
    field_value: str = "",
    source: str = "*",
    priority: int = 300,
    builtin: bool = True,
    rule_id: int = 1,
) -> CategoryRule:
    return CategoryRule(
        id=rule_id,
        type_match=type_match,
        type_operator=type_operator,
        field_name=field_name,
        field_operator=field_operator,
        field_value=field_value,
        source=source,
        result_category=result_category,
        priority=priority,
        builtin=builtin,
    )


class TestEffectiveType:
    def test_no_metadata(self) -> None:
        tx = _tx()
        assert effective_type(tx, None) == "trade"

    def test_no_override(self) -> None:
        tx = _tx()
        meta = TransactionMetadata(transaction_id=1, category="spot_trade")
        assert effective_type(tx, meta) == "trade"

    def test_with_override(self) -> None:
        tx = _tx()
        meta = TransactionMetadata(transaction_id=1, type_override="spend")
        assert effective_type(tx, meta) == "spend"


class TestCompoundRuleMatching:
    def test_type_only_rule_matches(self) -> None:
        tx = _tx(tx_type=TransactionType.TRADE)
        rules = [_rule(type_match="trade", result_category="spot_trade")]
        result = categorize_transaction(tx, rules)
        assert result is not None
        assert result.category == "spot_trade"
        assert result.source == "rule"

    def test_type_mismatch_skips(self) -> None:
        tx = _tx(tx_type=TransactionType.SPEND)
        rules = [_rule(type_match="trade", result_category="spot_trade")]
        result = categorize_transaction(tx, rules)
        assert result is None  # No fallback for spend+crypto

    def test_compound_rule_with_field(self) -> None:
        tx = _tx(
            source_name="kbank",
            tx_type=TransactionType.SPEND,
            raw_json='{"description": "Debit Card Spending"}',
        )
        rules = [
            _rule(
                type_match="spend",
                field_name="description",
                field_operator="eq",
                field_value="Debit Card Spending",
                result_category="shopping",
                source="kbank",
                priority=100,
                rule_id=1,
            ),
            _rule(type_match="spend", result_category="other_spend", priority=300, rule_id=2),
        ]
        result = categorize_transaction(tx, rules)
        assert result is not None
        assert result.category == "shopping"
        assert result.rule_id == 1

    def test_compound_rule_priority_over_type_only(self) -> None:
        tx = _tx(
            source_name="kbank",
            tx_type=TransactionType.FEE,
            raw_json='{"description": "Annual Debit Card Fee"}',
        )
        rules = [
            _rule(
                type_match="fee",
                field_name="description",
                field_operator="eq",
                field_value="Annual Debit Card Fee",
                result_category="bank_fee",
                source="kbank",
                priority=100,
                rule_id=1,
            ),
            _rule(type_match="fee", result_category="fee", priority=300, rule_id=2),
        ]
        result = categorize_transaction(tx, rules)
        assert result is not None
        assert result.category == "bank_fee"

    def test_source_filter(self) -> None:
        tx = _tx(source_name="wise", tx_type=TransactionType.FEE)
        rules = [
            _rule(type_match="fee", result_category="bank_fee", source="kbank", priority=200, rule_id=1),
            _rule(type_match="fee", result_category="fee", priority=300, rule_id=2),
        ]
        result = categorize_transaction(tx, rules)
        assert result is not None
        # kbank rule skipped (source filter), generic fee matched.
        assert result.category == "fee"

    def test_contains_operator(self) -> None:
        tx = _tx(
            source_name="kbank",
            tx_type=TransactionType.SPEND,
            raw_json='{"description": "QR code payment at shop"}',
        )
        rules = [
            _rule(
                type_match="spend",
                field_name="description",
                field_operator="contains",
                field_value="qr",
                result_category="other_spend",
                priority=100,
            ),
        ]
        result = categorize_transaction(tx, rules)
        assert result is not None
        assert result.category == "other_spend"

    def test_array_field_value(self) -> None:
        tx = _tx(
            source_name="kbank",
            tx_type=TransactionType.SPEND,
            raw_json='{"description": "Direct Debit"}',
        )
        rules = [
            _rule(
                type_match="spend",
                field_name="description",
                field_operator="eq",
                field_value='["Payment", "Direct Debit"]',
                result_category="subscriptions",
                priority=100,
            ),
        ]
        result = categorize_transaction(tx, rules)
        assert result is not None
        assert result.category == "subscriptions"

    def test_deleted_rule_skipped(self) -> None:
        tx = _tx(tx_type=TransactionType.TRADE)
        rule = CategoryRule(
            id=1,
            type_match="trade",
            result_category="spot_trade",
            priority=300,
            deleted=True,
        )
        result = categorize_transaction(tx, [rule])
        assert result is None  # Deleted rule skipped, no heuristic for crypto+trade

    def test_type_override_changes_matching(self) -> None:
        tx = _tx(tx_type=TransactionType.WITHDRAWAL)
        meta = TransactionMetadata(transaction_id=1, type_override="spend")
        rules = [
            _rule(type_match="spend", result_category="other_spend", priority=300),
        ]
        result = categorize_transaction(tx, rules, meta)
        assert result is not None
        assert result.category == "other_spend"

    def test_type_override_no_rule_match_returns_heuristic_or_none(self) -> None:
        """After type override, if no rule matches, heuristic may match (bank) or None (crypto)."""
        # Crypto source with overridden type "yield" — no rule, no heuristic.
        tx = _tx(source_name="okx", tx_type=TransactionType.TRADE)
        meta = TransactionMetadata(transaction_id=1, type_override="yield")
        result = categorize_transaction(tx, [], meta)
        assert result is None  # No rule, no bank heuristic for crypto.

    def test_type_override_falls_to_bank_heuristic(self) -> None:
        """Bank transaction with type override still gets bank description heuristic."""
        tx = _tx(
            source_name="kbank",
            tx_type=TransactionType.WITHDRAWAL,
            raw_json='{"description": "QRyment"}',
        )
        meta = TransactionMetadata(transaction_id=1, type_override="spend")
        result = categorize_transaction(tx, [], meta)
        assert result is not None
        assert result.category == "other_spend"
        assert result.source == "heuristic"

    def test_builtin_vs_user_confidence(self) -> None:
        tx = _tx(tx_type=TransactionType.TRADE)
        builtin_rule = _rule(builtin=True)
        user_rule = _rule(builtin=False)

        result_builtin = categorize_transaction(tx, [builtin_rule])
        result_user = categorize_transaction(tx, [user_rule])

        assert result_builtin is not None
        assert result_user is not None
        assert result_builtin.confidence == 0.90
        assert result_user.confidence == 0.95


class TestBankDescriptionHeuristics:
    def test_qr_payment_is_spend(self) -> None:
        tx = _tx(
            source_name="kbank",
            tx_type=TransactionType.SPEND,
            asset="THB",
            raw_json='{"description": "QRyment", "balance": "45457.78"}',
        )
        result = categorize_transaction(tx, [])
        assert result is not None
        assert result.category == "other_spend"
        assert result.confidence == 0.7

    def test_card_spending_is_shopping(self) -> None:
        tx = _tx(
            source_name="kbank",
            tx_type=TransactionType.SPEND,
            asset="THB",
            raw_json='{"description": "Pabit Card Spending", "balance": "3141.94"}',
        )
        result = categorize_transaction(tx, [])
        assert result is not None
        assert result.category == "shopping"

    def test_salary_deposit(self) -> None:
        tx = _tx(
            source_name="kbank",
            tx_type=TransactionType.DEPOSIT,
            asset="THB",
            raw_json='{"description": "Salary", "balance": "90000.00"}',
        )
        result = categorize_transaction(tx, [])
        assert result is not None
        assert result.category == "salary"

    def test_non_bank_ignores_description(self) -> None:
        tx = _tx(
            source_name="okx",
            tx_type=TransactionType.WITHDRAWAL,
            raw_json='{"description": "QRyment", "balance": "100"}',
        )
        result = categorize_transaction(tx, [])
        # OKX is crypto, not bank — description heuristic skipped.
        assert result is None

    def test_direct_debit_is_subscriptions(self) -> None:
        tx = _tx(
            source_name="kbank",
            tx_type=TransactionType.SPEND,
            asset="THB",
            raw_json='{"description": "Direct Debit subscription", "balance": "1000"}',
        )
        result = categorize_transaction(tx, [])
        assert result is not None
        assert result.category == "subscriptions"


class TestCategorizeBatch:
    def test_batch_returns_all_results(self) -> None:
        txs = [
            _tx(source_name="okx", tx_id=1, tx_type=TransactionType.TRADE),
            _tx(source_name="kbank", tx_type=TransactionType.SPEND, tx_id=2, raw_json='{"description": "QRyment"}'),
        ]
        rules = [_rule(type_match="trade", result_category="spot_trade")]
        results = categorize_batch(txs, rules)
        assert len(results) == 2
        # OKX trade matched by rule.
        assert results[0][1] is not None
        assert results[0][1].category == "spot_trade"
        # KBank spend: no rule, falls to heuristic.
        assert results[1][1] is not None
        assert results[1][1].category == "other_spend"
