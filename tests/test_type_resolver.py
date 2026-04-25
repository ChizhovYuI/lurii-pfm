"""Tests for type_resolver module."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

from pfm.analytics.type_resolver import resolve_type, resolve_type_batch
from pfm.db.models import Transaction, TransactionType, TypeRule

# ── Fixtures ──────────────────────────────────────────────────────────


def _tx(
    *,
    source: str = "okx",
    source_name: str = "",
    raw_json: str = "",
    tx_id: int | None = 1,
) -> Transaction:
    return Transaction(
        id=tx_id,
        date=date(2026, 3, 16),
        source=source,
        source_name=source_name or source,
        tx_type=TransactionType.UNKNOWN,
        asset="USDC",
        amount=Decimal(100),
        usd_value=Decimal(100),
        raw_json=raw_json,
    )


def _rule(
    source: str = "*",
    field_name: str = "",
    field_operator: str = "eq",
    field_value: str = "",
    result_type: str = "trade",
    priority: int = 100,
    deleted: bool = False,
) -> TypeRule:
    # Stage 3 (ADR-030): rules carry source_type instead of source. Map the
    # legacy alias ("*" = catch-all) to the new XOR-pair shape.
    source_type = None if source in {"*", ""} else source
    return TypeRule(
        source_type=source_type,
        field_name=field_name,
        field_operator=field_operator,
        field_value=field_value,
        result_type=result_type,
        priority=priority,
        deleted=deleted,
    )


class TestResolveType:
    def test_source_only_fallback(self):
        rules = [_rule(source="ibkr", result_type="trade", priority=300)]
        assert resolve_type(_tx(source="ibkr"), rules) == TransactionType.TRADE

    def test_source_mismatch(self):
        rules = [_rule(source="ibkr", result_type="trade", priority=300)]
        assert resolve_type(_tx(source="okx"), rules) is None

    def test_wildcard_source(self):
        rules = [_rule(field_name="type", field_value="TRADE", result_type="trade")]
        assert resolve_type(_tx(raw_json='{"type": "TRADE"}'), rules) == TransactionType.TRADE

    def test_field_eq_match(self):
        rules = [_rule(source="bybit", field_name="type", field_value="DEPOSIT", result_type="deposit")]
        assert resolve_type(_tx(source="bybit", raw_json='{"type": "DEPOSIT"}'), rules) == TransactionType.DEPOSIT

    def test_field_eq_no_match(self):
        rules = [_rule(source="bybit", field_name="type", field_value="DEPOSIT", result_type="deposit")]
        assert resolve_type(_tx(source="bybit", raw_json='{"type": "TRADE"}'), rules) is None

    def test_field_contains_match(self):
        rule = _rule(
            source="bybit",
            field_name="type",
            field_operator="contains",
            field_value="INTEREST",
            result_type="yield",
        )
        tx = _tx(source="bybit", raw_json='{"type": "EARN_INTEREST"}')
        assert resolve_type(tx, [rule]) == TransactionType.YIELD

    def test_json_array_values(self):
        rules = [_rule(source="okx", field_name="subType", field_value='["1","2"]', result_type="trade")]
        assert resolve_type(_tx(source="okx", raw_json='{"subType": "1"}'), rules) == TransactionType.TRADE
        assert resolve_type(_tx(source="okx", raw_json='{"subType": "3"}'), rules) is None

    def test_priority_order_first_match_wins(self):
        rules = [
            _rule(source="coinex", field_name="type", field_value="trade", result_type="trade", priority=100),
            _rule(source="coinex", result_type="transfer", priority=300),
        ]
        assert resolve_type(_tx(source="coinex", raw_json='{"type": "trade"}'), rules) == TransactionType.TRADE
        assert resolve_type(_tx(source="coinex", raw_json='{"type": "other"}'), rules) == TransactionType.TRANSFER

    def test_deleted_rules_skipped(self):
        rules = [_rule(source="ibkr", result_type="trade", priority=300, deleted=True)]
        assert resolve_type(_tx(source="ibkr"), rules) is None

    def test_missing_raw_json_field(self):
        rules = [_rule(source="okx", field_name="subType", field_value="1", result_type="trade")]
        assert resolve_type(_tx(source="okx", raw_json='{"other": "field"}'), rules) is None

    def test_empty_raw_json(self):
        rules = [_rule(source="okx", field_name="subType", field_value="1", result_type="trade")]
        assert resolve_type(_tx(source="okx", raw_json=""), rules) is None

    def test_synthetic_underscore_fields(self):
        rules = [
            _rule(source="revolut", field_name="_amount_sign", field_value="positive", result_type="deposit"),
            _rule(source="revolut", field_name="_amount_sign", field_value="negative", result_type="withdrawal"),
        ]
        tx_pos = _tx(source="revolut", raw_json=json.dumps({"_amount_sign": "positive"}))
        assert resolve_type(tx_pos, rules) == TransactionType.DEPOSIT

        tx_neg = _tx(source="revolut", raw_json=json.dumps({"_amount_sign": "negative"}))
        assert resolve_type(tx_neg, rules) == TransactionType.WITHDRAWAL


class TestResolveTypeBatch:
    def test_batch_returns_updates(self):
        rules = [_rule(source="ibkr", result_type="trade", priority=300)]
        txs = [_tx(source="ibkr", tx_id=1), _tx(source="okx", tx_id=2), _tx(source="ibkr", tx_id=3)]
        updates = resolve_type_batch(txs, rules)
        assert len(updates) == 2
        assert updates[0] == (1, TransactionType.TRADE)
        assert updates[1] == (3, TransactionType.TRADE)

    def test_batch_skips_none_ids(self):
        rules = [_rule(source="ibkr", result_type="trade", priority=300)]
        assert resolve_type_batch([_tx(source="ibkr", tx_id=None)], rules) == []

    def test_batch_empty(self):
        assert resolve_type_batch([], []) == []


# ── Integration: full pipeline (UNKNOWN → type resolve → categorize) ──


async def test_full_pipeline_resolves_types_then_categorizes(tmp_path):
    """Stage 0 resolves UNKNOWN → real type, then Stage 1 categorizes."""
    from pfm.analytics.categorization_runner import run_categorization
    from pfm.db.metadata_store import MetadataStore
    from pfm.db.repository import Repository

    async with Repository(tmp_path / "pipeline.db") as repo:
        # Save a bybit trade transaction with tx_type=UNKNOWN.
        tx = Transaction(
            date=date(2026, 3, 16),
            source="bybit",
            source_name="bybit",
            tx_type=TransactionType.UNKNOWN,
            asset="BTC",
            amount=Decimal(1),
            usd_value=Decimal(50000),
            tx_id="bybit-tx-1",
            raw_json=json.dumps({"type": "TRADE", "currency": "BTC"}),
        )
        await repo.save_transactions([tx])

        store = MetadataStore(repo.connection)
        summary = await run_categorization(repo, store)

        # Stage 0 should have resolved the type.
        assert summary["type_resolved"] == 1

        # Verify the DB now has the resolved type.
        txs = await repo.get_transactions(source="bybit")
        assert len(txs) == 1
        assert txs[0].tx_type == TransactionType.TRADE

        # Stage 1 should have categorized it (default rule: trade → trade).
        assert summary["categorized"] >= 1
        meta = await store.get_metadata(txs[0].id)
        assert meta is not None
        assert meta.category == "trade"
