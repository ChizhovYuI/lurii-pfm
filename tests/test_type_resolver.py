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


def _transfer_pair_txs() -> list[Transaction]:
    """A cross-source outflow/inflow pair the detector should match."""
    return [
        Transaction(
            date=date(2026, 4, 1),
            source="okx",
            source_name="okx-main",
            tx_type=TransactionType.WITHDRAWAL,
            asset="USDC",
            amount=Decimal(500),
            usd_value=Decimal(500),
            tx_id="okx-out",
        ),
        Transaction(
            date=date(2026, 4, 2),
            source="wise",
            source_name="wise-main",
            tx_type=TransactionType.DEPOSIT,
            asset="USDC",
            amount=Decimal(500),
            usd_value=Decimal(500),
            tx_id="wise-in",
        ),
    ]


async def _assert_symmetric(store, a_id: int, b_id: int) -> None:
    meta_a = await store.get_metadata(a_id)
    meta_b = await store.get_metadata(b_id)
    assert meta_a is not None
    assert meta_b is not None
    assert meta_a.is_internal_transfer is True
    assert meta_b.is_internal_transfer is True
    assert meta_a.transfer_pair_id == b_id
    assert meta_b.transfer_pair_id == a_id


async def test_categorization_preserves_transfer_pairing_under_force(tmp_path):
    """Re-running categorization (even forced) must not clobber a linked pair."""
    from pfm.analytics.categorization_runner import run_categorization
    from pfm.db.metadata_store import MetadataStore
    from pfm.db.repository import Repository

    async with Repository(tmp_path / "x.db") as repo:
        store = MetadataStore(repo.connection)
        await repo.save_transactions(_transfer_pair_txs())
        ids = {t.tx_id: t.id for t in await repo.get_transactions()}
        out_id, in_id = ids["okx-out"], ids["wise-in"]
        assert out_id is not None
        assert in_id is not None

        first = await run_categorization(repo, store)
        assert first["transfers"] == 1
        await _assert_symmetric(store, out_id, in_id)

        # A forced re-run previously rebuilt category metadata and dropped the
        # transfer overlay on one side — assert it now survives.
        await run_categorization(repo, store, force=True)
        await _assert_symmetric(store, out_id, in_id)


async def test_categorization_preserves_type_override_and_review_under_force(tmp_path):
    """A forced rule-categorization must not drop a row's type_override/reviewed."""
    from pfm.analytics.categorization_runner import run_categorization
    from pfm.db.metadata_store import MetadataStore
    from pfm.db.repository import Repository

    async with Repository(tmp_path / "x.db") as repo:
        store = MetadataStore(repo.connection)
        await repo.save_transactions(
            [
                Transaction(
                    date=date(2026, 4, 1),
                    source="kbank",
                    source_name="kbank-main",
                    tx_type=TransactionType.SPEND,
                    asset="THB",
                    amount=Decimal(120),
                    usd_value=Decimal(0),
                    tx_id="thb-spend",
                )
            ]
        )
        tx_id = (await repo.get_transactions())[0].id
        assert tx_id is not None
        # An overlay set by other tooling: a manual type override + reviewed flag,
        # no category yet, not a transfer.
        await store.upsert_metadata(tx_id, type_override="spend", reviewed=True, notes="keep me")
        await store.create_category_rule(
            "spend", "groceries", field_name="asset", field_operator="eq", field_value="THB"
        )

        await run_categorization(repo, store, force=True)

        meta = await store.get_metadata(tx_id)
        assert meta is not None
        assert meta.category == "groceries"  # rule applied
        assert meta.type_override == "spend"  # overlay preserved
        assert meta.reviewed is True
        assert meta.notes == "keep me"


async def test_detection_does_not_repair_or_repair_already_linked(tmp_path):
    """A second pass must not re-pair an already-linked transaction."""
    from pfm.analytics.categorization_runner import run_categorization
    from pfm.db.metadata_store import MetadataStore
    from pfm.db.repository import Repository

    async with Repository(tmp_path / "x.db") as repo:
        store = MetadataStore(repo.connection)
        await repo.save_transactions(_transfer_pair_txs())
        await run_categorization(repo, store)
        # Second pass finds no *new* transfers (already paired).
        second = await run_categorization(repo, store)
        assert second["transfers"] == 0


async def test_repair_transfer_pairs_restores_one_sided_link(tmp_path):
    """Mirror of the live bug: A→B but B lost its back-link."""
    from pfm.db.metadata_store import MetadataStore
    from pfm.db.repository import Repository

    async with Repository(tmp_path / "x.db") as repo:
        store = MetadataStore(repo.connection)
        await repo.save_transactions(_transfer_pair_txs())
        ids = {t.tx_id: t.id for t in await repo.get_transactions()}
        a_id, b_id = ids["okx-out"], ids["wise-in"]
        assert a_id is not None
        assert b_id is not None

        await store.link_transfer(a_id, b_id)
        # Simulate the clobber: clear B's transfer overlay only.
        await repo.connection.execute(
            "UPDATE transaction_metadata SET is_internal_transfer = 0, transfer_pair_id = NULL"
            " WHERE transaction_id = ?",
            (b_id,),
        )
        await repo.connection.commit()

        result = await store.repair_transfer_pairs()
        assert result["repaired"] == 1
        assert result["cleared"] == 0
        await _assert_symmetric(store, a_id, b_id)


async def test_repair_transfer_pairs_clears_orphan(tmp_path):
    """A dangling transfer flag (internal=1, no pair) is cleared by repair.

    This is the post-SET-NULL orphan shape that legacy rows (or rows broken by a
    path the delete trigger does not cover) can still carry. The FK forbids a
    bogus ``transfer_pair_id``, so the orphan is fabricated directly.
    """
    from pfm.db.metadata_store import MetadataStore
    from pfm.db.repository import Repository

    async with Repository(tmp_path / "x.db") as repo:
        store = MetadataStore(repo.connection)
        await repo.save_transactions(_transfer_pair_txs())
        ids = {t.tx_id: t.id for t in await repo.get_transactions()}
        a_id = ids["okx-out"]
        assert a_id is not None

        # Dangling flag: marked an internal transfer but claiming no partner.
        await store.upsert_metadata(a_id, is_internal_transfer=True)
        orphan = await store.get_metadata(a_id)
        assert orphan is not None
        assert orphan.is_internal_transfer is True
        assert orphan.transfer_pair_id is None

        result = await store.repair_transfer_pairs()
        assert result["cleared"] == 1
        assert result["repaired"] == 0
        meta_a = await store.get_metadata(a_id)
        assert meta_a is not None
        assert meta_a.is_internal_transfer is False
        assert meta_a.transfer_pair_id is None


async def test_repair_transfer_pairs_does_not_clobber_real_categorized_partner(tmp_path):
    """A stale claim onto a genuine non-transfer row clears the claim, not the row."""
    from pfm.db.metadata_store import MetadataStore
    from pfm.db.repository import Repository

    async with Repository(tmp_path / "x.db") as repo:
        store = MetadataStore(repo.connection)
        await repo.save_transactions(_transfer_pair_txs())
        ids = {t.tx_id: t.id for t in await repo.get_transactions()}
        a_id, b_id = ids["okx-out"], ids["wise-in"]
        assert a_id is not None
        assert b_id is not None

        # B is a genuinely categorized NON-transfer row.
        await store.upsert_metadata(b_id, category="groceries", category_source="manual")
        # A carries a stale one-sided claim onto B (FK allows it: B exists).
        await store.upsert_metadata(a_id, is_internal_transfer=True)
        await repo.connection.execute(
            "UPDATE transaction_metadata SET transfer_pair_id = ? WHERE transaction_id = ?",
            (b_id, a_id),
        )
        await repo.connection.commit()

        result = await store.repair_transfer_pairs()
        assert result == {"repaired": 0, "cleared": 1}

        meta_a = await store.get_metadata(a_id)
        meta_b = await store.get_metadata(b_id)
        assert meta_a is not None
        assert meta_b is not None
        # A's stale claim is cleared; B's real categorization is untouched.
        assert meta_a.is_internal_transfer is False
        assert meta_a.transfer_pair_id is None
        assert meta_b.is_internal_transfer is False
        assert meta_b.category == "groceries"
        assert meta_b.category_source == "manual"


async def test_repair_transfer_pairs_is_order_independent(tmp_path):
    """A half-orphan heals to a symmetric pair regardless of metadata row order.

    The decide-then-apply rewrite removes the iteration-order dependence the
    original loop had: B still claims A and A is still flagged, so there is
    enough signal to restore the pair — deterministically, either visit order.
    """
    from pfm.db.metadata_store import MetadataStore
    from pfm.db.repository import Repository

    async with Repository(tmp_path / "x.db") as repo:
        store = MetadataStore(repo.connection)
        await repo.save_transactions(_transfer_pair_txs())
        ids = {t.tx_id: t.id for t in await repo.get_transactions()}
        a_id, b_id = ids["okx-out"], ids["wise-in"]
        assert a_id is not None
        assert b_id is not None

        await store.link_transfer(a_id, b_id)
        # Half-orphan: B keeps its back-link to A, but A drops its claim while
        # staying flagged as an internal transfer.
        await repo.connection.execute(
            "UPDATE transaction_metadata SET transfer_pair_id = NULL WHERE transaction_id = ?",
            (a_id,),
        )
        await repo.connection.commit()

        result = await store.repair_transfer_pairs()
        assert result == {"repaired": 1, "cleared": 0}
        await _assert_symmetric(store, a_id, b_id)


async def test_deleting_one_side_clears_partner_transfer_overlay(tmp_path):
    """The BEFORE DELETE trigger clears a partner's overlay so no orphan accrues."""
    from pfm.db.metadata_store import MetadataStore
    from pfm.db.repository import Repository

    async with Repository(tmp_path / "x.db") as repo:
        store = MetadataStore(repo.connection)
        await repo.save_transactions(_transfer_pair_txs())
        ids = {t.tx_id: t.id for t in await repo.get_transactions()}
        a_id, b_id = ids["okx-out"], ids["wise-in"]
        assert a_id is not None
        assert b_id is not None

        await store.link_transfer(a_id, b_id)
        # Deleting B must clear A's transfer overlay (trigger fires before the FK
        # SET-NULL), leaving no one-sided link behind — repair has nothing to do.
        await repo.connection.execute("DELETE FROM transactions WHERE id = ?", (b_id,))
        await repo.connection.commit()

        meta_a = await store.get_metadata(a_id)
        assert meta_a is not None
        assert meta_a.is_internal_transfer is False
        assert meta_a.transfer_pair_id is None
        assert meta_a.category != "transfer"

        result = await store.repair_transfer_pairs()
        assert result == {"repaired": 0, "cleared": 0}


async def test_repair_transfer_pairs_preserves_manual_detection_source(tmp_path):
    """Restoring a one-sided MANUAL link must not downgrade either side to 'auto'."""
    from pfm.db.metadata_store import MetadataStore
    from pfm.db.repository import Repository

    async with Repository(tmp_path / "x.db") as repo:
        store = MetadataStore(repo.connection)
        await repo.save_transactions(_transfer_pair_txs())
        ids = {t.tx_id: t.id for t in await repo.get_transactions()}
        a_id, b_id = ids["okx-out"], ids["wise-in"]
        assert a_id is not None
        assert b_id is not None

        await store.link_transfer(a_id, b_id)  # manual on both sides
        # Drop B's overlay entirely (cascade-style clobber): no row to inherit from.
        await repo.connection.execute("DELETE FROM transaction_metadata WHERE transaction_id = ?", (b_id,))
        await repo.connection.commit()

        result = await store.repair_transfer_pairs()
        assert result["repaired"] == 1
        await _assert_symmetric(store, a_id, b_id)
        meta_a = await store.get_metadata(a_id)
        meta_b = await store.get_metadata(b_id)
        assert meta_a is not None
        assert meta_b is not None
        # A keeps its own manual source; B inherits the claimer's manual source
        # rather than being silently downgraded to 'auto'.
        assert meta_a.transfer_detected_by == "manual"
        assert meta_b.transfer_detected_by == "manual"
        assert meta_a.category_source == "manual"
        assert meta_b.category_source == "manual"
