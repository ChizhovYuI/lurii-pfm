"""Tests for pfm.analytics.rule_dryrun."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

import pytest

from pfm.analytics.rule_dryrun import dry_run_category_rule, dry_run_type_rule
from pfm.db.metadata_store import MetadataStore
from pfm.db.models import Transaction, TransactionType
from pfm.db.repository import Repository


def _tx(
    *,
    source_name: str = "kbank",
    tx_type: TransactionType = TransactionType.SPEND,
    tx_id: str = "",
    raw_json: str = "",
    asset: str = "USD",
    d: date = date(2026, 3, 1),
) -> Transaction:
    return Transaction(
        date=d,
        source=source_name,
        source_name=source_name,
        tx_type=tx_type,
        asset=asset,
        amount=Decimal(10),
        usd_value=Decimal(10),
        tx_id=tx_id,
        raw_json=raw_json,
    )


# ── dry_run_category_rule ─────────────────────────────────────────────


class TestDryRunCategoryRule:
    async def test_empty_db(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            result = await dry_run_category_rule(
                repo,
                store,
                type_match="spend",
                result_category="other_spend",
            )
            assert result == {
                "matched": 0,
                "unchanged": [],
                "changed": [],
                "shadowed_by_higher": [],
                "overlapping_rules": [],
                "raw_field_samples": [],
            }

    async def test_changed_no_overlap(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions([_tx(tx_id="s1", raw_json=json.dumps({"description": "FX 100 USD"}))])
            result = await dry_run_category_rule(
                repo,
                store,
                type_match="spend",
                result_category="fx",
                field_name="description",
                field_operator="contains",
                field_value="FX",
            )
            assert result["matched"] == 1
            assert result["overlapping_rules"] == []
            changed = result["changed"]
            assert isinstance(changed, list)
            assert changed == [{"tx_id": "s1", "current_category": None, "proposed_category": "fx"}]
            assert result["unchanged"] == []
            assert result["raw_field_samples"] == ["FX 100 USD"]

    async def test_unchanged_when_already_categorized(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions([_tx(tx_id="s1", raw_json=json.dumps({"description": "FX 1"}))])
            txs = await repo.get_transactions()
            assert txs[0].id is not None
            await store.upsert_metadata(txs[0].id, category="fx")

            result = await dry_run_category_rule(
                repo,
                store,
                type_match="spend",
                result_category="fx",
                field_name="description",
                field_operator="contains",
                field_value="FX",
            )
            assert result["matched"] == 1
            assert result["changed"] == []
            assert result["unchanged"] == [{"tx_id": "s1", "current_category": "fx"}]

    async def test_overlap_with_existing_rule(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions([_tx(tx_id="s1", raw_json=json.dumps({"description": "FX 1 USD"}))])
            existing = await store.create_category_rule(
                "spend",
                "fx",
                field_name="description",
                field_operator="contains",
                field_value="FX",
            )

            result = await dry_run_category_rule(
                repo,
                store,
                type_match="spend",
                result_category="forex",
                field_name="description",
                field_operator="regex",
                field_value=r"^FX\b",
            )
            assert result["matched"] == 1
            overlapping = result["overlapping_rules"]
            assert isinstance(overlapping, list)
            assert len(overlapping) == 1
            assert overlapping[0]["id"] == existing.id
            assert overlapping[0]["result_category"] == "fx"

    async def test_scope_source_filter(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions(
                [
                    _tx(source_name="kbank", tx_id="k1", raw_json=json.dumps({"description": "FX"})),
                    _tx(source_name="wise", tx_id="w1", raw_json=json.dumps({"description": "FX"})),
                ]
            )
            result = await dry_run_category_rule(
                repo,
                store,
                type_match="spend",
                result_category="fx",
                field_name="description",
                field_operator="contains",
                field_value="FX",
                scope_source="kbank",
            )
            assert result["matched"] == 1
            changed = result["changed"]
            assert isinstance(changed, list)
            assert changed[0]["tx_id"] == "k1"

    async def test_limit_caps_candidate_set(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions(
                [
                    _tx(tx_id=f"s{i}", raw_json=json.dumps({"description": "FX"}), d=date(2026, 3, i))
                    for i in range(1, 6)
                ]
            )
            result = await dry_run_category_rule(
                repo,
                store,
                type_match="spend",
                result_category="fx",
                field_name="description",
                field_operator="contains",
                field_value="FX",
                limit=2,
            )
            assert result["matched"] == 2

    async def test_raw_field_samples_dedup_and_truncate(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            long_value = "FX " + ("X" * 300)
            await repo.save_transactions(
                [
                    _tx(tx_id="s1", raw_json=json.dumps({"description": "FX A"})),
                    _tx(tx_id="s2", raw_json=json.dumps({"description": "FX A"})),  # dup
                    _tx(tx_id="s3", raw_json=json.dumps({"description": "FX B"})),
                    _tx(tx_id="s4", raw_json=json.dumps({"description": long_value})),
                ]
            )
            result = await dry_run_category_rule(
                repo,
                store,
                type_match="spend",
                result_category="fx",
                field_name="description",
                field_operator="contains",
                field_value="FX",
            )
            samples = result["raw_field_samples"]
            assert isinstance(samples, list)
            assert "FX A" in samples
            assert "FX B" in samples
            assert len(samples) == 3  # FX A, FX B, truncated long
            assert all(len(s) <= 200 for s in samples)

    async def test_malformed_regex_raises(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            with pytest.raises(ValueError, match="invalid regex"):
                await dry_run_category_rule(
                    repo,
                    store,
                    type_match="spend",
                    result_category="fx",
                    field_name="description",
                    field_operator="regex",
                    field_value="(",
                )

    async def test_no_field_no_samples(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions([_tx(tx_id="s1")])
            result = await dry_run_category_rule(
                repo,
                store,
                type_match="spend",
                result_category="other_spend",
            )
            assert result["matched"] == 1
            assert result["raw_field_samples"] == []


# ── dry_run_type_rule ─────────────────────────────────────────────────


class TestDryRunTypeRule:
    async def test_changed_smoke(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions(
                [
                    _tx(
                        tx_id="u1",
                        tx_type=TransactionType.UNKNOWN,
                        raw_json=json.dumps({"kind": "purchase"}),
                    )
                ]
            )
            result = await dry_run_type_rule(
                repo,
                store,
                result_type="spend",
                field_name="kind",
                field_operator="eq",
                field_value="purchase",
            )
            assert result["matched"] == 1
            changed = result["changed"]
            assert isinstance(changed, list)
            assert changed[0]["tx_id"] == "u1"
            assert changed[0]["proposed_type"] == "spend"
            assert changed[0]["current_type"] == "unknown"

    async def test_overlap_with_existing_type_rule(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions(
                [
                    _tx(
                        tx_id="u1",
                        tx_type=TransactionType.UNKNOWN,
                        raw_json=json.dumps({"kind": "purchase"}),
                    )
                ]
            )
            existing = await store.create_type_rule(
                "spend",
                field_name="kind",
                field_operator="eq",
                field_value="purchase",
            )

            result = await dry_run_type_rule(
                repo,
                store,
                result_type="spend",
                field_name="kind",
                field_operator="contains",
                field_value="purch",
            )
            overlapping = result["overlapping_rules"]
            assert isinstance(overlapping, list)
            assert len(overlapping) == 1
            assert overlapping[0]["id"] == existing.id

    async def test_source_filter(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions(
                [
                    _tx(
                        source_name="kbank",
                        tx_id="k1",
                        tx_type=TransactionType.UNKNOWN,
                        raw_json=json.dumps({"kind": "purchase"}),
                    ),
                    _tx(
                        source_name="wise",
                        tx_id="w1",
                        tx_type=TransactionType.UNKNOWN,
                        raw_json=json.dumps({"kind": "purchase"}),
                    ),
                ]
            )
            result = await dry_run_type_rule(
                repo,
                store,
                result_type="spend",
                field_name="kind",
                field_operator="eq",
                field_value="purchase",
                scope_source="kbank",
            )
            assert result["matched"] == 1
            changed = result["changed"]
            assert isinstance(changed, list)
            assert changed[0]["tx_id"] == "k1"


# ── priority-aware semantics ─────────────────────────────────────────


class TestDryRunCategoryRulePriority:
    async def test_shadowed_by_higher_priority_existing(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions([_tx(tx_id="s1", raw_json=json.dumps({"description": "FX 1"}))])
            existing = await store.create_category_rule(
                "spend",
                "fx",
                field_name="description",
                field_operator="contains",
                field_value="FX",
                priority=100,
            )

            result = await dry_run_category_rule(
                repo,
                store,
                type_match="spend",
                result_category="other_spend",
                field_name="description",
                field_operator="contains",
                field_value="FX",
                priority=300,
            )
            assert result["matched"] == 1
            assert result["changed"] == []
            assert result["unchanged"] == []
            shadowed = result["shadowed_by_higher"]
            assert isinstance(shadowed, list)
            assert len(shadowed) == 1
            assert shadowed[0]["winning_rule_id"] == existing.id
            assert shadowed[0]["winning_priority"] == 100
            assert shadowed[0]["winning_category"] == "fx"

    async def test_candidate_wins_at_higher_precedence(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions([_tx(tx_id="s1", raw_json=json.dumps({"description": "FX 1"}))])
            await store.create_category_rule(
                "spend",
                "other_spend",
                field_name="description",
                field_operator="contains",
                field_value="FX",
                priority=300,
            )

            result = await dry_run_category_rule(
                repo,
                store,
                type_match="spend",
                result_category="fx",
                field_name="description",
                field_operator="contains",
                field_value="FX",
                priority=100,
            )
            assert result["matched"] == 1
            assert result["shadowed_by_higher"] == []
            changed = result["changed"]
            assert isinstance(changed, list)
            assert len(changed) == 1
            assert changed[0]["proposed_category"] == "fx"

    async def test_same_priority_existing_wins_tiebreak(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions([_tx(tx_id="s1", raw_json=json.dumps({"description": "FX 1"}))])
            existing = await store.create_category_rule(
                "spend",
                "fx",
                field_name="description",
                field_operator="contains",
                field_value="FX",
                priority=200,
            )

            result = await dry_run_category_rule(
                repo,
                store,
                type_match="spend",
                result_category="other_spend",
                field_name="description",
                field_operator="contains",
                field_value="FX",
                priority=200,
            )
            assert result["matched"] == 1
            assert result["changed"] == []
            shadowed = result["shadowed_by_higher"]
            assert isinstance(shadowed, list)
            assert len(shadowed) == 1
            assert shadowed[0]["winning_rule_id"] == existing.id


class TestDryRunTypeRulePriority:
    async def test_shadowed_by_higher_priority_existing(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions(
                [
                    _tx(
                        tx_id="u1",
                        tx_type=TransactionType.UNKNOWN,
                        raw_json=json.dumps({"kind": "purchase"}),
                    )
                ]
            )
            existing = await store.create_type_rule(
                "spend",
                field_name="kind",
                field_operator="eq",
                field_value="purchase",
                priority=50,
            )

            result = await dry_run_type_rule(
                repo,
                store,
                result_type="receive",
                field_name="kind",
                field_operator="eq",
                field_value="purchase",
                priority=200,
            )
            assert result["matched"] == 1
            assert result["changed"] == []
            shadowed = result["shadowed_by_higher"]
            assert isinstance(shadowed, list)
            assert len(shadowed) == 1
            assert shadowed[0]["winning_rule_id"] == existing.id
            assert shadowed[0]["winning_priority"] == 50
            assert shadowed[0]["winning_type"] == "spend"

    async def test_candidate_wins_at_higher_precedence(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions(
                [
                    _tx(
                        tx_id="u1",
                        tx_type=TransactionType.UNKNOWN,
                        raw_json=json.dumps({"kind": "purchase"}),
                    )
                ]
            )
            await store.create_type_rule(
                "receive",
                field_name="kind",
                field_operator="eq",
                field_value="purchase",
                priority=300,
            )

            result = await dry_run_type_rule(
                repo,
                store,
                result_type="spend",
                field_name="kind",
                field_operator="eq",
                field_value="purchase",
                priority=50,
            )
            assert result["matched"] == 1
            assert result["shadowed_by_higher"] == []
            changed = result["changed"]
            assert isinstance(changed, list)
            assert len(changed) == 1
            assert changed[0]["proposed_type"] == "spend"

    async def test_same_priority_existing_wins_tiebreak(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions(
                [
                    _tx(
                        tx_id="u1",
                        tx_type=TransactionType.UNKNOWN,
                        raw_json=json.dumps({"kind": "purchase"}),
                    )
                ]
            )
            existing = await store.create_type_rule(
                "spend",
                field_name="kind",
                field_operator="eq",
                field_value="purchase",
                priority=150,
            )

            result = await dry_run_type_rule(
                repo,
                store,
                result_type="receive",
                field_name="kind",
                field_operator="eq",
                field_value="purchase",
                priority=150,
            )
            assert result["matched"] == 1
            assert result["changed"] == []
            shadowed = result["shadowed_by_higher"]
            assert isinstance(shadowed, list)
            assert len(shadowed) == 1
            assert shadowed[0]["winning_rule_id"] == existing.id
