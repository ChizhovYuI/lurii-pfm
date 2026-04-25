"""Tests for MetadataStore.get_categorization_summary and get_uncategorized_transactions."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pfm.db.metadata_store import MetadataStore
from pfm.db.models import Transaction, TransactionType
from pfm.db.repository import Repository


def _tx(
    *,
    source_name: str = "kbank",
    tx_type: TransactionType = TransactionType.SPEND,
    tx_id: str = "",
    raw_json: str = "",
    d: date = date(2026, 3, 1),
) -> Transaction:
    return Transaction(
        date=d,
        source=source_name,
        source_name=source_name,
        tx_type=tx_type,
        asset="USD",
        amount=Decimal(10),
        usd_value=Decimal(10),
        tx_id=tx_id,
        raw_json=raw_json,
    )


# ── get_categorization_summary ────────────────────────────────────────


class TestCategorizationSummary:
    async def test_empty_db(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            assert await store.get_categorization_summary() == []

    async def test_counts_per_source(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions(
                [
                    _tx(source_name="kbank", tx_type=TransactionType.UNKNOWN, tx_id="k1"),
                    _tx(source_name="kbank", tx_type=TransactionType.SPEND, tx_id="k2"),
                    _tx(source_name="kbank", tx_type=TransactionType.SPEND, tx_id="k3"),
                    _tx(source_name="wise", tx_type=TransactionType.SPEND, tx_id="w1"),
                ]
            )
            txs = await repo.get_transactions()
            # Mark k2 as internal_transfer + categorized; k3 stays no_category.
            k2 = next(t for t in txs if t.tx_id == "k2")
            assert k2.id is not None
            await store.upsert_metadata(
                k2.id,
                category="transfer",
                is_internal_transfer=True,
            )

            summary = await store.get_categorization_summary()
            by_source = {row["source_name"]: row for row in summary}
            assert by_source["kbank"] == {
                "source_name": "kbank",
                "total": 3,
                "unknown_type": 1,
                "no_category": 2,  # k1 (unknown, no cat), k3 (spend, no cat)
                "internal_transfer": 1,  # k2
            }
            assert by_source["wise"] == {
                "source_name": "wise",
                "total": 1,
                "unknown_type": 0,
                "no_category": 1,
                "internal_transfer": 0,
            }

    async def test_source_filter(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions(
                [
                    _tx(source_name="kbank", tx_id="k1"),
                    _tx(source_name="wise", tx_id="w1"),
                ]
            )
            summary = await store.get_categorization_summary(source_name="wise")
            assert len(summary) == 1
            assert summary[0]["source_name"] == "wise"

    async def test_type_override_excludes_unknown(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions([_tx(tx_type=TransactionType.UNKNOWN, tx_id="k1")])
            txs = await repo.get_transactions()
            assert txs[0].id is not None
            await store.upsert_metadata(txs[0].id, type_override="spend")

            summary = await store.get_categorization_summary()
            assert summary[0]["unknown_type"] == 0


# ── get_uncategorized_transactions ───────────────────────────────────


class TestGetUncategorizedTransactions:
    async def test_empty(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            items, total = await store.get_uncategorized_transactions()
            assert items == []
            assert total == 0

    async def test_default_or_logic(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions(
                [
                    _tx(tx_type=TransactionType.UNKNOWN, tx_id="u1"),  # missing type
                    _tx(tx_type=TransactionType.SPEND, tx_id="s1"),  # missing category
                    _tx(tx_type=TransactionType.SPEND, tx_id="s2"),  # categorized
                ]
            )
            txs = {t.tx_id: t for t in await repo.get_transactions()}
            assert txs["s2"].id is not None
            await store.upsert_metadata(txs["s2"].id, category="other_spend")

            items, total = await store.get_uncategorized_transactions()
            assert total == 2
            assert {t.tx_id for t, _ in items} == {"u1", "s1"}

    async def test_missing_type_only(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions(
                [
                    _tx(tx_type=TransactionType.UNKNOWN, tx_id="u1"),
                    _tx(tx_type=TransactionType.SPEND, tx_id="s1"),  # missing cat but not unknown
                ]
            )
            items, total = await store.get_uncategorized_transactions(missing_type=True)
            assert total == 1
            assert items[0][0].tx_id == "u1"

    async def test_missing_category_only_excludes_transfers(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions(
                [
                    _tx(tx_type=TransactionType.SPEND, tx_id="s1"),  # missing cat
                    _tx(tx_type=TransactionType.SPEND, tx_id="s2"),  # transfer
                ]
            )
            txs = {t.tx_id: t for t in await repo.get_transactions()}
            assert txs["s2"].id is not None
            await store.upsert_metadata(txs["s2"].id, is_internal_transfer=True)

            items, total = await store.get_uncategorized_transactions(missing_category=True)
            assert total == 1
            assert items[0][0].tx_id == "s1"

    async def test_both_flags_and_logic(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions(
                [
                    _tx(tx_type=TransactionType.UNKNOWN, tx_id="u_nocat"),  # both
                    _tx(tx_type=TransactionType.UNKNOWN, tx_id="u_cat"),  # unknown but categorized
                    _tx(tx_type=TransactionType.SPEND, tx_id="s_nocat"),  # only no-cat
                ]
            )
            txs = {t.tx_id: t for t in await repo.get_transactions()}
            assert txs["u_cat"].id is not None
            await store.upsert_metadata(txs["u_cat"].id, category="other_spend")

            items, total = await store.get_uncategorized_transactions(missing_type=True, missing_category=True)
            assert total == 1
            assert items[0][0].tx_id == "u_nocat"

    async def test_pagination(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions(
                [_tx(tx_id=f"u{i}", tx_type=TransactionType.UNKNOWN, d=date(2026, 3, i)) for i in range(1, 6)]
            )
            items, total = await store.get_uncategorized_transactions(missing_type=True, limit=2)
            assert total == 5
            assert len(items) == 2

            items_offset, _ = await store.get_uncategorized_transactions(missing_type=True, limit=2, offset=2)
            seen = {t.tx_id for t, _ in items} | {t.tx_id for t, _ in items_offset}
            assert len(seen) == 4

    async def test_source_filter(self, tmp_path) -> None:
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            await repo.save_transactions(
                [
                    _tx(source_name="kbank", tx_type=TransactionType.UNKNOWN, tx_id="k1"),
                    _tx(source_name="wise", tx_type=TransactionType.UNKNOWN, tx_id="w1"),
                ]
            )
            items, total = await store.get_uncategorized_transactions(source_name="kbank", missing_type=True)
            assert total == 1
            assert items[0][0].tx_id == "k1"


# ── get_category_suggestions: non-discriminating filter ──────────────


class TestSuggestionFilter:
    async def _seed_choice(
        self,
        store: MetadataStore,
        repo: Repository,
        *,
        tx_id: str,
        source: str,
        category: str,
        snapshot: str,
    ) -> None:
        import json

        await repo.save_transactions(
            [_tx(source_name=source, tx_id=tx_id, raw_json=snapshot)],
        )
        txs = await repo.get_transactions()
        target = next(t for t in txs if t.tx_id == tx_id)
        assert target.id is not None
        await store.record_category_choice(
            target.id,
            source,
            "spend",
            category,
            field_snapshot=json.dumps(json.loads(snapshot)),
        )

    async def test_filters_when_same_field_value_maps_to_multiple_categories(
        self,
        tmp_path,
    ) -> None:
        import json

        snap = json.dumps({"_balance_direction": "decrease"})
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            for i in range(2):
                await self._seed_choice(
                    store,
                    repo,
                    tx_id=f"d{i}",
                    source="kbank",
                    category="dining",
                    snapshot=snap,
                )
            for i in range(2):
                await self._seed_choice(
                    store,
                    repo,
                    tx_id=f"g{i}",
                    source="kbank",
                    category="groceries",
                    snapshot=snap,
                )
            sugs = await store.get_category_suggestions()
            # Both suggestions share (kbank, _balance_direction, decrease) → suppressed.
            for s in sugs:
                rule = s.get("suggested_rule")
                assert isinstance(rule, dict)
                assert rule.get("field_value") != "decrease"

    async def test_include_non_discriminating_surfaces_with_flag(self, tmp_path) -> None:
        import json

        snap = json.dumps({"_balance_direction": "decrease"})
        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            for i in range(2):
                await self._seed_choice(
                    store,
                    repo,
                    tx_id=f"d{i}",
                    source="kbank",
                    category="dining",
                    snapshot=snap,
                )
            for i in range(2):
                await self._seed_choice(
                    store,
                    repo,
                    tx_id=f"g{i}",
                    source="kbank",
                    category="groceries",
                    snapshot=snap,
                )
            sugs = await store.get_category_suggestions(include_non_discriminating=True)
            non_disc = [s for s in sugs if s.get("non_discriminating")]
            assert len(non_disc) == 2
            for s in non_disc:
                conflicting = s["conflicting_categories"]
                assert isinstance(conflicting, list)
                assert sorted(conflicting) == ["dining", "groceries"]

    async def test_keeps_discriminating_field(self, tmp_path) -> None:
        import json

        async with Repository(tmp_path / "x.db") as repo:
            store = MetadataStore(repo.connection)
            for i in range(2):
                await self._seed_choice(
                    store,
                    repo,
                    tx_id=f"d{i}",
                    source="kbank",
                    category="dining",
                    snapshot=json.dumps({"merchant_category": "restaurant"}),
                )
            for i in range(2):
                await self._seed_choice(
                    store,
                    repo,
                    tx_id=f"g{i}",
                    source="kbank",
                    category="groceries",
                    snapshot=json.dumps({"merchant_category": "supermarket"}),
                )
            sugs = await store.get_category_suggestions()
            # Distinct values per category → both kept, neither flagged.
            assert len(sugs) == 2
            assert all(not s.get("non_discriminating") for s in sugs)
            cats: set[object] = set()
            for s in sugs:
                rule = s["suggested_rule"]
                assert isinstance(rule, dict)
                cats.add(rule["result_category"])
            assert cats == {"dining", "groceries"}
