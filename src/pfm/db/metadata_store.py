"""CRUD operations for transaction metadata, categories, and category rules."""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING

from pfm.db.models import CategoryRule, TransactionCategory, TransactionMetadata, TypeRule

if TYPE_CHECKING:
    from datetime import date

    import aiosqlite

    from pfm.db.models import Transaction


def _safe_column(row: aiosqlite.Row, name: str, default: object = None) -> object:
    """Read a column that may not exist (for backward compat with old schemas)."""
    try:
        return row[name]
    except IndexError:
        return default


class MetadataStore:
    """Async store for transaction metadata, categories, and rules."""

    def __init__(self, conn: aiosqlite.Connection) -> None:
        self._db = conn

    # ── Categories ─────────────────────────────────────────────────────

    async def get_categories(self, tx_type: str | None = None) -> list[TransactionCategory]:
        """List all categories, optionally filtered by tx_type."""
        query = "SELECT * FROM transaction_categories"
        params: list[str] = []
        if tx_type is not None:
            query += " WHERE tx_type = ?"
            params.append(tx_type)
        query += " ORDER BY tx_type, sort_order"
        cursor = await self._db.execute(query, params)
        rows = await cursor.fetchall()
        return [self._row_to_category(row) for row in rows]

    async def create_category(
        self,
        tx_type: str,
        category: str,
        display_name: str,
        sort_order: int = 0,
    ) -> TransactionCategory:
        """Create a custom category."""
        cursor = await self._db.execute(
            "INSERT INTO transaction_categories (tx_type, category, display_name, sort_order) VALUES (?, ?, ?, ?)",
            (tx_type, category, display_name, sort_order),
        )
        await self._db.commit()
        row = await (
            await self._db.execute("SELECT * FROM transaction_categories WHERE id = ?", (cursor.lastrowid,))
        ).fetchone()
        assert row is not None  # noqa: S101
        return self._row_to_category(row)

    @staticmethod
    def _row_to_category(row: aiosqlite.Row) -> TransactionCategory:
        return TransactionCategory(
            id=row["id"],
            tx_type=row["tx_type"],
            category=row["category"],
            display_name=row["display_name"],
            sort_order=row["sort_order"],
        )

    # ── Metadata ───────────────────────────────────────────────────────

    async def get_metadata(self, transaction_id: int) -> TransactionMetadata | None:
        """Get metadata for a single transaction."""
        cursor = await self._db.execute(
            "SELECT * FROM transaction_metadata WHERE transaction_id = ?",
            (transaction_id,),
        )
        row = await cursor.fetchone()
        return self._row_to_metadata(row) if row else None

    async def get_metadata_batch(self, transaction_ids: list[int]) -> dict[int, TransactionMetadata]:
        """Get metadata for multiple transactions."""
        if not transaction_ids:
            return {}
        placeholders = ",".join("?" for _ in transaction_ids)
        cursor = await self._db.execute(
            f"SELECT * FROM transaction_metadata WHERE transaction_id IN ({placeholders})",  # noqa: S608
            [str(tid) for tid in transaction_ids],
        )
        rows = await cursor.fetchall()
        return {row["transaction_id"]: self._row_to_metadata(row) for row in rows}

    async def upsert_metadata(  # noqa: PLR0913
        self,
        transaction_id: int,
        *,
        category: str | None = None,
        category_source: str = "auto",
        category_confidence: float | None = None,
        type_override: str | None = None,
        is_internal_transfer: bool = False,
        transfer_pair_id: int | None = None,
        transfer_detected_by: str | None = None,
        reviewed: bool = False,
        notes: str = "",
    ) -> TransactionMetadata:
        """Insert or update metadata for a transaction."""
        await self._db.execute(
            "INSERT INTO transaction_metadata"
            " (transaction_id, category, category_source, category_confidence,"
            "  type_override, is_internal_transfer, transfer_pair_id, transfer_detected_by,"
            "  reviewed, notes, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))"
            " ON CONFLICT(transaction_id) DO UPDATE SET"
            "  category = excluded.category,"
            "  category_source = excluded.category_source,"
            "  category_confidence = excluded.category_confidence,"
            "  type_override = excluded.type_override,"
            "  is_internal_transfer = excluded.is_internal_transfer,"
            "  transfer_pair_id = excluded.transfer_pair_id,"
            "  transfer_detected_by = excluded.transfer_detected_by,"
            "  reviewed = excluded.reviewed,"
            "  notes = excluded.notes,"
            "  updated_at = excluded.updated_at",
            (
                transaction_id,
                category,
                category_source,
                category_confidence,
                type_override,
                1 if is_internal_transfer else 0,
                transfer_pair_id,
                transfer_detected_by,
                1 if reviewed else 0,
                notes,
            ),
        )
        await self._db.commit()
        result = await self.get_metadata(transaction_id)
        assert result is not None  # noqa: S101
        return result

    async def upsert_metadata_batch(self, items: list[TransactionMetadata]) -> None:
        """Batch upsert category metadata. Preserves type_override, transfer, and review fields."""
        if not items:
            return
        await self._db.executemany(
            "INSERT INTO transaction_metadata"
            " (transaction_id, category, category_source, category_confidence,"
            "  type_override, is_internal_transfer, transfer_pair_id, transfer_detected_by,"
            "  reviewed, notes, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))"
            " ON CONFLICT(transaction_id) DO UPDATE SET"
            "  category = excluded.category,"
            "  category_source = excluded.category_source,"
            "  category_confidence = excluded.category_confidence,"
            "  updated_at = excluded.updated_at",
            [
                (
                    m.transaction_id,
                    m.category,
                    m.category_source,
                    m.category_confidence,
                    m.type_override,
                    1 if m.is_internal_transfer else 0,
                    m.transfer_pair_id,
                    m.transfer_detected_by,
                    1 if m.reviewed else 0,
                    m.notes,
                )
                for m in items
            ],
        )
        await self._db.commit()

    @staticmethod
    def _row_to_metadata(row: aiosqlite.Row) -> TransactionMetadata:
        return TransactionMetadata(
            transaction_id=row["transaction_id"],
            category=row["category"],
            category_source=row["category_source"],
            category_confidence=row["category_confidence"],
            type_override=_safe_column(row, "type_override"),  # type: ignore[arg-type]
            is_internal_transfer=bool(row["is_internal_transfer"]),
            transfer_pair_id=row["transfer_pair_id"],
            transfer_detected_by=row["transfer_detected_by"],
            reviewed=bool(row["reviewed"]),
            notes=row["notes"] or "",
        )

    # ── Review queue ───────────────────────────────────────────────────

    async def get_review_queue(
        self,
        limit: int = 50,
        offset: int = 0,
    ) -> list[tuple[Transaction, TransactionMetadata]]:
        """Get transactions needing review (unreviewed with low confidence)."""
        from pfm.db.repository import Repository

        cursor = await self._db.execute(
            "SELECT t.*, m.category, m.category_source, m.category_confidence,"
            " m.type_override, m.is_internal_transfer, m.transfer_pair_id,"
            " m.transfer_detected_by, m.reviewed, m.notes, m.updated_at AS m_updated_at"
            " FROM transactions t"
            " INNER JOIN transaction_metadata m ON t.id = m.transaction_id"
            " WHERE m.reviewed = 0"
            " ORDER BY m.category_confidence ASC, t.date DESC"
            " LIMIT ? OFFSET ?",
            (limit, offset),
        )
        rows = await cursor.fetchall()
        result: list[tuple[Transaction, TransactionMetadata]] = []
        for row in rows:
            tx = Repository.row_to_transaction(row)
            meta = TransactionMetadata(
                transaction_id=row["id"],
                category=row["category"],
                category_source=row["category_source"],
                category_confidence=row["category_confidence"],
                type_override=_safe_column(row, "type_override"),  # type: ignore[arg-type]
                is_internal_transfer=bool(row["is_internal_transfer"]),
                transfer_pair_id=row["transfer_pair_id"],
                transfer_detected_by=row["transfer_detected_by"],
                reviewed=bool(row["reviewed"]),
                notes=row["notes"] or "",
            )
            result.append((tx, meta))
        return result

    # ── Transfer linking ───────────────────────────────────────────────

    async def link_transfer(self, tx_id_a: int, tx_id_b: int) -> None:
        """Link two transactions as an internal transfer pair.

        Sets type_override=transfer and category=transfer for both sides.
        """
        for tx_id, pair_id in [(tx_id_a, tx_id_b), (tx_id_b, tx_id_a)]:
            await self._db.execute(
                "INSERT INTO transaction_metadata"
                " (transaction_id, is_internal_transfer, transfer_pair_id,"
                "  transfer_detected_by, type_override, category, category_source,"
                "  category_confidence, updated_at)"
                " VALUES (?, 1, ?, 'manual', 'transfer', 'transfer', 'manual', 1.0, datetime('now'))"
                " ON CONFLICT(transaction_id) DO UPDATE SET"
                "  is_internal_transfer = 1,"
                "  transfer_pair_id = excluded.transfer_pair_id,"
                "  transfer_detected_by = 'manual',"
                "  type_override = 'transfer',"
                "  category = 'transfer',"
                "  category_source = 'manual',"
                "  category_confidence = 1.0,"
                "  updated_at = excluded.updated_at",
                (tx_id, pair_id),
            )
        await self._db.commit()

    async def unlink_transfer(self, tx_id: int) -> None:
        """Unlink a transaction from its transfer pair."""
        meta = await self.get_metadata(tx_id)
        if meta and meta.transfer_pair_id:
            pair_id = meta.transfer_pair_id
            # Clear transfer fields on both sides.
            for tid in [tx_id, pair_id]:
                await self._db.execute(
                    "UPDATE transaction_metadata SET"
                    " is_internal_transfer = 0, transfer_pair_id = NULL,"
                    " transfer_detected_by = NULL,"
                    " type_override = NULL, category = NULL,"
                    " category_source = 'auto', category_confidence = NULL,"
                    " updated_at = datetime('now')"
                    " WHERE transaction_id = ?",
                    (tid,),
                )
            await self._db.commit()

    # ── Category rules ────────────────────────────────────────────────

    async def get_category_rules(
        self,
        *,
        source: str | None = None,
        include_deleted: bool = False,
    ) -> list[CategoryRule]:
        """List category rules ordered by priority."""
        query = "SELECT * FROM category_rules"
        conditions: list[str] = []
        params: list[str | int] = []
        if not include_deleted:
            conditions.append("deleted = 0")
        if source is not None:
            conditions.append("(source = ? OR source = '*')")
            params.append(source)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY priority ASC, id ASC"
        cursor = await self._db.execute(query, params)
        rows = await cursor.fetchall()
        return [self._row_to_category_rule(row) for row in rows]

    async def create_category_rule(  # noqa: PLR0913
        self,
        type_match: str,
        result_category: str,
        *,
        type_operator: str = "eq",
        field_name: str = "",
        field_operator: str = "",
        field_value: str = "",
        source: str = "*",
        priority: int | None = None,
    ) -> CategoryRule:
        """Create a category rule. Priority is auto-computed if not specified."""
        if field_operator == "regex" and field_value:
            _validate_regex_value(field_value)
        if priority is None:
            priority = _auto_priority(field_name=field_name, source=source)
        cursor = await self._db.execute(
            "INSERT INTO category_rules"
            " (type_match, type_operator, field_name, field_operator, field_value,"
            "  source, result_category, priority, builtin)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)",
            (
                type_match,
                type_operator,
                field_name or None,
                field_operator or None,
                field_value or None,
                source,
                result_category,
                priority,
            ),
        )
        await self._db.commit()
        row = await (
            await self._db.execute("SELECT * FROM category_rules WHERE id = ?", (cursor.lastrowid,))
        ).fetchone()
        assert row is not None  # noqa: S101
        return self._row_to_category_rule(row)

    async def delete_category_rule(self, rule_id: int) -> bool:
        """Delete a category rule. Builtin rules are soft-deleted."""
        # Check if builtin.
        row = await (await self._db.execute("SELECT builtin FROM category_rules WHERE id = ?", (rule_id,))).fetchone()
        if row is None:
            return False
        if row["builtin"]:
            await self._db.execute(
                "UPDATE category_rules SET deleted = 1 WHERE id = ?",
                (rule_id,),
            )
        else:
            await self._db.execute("DELETE FROM category_rules WHERE id = ?", (rule_id,))
        await self._db.commit()
        return True

    async def reset_category_rules(self, source: str | None = None) -> None:
        """Reset rules: soft-delete custom rules, restore builtin rules."""
        if source:
            await self._db.execute(
                "UPDATE category_rules SET deleted = 1 WHERE builtin = 0 AND source = ?",
                (source,),
            )
            await self._db.execute(
                "UPDATE category_rules SET deleted = 0 WHERE builtin = 1 AND (source = ? OR source = '*')",
                (source,),
            )
        else:
            await self._db.execute("UPDATE category_rules SET deleted = 1 WHERE builtin = 0")
            await self._db.execute("UPDATE category_rules SET deleted = 0 WHERE builtin = 1")
        await self._db.commit()

    @staticmethod
    def _row_to_category_rule(row: aiosqlite.Row) -> CategoryRule:
        return CategoryRule(
            id=row["id"],
            type_match=row["type_match"],
            type_operator=row["type_operator"],
            field_name=row["field_name"] or "",
            field_operator=row["field_operator"] or "",
            field_value=row["field_value"] or "",
            source=row["source"],
            result_category=row["result_category"],
            priority=row["priority"],
            builtin=bool(row["builtin"]),
            deleted=bool(row["deleted"]),
        )

    # ── Suggestions (learning from user choices) ────────────────────

    async def get_category_suggestions(
        self,
        min_evidence: int = 2,
    ) -> list[dict[str, object]]:
        """Analyze user_category_choices to suggest new rules.

        Groups choices by (source, effective_type, chosen_category) and
        extracts common field values from field_snapshot JSON.
        Returns suggestions with evidence count and sample transactions.
        """
        import json

        cursor = await self._db.execute(
            "SELECT source, effective_type, chosen_category, field_snapshot"
            " FROM user_category_choices"
            " ORDER BY created_at DESC"
        )
        rows = await cursor.fetchall()
        if not rows:
            return []

        # Group by (source, type, category) and collect field snapshots.
        groups: dict[tuple[str, str, str], list[dict[str, str]]] = {}
        for row in rows:
            key = (row["source"], row["effective_type"], row["chosen_category"])
            snapshot: dict[str, str] = {}
            if row["field_snapshot"]:
                import contextlib

                with contextlib.suppress(json.JSONDecodeError, TypeError):
                    snapshot = json.loads(row["field_snapshot"])
            groups.setdefault(key, []).append(snapshot)

        # Check existing rules to avoid duplicates.
        existing_rules = await self.get_category_rules()
        existing_keys = {
            (r.source, r.type_match, r.result_category, r.field_name, r.field_value)
            for r in existing_rules
            if not r.deleted
        }

        suggestions: list[dict[str, object]] = []
        for (source, etype, category), snapshots in groups.items():
            if len(snapshots) < min_evidence:
                continue

            # Find common field values across snapshots.
            best_field, best_value = _find_common_field(snapshots)

            # Skip if a matching rule already exists.
            rule_key = (source, etype, category, best_field or "", best_value or "")
            if rule_key in existing_keys:
                continue

            suggested_rule: dict[str, object] = {
                "type_match": etype,
                "result_category": category,
                "source": source,
            }
            if best_field and best_value:
                suggested_rule["field_name"] = best_field
                suggested_rule["field_operator"] = "eq"
                suggested_rule["field_value"] = best_value

            suggestions.append(
                {
                    "suggested_rule": suggested_rule,
                    "evidence_count": len(snapshots),
                }
            )

        def _sort_key(s: dict[str, object]) -> int:
            v = s.get("evidence_count", 0)
            return v if isinstance(v, int) else 0

        suggestions.sort(key=_sort_key, reverse=True)
        return suggestions

    # ── User category choices (recording) ─────────────────────────────

    async def record_category_choice(  # noqa: PLR0913
        self,
        transaction_id: int,
        source: str,
        effective_type: str,
        chosen_category: str,
        *,
        field_snapshot: str = "",
        previous_category: str = "",
    ) -> None:
        """Record a manual category selection for learning."""
        await self._db.execute(
            "INSERT INTO user_category_choices"
            " (transaction_id, source, effective_type, field_snapshot,"
            "  chosen_category, previous_category)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (transaction_id, source, effective_type, field_snapshot, chosen_category, previous_category),
        )
        await self._db.commit()

    # ── Type rules ──────────────────────────────────────────────────

    async def get_type_rules(
        self,
        *,
        source: str | None = None,
        include_deleted: bool = False,
    ) -> list[TypeRule]:
        """List type rules ordered by priority."""
        query = "SELECT * FROM type_rules"
        conditions: list[str] = []
        params: list[str | int] = []
        if not include_deleted:
            conditions.append("deleted = 0")
        if source is not None:
            conditions.append("(source = ? OR source = '*')")
            params.append(source)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY priority ASC, id ASC"
        cursor = await self._db.execute(query, params)
        rows = await cursor.fetchall()
        return [self._row_to_type_rule(row) for row in rows]

    async def create_type_rule(  # noqa: PLR0913
        self,
        result_type: str,
        *,
        source: str = "*",
        field_name: str = "",
        field_operator: str = "eq",
        field_value: str = "",
        priority: int | None = None,
    ) -> TypeRule:
        """Create a type rule. Priority is auto-computed if not specified."""
        if field_operator == "regex" and field_value:
            _validate_regex_value(field_value)
        if priority is None:
            priority = _auto_priority(field_name=field_name, source=source)
        cursor = await self._db.execute(
            "INSERT INTO type_rules"
            " (source, field_name, field_operator, field_value,"
            "  result_type, priority, builtin)"
            " VALUES (?, ?, ?, ?, ?, ?, 0)",
            (
                source,
                field_name or None,
                field_operator,
                field_value or None,
                result_type,
                priority,
            ),
        )
        await self._db.commit()
        row = await (await self._db.execute("SELECT * FROM type_rules WHERE id = ?", (cursor.lastrowid,))).fetchone()
        assert row is not None  # noqa: S101
        return self._row_to_type_rule(row)

    async def delete_type_rule(self, rule_id: int) -> bool:
        """Delete a type rule. Builtin rules are soft-deleted."""
        row = await (await self._db.execute("SELECT builtin FROM type_rules WHERE id = ?", (rule_id,))).fetchone()
        if row is None:
            return False
        if row["builtin"]:
            await self._db.execute(
                "UPDATE type_rules SET deleted = 1 WHERE id = ?",
                (rule_id,),
            )
        else:
            await self._db.execute("DELETE FROM type_rules WHERE id = ?", (rule_id,))
        await self._db.commit()
        return True

    async def reset_type_rules(self, source: str | None = None) -> None:
        """Reset rules: soft-delete custom rules, restore builtin rules."""
        if source:
            await self._db.execute(
                "UPDATE type_rules SET deleted = 1 WHERE builtin = 0 AND source = ?",
                (source,),
            )
            await self._db.execute(
                "UPDATE type_rules SET deleted = 0 WHERE builtin = 1 AND (source = ? OR source = '*')",
                (source,),
            )
        else:
            await self._db.execute("UPDATE type_rules SET deleted = 1 WHERE builtin = 0")
            await self._db.execute("UPDATE type_rules SET deleted = 0 WHERE builtin = 1")
        await self._db.commit()

    @staticmethod
    def _row_to_type_rule(row: aiosqlite.Row) -> TypeRule:
        return TypeRule(
            id=row["id"],
            source=row["source"],
            field_name=row["field_name"] or "",
            field_operator=row["field_operator"],
            field_value=row["field_value"] or "",
            result_type=row["result_type"],
            priority=row["priority"],
            builtin=bool(row["builtin"]),
            deleted=bool(row["deleted"]),
        )

    # ── Paginated transactions with metadata ──────────────────────────

    async def get_transactions_paginated(  # noqa: PLR0913
        self,
        *,
        source_name: str | None = None,
        tx_type: str | None = None,
        category: str | None = None,
        start: date | None = None,
        end: date | None = None,
        search: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[tuple[Transaction, TransactionMetadata | None]], int]:
        """Get paginated transactions with optional metadata.

        Returns (items, total_count).
        """
        from pfm.db.repository import Repository

        where_clauses: list[str] = []
        params: list[str | int] = []

        if source_name is not None:
            where_clauses.append("t.source_name = ?")
            params.append(source_name)
        if tx_type is not None:
            where_clauses.append("t.tx_type = ?")
            params.append(tx_type)
        if category is not None:
            where_clauses.append("m.category = ?")
            params.append(category)
        if start is not None:
            where_clauses.append("t.date >= ?")
            params.append(str(start))
        if end is not None:
            where_clauses.append("t.date <= ?")
            params.append(str(end))
        if search:
            escaped = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            where_clauses.append(
                "(t.asset LIKE ? ESCAPE '\\' OR t.source_name LIKE ? ESCAPE '\\' OR t.tx_id LIKE ? ESCAPE '\\')"
            )
            pattern = f"%{escaped}%"
            params.extend([pattern, pattern, pattern])

        where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        # Count query.
        count_cursor = await self._db.execute(
            f"SELECT COUNT(*) FROM transactions t"  # noqa: S608
            f" LEFT JOIN transaction_metadata m ON t.id = m.transaction_id"
            f"{where_sql}",
            params,
        )
        count_row = await count_cursor.fetchone()
        total = count_row[0] if count_row else 0

        # Data query.
        cursor = await self._db.execute(
            f"SELECT t.*,"  # noqa: S608
            f" m.category AS m_category, m.category_source AS m_category_source,"
            f" m.category_confidence AS m_category_confidence,"
            f" m.type_override AS m_type_override,"
            f" m.is_internal_transfer AS m_is_internal_transfer,"
            f" m.transfer_pair_id AS m_transfer_pair_id,"
            f" m.transfer_detected_by AS m_transfer_detected_by,"
            f" m.reviewed AS m_reviewed, m.notes AS m_notes"
            f" FROM transactions t"
            f" LEFT JOIN transaction_metadata m ON t.id = m.transaction_id"
            f"{where_sql}"
            f" ORDER BY t.date DESC, t.id DESC"
            f" LIMIT ? OFFSET ?",
            [*params, limit, offset],
        )
        rows = await cursor.fetchall()

        result: list[tuple[Transaction, TransactionMetadata | None]] = []
        for row in rows:
            tx = Repository.row_to_transaction(row)
            meta = None
            if row["m_category"] is not None or row["m_category_source"] is not None:
                meta = TransactionMetadata(
                    transaction_id=row["id"],
                    category=row["m_category"],
                    category_source=row["m_category_source"] or "auto",
                    category_confidence=row["m_category_confidence"],
                    type_override=row["m_type_override"],
                    is_internal_transfer=bool(row["m_is_internal_transfer"]),
                    transfer_pair_id=row["m_transfer_pair_id"],
                    transfer_detected_by=row["m_transfer_detected_by"],
                    reviewed=bool(row["m_reviewed"]) if row["m_reviewed"] is not None else False,
                    notes=row["m_notes"] or "",
                )
            result.append((tx, meta))
        return result, total

    async def get_transaction_by_id(
        self,
        transaction_id: int,
    ) -> tuple[Transaction, TransactionMetadata | None] | None:
        """Get a single transaction with its metadata."""
        from pfm.db.repository import Repository

        cursor = await self._db.execute(
            "SELECT t.*,"
            " m.category AS m_category, m.category_source AS m_category_source,"
            " m.category_confidence AS m_category_confidence,"
            " m.type_override AS m_type_override,"
            " m.is_internal_transfer AS m_is_internal_transfer,"
            " m.transfer_pair_id AS m_transfer_pair_id,"
            " m.transfer_detected_by AS m_transfer_detected_by,"
            " m.reviewed AS m_reviewed, m.notes AS m_notes"
            " FROM transactions t"
            " LEFT JOIN transaction_metadata m ON t.id = m.transaction_id"
            " WHERE t.id = ?",
            (transaction_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None

        tx = Repository.row_to_transaction(row)
        meta = None
        if row["m_category"] is not None or row["m_category_source"] is not None:
            meta = TransactionMetadata(
                transaction_id=row["id"],
                category=row["m_category"],
                category_source=row["m_category_source"] or "auto",
                category_confidence=row["m_category_confidence"],
                type_override=row["m_type_override"],
                is_internal_transfer=bool(row["m_is_internal_transfer"]),
                transfer_pair_id=row["m_transfer_pair_id"],
                transfer_detected_by=row["m_transfer_detected_by"],
                reviewed=bool(row["m_reviewed"]) if row["m_reviewed"] is not None else False,
                notes=row["m_notes"] or "",
            )
        return tx, meta

    # ── Categorization summary / discovery ────────────────────────────

    async def get_categorization_summary(
        self,
        *,
        source_name: str | None = None,
    ) -> list[dict[str, object]]:
        """Per-source counts for the categorization workflow.

        Each entry: source_name, total, unknown_type, no_category, internal_transfer.
        """
        params: list[str] = []
        where_sql = ""
        if source_name is not None:
            where_sql = " WHERE t.source_name = ?"
            params.append(source_name)
        cursor = await self._db.execute(
            "SELECT t.source_name AS source_name,"  # noqa: S608
            "  COUNT(*) AS total,"
            "  SUM(CASE WHEN t.tx_type = 'unknown'"
            "       AND (m.type_override IS NULL OR m.type_override = '') THEN 1 ELSE 0 END) AS unknown_type,"
            "  SUM(CASE WHEN m.category IS NULL"
            "       AND COALESCE(m.is_internal_transfer, 0) = 0 THEN 1 ELSE 0 END) AS no_category,"
            "  SUM(CASE WHEN COALESCE(m.is_internal_transfer, 0) = 1 THEN 1 ELSE 0 END) AS internal_transfer"
            " FROM transactions t"
            " LEFT JOIN transaction_metadata m ON t.id = m.transaction_id"
            f"{where_sql}"
            " GROUP BY t.source_name"
            " ORDER BY t.source_name",
            params,
        )
        rows = await cursor.fetchall()
        return [
            {
                "source_name": row["source_name"],
                "total": int(row["total"]),
                "unknown_type": int(row["unknown_type"] or 0),
                "no_category": int(row["no_category"] or 0),
                "internal_transfer": int(row["internal_transfer"] or 0),
            }
            for row in rows
        ]

    async def get_uncategorized_transactions(
        self,
        *,
        source_name: str | None = None,
        missing_type: bool = False,
        missing_category: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[tuple[Transaction, TransactionMetadata | None]], int]:
        """Paginated transactions missing type and/or category.

        - missing_type: tx_type='unknown' AND no type_override.
        - missing_category: m.category IS NULL AND not internal_transfer.
        - Both False (default): OR — surface anything needing attention.
        - Both True: AND.
        """
        from pfm.db.repository import Repository

        type_clause = "(t.tx_type = 'unknown' AND (m.type_override IS NULL OR m.type_override = ''))"
        cat_clause = "(m.category IS NULL AND COALESCE(m.is_internal_transfer, 0) = 0)"

        if missing_type and missing_category:
            filter_clause = f"({type_clause} AND {cat_clause})"
        elif missing_type:
            filter_clause = type_clause
        elif missing_category:
            filter_clause = cat_clause
        else:
            filter_clause = f"({type_clause} OR {cat_clause})"

        where_clauses: list[str] = [filter_clause]
        params: list[str | int] = []
        if source_name is not None:
            where_clauses.append("t.source_name = ?")
            params.append(source_name)
        where_sql = " WHERE " + " AND ".join(where_clauses)

        count_cursor = await self._db.execute(
            f"SELECT COUNT(*) FROM transactions t"  # noqa: S608
            f" LEFT JOIN transaction_metadata m ON t.id = m.transaction_id"
            f"{where_sql}",
            params,
        )
        count_row = await count_cursor.fetchone()
        total = count_row[0] if count_row else 0

        cursor = await self._db.execute(
            f"SELECT t.*,"  # noqa: S608
            f" m.category AS m_category, m.category_source AS m_category_source,"
            f" m.category_confidence AS m_category_confidence,"
            f" m.type_override AS m_type_override,"
            f" m.is_internal_transfer AS m_is_internal_transfer,"
            f" m.transfer_pair_id AS m_transfer_pair_id,"
            f" m.transfer_detected_by AS m_transfer_detected_by,"
            f" m.reviewed AS m_reviewed, m.notes AS m_notes"
            f" FROM transactions t"
            f" LEFT JOIN transaction_metadata m ON t.id = m.transaction_id"
            f"{where_sql}"
            f" ORDER BY t.date DESC, t.id DESC"
            f" LIMIT ? OFFSET ?",
            [*params, limit, offset],
        )
        rows = await cursor.fetchall()

        result: list[tuple[Transaction, TransactionMetadata | None]] = []
        for row in rows:
            tx = Repository.row_to_transaction(row)
            meta = None
            if row["m_category"] is not None or row["m_category_source"] is not None:
                meta = TransactionMetadata(
                    transaction_id=row["id"],
                    category=row["m_category"],
                    category_source=row["m_category_source"] or "auto",
                    category_confidence=row["m_category_confidence"],
                    type_override=row["m_type_override"],
                    is_internal_transfer=bool(row["m_is_internal_transfer"]),
                    transfer_pair_id=row["m_transfer_pair_id"],
                    transfer_detected_by=row["m_transfer_detected_by"],
                    reviewed=bool(row["m_reviewed"]) if row["m_reviewed"] is not None else False,
                    notes=row["m_notes"] or "",
                )
            result.append((tx, meta))
        return result, total


def _validate_regex_value(field_value: str) -> None:
    """Compile each pattern (JSON-array or plain) so create rejects invalid regex."""
    if field_value.startswith("["):
        try:
            parsed = json.loads(field_value)
        except (json.JSONDecodeError, TypeError):
            parsed = [field_value]
        if not isinstance(parsed, list):
            parsed = [field_value]
    else:
        parsed = [field_value]
    for pat in parsed:
        try:
            re.compile(str(pat))
        except re.error as exc:
            msg = f"invalid regex pattern {pat!r}: {exc}"
            raise ValueError(msg) from exc


def _auto_priority(*, field_name: str, source: str) -> int:
    """Compute priority based on rule specificity. Lower = higher priority."""
    has_field = bool(field_name)
    has_source = source != "*"
    if has_field and has_source:
        return 100
    if has_field:
        return 150
    if has_source:
        return 200
    return 300


_SKIP_FIELDS = {"balance", "time", "ts", "transactionTime", "datetime", "dateTime", "timestamp"}


def _find_common_field(
    snapshots: list[dict[str, str]],
) -> tuple[str | None, str | None]:
    """Find the most common (field, value) pair across snapshots.

    Skips non-useful fields (balance, timestamps). Returns (None, None)
    if no common field is found in >50% of snapshots.
    """
    from collections import Counter

    threshold = len(snapshots) / 2
    pair_counts: Counter[tuple[str, str]] = Counter()
    for snap in snapshots:
        for field, value in snap.items():
            if field in _SKIP_FIELDS or not value:
                continue
            pair_counts[(field, value)] += 1

    if not pair_counts:
        return None, None

    (best_field, best_value), count = pair_counts.most_common(1)[0]
    if count > threshold:
        return best_field, best_value
    return None, None
