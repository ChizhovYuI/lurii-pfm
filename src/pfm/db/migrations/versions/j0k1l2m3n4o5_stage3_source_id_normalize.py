"""Stage 3 of source-identity normalization (ADR-030).

Destructive — finalizes the FK design started in Stage 1+2.

Steps (in order, single transaction):

1. **Pre-flight: orphan check.** Abort if any ``transactions`` /
   ``snapshots`` row still has ``source_id IS NULL`` (Stage 1 backfill
   leaves orphans for unmatched ``source_name`` values).
2. **Pre-flight: coinex tx_id collision.** When two ``sources`` rows
   share the same ``type``, the merge step will collapse them. Abort if
   any ``tx_id`` collides between the rows being merged. Mirrors the
   `f2c7e6a9d1b4` KBank empties pattern.
3. **Source merge.** For each duplicate-type group, pick the canonical
   row (the one whose ``name`` ends in ``-main``, else the lowest id),
   point all data + rule rows to it, delete the others.
4. **Rule rewrite.** Map the existing ``category_rules.source`` /
   ``type_rules.source`` text column into the new
   ``source_type`` (text) + ``source_id`` (FK) pair:
     - ``"*"`` → both NULL (catch-all)
     - matches ``sources.name`` → ``source_id`` populated
     - else → ``source_type`` populated (covers existing ``sources.type``
       values plus pre-source historical strings like ``"revolut"``)
   New ``CHECK`` constraint forbids both being non-NULL simultaneously.
5. **Drop ``source_name`` columns** from ``transactions``, ``snapshots``.
6. **Tighten ``source_id`` NOT NULL** on ``transactions``, ``snapshots``.
7. **Swap dedup index** — ``(source_name, tx_id)`` →
   ``(source_id, tx_id)``.
8. **Drop ``category_rules.source`` and ``type_rules.source``** text
   columns (replaced by ``source_type`` + ``source_id``).

Out of scope here (deferred to a future ADR if needed):
- Renaming ``transactions.source`` / ``snapshots.source`` columns to
  ``source_type`` — the column already stores the source type string,
  so the rename is purely cosmetic and not worth the ~150 read-site
  churn. The Python ``Transaction.source`` field continues to hold the
  type. ADR-030 documents this deferral.

Revision ID: j0k1l2m3n4o5
Revises: i9j0k1l2m3n4
Create Date: 2026-04-25 23:30:00.000000

"""

from __future__ import annotations

from typing import Final

import sqlalchemy as sa
from alembic import op

revision: Final[str] = "j0k1l2m3n4o5"
down_revision: Final[str | None] = "i9j0k1l2m3n4"
branch_labels: Final[str | None] = None
depends_on: Final[str | None] = None


class _Stage3Error(RuntimeError):
    """Raised when a Stage 3 pre-flight check fails."""


def _pick_canonical(rows: list[tuple[int, str]]) -> int:
    """Pick the canonical id from a duplicate-type group.

    Prefers the row whose ``name`` ends in ``-main`` (the project's
    convention), then the lowest id. Deterministic.
    """
    main = [(rid, name) for rid, name in rows if name.endswith("-main")]
    if main:
        return sorted(main, key=lambda r: r[0])[0][0]
    return sorted(rows, key=lambda r: r[0])[0][0]


def _merge_duplicate_sources(bind: sa.engine.Connection) -> None:
    """Collapse ``sources`` rows that share a ``type`` into one canonical row."""
    groups = bind.execute(
        sa.text(
            "SELECT type, GROUP_CONCAT(id || ':' || name, '|') AS members"
            " FROM sources GROUP BY type HAVING COUNT(*) > 1"
        )
    ).fetchall()
    for source_type, members in groups:
        rows: list[tuple[int, str]] = []
        for entry in str(members).split("|"):
            rid_str, name = entry.split(":", 1)
            rows.append((int(rid_str), name))
        canonical = _pick_canonical(rows)
        others = [rid for rid, _ in rows if rid != canonical]
        if not others:
            continue

        # tx_id collision pre-flight before the merge.
        placeholders = ",".join(str(o) for o in others)
        # safe — values are integers from above query
        collision = bind.execute(
            sa.text(
                f"SELECT tx_id FROM transactions"  # noqa: S608
                f" WHERE source_id IN ({canonical},{placeholders})"
                f" AND tx_id != ''"
                f" GROUP BY tx_id HAVING COUNT(DISTINCT source_id) > 1"
                f" LIMIT 1"
            )
        ).fetchone()
        if collision is not None:
            msg = (
                f"Stage 3 abort: tx_id collision across sources of type"
                f" {source_type!r} (canonical={canonical}, others={others})."
                f" Resolve duplicates manually before re-running the migration."
            )
            raise _Stage3Error(msg)

        for table in ("transactions", "snapshots", "user_category_choices", "category_rules", "type_rules"):
            # Table names hardcoded — no injection vector.
            bind.execute(
                sa.text(
                    f"UPDATE {table} SET source_id = :canonical"  # noqa: S608
                    f" WHERE source_id IN ({placeholders})"
                ),
                {"canonical": canonical},
            )
        bind.execute(
            sa.text(f"DELETE FROM sources WHERE id IN ({placeholders})")  # noqa: S608
        )


def _rewrite_rule_source_columns(bind: sa.engine.Connection) -> None:
    """Map ``rules.source`` text → ``source_type`` + ``source_id`` pair.

    Idempotent — when ``source`` column is already gone (re-run after a
    successful Stage 3), only ensures ``source_type`` exists and skips the
    rewrite.
    """
    name_to_id: dict[str, int] = {
        str(name): int(rid) for rid, name in bind.execute(sa.text("SELECT id, name FROM sources")).fetchall()
    }
    types: set[str] = {str(t) for (t,) in bind.execute(sa.text("SELECT DISTINCT type FROM sources")).fetchall()}

    for table in ("category_rules", "type_rules"):
        cols = {c["name"] for c in sa.inspect(bind).get_columns(table)}
        if "source_type" not in cols:
            op.execute(sa.text(f"ALTER TABLE {table} ADD COLUMN source_type TEXT"))
        if "source" not in cols:
            continue  # already migrated

        # safe — table from hardcoded loop
        rows = bind.execute(sa.text(f"SELECT id, source FROM {table}")).fetchall()  # noqa: S608
        for rule_id, raw_source in rows:
            source_str = str(raw_source or "")
            if source_str == "*" or not source_str:
                source_type: str | None = None
                source_id: int | None = None
            elif source_str in name_to_id:
                source_type = None
                source_id = name_to_id[source_str]
            elif source_str in types:
                source_type = source_str
                source_id = None
            else:
                source_type = source_str
                source_id = None
            bind.execute(
                sa.text(
                    f"UPDATE {table} SET source_type = :st, source_id = :sid"  # noqa: S608
                    f" WHERE id = :rid"
                ),
                {"st": source_type, "sid": source_id, "rid": rule_id},
            )


def upgrade() -> None:  # noqa: C901, PLR0912
    """Stage 3 destructive migration."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())
    if "sources" not in tables:
        return

    # 1. Orphan pre-flight: any data row with source_id IS NULL is unsafe to tighten.
    for table in ("transactions", "snapshots"):
        if table not in tables:
            continue
        cols = {c["name"] for c in inspector.get_columns(table)}
        if "source_id" not in cols:
            continue
        # safe — table from hardcoded loop
        orphans = bind.execute(
            sa.text(f"SELECT COUNT(*) FROM {table} WHERE source_id IS NULL")  # noqa: S608
        ).scalar()
        if orphans:
            msg = (
                f"Stage 3 abort: {orphans} row(s) in {table} have NULL source_id."
                " Stage 1 backfill could not link these to a sources row."
                " Either delete the orphans or add the missing sources row,"
                " then re-run the migration."
            )
            raise _Stage3Error(msg)

    # 2 + 3. Coinex (and any future) duplicate-type source merge with collision check.
    _merge_duplicate_sources(bind)

    # 4. Rule rewrite — map text source to (source_type, source_id) pair.
    _rewrite_rule_source_columns(bind)

    # 5. Drop source_name from transactions, snapshots.
    for table in ("transactions", "snapshots"):
        if table not in tables:
            continue
        cols = {c["name"] for c in inspector.get_columns(table)}
        if "source_name" not in cols:
            continue
        existing_indexes = {idx["name"] for idx in inspector.get_indexes(table)}
        with op.batch_alter_table(table) as batch:
            for idx in (
                "idx_transactions_source_name_tx_id_unique",
                "idx_transactions_source_name_date",
                "idx_snapshots_source_name",
            ):
                if idx in existing_indexes:
                    batch.drop_index(idx)
            batch.drop_column("source_name")

    # 6. Tighten source_id NOT NULL on data tables.
    for table in ("transactions", "snapshots"):
        if table not in tables:
            continue
        col_info = {c["name"]: c for c in inspector.get_columns(table)}
        if "source_id" in col_info and col_info["source_id"]["nullable"]:
            with op.batch_alter_table(table) as batch:
                batch.alter_column("source_id", existing_type=sa.Integer(), nullable=False)

    # 7. Swap dedup index → (source_id, tx_id).
    if "transactions" in tables:
        existing_indexes = {idx["name"] for idx in sa.inspect(bind).get_indexes("transactions")}
        if "idx_transactions_source_id_tx_id_unique" not in existing_indexes:
            op.execute(
                sa.text(
                    "CREATE UNIQUE INDEX idx_transactions_source_id_tx_id_unique"
                    " ON transactions(source_id, tx_id) WHERE tx_id != ''"
                )
            )

    # 8. Drop the old text rules.source column + rebuild it under XOR check.
    for table in ("category_rules", "type_rules"):
        if table not in tables:
            continue
        cols = {c["name"] for c in inspector.get_columns(table)}
        if "source" in cols:
            existing_indexes = {idx["name"] for idx in sa.inspect(bind).get_indexes(table)}
            with op.batch_alter_table(table) as batch:
                for idx in (
                    "idx_category_rules_source",
                    "idx_typerules_source",
                    "idx_type_rules_source",
                ):
                    if idx in existing_indexes:
                        batch.drop_index(idx)
                batch.drop_column("source")

    # 9. Add the XOR check on rule tables (NOT both source_type and source_id set).
    #    Use batch_alter_table so SQLite recreates the table with the constraint.
    for table in ("category_rules", "type_rules"):
        if table not in tables:
            continue
        with op.batch_alter_table(table) as batch:
            batch.create_check_constraint(
                f"ck_{table}_source_xor",
                "NOT (source_type IS NOT NULL AND source_id IS NOT NULL)",
            )


def downgrade() -> None:
    """Best-effort downgrade.

    Adding the dropped ``source_name`` column back is destructive (data
    is gone), so this only restores the rules ``source`` text column and
    relaxes the data-table NOT NULL — enough to roll back the rule
    rewrite if needed. Source merge is not reversed (deleted rows lost).
    """
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())

    for table in ("category_rules", "type_rules"):
        if table not in tables:
            continue
        cols = {c["name"] for c in inspector.get_columns(table)}
        if "source" not in cols:
            op.execute(sa.text(f"ALTER TABLE {table} ADD COLUMN source TEXT NOT NULL DEFAULT '*'"))
        # Best-effort: recover source from source_type / source_id mapping.
        bind.execute(
            sa.text(
                f"UPDATE {table} SET source = COALESCE("  # noqa: S608
                f"  source_type,"
                f"  (SELECT name FROM sources WHERE sources.id = {table}.source_id),"
                f"  '*'"
                f")"
            )
        )

    if "transactions" in tables:
        existing_indexes = {idx["name"] for idx in sa.inspect(bind).get_indexes("transactions")}
        if "idx_transactions_source_id_tx_id_unique" in existing_indexes:
            op.drop_index("idx_transactions_source_id_tx_id_unique", table_name="transactions")

    for table in ("transactions", "snapshots"):
        if table not in tables:
            continue
        col_info = {c["name"]: c for c in inspector.get_columns(table)}
        if "source_id" in col_info and not col_info["source_id"]["nullable"]:
            with op.batch_alter_table(table) as batch:
                batch.alter_column("source_id", existing_type=sa.Integer(), nullable=True)
