"""Backfill source_id on transactions, snapshots, user_category_choices.

Stage 1 of source-identity normalization (ADR-030). Two-pass backfill:

1. Exact-match pass: source_name = sources.name. Handles every modern row
   (collector emits source_name = sources.name by convention).
2. Fallback pass: when source_name does not match any sources.name and
   exactly one sources row exists for that type, link to that row.
   Handles historical rows pre-dating the source_name convention.

Rule tables (category_rules, type_rules) keep source_id NULL — their
text source column carries the existing rule semantics in stage 1.

Rows that resolve to no sources row stay NULL (logged via row count).
A future migration will tighten to NOT NULL after final cleanup.

Revision ID: i9j0k1l2m3n4
Revises: h8i9j0k1l2m3
Create Date: 2026-04-25 10:30:00.000000

"""

from __future__ import annotations

from typing import Final

import sqlalchemy as sa
from alembic import op

revision: Final[str] = "i9j0k1l2m3n4"
down_revision: Final[str | None] = "h8i9j0k1l2m3"
branch_labels: Final[str | None] = None
depends_on: Final[str | None] = None


_DATA_TABLES: tuple[str, ...] = ("transactions", "snapshots", "user_category_choices")


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())
    if "sources" not in existing_tables:
        return

    for table in _DATA_TABLES:
        if table not in existing_tables:
            continue
        cols = {col["name"] for col in inspector.get_columns(table)}
        if "source_id" not in cols:
            continue

        # `table` comes from the hardcoded _DATA_TABLES tuple — no injection vector.
        if "source_name" in cols:
            sql_match = f"UPDATE {table} SET source_id = (SELECT id FROM sources WHERE sources.name = {table}.source_name) WHERE source_id IS NULL AND EXISTS (SELECT 1 FROM sources WHERE sources.name = {table}.source_name)"  # noqa: S608, E501
            bind.execute(sa.text(sql_match))

        if "source" in cols:
            sql_fallback = f"UPDATE {table} SET source_id = (SELECT id FROM sources WHERE sources.type = {table}.source LIMIT 1) WHERE source_id IS NULL AND (SELECT COUNT(*) FROM sources WHERE sources.type = {table}.source) = 1"  # noqa: S608, E501
            bind.execute(sa.text(sql_fallback))


def downgrade() -> None:
    """Downgrade schema.

    Reversible — wipes the populated source_id values back to NULL.
    The h8i9j0k1l2m3 downgrade drops the columns themselves.
    """
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())
    for table in _DATA_TABLES:
        if table not in existing_tables:
            continue
        cols = {col["name"] for col in inspector.get_columns(table)}
        if "source_id" in cols:
            bind.execute(sa.text(f"UPDATE {table} SET source_id = NULL"))  # noqa: S608
