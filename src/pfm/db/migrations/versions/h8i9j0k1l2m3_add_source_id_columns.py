"""Add nullable source_id FK columns to data and rule tables.

Stage 1 of source-identity normalization (ADR-030). Additive only:
nullable INTEGER REFERENCES sources(id) on transactions, snapshots,
user_category_choices, category_rules, type_rules. Backfill in the
next migration.

Revision ID: h8i9j0k1l2m3
Revises: g7h8i9j0k1l2
Create Date: 2026-04-25 10:00:00.000000

"""

from __future__ import annotations

from typing import Final

import sqlalchemy as sa
from alembic import op

revision: Final[str] = "h8i9j0k1l2m3"
down_revision: Final[str | None] = "g7h8i9j0k1l2"
branch_labels: Final[str | None] = None
depends_on: Final[str | None] = None


_TABLES: tuple[tuple[str, str], ...] = (
    ("transactions", "idx_transactions_source_id"),
    ("snapshots", "idx_snapshots_source_id"),
    ("user_category_choices", "idx_choices_source_id"),
    ("category_rules", "idx_category_rules_source_id"),
    ("type_rules", "idx_type_rules_source_id"),
)


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())
    for table, index_name in _TABLES:
        if table not in existing_tables:
            continue
        cols = {col["name"] for col in inspector.get_columns(table)}
        if "source_id" not in cols:
            op.execute(sa.text(f"ALTER TABLE {table} ADD COLUMN source_id INTEGER REFERENCES sources(id)"))
        existing_indexes = {idx["name"] for idx in inspector.get_indexes(table)}
        if index_name not in existing_indexes:
            op.create_index(index_name, table, ["source_id"])


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())
    for table, index_name in _TABLES:
        if table not in existing_tables:
            continue
        existing_indexes = {idx["name"] for idx in inspector.get_indexes(table)}
        if index_name in existing_indexes:
            op.drop_index(index_name, table_name=table)
        cols = {col["name"] for col in inspector.get_columns(table)}
        if "source_id" in cols:
            with op.batch_alter_table(table) as batch:
                batch.drop_column("source_id")
