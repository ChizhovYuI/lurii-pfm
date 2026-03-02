"""Add source_name column to snapshots.

Revision ID: 5f3f6d1f2e11
Revises: 8d775e055451
Create Date: 2026-03-02 18:00:00.000000

"""

from typing import Final

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: Final[str] = "5f3f6d1f2e11"
down_revision: Final[str | None] = "8d775e055451"
branch_labels: Final[str | None] = None
depends_on: Final[str | None] = None


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col["name"] for col in inspector.get_columns("snapshots")}
    indexes = {idx["name"] for idx in inspector.get_indexes("snapshots")}

    with op.batch_alter_table("snapshots") as batch_op:
        if "source_name" not in columns:
            batch_op.add_column(sa.Column("source_name", sa.Text(), nullable=False, server_default=sa.text("''")))
        if "idx_snapshots_source_name" not in indexes:
            batch_op.create_index("idx_snapshots_source_name", ["source_name"], unique=False)

    # If there is exactly one configured source for a type, align to its instance name.
    op.execute(
        "UPDATE snapshots "
        "SET source_name = (SELECT MIN(name) FROM sources WHERE type = snapshots.source) "
        "WHERE (source_name = '' OR source_name IS NULL) "
        "  AND (SELECT COUNT(*) FROM sources WHERE type = snapshots.source) = 1"
    )

    # Rows that cannot be mapped unambiguously are invalid for the new schema.
    op.execute("DELETE FROM snapshots WHERE source_name = '' OR source_name IS NULL")


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col["name"] for col in inspector.get_columns("snapshots")}
    indexes = {idx["name"] for idx in inspector.get_indexes("snapshots")}

    with op.batch_alter_table("snapshots") as batch_op:
        if "idx_snapshots_source_name" in indexes:
            batch_op.drop_index("idx_snapshots_source_name")
        if "source_name" in columns:
            batch_op.drop_column("source_name")
