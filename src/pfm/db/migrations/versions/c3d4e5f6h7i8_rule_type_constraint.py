"""Add type_constraint column to categorization_rules.

Revision ID: c3d4e5f6h7i8
Revises: b2c3d4e5f6g7
Create Date: 2026-03-15 18:00:00.000000

"""

from typing import Final

import sqlalchemy as sa
from alembic import op

revision: Final[str] = "c3d4e5f6h7i8"
down_revision: Final[str | None] = "b2c3d4e5f6g7"
branch_labels: Final[str | None] = None
depends_on: Final[str | None] = None


def upgrade() -> None:
    """Add type_constraint column to categorization_rules."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "categorization_rules" not in inspector.get_table_names():
        return
    columns = {col["name"] for col in inspector.get_columns("categorization_rules")}
    if "type_constraint" not in columns:
        op.add_column(
            "categorization_rules",
            sa.Column("type_constraint", sa.Text, nullable=True, server_default=None),
        )


def downgrade() -> None:
    """Remove type_constraint column from categorization_rules."""
    op.drop_column("categorization_rules", "type_constraint")
