"""Add price and apy columns to snapshots.

Revision ID: c4a2e7d6f9b1
Revises: b7a1c91c9d5f
Create Date: 2026-03-08 17:10:00.000000

"""

from typing import Final

import sqlalchemy as sa
from alembic import op

revision: Final[str] = "c4a2e7d6f9b1"
down_revision: Final[str | None] = "b7a1c91c9d5f"
branch_labels: Final[str | None] = None
depends_on: Final[str | None] = None


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col["name"] for col in inspector.get_columns("snapshots")}

    with op.batch_alter_table("snapshots") as batch_op:
        if "price" not in columns:
            batch_op.add_column(sa.Column("price", sa.Text(), nullable=False, server_default=sa.text("'0'")))
        if "apy" not in columns:
            batch_op.add_column(sa.Column("apy", sa.Text(), nullable=False, server_default=sa.text("'0'")))


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col["name"] for col in inspector.get_columns("snapshots")}

    with op.batch_alter_table("snapshots") as batch_op:
        if "apy" in columns:
            batch_op.drop_column("apy")
        if "price" in columns:
            batch_op.drop_column("price")
