"""Add source_name/trade_side columns and dedupe index to transactions.

Revision ID: b7a1c91c9d5f
Revises: 5f3f6d1f2e11
Create Date: 2026-03-07 20:30:00.000000

"""

from typing import Final

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: Final[str] = "b7a1c91c9d5f"
down_revision: Final[str | None] = "5f3f6d1f2e11"
branch_labels: Final[str | None] = None
depends_on: Final[str | None] = None


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col["name"] for col in inspector.get_columns("transactions")}
    indexes = {idx["name"] for idx in inspector.get_indexes("transactions")}

    with op.batch_alter_table("transactions") as batch_op:
        if "source_name" not in columns:
            batch_op.add_column(sa.Column("source_name", sa.Text(), nullable=False, server_default=sa.text("''")))
        if "trade_side" not in columns:
            batch_op.add_column(sa.Column("trade_side", sa.Text(), nullable=False, server_default=sa.text("''")))
        if "idx_transactions_source_name_date" not in indexes:
            batch_op.create_index("idx_transactions_source_name_date", ["source_name", "date"], unique=False)
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_source_name_tx_id_unique "
        "ON transactions(source_name, tx_id) WHERE tx_id != '' AND source_name != ''"
    )


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col["name"] for col in inspector.get_columns("transactions")}
    indexes = {idx["name"] for idx in inspector.get_indexes("transactions")}

    op.execute("DROP INDEX IF EXISTS idx_transactions_source_name_tx_id_unique")

    with op.batch_alter_table("transactions") as batch_op:
        if "idx_transactions_source_name_date" in indexes:
            batch_op.drop_index("idx_transactions_source_name_date")
        if "trade_side" in columns:
            batch_op.drop_column("trade_side")
        if "source_name" in columns:
            batch_op.drop_column("source_name")
