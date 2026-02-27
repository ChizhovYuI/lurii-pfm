"""Initial schema.

Revision ID: 9cd516d0ab26
Revises:
Create Date: 2026-02-27 21:00:53.408220

"""

from typing import Final

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: Final[str] = "9cd516d0ab26"
down_revision: Final[str | None] = None
branch_labels: Final[str | None] = None
depends_on: Final[str | None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "snapshots",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("date", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("asset", sa.Text(), nullable=False),
        sa.Column("amount", sa.Text(), nullable=False),
        sa.Column("usd_value", sa.Text(), nullable=False),
        sa.Column("raw_json", sa.Text(), nullable=False, server_default=sa.text("''")),
        sa.Column("created_at", sa.Text(), nullable=False, server_default=sa.text("(datetime('now'))")),
    )
    op.create_index("idx_snapshots_date", "snapshots", ["date"], unique=False)
    op.create_index("idx_snapshots_source", "snapshots", ["source"], unique=False)

    op.create_table(
        "transactions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("date", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("tx_type", sa.Text(), nullable=False),
        sa.Column("asset", sa.Text(), nullable=False),
        sa.Column("amount", sa.Text(), nullable=False),
        sa.Column("usd_value", sa.Text(), nullable=False),
        sa.Column("counterparty_asset", sa.Text(), nullable=False, server_default=sa.text("''")),
        sa.Column("counterparty_amount", sa.Text(), nullable=False, server_default=sa.text("'0'")),
        sa.Column("tx_id", sa.Text(), nullable=False, server_default=sa.text("''")),
        sa.Column("raw_json", sa.Text(), nullable=False, server_default=sa.text("''")),
        sa.Column("created_at", sa.Text(), nullable=False, server_default=sa.text("(datetime('now'))")),
    )
    op.create_index("idx_transactions_date", "transactions", ["date"], unique=False)
    op.create_index("idx_transactions_source", "transactions", ["source"], unique=False)

    op.create_table(
        "prices",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("date", sa.Text(), nullable=False),
        sa.Column("asset", sa.Text(), nullable=False),
        sa.Column("currency", sa.Text(), nullable=False),
        sa.Column("price", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False, server_default=sa.text("'coingecko'")),
        sa.Column("created_at", sa.Text(), nullable=False, server_default=sa.text("(datetime('now'))")),
    )
    op.create_index("idx_prices_date_asset", "prices", ["date", "asset"], unique=False)

    op.create_table(
        "raw_responses",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("date", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("endpoint", sa.Text(), nullable=False),
        sa.Column("response_body", sa.Text(), nullable=False),
        sa.Column("created_at", sa.Text(), nullable=False, server_default=sa.text("(datetime('now'))")),
    )

    op.create_table(
        "analytics_cache",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("date", sa.Text(), nullable=False),
        sa.Column("metric_name", sa.Text(), nullable=False),
        sa.Column("metric_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.Text(), nullable=False, server_default=sa.text("(datetime('now'))")),
    )
    op.create_index("idx_analytics_cache_date", "analytics_cache", ["date"], unique=False)

    op.create_table(
        "sources",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.Text(), nullable=False, unique=True),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column("credentials", sa.Text(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("enabled", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.Text(), nullable=False, server_default=sa.text("(datetime('now'))")),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("sources")

    op.drop_index("idx_analytics_cache_date", table_name="analytics_cache")
    op.drop_table("analytics_cache")

    op.drop_table("raw_responses")

    op.drop_index("idx_prices_date_asset", table_name="prices")
    op.drop_table("prices")

    op.drop_index("idx_transactions_source", table_name="transactions")
    op.drop_index("idx_transactions_date", table_name="transactions")
    op.drop_table("transactions")

    op.drop_index("idx_snapshots_source", table_name="snapshots")
    op.drop_index("idx_snapshots_date", table_name="snapshots")
    op.drop_table("snapshots")
