"""Add transaction categories, metadata, and categorization rules tables.

Revision ID: a1b2c3d4e5f6
Revises: f2c7e6a9d1b4
Create Date: 2026-03-14 12:00:00.000000

"""

from typing import Final

import sqlalchemy as sa
from alembic import op

revision: Final[str] = "a1b2c3d4e5f6"
down_revision: Final[str | None] = "f2c7e6a9d1b4"
branch_labels: Final[str | None] = None
depends_on: Final[str | None] = None

_DEFAULT_CATEGORIES: list[tuple[str, str, str, int]] = [
    ("deposit", "salary", "Salary", 10),
    ("deposit", "freelance", "Freelance Income", 20),
    ("deposit", "refund", "Refund", 30),
    ("deposit", "external_deposit", "External Deposit", 40),
    ("deposit", "internal_transfer_in", "Internal Transfer In", 50),
    ("withdrawal", "expense_food", "Food & Dining", 10),
    ("withdrawal", "expense_housing", "Housing & Rent", 20),
    ("withdrawal", "expense_transport", "Transportation", 30),
    ("withdrawal", "expense_shopping", "Shopping", 40),
    ("withdrawal", "expense_entertainment", "Entertainment", 50),
    ("withdrawal", "expense_healthcare", "Healthcare", 60),
    ("withdrawal", "expense_utilities", "Utilities & Bills", 70),
    ("withdrawal", "expense_subscriptions", "Subscriptions", 80),
    ("withdrawal", "expense_other", "Other Expense", 90),
    ("withdrawal", "internal_transfer_out", "Internal Transfer Out", 100),
    ("withdrawal", "external_withdrawal", "External Withdrawal", 110),
    ("trade", "spot_trade", "Spot Trade", 10),
    ("trade", "conversion", "Currency Conversion", 20),
    ("trade", "dca_purchase", "DCA Purchase", 30),
    ("yield", "defi_yield", "DeFi Yield", 10),
    ("yield", "staking_reward", "Staking Reward", 20),
    ("dividend", "stock_dividend", "Stock Dividend", 10),
    ("dividend", "etf_distribution", "ETF Distribution", 20),
    ("interest", "savings_interest", "Savings Interest", 10),
    ("interest", "earn_interest", "Earn Interest", 20),
    ("fee", "trading_fee", "Trading Fee", 10),
    ("fee", "network_fee", "Network Fee", 20),
    ("transfer", "internal_move", "Internal Move", 10),
    ("transfer", "bridge", "Cross-chain Bridge", 20),
]


def upgrade() -> None:
    """Create transaction_categories, transaction_metadata, and categorization_rules tables."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())

    if "transaction_categories" in existing_tables:
        return

    op.create_table(
        "transaction_categories",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("tx_type", sa.Text, nullable=False),
        sa.Column("category", sa.Text, nullable=False),
        sa.Column("display_name", sa.Text, nullable=False),
        sa.Column("sort_order", sa.Integer, nullable=False, server_default="0"),
        sa.Column("created_at", sa.Text, nullable=False, server_default=sa.text("(datetime('now'))")),
        sa.UniqueConstraint("tx_type", "category", name="uq_tx_type_category"),
    )

    op.create_table(
        "transaction_metadata",
        sa.Column(
            "transaction_id",
            sa.Integer,
            sa.ForeignKey("transactions.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("category", sa.Text, nullable=True),
        sa.Column("category_source", sa.Text, nullable=False, server_default="auto"),
        sa.Column("category_confidence", sa.Float, nullable=True),
        sa.Column("is_internal_transfer", sa.Integer, nullable=False, server_default="0"),
        sa.Column(
            "transfer_pair_id",
            sa.Integer,
            sa.ForeignKey("transactions.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("transfer_detected_by", sa.Text, nullable=True),
        sa.Column("reviewed", sa.Integer, nullable=False, server_default="0"),
        sa.Column("notes", sa.Text, server_default=""),
        sa.Column("updated_at", sa.Text, nullable=False, server_default=sa.text("(datetime('now'))")),
    )
    op.create_index("idx_metadata_category", "transaction_metadata", ["category"])
    op.create_index("idx_metadata_transfer_pair", "transaction_metadata", ["transfer_pair_id"])

    op.create_table(
        "categorization_rules",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("priority", sa.Integer, nullable=False, server_default="100"),
        sa.Column("match_field", sa.Text, nullable=False),
        sa.Column("match_value", sa.Text, nullable=False),
        sa.Column("match_operator", sa.Text, nullable=False, server_default="eq"),
        sa.Column("result_category", sa.Text, nullable=False),
        sa.Column("auto_generated", sa.Integer, nullable=False, server_default="0"),
        sa.Column("hit_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("created_at", sa.Text, nullable=False, server_default=sa.text("(datetime('now'))")),
    )

    # Seed default categories.
    for tx_type, category, display_name, sort_order in _DEFAULT_CATEGORIES:
        bind.execute(
            sa.text(
                "INSERT INTO transaction_categories (tx_type, category, display_name, sort_order)"
                " VALUES (:tx_type, :category, :display_name, :sort_order)"
            ),
            {"tx_type": tx_type, "category": category, "display_name": display_name, "sort_order": sort_order},
        )


def downgrade() -> None:
    """Drop transaction category tables."""
    op.drop_table("categorization_rules")
    op.drop_index("idx_metadata_transfer_pair", table_name="transaction_metadata")
    op.drop_index("idx_metadata_category", table_name="transaction_metadata")
    op.drop_table("transaction_metadata")
    op.drop_table("transaction_categories")
