"""Add spend categories, bank_fee category, and type_recognition_rules table.

Revision ID: b2c3d4e5f6g7
Revises: a1b2c3d4e5f6
Create Date: 2026-03-15 12:00:00.000000

"""

from typing import Final

import sqlalchemy as sa
from alembic import op

revision: Final[str] = "b2c3d4e5f6g7"
down_revision: Final[str | None] = "a1b2c3d4e5f6"
branch_labels: Final[str | None] = None
depends_on: Final[str | None] = None

_SPEND_CATEGORIES: list[tuple[str, str, str, int]] = [
    ("spend", "groceries", "Groceries", 10),
    ("spend", "dining", "Dining & Restaurants", 20),
    ("spend", "shopping", "Shopping", 30),
    ("spend", "transport", "Transportation", 40),
    ("spend", "entertainment", "Entertainment", 50),
    ("spend", "healthcare", "Healthcare", 60),
    ("spend", "utilities", "Utilities & Bills", 70),
    ("spend", "subscriptions", "Subscriptions", 80),
    ("spend", "education", "Education", 90),
    ("spend", "personal_care", "Personal Care", 100),
    ("spend", "travel", "Travel", 110),
    ("spend", "other_spend", "Other Spending", 120),
    ("fee", "bank_fee", "Bank Fee", 30),
]

_BUILTIN_RULES: list[tuple[str, str, str, str, str, str, int]] = [
    # KBank
    ("kbank", "description", "eq", "Payment", "spend", "other_spend", 100),
    ("kbank", "description", "eq", "Debit Card Spending", "spend", "shopping", 100),
    ("kbank", "description", "eq", "Direct Debit", "spend", "subscriptions", 100),
    ("kbank", "description", "eq", "Annual Debit Card Fee", "fee", "bank_fee", 100),
    ("kbank", "description", "eq", "Transfer Withdrawal", "transfer", "", 100),
    ("kbank", "description", "eq", "Transfer Deposit", "transfer", "", 100),
    ("kbank", "description", "contains", "Transfer Deposit", "transfer", "", 110),
    ("kbank", "description", "eq", "Refund", "deposit", "", 100),
    ("kbank", "channel", "eq", "EDC/E-Commerce", "spend", "shopping", 100),
    ("kbank", "channel", "contains", "EDC/K SHOP", "spend", "shopping", 100),
    # IBKR
    ("ibkr", "buySell", "eq", "BUY", "trade", "spot_trade", 100),
    ("ibkr", "buySell", "eq", "SELL", "trade", "spot_trade", 100),
    ("ibkr", "assetCategory", "eq", "STK", "trade", "spot_trade", 100),
    ("ibkr", "assetCategory", "eq", "CASH", "trade", "conversion", 100),
    # OKX
    ("okx", "subType", "eq", "1", "trade", "spot_trade", 100),
    ("okx", "subType", "eq", "2", "trade", "spot_trade", 100),
    ("okx", "subType", "eq", "13", "deposit", "external_deposit", 100),
    ("okx", "subType", "eq", "14", "withdrawal", "external_withdrawal", 100),
    # Bybit
    ("bybit", "type", "eq", "TRADE", "trade", "spot_trade", 100),
    ("bybit", "type", "eq", "DEPOSIT", "deposit", "external_deposit", 100),
    ("bybit", "type", "eq", "WITHDRAWAL", "withdrawal", "external_withdrawal", 100),
    ("bybit", "type", "contains", "INTEREST", "interest", "earn_interest", 100),
    # CoinEx
    ("coinex", "type", "eq", "trade", "trade", "spot_trade", 100),
    ("coinex", "type", "eq", "deposit", "deposit", "external_deposit", 100),
    ("coinex", "type", "eq", "withdraw", "withdrawal", "external_withdrawal", 100),
    ("coinex", "type", "eq", "investment_interest", "interest", "earn_interest", 100),
    # Trading212
    ("trading212", "type", "contains", "DEPOSIT", "deposit", "external_deposit", 100),
    ("trading212", "type", "contains", "WITHDRAW", "withdrawal", "external_withdrawal", 100),
    ("trading212", "type", "contains", "FEE", "fee", "trading_fee", 100),
    ("trading212", "type", "contains", "INTEREST", "interest", "savings_interest", 100),
    # Rabby
    ("rabby", "cate_id", "contains", "swap", "trade", "spot_trade", 100),
    ("rabby", "cate_id", "contains", "trade", "trade", "spot_trade", 100),
]


def upgrade() -> None:
    """Add spend categories and type_recognition_rules table."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())

    # Seed spend categories + bank_fee.
    for tx_type, category, display_name, sort_order in _SPEND_CATEGORIES:
        bind.execute(
            sa.text(
                "INSERT OR IGNORE INTO transaction_categories"
                " (tx_type, category, display_name, sort_order)"
                " VALUES (:tx_type, :category, :display_name, :sort_order)"
            ),
            {"tx_type": tx_type, "category": category, "display_name": display_name, "sort_order": sort_order},
        )

    if "type_recognition_rules" not in existing_tables:
        op.create_table(
            "type_recognition_rules",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("source", sa.Text, nullable=False),
            sa.Column("field", sa.Text, nullable=False),
            sa.Column("operator", sa.Text, nullable=False, server_default="eq"),
            sa.Column("value", sa.Text, nullable=False),
            sa.Column("result_type", sa.Text, nullable=False),
            sa.Column("result_category", sa.Text, server_default=""),
            sa.Column("priority", sa.Integer, server_default="100"),
            sa.Column("builtin", sa.Integer, server_default="0"),
            sa.Column("enabled", sa.Integer, server_default="1"),
            sa.Column("created_at", sa.Text, nullable=False, server_default=sa.text("(datetime('now'))")),
        )
        op.create_index("idx_type_rules_source", "type_recognition_rules", ["source"])
        op.create_index("idx_type_rules_enabled", "type_recognition_rules", ["enabled"])

    # Seed builtin rules.
    for source, field, operator, value, result_type, result_category, priority in _BUILTIN_RULES:
        bind.execute(
            sa.text(
                "INSERT OR IGNORE INTO type_recognition_rules"
                " (source, field, operator, value, result_type, result_category, priority, builtin)"
                " VALUES (:source, :field, :operator, :value, :result_type, :result_category, :priority, 1)"
            ),
            {
                "source": source,
                "field": field,
                "operator": operator,
                "value": value,
                "result_type": result_type,
                "result_category": result_category,
                "priority": priority,
            },
        )


def downgrade() -> None:
    """Remove type_recognition_rules table and spend categories."""
    op.drop_index("idx_type_rules_enabled", table_name="type_recognition_rules")
    op.drop_index("idx_type_rules_source", table_name="type_recognition_rules")
    op.drop_table("type_recognition_rules")
    bind = op.get_bind()
    bind.execute(sa.text("DELETE FROM transaction_categories WHERE tx_type = 'spend'"))
    bind.execute(sa.text("DELETE FROM transaction_categories WHERE category = 'bank_fee'"))
