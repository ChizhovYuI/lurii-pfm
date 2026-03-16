"""Simplify transaction categories to 14 values.

Revision ID: e5f6g7h8i9j0
Revises: d4e5f6g7h8i9
Create Date: 2026-03-16 18:00:00.000000

"""

from typing import Final

import sqlalchemy as sa
from alembic import op

revision: Final[str] = "e5f6g7h8i9j0"
down_revision: Final[str | None] = "d4e5f6g7h8i9"
branch_labels: Final[str | None] = None
depends_on: Final[str | None] = None

# New simplified categories (14 total).
_NEW_CATEGORIES: list[tuple[str, str, str, int]] = [
    ("spend", "dining", "Dining", 10),
    ("spend", "groceries", "Groceries", 20),
    ("spend", "transport", "Transport", 30),
    ("spend", "shopping", "Shopping", 40),
    ("spend", "bills", "Bills & Subscriptions", 50),
    ("spend", "healthcare", "Healthcare", 60),
    ("spend", "entertainment", "Entertainment", 70),
    ("spend", "other_spend", "Other Spending", 80),
    ("deposit", "income", "Income", 10),
    ("trade", "trade", "Trade", 10),
    ("yield", "yield", "Yield", 10),
    ("fee", "fee", "Fee", 10),
    ("transfer", "transfer", "Transfer", 10),
    ("withdrawal", "withdrawal", "Withdrawal", 10),
]

# Default rules for the simplified categories.
_NEW_DEFAULT_RULES: list[tuple[str, str]] = [
    ("trade", "trade"),
    ("fee", "fee"),
    ("deposit", "income"),
    ("withdrawal", "withdrawal"),
    ("yield", "yield"),
    ("transfer", "transfer"),
]


def upgrade() -> None:
    """Replace categories with simplified set, drop stale data."""
    bind = op.get_bind()

    # 1. Replace categories.
    bind.execute(sa.text("DELETE FROM transaction_categories"))
    for tx_type, category, display_name, sort_order in _NEW_CATEGORIES:
        bind.execute(
            sa.text(
                "INSERT OR IGNORE INTO transaction_categories"
                " (tx_type, category, display_name, sort_order)"
                " VALUES (:tx_type, :category, :display_name, :sort_order)"
            ),
            {"tx_type": tx_type, "category": category, "display_name": display_name, "sort_order": sort_order},
        )

    # 2. Replace default category rules.
    bind.execute(sa.text("DELETE FROM category_rules"))
    for type_match, result_category in _NEW_DEFAULT_RULES:
        bind.execute(
            sa.text(
                "INSERT INTO category_rules"
                " (type_match, type_operator, source, result_category, priority, builtin)"
                " VALUES (:type_match, 'eq', '*', :result_category, 300, 1)"
            ),
            {"type_match": type_match, "result_category": result_category},
        )

    # 3. Drop stale data — old categories/types are incompatible. Re-import will repopulate.
    bind.execute(sa.text("DELETE FROM transaction_metadata"))
    bind.execute(sa.text("DELETE FROM user_category_choices"))
    bind.execute(sa.text("DELETE FROM transactions"))


def downgrade() -> None:
    """Cannot fully reverse category simplification — data was dropped."""
