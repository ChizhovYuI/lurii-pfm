"""Redesign category rules: compound rules, type_override, drop type_recognition_rules.

Revision ID: d4e5f6g7h8i9
Revises: c3d4e5f6h7i8
Create Date: 2026-03-15 20:00:00.000000

"""

from typing import Final

import sqlalchemy as sa
from alembic import op

revision: Final[str] = "d4e5f6g7h8i9"
down_revision: Final[str | None] = "c3d4e5f6h7i8"
branch_labels: Final[str | None] = None
depends_on: Final[str | None] = None

# Default: one category per transaction type (priority 300 = lowest).
_DEFAULT_RULES: list[tuple[str, str, str, int]] = [
    ("trade", "spot_trade", "*", 300),
    ("spend", "other_spend", "*", 300),
    ("fee", "fee", "*", 300),
    ("deposit", "external_deposit", "*", 300),
    ("withdrawal", "external_withdrawal", "*", 300),
    ("yield", "yield_income", "*", 300),
    ("interest", "interest_income", "*", 300),
    ("dividend", "dividend_income", "*", 300),
    ("transfer", "internal_move", "*", 300),
]

# New generic categories to add (avoid referencing non-existent categories).
_NEW_CATEGORIES: list[tuple[str, str, str, int]] = [
    ("fee", "fee", "Fee", 0),
    ("yield", "yield_income", "Yield Income", 0),
    ("interest", "interest_income", "Interest Income", 0),
    ("dividend", "dividend_income", "Dividend Income", 0),
]


def upgrade() -> None:
    """Create category_rules, user_category_choices; add type_override; drop old tables."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())

    # 1. Create category_rules table.
    if "category_rules" not in existing_tables:
        op.create_table(
            "category_rules",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            # Condition 1: type (required)
            sa.Column("type_match", sa.Text, nullable=False),
            sa.Column("type_operator", sa.Text, nullable=False, server_default="eq"),
            # Condition 2: field (optional)
            sa.Column("field_name", sa.Text, nullable=True),
            sa.Column("field_operator", sa.Text, nullable=True),
            sa.Column("field_value", sa.Text, nullable=True),
            # Source filter (optional, "*" = any)
            sa.Column("source", sa.Text, nullable=False, server_default="*"),
            # Result
            sa.Column("result_category", sa.Text, nullable=False),
            # Management
            sa.Column("priority", sa.Integer, nullable=False, server_default="300"),
            sa.Column("builtin", sa.Integer, nullable=False, server_default="0"),
            sa.Column("deleted", sa.Integer, nullable=False, server_default="0"),
            sa.Column("created_at", sa.Text, nullable=False, server_default=sa.text("(datetime('now'))")),
        )
        op.create_index("idx_category_rules_priority", "category_rules", ["priority"])
        op.create_index("idx_category_rules_source", "category_rules", ["source"])

    # 2. Seed generic categories that may not exist yet.
    for tx_type, category, display_name, sort_order in _NEW_CATEGORIES:
        bind.execute(
            sa.text(
                "INSERT OR IGNORE INTO transaction_categories"
                " (tx_type, category, display_name, sort_order)"
                " VALUES (:tx_type, :category, :display_name, :sort_order)"
            ),
            {"tx_type": tx_type, "category": category, "display_name": display_name, "sort_order": sort_order},
        )

    # 3. Seed default category rules (one per type).
    for type_match, result_category, source, priority in _DEFAULT_RULES:
        bind.execute(
            sa.text(
                "INSERT INTO category_rules"
                " (type_match, type_operator, source, result_category, priority, builtin)"
                " VALUES (:type_match, 'eq', :source, :result_category, :priority, 1)"
            ),
            {"type_match": type_match, "source": source, "result_category": result_category, "priority": priority},
        )

    # 4. Add type_override to transaction_metadata.
    columns = {col["name"] for col in inspector.get_columns("transaction_metadata")}
    if "type_override" not in columns:
        op.add_column(
            "transaction_metadata",
            sa.Column("type_override", sa.Text, nullable=True, server_default=None),
        )

    # 5. Create user_category_choices table.
    if "user_category_choices" not in existing_tables:
        op.create_table(
            "user_category_choices",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("transaction_id", sa.Integer, nullable=False),
            sa.Column("source", sa.Text, nullable=False),
            sa.Column("effective_type", sa.Text, nullable=False),
            sa.Column("field_snapshot", sa.Text, server_default=""),
            sa.Column("chosen_category", sa.Text, nullable=False),
            sa.Column("previous_category", sa.Text, server_default=""),
            sa.Column("created_at", sa.Text, nullable=False, server_default=sa.text("(datetime('now'))")),
        )
        op.create_index("idx_choices_source", "user_category_choices", ["source"])
        op.create_index("idx_choices_type", "user_category_choices", ["effective_type"])

    # 6. Drop type_recognition_rules (dead code, never used at runtime).
    if "type_recognition_rules" in existing_tables:
        op.drop_table("type_recognition_rules")

    # 7. Drop old categorization_rules (replaced by category_rules).
    if "categorization_rules" in existing_tables:
        op.drop_table("categorization_rules")


def downgrade() -> None:
    """Reverse the redesign migration."""
    op.drop_index("idx_choices_type", table_name="user_category_choices")
    op.drop_index("idx_choices_source", table_name="user_category_choices")
    op.drop_table("user_category_choices")
    op.drop_column("transaction_metadata", "type_override")
    op.drop_index("idx_category_rules_source", table_name="category_rules")
    op.drop_index("idx_category_rules_priority", table_name="category_rules")
    op.drop_table("category_rules")
    # Recreate old tables (schema only, no data).
    op.create_table(
        "categorization_rules",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("priority", sa.Integer, nullable=False, server_default="100"),
        sa.Column("match_field", sa.Text, nullable=False),
        sa.Column("match_value", sa.Text, nullable=False),
        sa.Column("match_operator", sa.Text, nullable=False, server_default="eq"),
        sa.Column("result_category", sa.Text, nullable=False),
        sa.Column("auto_generated", sa.Integer, nullable=False, server_default="0"),
        sa.Column("type_constraint", sa.Text, nullable=True),
        sa.Column("hit_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("created_at", sa.Text, nullable=False, server_default=sa.text("(datetime('now'))")),
    )
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
