"""Add type_rules table and seed default rules.

Revision ID: f6g7h8i9j0k1
Revises: e5f6g7h8i9j0
Create Date: 2026-03-16 22:00:00.000000

"""

from typing import Final

import sqlalchemy as sa
from alembic import op

revision: Final[str] = "f6g7h8i9j0k1"
down_revision: Final[str | None] = "e5f6g7h8i9j0"
branch_labels: Final[str | None] = None
depends_on: Final[str | None] = None

_KBANK_DEPOSIT_DESC = '["Transfer Deposit","QR Transfer Deposit","QRTransfer Deposit","Refund"]'

_DEFAULT_RULES: list[tuple[str, str, str, str, str, int]] = [
    # KBank
    ("kbank", "description", "eq", '["Payment","Debit Card Spending","Direct Debit"]', "spend", 100),
    ("kbank", "description", "eq", "Annual Debit Card Fee", "fee", 100),
    ("kbank", "description", "eq", _KBANK_DEPOSIT_DESC, "deposit", 100),
    ("kbank", "description", "eq", "Transfer Withdrawal", "withdrawal", 100),
    ("kbank", "_balance_direction", "eq", "increase", "deposit", 300),
    ("kbank", "_balance_direction", "eq", "decrease", "withdrawal", 300),
    # OKX
    ("okx", "subType", "eq", '["1","2"]', "trade", 100),
    ("okx", "subType", "eq", "13", "deposit", 100),
    ("okx", "subType", "eq", "14", "withdrawal", 100),
    ("okx", "", "eq", "", "transfer", 300),
    # Bybit
    ("bybit", "type", "eq", "TRADE", "trade", 100),
    ("bybit", "type", "eq", "DEPOSIT", "deposit", 100),
    ("bybit", "type", "eq", "WITHDRAWAL", "withdrawal", 100),
    ("bybit", "type", "contains", "INTEREST", "yield", 100),
    ("bybit", "", "eq", "", "transfer", 300),
    # CoinEx
    ("coinex", "type", "eq", "trade", "trade", 100),
    ("coinex", "type", "eq", "deposit", "deposit", 100),
    ("coinex", "type", "eq", "withdraw", "withdrawal", 100),
    ("coinex", "type", "eq", "investment_interest", "yield", 100),
    ("coinex", "type", "eq", '["maker_cash_back","exchange_order_transfer"]', "transfer", 100),
    # Binance
    ("binance", "_direction", "eq", "deposit", "deposit", 100),
    ("binance", "_direction", "eq", "withdrawal", "withdrawal", 100),
    # Binance TH
    ("binance_th", "_direction", "eq", "withdrawal", "withdrawal", 100),
    # MEXC
    ("mexc", "_direction", "eq", "deposit", "deposit", 100),
    ("mexc", "_direction", "eq", "withdrawal", "withdrawal", 100),
    # IBKR
    ("ibkr", "", "eq", "", "trade", 300),
    # Trading 212
    ("trading212", "_endpoint", "eq", "orders", "trade", 100),
    ("trading212", "_endpoint", "eq", "dividends", "yield", 100),
    ("trading212", "type", "contains", "DEPOSIT", "deposit", 100),
    ("trading212", "type", "contains", "WITHDRAW", "withdrawal", 100),
    ("trading212", "type", "contains", "FEE", "fee", 100),
    ("trading212", "type", "contains", "INTEREST", "yield", 100),
    ("trading212", "_endpoint", "eq", "cash", "transfer", 300),
    # Revolut
    ("revolut", "_amount_sign", "eq", "positive", "deposit", 100),
    ("revolut", "_amount_sign", "eq", "negative", "withdrawal", 100),
    # Lobstr
    ("lobstr", "_direction", "eq", "incoming", "deposit", 100),
    ("lobstr", "_direction", "eq", "outgoing", "withdrawal", 100),
    # Rabby
    ("rabby", "_flow", "eq", "receive_only", "deposit", 100),
    ("rabby", "_flow", "eq", "send_only", "withdrawal", 100),
    ("rabby", "cate_id", "contains", '["swap","trade"]', "trade", 100),
    ("rabby", "_flow", "eq", "both", "transfer", 300),
    # yo
    ("yo", "type", "contains", "deposit", "deposit", 100),
    ("yo", "type", "contains", '["redeem","withdraw"]', "withdrawal", 100),
    ("yo", "type", "contains", "claim", "yield", 100),
    ("yo", "", "eq", "", "transfer", 300),
    # Bitget Wallet
    ("bitget_wallet", "__typename", "eq", "UserSupplyTransaction", "deposit", 100),
    ("bitget_wallet", "__typename", "eq", "UserWithdrawTransaction", "withdrawal", 100),
]


def upgrade() -> None:
    """Create type_rules table, seed rules, drop stale data."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())

    # 1. Create type_rules table.
    if "type_rules" not in existing_tables:
        op.create_table(
            "type_rules",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("source", sa.Text, nullable=False, server_default="*"),
            sa.Column("field_name", sa.Text, nullable=True),
            sa.Column("field_operator", sa.Text, nullable=False, server_default="eq"),
            sa.Column("field_value", sa.Text, nullable=True),
            sa.Column("result_type", sa.Text, nullable=False),
            sa.Column("priority", sa.Integer, nullable=False, server_default="100"),
            sa.Column("builtin", sa.Integer, nullable=False, server_default="0"),
            sa.Column("deleted", sa.Integer, nullable=False, server_default="0"),
            sa.Column("created_at", sa.Text, nullable=False, server_default=sa.text("(datetime('now'))")),
        )
        op.create_index("idx_typerules_priority", "type_rules", ["priority"])
        op.create_index("idx_typerules_source", "type_rules", ["source"])

    # 2. Seed default rules.
    for source, field_name, field_operator, field_value, result_type, priority in _DEFAULT_RULES:
        bind.execute(
            sa.text(
                "INSERT INTO type_rules"
                " (source, field_name, field_operator, field_value,"
                "  result_type, priority, builtin)"
                " VALUES (:source, :field_name, :field_operator, :field_value,"
                "  :result_type, :priority, 1)"
            ),
            {
                "source": source,
                "field_name": field_name or None,
                "field_operator": field_operator,
                "field_value": field_value or None,
                "result_type": result_type,
                "priority": priority,
            },
        )


def downgrade() -> None:
    """Drop type_rules table."""
    op.drop_index("idx_typerules_source", table_name="type_rules")
    op.drop_index("idx_typerules_priority", table_name="type_rules")
    op.drop_table("type_rules")
