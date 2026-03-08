"""Add ai_providers table.

Revision ID: d3f1b8a4c2e6
Revises: c4a2e7d6f9b1
Create Date: 2026-03-08 17:15:00.000000

"""

from typing import Final

import sqlalchemy as sa
from alembic import op

revision: Final[str] = "d3f1b8a4c2e6"
down_revision: Final[str | None] = "c4a2e7d6f9b1"
branch_labels: Final[str | None] = None
depends_on: Final[str | None] = None


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())
    if "ai_providers" in tables:
        return

    op.create_table(
        "ai_providers",
        sa.Column("type", sa.Text(), primary_key=True),
        sa.Column("api_key", sa.Text(), nullable=False, server_default=sa.text("''")),
        sa.Column("model", sa.Text(), nullable=False, server_default=sa.text("''")),
        sa.Column("base_url", sa.Text(), nullable=False, server_default=sa.text("''")),
        sa.Column("active", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("created_at", sa.Text(), nullable=False, server_default=sa.text("(datetime('now'))")),
        sa.Column("updated_at", sa.Text(), nullable=False, server_default=sa.text("(datetime('now'))")),
    )


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())
    if "ai_providers" in tables:
        op.drop_table("ai_providers")
