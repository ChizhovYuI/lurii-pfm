"""Add app settings table.

Revision ID: 8d775e055451
Revises: 9cd516d0ab26
Create Date: 2026-02-27 21:25:29.707758

"""

from typing import Final

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: Final[str] = "8d775e055451"
down_revision: Final[str | None] = "9cd516d0ab26"
branch_labels: Final[str | None] = None
depends_on: Final[str | None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "app_settings",
        sa.Column("key", sa.Text(), primary_key=True),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.Text(), nullable=False, server_default=sa.text("(datetime('now'))")),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("app_settings")
