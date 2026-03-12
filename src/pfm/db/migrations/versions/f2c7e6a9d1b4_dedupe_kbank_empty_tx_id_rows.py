"""Deduplicate historical KBank transactions with empty tx_id.

Revision ID: f2c7e6a9d1b4
Revises: e7b9c1d4a5f0
Create Date: 2026-03-12 22:30:00.000000

"""

from typing import Final

import sqlalchemy as sa
from alembic import op

revision: Final[str] = "f2c7e6a9d1b4"
down_revision: Final[str | None] = "e7b9c1d4a5f0"
branch_labels: Final[str | None] = None
depends_on: Final[str | None] = None


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())
    if "transactions" not in tables:
        return

    op.execute(
        """
        DELETE FROM transactions
        WHERE source = 'kbank'
          AND tx_id = ''
          AND id NOT IN (
            SELECT MIN(id)
            FROM transactions
            WHERE source = 'kbank' AND tx_id = ''
            GROUP BY
              source,
              source_name,
              date,
              tx_type,
              asset,
              amount,
              usd_value,
              counterparty_asset,
              counterparty_amount,
              trade_side,
              raw_json
          )
        """
    )


def downgrade() -> None:
    """Downgrade schema.

    Irreversible data-cleanup migration (deleted duplicate rows cannot be restored).
    """
