"""Drop AI provider table and AI/Telegram/report app_settings + metrics.

Removes leftover state from the deprecated local-AI commentary and Telegram
reporting features. Portfolio analytics now flow through MCP + Claude.

Drops:
    - ``ai_providers`` table (entire AI provider registry)
    - ``app_settings`` rows: ``telegram_bot_token``, ``telegram_chat_id``,
      ``ai_report_memory``, ``gemini_api_key``, ``ai_provider``,
      ``ai_provider_api_key``, ``ai_provider_model``, ``ai_provider_base_url``
    - ``analytics_metrics`` rows where ``metric_name = 'ai_commentary'``

Downgrade is best-effort: recreates the empty ``ai_providers`` table only.
Lost row data cannot be reconstructed.
"""

from __future__ import annotations

from typing import Final

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: Final[str] = "k1l2m3n4o5p6"
down_revision: Final[str | None] = "j0k1l2m3n4o5"
branch_labels: Final[str | None] = None
depends_on: Final[str | None] = None


_DEAD_SETTINGS_KEYS: Final[tuple[str, ...]] = (
    "telegram_bot_token",
    "telegram_chat_id",
    "ai_report_memory",
    "gemini_api_key",
    "ai_provider",
    "ai_provider_api_key",
    "ai_provider_model",
    "ai_provider_base_url",
)


def upgrade() -> None:
    """Drop dead AI/Telegram schema and rows."""
    op.execute("DROP TABLE IF EXISTS ai_providers")

    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())

    if "app_settings" in tables:
        quoted_keys = ", ".join(f"'{key}'" for key in _DEAD_SETTINGS_KEYS)
        op.execute(f"DELETE FROM app_settings WHERE key IN ({quoted_keys})")  # noqa: S608

    if "analytics_metrics" in tables:
        op.execute("DELETE FROM analytics_metrics WHERE metric_name = 'ai_commentary'")


def downgrade() -> None:
    """Recreate the empty ai_providers table. Row data is not recoverable."""
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_providers (
            type TEXT PRIMARY KEY,
            api_key TEXT NOT NULL DEFAULT '',
            model TEXT NOT NULL DEFAULT '',
            base_url TEXT NOT NULL DEFAULT '',
            active INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
