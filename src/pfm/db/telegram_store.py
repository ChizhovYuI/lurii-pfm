"""CRUD operations for Telegram bot credentials in app_settings."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import aiosqlite

if TYPE_CHECKING:
    from pathlib import Path

_BOT_TOKEN_KEY = "telegram_bot_token"  # noqa: S105
_CHAT_ID_KEY = "telegram_chat_id"


@dataclass(frozen=True, slots=True)
class TelegramCredentials:
    """Stored Telegram bot credentials."""

    bot_token: str
    chat_id: str


class TelegramStore:
    """Async CRUD for Telegram bot credentials."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)

    async def set(self, bot_token: str, chat_id: str) -> TelegramCredentials:
        """Upsert Telegram bot token and chat id."""
        token = bot_token.strip()
        destination = chat_id.strip()
        if not token:
            msg = "Telegram bot token cannot be empty."
            raise ValueError(msg)
        if not destination:
            msg = "Telegram chat ID cannot be empty."
            raise ValueError(msg)

        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "INSERT INTO app_settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')",
                (_BOT_TOKEN_KEY, token),
            )
            await db.execute(
                "INSERT INTO app_settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')",
                (_CHAT_ID_KEY, destination),
            )
            await db.commit()

        return TelegramCredentials(bot_token=token, chat_id=destination)

    async def get(self) -> TelegramCredentials | None:
        """Get Telegram credentials or None if not configured."""
        async with aiosqlite.connect(self._db_path) as db:
            rows = await (
                await db.execute(
                    "SELECT key, value FROM app_settings WHERE key IN (?, ?)",
                    (_BOT_TOKEN_KEY, _CHAT_ID_KEY),
                )
            ).fetchall()

        values = {str(row[0]): str(row[1]) for row in rows}
        bot_token = values.get(_BOT_TOKEN_KEY, "")
        chat_id = values.get(_CHAT_ID_KEY, "")
        if not bot_token or not chat_id:
            return None
        return TelegramCredentials(bot_token=bot_token, chat_id=chat_id)

    async def clear(self) -> bool:
        """Delete stored Telegram credentials."""
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute(
                "DELETE FROM app_settings WHERE key IN (?, ?)",
                (_BOT_TOKEN_KEY, _CHAT_ID_KEY),
            )
            await db.commit()

        return cursor.rowcount > 0
