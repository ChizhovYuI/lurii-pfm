"""CRUD operations for Gemini API key in app_settings."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import aiosqlite

if TYPE_CHECKING:
    from pathlib import Path

_GEMINI_API_KEY = "gemini_api_key"


@dataclass(frozen=True, slots=True)
class GeminiConfig:
    """Stored Gemini API configuration."""

    api_key: str


class GeminiStore:
    """Async CRUD for Gemini API key."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)

    async def set(self, api_key: str) -> GeminiConfig:
        """Upsert Gemini API key."""
        value = api_key.strip()
        if not value:
            msg = "Gemini API key cannot be empty."
            raise ValueError(msg)

        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "INSERT INTO app_settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')",
                (_GEMINI_API_KEY, value),
            )
            await db.commit()

        return GeminiConfig(api_key=value)

    async def get(self) -> GeminiConfig | None:
        """Get Gemini API key or None if not configured."""
        async with aiosqlite.connect(self._db_path) as db:
            row = await (
                await db.execute(
                    "SELECT value FROM app_settings WHERE key = ?",
                    (_GEMINI_API_KEY,),
                )
            ).fetchone()

        if row is None:
            return None
        value = str(row[0]).strip()
        if not value:
            return None
        return GeminiConfig(api_key=value)

    async def clear(self) -> bool:
        """Delete stored Gemini API key."""
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute("DELETE FROM app_settings WHERE key = ?", (_GEMINI_API_KEY,))
            await db.commit()

        return cursor.rowcount > 0
