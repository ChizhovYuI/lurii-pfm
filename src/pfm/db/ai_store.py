"""CRUD operations for AI provider configuration in app_settings."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import aiosqlite

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

_PROVIDER_KEY = "ai_provider"
_API_KEY_KEY = "ai_provider_api_key"
_MODEL_KEY = "ai_provider_model"
_BASE_URL_KEY = "ai_provider_base_url"
_LEGACY_GEMINI_KEY = "gemini_api_key"

_ALL_KEYS = (_PROVIDER_KEY, _API_KEY_KEY, _MODEL_KEY, _BASE_URL_KEY)


@dataclass(frozen=True, slots=True)
class AIConfig:
    """Stored AI provider configuration."""

    provider: str
    api_key: str
    model: str
    base_url: str


class AIStore:
    """Async CRUD for AI provider configuration."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)

    async def set(
        self,
        *,
        provider: str,
        api_key: str = "",
        model: str = "",
        base_url: str = "",
    ) -> AIConfig:
        """Upsert AI provider configuration."""
        provider = provider.strip()
        if not provider:
            msg = "AI provider name cannot be empty."
            raise ValueError(msg)

        values = {
            _PROVIDER_KEY: provider,
            _API_KEY_KEY: api_key.strip(),
            _MODEL_KEY: model.strip(),
            _BASE_URL_KEY: base_url.strip(),
        }

        async with aiosqlite.connect(self._db_path) as db:
            for key, value in values.items():
                await db.execute(
                    "INSERT INTO app_settings (key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')",
                    (key, value),
                )
            await db.commit()

        return AIConfig(
            provider=provider,
            api_key=api_key.strip(),
            model=model.strip(),
            base_url=base_url.strip(),
        )

    async def get(self) -> AIConfig | None:
        """Get AI configuration or None if not configured."""
        async with aiosqlite.connect(self._db_path) as db:
            rows = await (
                await db.execute(
                    "SELECT key, value FROM app_settings WHERE key IN (?, ?, ?, ?)",
                    _ALL_KEYS,
                )
            ).fetchall()

        values = {str(row[0]): str(row[1]) for row in rows}
        provider = values.get(_PROVIDER_KEY, "").strip()
        if not provider:
            return None
        return AIConfig(
            provider=provider,
            api_key=values.get(_API_KEY_KEY, "").strip(),
            model=values.get(_MODEL_KEY, "").strip(),
            base_url=values.get(_BASE_URL_KEY, "").strip(),
        )

    async def clear(self) -> bool:
        """Delete stored AI configuration."""
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute(
                "DELETE FROM app_settings WHERE key IN (?, ?, ?, ?)",
                _ALL_KEYS,
            )
            await db.commit()

        return cursor.rowcount > 0

    async def migrate_from_gemini(self) -> bool:
        """Migrate legacy ``gemini_api_key`` to new AI config if no new config exists.

        Returns True if migration was performed.
        """
        existing = await self.get()
        if existing is not None:
            return False

        async with aiosqlite.connect(self._db_path) as db:
            row = await (
                await db.execute(
                    "SELECT value FROM app_settings WHERE key = ?",
                    (_LEGACY_GEMINI_KEY,),
                )
            ).fetchone()

        if row is None:
            return False
        api_key = str(row[0]).strip()
        if not api_key:
            return False

        await self.set(provider="gemini", api_key=api_key)
        logger.info("Migrated legacy gemini_api_key to new AI provider config.")
        return True
