"""CRUD operations for AI provider configuration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import aiosqlite

from pfm.db.models import AIProvider

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

# Legacy app_settings keys (used for migration)
_PROVIDER_KEY = "ai_provider"
_API_KEY_KEY = "ai_provider_api_key"
_MODEL_KEY = "ai_provider_model"
_BASE_URL_KEY = "ai_provider_base_url"
_LEGACY_GEMINI_KEY = "gemini_api_key"

_ALL_LEGACY_KEYS = (_PROVIDER_KEY, _API_KEY_KEY, _MODEL_KEY, _BASE_URL_KEY)

# Backward-compat alias
AIConfig = AIProvider


class AIProviderStore:
    """Async CRUD for AI provider configurations (multi-provider)."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)

    async def _ensure_table(self) -> None:
        """Create the ai_providers table if it doesn't exist."""
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "CREATE TABLE IF NOT EXISTS ai_providers ("
                "type TEXT PRIMARY KEY, "
                "api_key TEXT NOT NULL DEFAULT '', "
                "model TEXT NOT NULL DEFAULT '', "
                "base_url TEXT NOT NULL DEFAULT '', "
                "active INTEGER NOT NULL DEFAULT 0, "
                "created_at TEXT NOT NULL DEFAULT (datetime('now')), "
                "updated_at TEXT NOT NULL DEFAULT (datetime('now'))"
                ")"
            )
            await db.commit()

    async def add(
        self,
        provider_type: str,
        *,
        api_key: str = "",
        model: str = "",
        base_url: str = "",
        activate: bool = False,
    ) -> AIProvider:
        """Add or update a provider configuration.

        When *activate* is True the provider is set as the active one
        (all others are deactivated first).
        """
        provider_type = provider_type.strip()
        if not provider_type:
            msg = "AI provider type cannot be empty."
            raise ValueError(msg)

        await self._ensure_table()

        async with aiosqlite.connect(self._db_path) as db:
            if activate:
                await db.execute("UPDATE ai_providers SET active = 0")
            await db.execute(
                "INSERT INTO ai_providers (type, api_key, model, base_url, active, updated_at) "
                "VALUES (?, ?, ?, ?, ?, datetime('now')) "
                "ON CONFLICT(type) DO UPDATE SET "
                "api_key = excluded.api_key, "
                "model = excluded.model, "
                "base_url = excluded.base_url, "
                "active = excluded.active, "
                "updated_at = datetime('now')",
                (provider_type, api_key.strip(), model.strip(), base_url.strip(), int(activate)),
            )
            await db.commit()

        return AIProvider(
            type=provider_type,
            api_key=api_key.strip(),
            model=model.strip(),
            base_url=base_url.strip(),
            active=activate,
        )

    async def get(self, provider_type: str) -> AIProvider | None:
        """Get a specific provider config by type, or None."""
        await self._ensure_table()
        async with aiosqlite.connect(self._db_path) as db:
            row = await (
                await db.execute(
                    "SELECT type, api_key, model, base_url, active FROM ai_providers WHERE type = ?",
                    (provider_type,),
                )
            ).fetchone()
        if row is None:
            return None
        return AIProvider(
            type=str(row[0]),
            api_key=str(row[1]),
            model=str(row[2]),
            base_url=str(row[3]),
            active=bool(row[4]),
        )

    async def get_active(self) -> AIProvider | None:
        """Get the currently active provider, or None."""
        await self._ensure_table()
        async with aiosqlite.connect(self._db_path) as db:
            row = await (
                await db.execute(
                    "SELECT type, api_key, model, base_url, active FROM ai_providers WHERE active = 1",
                )
            ).fetchone()
        if row is None:
            return None
        return AIProvider(
            type=str(row[0]),
            api_key=str(row[1]),
            model=str(row[2]),
            base_url=str(row[3]),
            active=True,
        )

    async def list_all(self) -> list[AIProvider]:
        """Return all configured providers."""
        await self._ensure_table()
        async with aiosqlite.connect(self._db_path) as db:
            rows = await (
                await db.execute(
                    "SELECT type, api_key, model, base_url, active FROM ai_providers ORDER BY type",
                )
            ).fetchall()
        return [
            AIProvider(
                type=str(r[0]),
                api_key=str(r[1]),
                model=str(r[2]),
                base_url=str(r[3]),
                active=bool(r[4]),
            )
            for r in rows
        ]

    async def activate(self, provider_type: str) -> AIProvider:
        """Set *provider_type* as the active provider.

        Raises ValueError if the provider is not configured.
        """
        await self._ensure_table()
        async with aiosqlite.connect(self._db_path) as db:
            row = await (
                await db.execute(
                    "SELECT type FROM ai_providers WHERE type = ?",
                    (provider_type,),
                )
            ).fetchone()
            if row is None:
                msg = f"Provider '{provider_type}' is not configured."
                raise ValueError(msg)
            await db.execute("UPDATE ai_providers SET active = 0")
            await db.execute(
                "UPDATE ai_providers SET active = 1, updated_at = datetime('now') WHERE type = ?",
                (provider_type,),
            )
            await db.commit()

        result = await self.get(provider_type)
        assert result is not None  # noqa: S101
        return result

    async def deactivate(self) -> bool:
        """Clear the active flag on all providers.

        Returns True if any provider was deactivated.
        """
        await self._ensure_table()
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute("UPDATE ai_providers SET active = 0 WHERE active = 1")
            await db.commit()
        return cursor.rowcount > 0

    async def remove(self, provider_type: str) -> bool:
        """Delete a provider config. Returns True if it existed."""
        await self._ensure_table()
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute(
                "DELETE FROM ai_providers WHERE type = ?",
                (provider_type,),
            )
            await db.commit()
        return cursor.rowcount > 0

    async def migrate_from_legacy(self) -> bool:
        """Migrate legacy app_settings AI keys to the ai_providers table.

        Handles both ``ai_provider*`` keys and the older ``gemini_api_key``.
        Returns True if any migration was performed.
        """
        await self._ensure_table()

        # Check if we already have any providers configured
        existing = await self.list_all()
        if existing:
            return False

        async with aiosqlite.connect(self._db_path) as db:
            # Try ai_provider* keys first
            rows = await (
                await db.execute(
                    "SELECT key, value FROM app_settings WHERE key IN (?, ?, ?, ?)",
                    _ALL_LEGACY_KEYS,
                )
            ).fetchall()

            values = {str(r[0]): str(r[1]) for r in rows}
            provider = values.get(_PROVIDER_KEY, "").strip()

            if provider:
                api_key = values.get(_API_KEY_KEY, "").strip()
                model = values.get(_MODEL_KEY, "").strip()
                base_url = values.get(_BASE_URL_KEY, "").strip()
                await self.add(provider, api_key=api_key, model=model, base_url=base_url, activate=True)
                logger.info("Migrated ai_provider settings to ai_providers table (provider=%s).", provider)
                return True

            # Fall back to gemini_api_key
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

        await self.add("gemini", api_key=api_key, activate=True)
        logger.info("Migrated legacy gemini_api_key to ai_providers table.")
        return True


# Backward-compat alias
AIStore = AIProviderStore
