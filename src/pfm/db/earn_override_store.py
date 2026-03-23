"""CRUD for per-source earn overrides stored in app_settings."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import aiosqlite

if TYPE_CHECKING:
    from pathlib import Path


class EarnOverrideStore:
    """Read/write earn overrides (APR, settlement) in app_settings."""

    _KEY_PREFIX = "earn_overrides:"

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)

    async def load(self, source_name: str) -> list[dict[str, str]]:
        """Load earn overrides for a source.  Each item: {category, coin, apr?, settlement_at?}."""
        key = self._key(source_name)
        async with aiosqlite.connect(self._db_path) as db:
            row = await (await db.execute("SELECT value FROM app_settings WHERE key = ?", (key,))).fetchone()
        if row is None:
            return []
        items: Any = json.loads(str(row[0]))
        return items if isinstance(items, list) else []

    async def save(self, source_name: str, overrides: list[dict[str, str]]) -> None:
        """Replace all earn overrides for a source."""
        key = self._key(source_name)
        value = json.dumps(overrides)
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "INSERT INTO app_settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, "
                "updated_at = datetime('now')",
                (key, value),
            )
            await db.commit()

    def _key(self, source_name: str) -> str:
        return f"{self._KEY_PREFIX}{source_name}"
