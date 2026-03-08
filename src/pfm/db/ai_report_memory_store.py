"""Helpers for persistent weekly AI report memory."""

from __future__ import annotations

import hashlib
from pathlib import Path

import aiosqlite

from pfm.db.models import init_db

AI_REPORT_MEMORY_KEY = "ai_report_memory"
AI_REPORT_MEMORY_MAX_CHARS = 4000


def normalize_ai_report_memory(text: str) -> str:
    """Normalize stored report memory while preserving user-authored headings."""
    normalized = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if len(normalized) > AI_REPORT_MEMORY_MAX_CHARS:
        msg = f"AI report memory must be {AI_REPORT_MEMORY_MAX_CHARS} characters or fewer."
        raise ValueError(msg)
    return normalized


def hash_ai_report_memory(text: str) -> str:
    """Return a stable hash for the normalized memory string."""
    normalized = normalize_ai_report_memory(text)
    if not normalized:
        return ""
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class AIReportMemoryStore:
    """Read/write access to the weekly report memory stored in app_settings."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)

    async def _ensure_schema(self) -> None:
        await init_db(Path(self._db_path))

    async def get(self) -> str:
        await self._ensure_schema()
        async with aiosqlite.connect(self._db_path) as db:
            row = await (
                await db.execute(
                    "SELECT value FROM app_settings WHERE key = ?",
                    (AI_REPORT_MEMORY_KEY,),
                )
            ).fetchone()
        if row is None or row[0] is None:
            return ""
        return normalize_ai_report_memory(str(row[0]))

    async def set(self, text: str) -> None:
        normalized = normalize_ai_report_memory(text)
        await self._ensure_schema()
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "INSERT INTO app_settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, "
                "updated_at = datetime('now')",
                (AI_REPORT_MEMORY_KEY, normalized),
            )
            await db.commit()
