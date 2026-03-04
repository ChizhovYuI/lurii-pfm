"""CRUD operations for the sources table."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import aiosqlite

from pfm.db.models import Source
from pfm.source_types import SOURCE_TYPES, validate_credentials

if TYPE_CHECKING:
    from pathlib import Path


class SourceError(Exception):
    """Base error for source operations."""


class SourceNotFoundError(SourceError):
    """Raised when a source name does not exist."""


class DuplicateSourceError(SourceError):
    """Raised when a source name already exists."""


class InvalidSourceTypeError(SourceError):
    """Raised when the source type is not in SOURCE_TYPES."""


class InvalidCredentialsError(SourceError):
    """Raised when required credential fields are missing."""


class SourceStore:
    """Async CRUD for the sources table."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)

    async def add(
        self,
        name: str,
        source_type: str,
        credentials: dict[str, str],
    ) -> Source:
        """Add a new source. Raises on duplicate name or invalid type."""
        if source_type not in SOURCE_TYPES:
            msg = f"Unknown source type: {source_type!r}. " f"Valid types: {', '.join(sorted(SOURCE_TYPES))}"
            raise InvalidSourceTypeError(msg)

        errors = validate_credentials(source_type, credentials)
        if errors:
            raise InvalidCredentialsError("; ".join(errors))

        creds_json = json.dumps(credentials)

        async with aiosqlite.connect(self._db_path) as db:
            try:
                cursor = await db.execute(
                    "INSERT INTO sources (name, type, credentials) VALUES (?, ?, ?)",
                    (name, source_type, creds_json),
                )
                await db.commit()
            except aiosqlite.IntegrityError as exc:
                msg = f"Source {name!r} already exists"
                raise DuplicateSourceError(msg) from exc

            row = await (await db.execute("SELECT * FROM sources WHERE id = ?", (cursor.lastrowid,))).fetchone()

        return self._row_to_source(row)  # type: ignore[arg-type]

    async def get(self, name: str) -> Source:
        """Get a source by name. Raises SourceNotFoundError if missing."""
        async with aiosqlite.connect(self._db_path) as db:
            row = await (await db.execute("SELECT * FROM sources WHERE name = ?", (name,))).fetchone()

        if row is None:
            msg = f"Source {name!r} not found"
            raise SourceNotFoundError(msg)
        return self._row_to_source(row)

    async def list_all(self) -> list[Source]:
        """List all sources."""
        async with aiosqlite.connect(self._db_path) as db:
            rows = await (await db.execute("SELECT * FROM sources ORDER BY id")).fetchall()
        return [self._row_to_source(r) for r in rows]

    async def list_enabled(self) -> list[Source]:
        """List only enabled sources."""
        async with aiosqlite.connect(self._db_path) as db:
            rows = await (await db.execute("SELECT * FROM sources WHERE enabled = 1 ORDER BY id")).fetchall()
        return [self._row_to_source(r) for r in rows]

    async def delete(self, name: str) -> bool:
        """Delete a source by name. Returns True if deleted, raises if not found."""
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute("DELETE FROM sources WHERE name = ?", (name,))
            await db.commit()

        if cursor.rowcount == 0:
            msg = f"Source {name!r} not found"
            raise SourceNotFoundError(msg)
        return True

    async def update(
        self,
        name: str,
        *,
        credentials: dict[str, str] | None = None,
        enabled: bool | None = None,
    ) -> Source:
        """Update a source's credentials and/or enabled flag."""
        existing = await self.get(name)

        sets: list[str] = []
        params: list[str | int] = []

        if credentials is not None:
            existing_creds: dict[str, str] = json.loads(existing.credentials)
            merged = {**existing_creds, **credentials}
            errors = validate_credentials(existing.type, merged)
            if errors:
                raise InvalidCredentialsError("; ".join(errors))
            sets.append("credentials = ?")
            params.append(json.dumps(merged))

        if enabled is not None:
            sets.append("enabled = ?")
            params.append(int(enabled))

        if not sets:
            return existing

        params.append(name)
        sql = f"UPDATE sources SET {', '.join(sets)} WHERE name = ?"  # noqa: S608

        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(sql, params)
            await db.commit()

        return await self.get(name)

    @staticmethod
    def _row_to_source(row: aiosqlite.Row) -> Source:
        """Convert a raw DB row to a Source dataclass."""
        return Source(
            id=row[0],
            name=row[1],
            type=row[2],
            credentials=row[3],
            enabled=bool(row[4]),
            created_at=None,
        )
