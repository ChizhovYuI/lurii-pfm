"""Encryption-aware database connection factory using SQLCipher."""

from __future__ import annotations

import logging
import re
import sqlite3
from typing import TYPE_CHECKING

import aiosqlite

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

_HEX_64 = re.compile(r"^[0-9a-fA-F]{64}$")


def validate_key_hex(key_hex: str) -> bool:
    """Return True if key_hex is a valid 64-char hex string (256-bit key)."""
    return bool(_HEX_64.match(key_hex))


def connect_encrypted(db_path: str | Path, key_hex: str) -> aiosqlite.Connection:
    """Return an aiosqlite Connection that uses sqlcipher3 + PRAGMA key.

    The returned connection is NOT yet started — caller must ``await conn``
    or use ``async with conn`` to open the underlying thread.
    """
    import sqlcipher3

    def connector() -> sqlite3.Connection:
        conn: sqlite3.Connection = sqlcipher3.connect(str(db_path))
        conn.execute(f"PRAGMA key = \"x'{key_hex}'\"")
        conn.execute("PRAGMA cipher_compatibility = 4")
        return conn

    return aiosqlite.Connection(connector, iter_chunk_size=64)


def connect_db(db_path: str | Path, *, key_hex: str | None = None) -> aiosqlite.Connection:
    """Return an aiosqlite Connection — encrypted if *key_hex* is provided, plain otherwise."""
    if key_hex is not None:
        return connect_encrypted(db_path, key_hex)
    return aiosqlite.Connection(lambda: sqlite3.connect(str(db_path)), iter_chunk_size=64)


async def init_encrypted_db(path: Path, key_hex: str) -> None:
    """Create or upgrade an encrypted DB via Alembic."""
    from pfm.db.migrations.runner import run_migrations

    await run_migrations(path, key_hex=key_hex)
    logger.info("Initialized encrypted database at %s", path)


async def migrate_to_encrypted(plain_path: Path, encrypted_path: Path, key_hex: str) -> None:
    """One-time migration: read plain DB -> write encrypted copy.

    Uses SQLCipher's ``sqlcipher_export()`` to stream all data from the
    plain DB into a new encrypted database.  The original file is left
    untouched — caller is responsible for swapping paths.
    """
    import sqlcipher3

    encrypted_path.parent.mkdir(parents=True, exist_ok=True)

    # Open plain DB via sqlcipher3 (no key → reads as plain SQLite).
    conn: sqlite3.Connection = sqlcipher3.connect(str(plain_path))
    try:
        # Attach encrypted target and export all data.
        conn.execute(
            f"ATTACH DATABASE ? AS encrypted KEY \"x'{key_hex}'\"",
            (str(encrypted_path),),
        )
        conn.execute("PRAGMA encrypted.cipher_compatibility = 4")
        conn.execute("SELECT sqlcipher_export('encrypted')")
        conn.execute("DETACH DATABASE encrypted")
    finally:
        conn.close()

    await init_encrypted_db(encrypted_path, key_hex)
    logger.info("Migrated plain DB %s -> encrypted %s", plain_path, encrypted_path)
