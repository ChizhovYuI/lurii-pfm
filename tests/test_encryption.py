"""Tests for SQLCipher encryption support."""

from __future__ import annotations

import sqlite3

import pytest

from pfm.db.encryption import (
    connect_db,
    connect_encrypted,
    init_encrypted_db,
    migrate_to_encrypted,
    validate_key_hex,
)
from pfm.db.models import init_db
from pfm.server.app import create_app
from pfm.server.state import get_runtime_state

# 256-bit test key (64 hex chars)
TEST_KEY = "a" * 64
WRONG_KEY = "b" * 64


# ── validate_key_hex ────────────────────────────────────────────────


def test_validate_key_hex_valid():
    assert validate_key_hex("a" * 64) is True
    assert validate_key_hex("0123456789abcdefABCDEF" + "0" * 42) is True


def test_validate_key_hex_invalid():
    assert validate_key_hex("") is False
    assert validate_key_hex("a" * 63) is False
    assert validate_key_hex("a" * 65) is False
    assert validate_key_hex("g" * 64) is False
    assert validate_key_hex("a" * 32) is False


# ── connect_encrypted roundtrip ─────────────────────────────────────


async def test_connect_encrypted_roundtrip(tmp_path):
    """Write + read with the same key succeeds."""
    db = tmp_path / "enc.db"
    conn = connect_encrypted(db, TEST_KEY)
    async with conn:
        await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        await conn.execute("INSERT INTO t (val) VALUES (?)", ("hello",))
        await conn.commit()

    # Re-open with same key
    conn2 = connect_encrypted(db, TEST_KEY)
    async with conn2:
        cursor = await conn2.execute("SELECT val FROM t")
        row = await cursor.fetchone()
        assert row is not None
        assert row[0] == "hello"


async def test_encrypted_db_rejects_plain_read(tmp_path):
    """Plain sqlite3 cannot read an encrypted database."""
    db = tmp_path / "enc.db"
    conn = connect_encrypted(db, TEST_KEY)
    async with conn:
        await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        await conn.commit()

    # Plain sqlite3 should fail
    plain = sqlite3.connect(str(db))
    with pytest.raises(sqlite3.DatabaseError):
        plain.execute("SELECT * FROM sqlite_master")
    plain.close()


async def test_encrypted_db_rejects_wrong_key(tmp_path):
    """Opening with the wrong key fails."""
    db = tmp_path / "enc.db"
    conn = connect_encrypted(db, TEST_KEY)
    async with conn:
        await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        await conn.commit()

    import sqlcipher3

    conn2 = connect_encrypted(db, WRONG_KEY)
    with pytest.raises(sqlcipher3.dbapi2.DatabaseError):
        async with conn2:
            await conn2.execute("SELECT * FROM sqlite_master")


# ── connect_db helper ───────────────────────────────────────────────


async def test_connect_db_plain(tmp_path):
    """connect_db without key returns a plain connection."""
    db = tmp_path / "plain.db"
    conn = connect_db(db)
    async with conn:
        await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        await conn.commit()

    # Verify readable with plain sqlite3
    plain = sqlite3.connect(str(db))
    rows = plain.execute("SELECT * FROM sqlite_master").fetchall()
    assert len(rows) > 0
    plain.close()


async def test_connect_db_encrypted(tmp_path):
    """connect_db with key returns an encrypted connection."""
    db = tmp_path / "enc.db"
    conn = connect_db(db, key_hex=TEST_KEY)
    async with conn:
        await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        await conn.commit()

    # Not readable with plain sqlite3
    plain = sqlite3.connect(str(db))
    with pytest.raises(sqlite3.DatabaseError):
        plain.execute("SELECT * FROM sqlite_master")
    plain.close()


# ── init_encrypted_db ───────────────────────────────────────────────


async def test_init_encrypted_db(tmp_path):
    """Schema is created in an encrypted database."""
    db = tmp_path / "enc.db"
    await init_encrypted_db(db, TEST_KEY)

    conn = connect_encrypted(db, TEST_KEY)
    async with conn:
        cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in await cursor.fetchall()]

    assert "snapshots" in tables
    assert "sources" in tables
    assert "prices" in tables


# ── init_db with key_hex ────────────────────────────────────────────


async def test_init_db_with_key(tmp_path):
    """init_db delegates to encrypted init when key_hex provided."""
    db = tmp_path / "enc.db"
    await init_db(db, key_hex=TEST_KEY)

    conn = connect_encrypted(db, TEST_KEY)
    async with conn:
        cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in await cursor.fetchall()]

    assert "snapshots" in tables


async def test_init_encrypted_db_migrates_legacy_transactions_schema(tmp_path):
    """Existing encrypted DBs gain transaction source metadata on init."""
    db = tmp_path / "legacy-enc.db"
    conn = connect_encrypted(db, TEST_KEY)
    async with conn:
        await conn.executescript(
            """
            CREATE TABLE snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                source TEXT NOT NULL,
                asset TEXT NOT NULL,
                amount TEXT NOT NULL,
                usd_value TEXT NOT NULL,
                raw_json TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                source TEXT NOT NULL,
                tx_type TEXT NOT NULL,
                asset TEXT NOT NULL,
                amount TEXT NOT NULL,
                usd_value TEXT NOT NULL,
                counterparty_asset TEXT NOT NULL DEFAULT '',
                counterparty_amount TEXT NOT NULL DEFAULT '0',
                tx_id TEXT NOT NULL DEFAULT '',
                raw_json TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                asset TEXT NOT NULL,
                currency TEXT NOT NULL,
                price TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'coingecko',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE analytics_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                metric_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                type TEXT NOT NULL,
                credentials TEXT NOT NULL DEFAULT '{}',
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            """
        )
        await conn.executemany(
            (
                "INSERT INTO transactions "
                "(date, source, tx_type, asset, amount, usd_value, tx_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
            ),
            [
                ("2024-01-10", "wise", "deposit", "EUR", "100", "110", "dup"),
                ("2024-01-11", "wise", "deposit", "EUR", "100", "110", "dup"),
            ],
        )
        await conn.commit()

    await init_encrypted_db(db, TEST_KEY)

    conn = connect_encrypted(db, TEST_KEY)
    async with conn:
        cursor = await conn.execute("PRAGMA table_info(transactions)")
        columns = {row[1] for row in await cursor.fetchall()}
        # Stage 3 (ADR-030) drops source_name; source_id is the FK.
        assert "source_name" not in columns
        assert "source_id" in columns
        assert "trade_side" in columns

        index_row = await (
            await conn.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'index' "
                "AND name = 'idx_transactions_source_id_tx_id_unique'"
            )
        ).fetchone()
        # Migration drops all transactions (stale data incompatible with new type rules).
        cursor = await conn.execute("SELECT COUNT(*) FROM transactions")
        count = (await cursor.fetchone())[0]

    assert count == 0
    assert index_row is not None
    assert "tx_id != ''" in str(index_row[0])


# ── migrate_to_encrypted ───────────────────────────────────────────


async def test_migrate_to_encrypted(tmp_path):
    """Plain -> encrypted migration preserves data."""
    plain_path = tmp_path / "plain.db"
    enc_path = tmp_path / "encrypted.db"

    # Create plain DB with schema + data
    await init_db(plain_path)
    plain = sqlite3.connect(str(plain_path))
    plain.execute(
        "INSERT INTO sources (name, type, credentials) VALUES (?, ?, ?)",
        ("test-src", "okx", '{"key": "val"}'),
    )
    plain.commit()
    plain.close()

    await migrate_to_encrypted(plain_path, enc_path, TEST_KEY)

    # Verify encrypted DB has the data
    conn = connect_encrypted(enc_path, TEST_KEY)
    async with conn:
        cursor = await conn.execute("SELECT name FROM sources")
        row = await cursor.fetchone()
        assert row is not None
        assert row[0] == "test-src"

    # Plain original is untouched
    plain2 = sqlite3.connect(str(plain_path))
    row = plain2.execute("SELECT name FROM sources").fetchone()
    assert row is not None
    assert row[0] == "test-src"
    plain2.close()


# ── Server integration: locked middleware ───────────────────────────


@pytest.fixture
async def db_path(tmp_path):
    path = tmp_path / "test.db"
    await init_db(path)
    return path


@pytest.fixture
async def locked_client(aiohttp_client, db_path):
    """Client for an app that starts in locked state."""
    app = create_app(db_path)
    get_runtime_state(app).db_locked = True
    app["encryption_enabled"] = True
    return await aiohttp_client(app)


@pytest.fixture
async def unlocked_client(aiohttp_client, db_path):
    """Client for a normal (unlocked) app."""
    return await aiohttp_client(create_app(db_path))


async def test_locked_middleware_blocks(locked_client):
    """Data endpoints return 423 when locked."""
    resp = await locked_client.get("/api/v1/sources")
    assert resp.status == 423
    data = await resp.json()
    assert data["error"] == "Database is locked"


async def test_locked_middleware_blocks_portfolio(locked_client):
    """Portfolio endpoint also returns 423 when locked."""
    resp = await locked_client.get("/api/v1/portfolio/summary")
    assert resp.status == 423


async def test_locked_middleware_allows_health(locked_client):
    """Health endpoint works when locked."""
    resp = await locked_client.get("/api/v1/health")
    assert resp.status == 200
    data = await resp.json()
    assert data["locked"] is True


async def test_locked_middleware_allows_encryption_status(locked_client):
    """Encryption status endpoint works when locked."""
    resp = await locked_client.get("/api/v1/encryption/status")
    assert resp.status == 200
    data = await resp.json()
    assert data["encryption_enabled"] is True
    assert data["locked"] is True


async def test_locked_middleware_allows_unlock(locked_client):
    """Unlock endpoint is accessible when locked (even if body is bad)."""
    resp = await locked_client.post("/api/v1/unlock", json={"key": "bad"})
    # 400 because key is not 64 hex chars — but NOT 423
    assert resp.status == 400


# ── Encryption status endpoint ──────────────────────────────────────


async def test_encryption_status_disabled(unlocked_client):
    """Returns encryption_enabled=false when encryption is not configured."""
    resp = await unlocked_client.get("/api/v1/encryption/status")
    assert resp.status == 200
    data = await resp.json()
    assert data["encryption_enabled"] is False
    assert data["locked"] is False


# ── Health endpoint locked field ────────────────────────────────────


async def test_health_includes_locked_field(unlocked_client):
    """Health response includes locked field."""
    resp = await unlocked_client.get("/api/v1/health")
    assert resp.status == 200
    data = await resp.json()
    assert data["locked"] is False


# ── Unlock endpoint ─────────────────────────────────────────────────


async def test_unlock_endpoint_bad_key_format(locked_client):
    """POST /unlock with non-hex key returns 400."""
    resp = await locked_client.post("/api/v1/unlock", json={"key": "short"})
    assert resp.status == 400
    data = await resp.json()
    assert "64-character hex" in data["error"]


async def test_unlock_endpoint_wrong_key(aiohttp_client, tmp_path):
    """POST /unlock with wrong key returns 401."""
    db_path = tmp_path / "enc.db"
    await init_encrypted_db(db_path, TEST_KEY)

    app = create_app(db_path)
    get_runtime_state(app).db_locked = True
    app["encryption_enabled"] = True
    client = await aiohttp_client(app)

    resp = await client.post("/api/v1/unlock", json={"key": WRONG_KEY})
    assert resp.status == 401
    data = await resp.json()
    assert data["error"] == "Invalid encryption key"


async def test_unlock_endpoint_success(aiohttp_client, tmp_path):
    """POST /unlock with correct key unlocks the database."""
    db_path = tmp_path / "enc.db"
    await init_encrypted_db(db_path, TEST_KEY)

    app = create_app(db_path)
    get_runtime_state(app).db_locked = True
    app["encryption_enabled"] = True
    client = await aiohttp_client(app)

    resp = await client.post("/api/v1/unlock", json={"key": TEST_KEY})
    assert resp.status == 200
    data = await resp.json()
    assert data["status"] == "unlocked"

    # Verify the app state has been updated
    state = get_runtime_state(app)
    assert state.db_locked is False
    assert state.db_key == TEST_KEY
    assert state.repo is not None

    # Health should now show unlocked
    resp = await client.get("/api/v1/health")
    data = await resp.json()
    assert data["locked"] is False


async def test_unlock_endpoint_invalid_json(locked_client):
    """POST /unlock with invalid JSON returns 400."""
    resp = await locked_client.post(
        "/api/v1/unlock",
        data=b"not json",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status == 400
