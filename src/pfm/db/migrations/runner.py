"""Programmatic Alembic runner for the application database."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, cast

import sqlcipher3
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect

if TYPE_CHECKING:
    import sqlite3

    from sqlalchemy.engine import Connection, Engine

_INITIAL_REVISION = "9cd516d0ab26"
_APP_SETTINGS_REVISION = "8d775e055451"
_SNAPSHOT_SOURCE_NAME_REVISION = "5f3f6d1f2e11"
_TRANSACTION_SOURCE_NAME_REVISION = "b7a1c91c9d5f"
_SNAPSHOT_PRICE_APY_REVISION = "c4a2e7d6f9b1"
_AI_PROVIDERS_REVISION = "d3f1b8a4c2e6"
_RAW_RESPONSES_REVISION = "e7b9c1d4a5f0"
_APP_BASE_TABLES = {"snapshots", "transactions", "prices", "analytics_cache", "sources"}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _sqlite_url(path: Path) -> str:
    return f"sqlite:///{path}"


def _make_config(db_path: Path, connection: Connection) -> Config:
    config = Config(str(_repo_root() / "alembic.ini"))
    config.set_main_option("script_location", str(_repo_root() / "src/pfm/db/migrations"))
    config.set_main_option("sqlalchemy.url", _sqlite_url(db_path))
    config.attributes["connection"] = connection
    return config


def _create_engine(db_path: Path, *, key_hex: str | None = None) -> Engine:
    if key_hex is None:
        return create_engine(_sqlite_url(db_path), future=True)

    def _creator() -> sqlite3.Connection:
        conn = cast("sqlite3.Connection", sqlcipher3.connect(str(db_path)))
        conn.execute(f"PRAGMA key = \"x'{key_hex}'\"")
        conn.execute("PRAGMA cipher_compatibility = 4")
        return conn

    return create_engine("sqlite://", creator=_creator, future=True)


def _infer_bootstrap_revision(connection: Connection) -> str | None:
    inspector = inspect(connection)
    tables = {name for name in inspector.get_table_names() if not name.startswith("sqlite_")}
    revision: str | None = None

    if "alembic_version" in tables:
        return None

    if not tables:
        return None

    if not _APP_BASE_TABLES.issubset(tables):
        msg = f"Unsupported legacy schema without alembic_version: {sorted(tables)}"
        raise RuntimeError(msg)

    snapshot_columns = {column["name"] for column in inspector.get_columns("snapshots")}

    transaction_columns = {column["name"] for column in inspector.get_columns("transactions")}
    if "app_settings" not in tables:
        revision = _INITIAL_REVISION
    elif "source_name" not in snapshot_columns:
        revision = _APP_SETTINGS_REVISION
    elif "source_name" not in transaction_columns or "trade_side" not in transaction_columns:
        revision = _SNAPSHOT_SOURCE_NAME_REVISION
    elif "price" not in snapshot_columns or "apy" not in snapshot_columns:
        revision = _TRANSACTION_SOURCE_NAME_REVISION
    elif "ai_providers" not in tables:
        revision = _SNAPSHOT_PRICE_APY_REVISION
    elif "raw_responses" not in tables:
        revision = _AI_PROVIDERS_REVISION
    else:
        revision = _RAW_RESPONSES_REVISION

    return revision


def _stamp_revision(connection: Connection, revision: str) -> None:
    connection.exec_driver_sql(
        "CREATE TABLE IF NOT EXISTS alembic_version ("
        "version_num VARCHAR(32) NOT NULL, "
        "CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)"
        ")"
    )
    connection.exec_driver_sql("DELETE FROM alembic_version")
    connection.exec_driver_sql("INSERT INTO alembic_version (version_num) VALUES (?)", (revision,))


def _run_migrations_sync(db_path: Path, *, key_hex: str | None = None) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    engine = _create_engine(db_path, key_hex=key_hex)
    try:
        with engine.connect() as connection:
            config = _make_config(db_path, connection)
            bootstrap_revision = _infer_bootstrap_revision(connection)
            if bootstrap_revision is not None:
                _stamp_revision(connection, bootstrap_revision)
                connection.commit()
            command.upgrade(config, "head")
            connection.commit()
    finally:
        engine.dispose()


async def run_migrations(db_path: Path, *, key_hex: str | None = None) -> None:
    """Upgrade the database schema to the latest Alembic revision."""
    await asyncio.to_thread(_run_migrations_sync, db_path, key_hex=key_hex)
