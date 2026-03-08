"""Database models and schema definition."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING

import aiosqlite

if TYPE_CHECKING:
    from datetime import date, datetime
    from pathlib import Path


class TransactionType(enum.StrEnum):
    """Normalized transaction types across all sources."""

    DEPOSIT = "deposit"
    WITHDRAWAL = "withdrawal"
    TRADE = "trade"
    YIELD = "yield"
    DIVIDEND = "dividend"
    INTEREST = "interest"
    FEE = "fee"
    TRANSFER = "transfer"


@dataclass(frozen=True, slots=True)
class RawBalance:
    """Unpriced balance from a source API.

    Collectors return these from ``fetch_raw_balances()``.  The orchestrator
    collects unique tickers and batch-fetches prices before building Snapshots.

    When the source API already provides a USD price (e.g. IBKR, Rabby), set
    *price* so the batch step skips the CoinGecko lookup for that asset.
    """

    asset: str
    amount: Decimal
    apy: Decimal = Decimal(0)
    raw_json: str = ""
    price: Decimal | None = None
    date: date | None = None  # override snapshot date (default: today)


@dataclass(frozen=True, slots=True)
class Snapshot:
    """A point-in-time balance for a single asset from a single source."""

    date: date
    source: str
    asset: str
    amount: Decimal
    usd_value: Decimal
    price: Decimal = Decimal(0)
    apy: Decimal = Decimal(0)
    raw_json: str = ""
    source_name: str = ""
    id: int | None = None
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class Transaction:
    """A normalized transaction from any source."""

    date: date
    source: str
    tx_type: TransactionType
    asset: str
    amount: Decimal
    usd_value: Decimal
    counterparty_asset: str = ""
    counterparty_amount: Decimal = Decimal(0)
    tx_id: str = ""
    raw_json: str = ""
    source_name: str = ""
    trade_side: str = ""
    id: int | None = None
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class Price:
    """Cached price data."""

    date: date
    asset: str
    currency: str
    price: Decimal
    source: str = "coingecko"
    id: int | None = None
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class Source:
    """A configured data source with credentials."""

    name: str
    type: str
    credentials: str  # JSON blob
    enabled: bool = True
    id: int | None = None
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class AIProvider:
    """A configured AI provider with credentials."""

    type: str
    api_key: str = ""
    model: str = ""
    base_url: str = ""
    active: bool = False


@dataclass(frozen=True, slots=True)
class SourceDeleteResult:
    """Summary of rows removed when deleting a configured source."""

    name: str
    snapshots: int = 0
    transactions: int = 0
    analytics_metrics: int = 0
    apy_rules: int = 0


@dataclass(slots=True)
class CollectorResult:
    """Result summary from a collector run."""

    source: str
    snapshots_count: int = 0
    snapshots_usd_total: Decimal = Decimal(0)
    transactions_count: int = 0
    errors: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0


SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    source TEXT NOT NULL,
    source_name TEXT NOT NULL DEFAULT '',
    asset TEXT NOT NULL,
    amount TEXT NOT NULL,
    usd_value TEXT NOT NULL,
    price TEXT NOT NULL DEFAULT '0',
    apy TEXT NOT NULL DEFAULT '0',
    raw_json TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_snapshots_date ON snapshots(date);
CREATE INDEX IF NOT EXISTS idx_snapshots_source ON snapshots(source);

CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    source TEXT NOT NULL,
    source_name TEXT NOT NULL DEFAULT '',
    tx_type TEXT NOT NULL,
    asset TEXT NOT NULL,
    amount TEXT NOT NULL,
    usd_value TEXT NOT NULL,
    counterparty_asset TEXT NOT NULL DEFAULT '',
    counterparty_amount TEXT NOT NULL DEFAULT '0',
    trade_side TEXT NOT NULL DEFAULT '',
    tx_id TEXT NOT NULL DEFAULT '',
    raw_json TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date);
CREATE INDEX IF NOT EXISTS idx_transactions_source ON transactions(source);

CREATE TABLE IF NOT EXISTS prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    asset TEXT NOT NULL,
    currency TEXT NOT NULL,
    price TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'coingecko',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_prices_date_asset ON prices(date, asset);

CREATE TABLE IF NOT EXISTS analytics_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_analytics_cache_date ON analytics_cache(date);

CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL,
    credentials TEXT NOT NULL DEFAULT '{}',
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS ai_providers (
    type TEXT PRIMARY KEY,
    api_key TEXT NOT NULL DEFAULT '',
    model TEXT NOT NULL DEFAULT '',
    base_url TEXT NOT NULL DEFAULT '',
    active INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


async def _migrate_snapshots_price(db: aiosqlite.Connection) -> None:
    """Add ``price`` column to snapshots table if missing and backfill."""
    cursor = await db.execute("PRAGMA table_info(snapshots)")
    columns = {row[1] for row in await cursor.fetchall()}
    if "price" in columns:
        return
    await db.execute("ALTER TABLE snapshots ADD COLUMN price TEXT NOT NULL DEFAULT '0'")
    await db.execute(
        "UPDATE snapshots SET price = CASE"
        " WHEN CAST(amount AS REAL) != 0 THEN CAST(CAST(usd_value AS REAL) / CAST(amount AS REAL) AS TEXT)"
        " ELSE '0' END"
    )


async def _migrate_snapshots_apy(db: aiosqlite.Connection) -> None:
    """Add ``apy`` column to snapshots table if missing and backfill as '0'."""
    cursor = await db.execute("PRAGMA table_info(snapshots)")
    columns = {row[1] for row in await cursor.fetchall()}
    if "apy" in columns:
        return
    await db.execute("ALTER TABLE snapshots ADD COLUMN apy TEXT NOT NULL DEFAULT '0'")


async def _migrate_snapshots_source_name(db: aiosqlite.Connection) -> None:
    """Add ``source_name`` column and backfill from sources table when possible."""
    cursor = await db.execute("PRAGMA table_info(snapshots)")
    columns = {row[1] for row in await cursor.fetchall()}
    if "source_name" not in columns:
        await db.execute("ALTER TABLE snapshots ADD COLUMN source_name TEXT NOT NULL DEFAULT ''")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_source_name ON snapshots(source_name)")

    # Baseline fallback for legacy rows.
    await db.execute("UPDATE snapshots SET source_name = source WHERE source_name = '' OR source_name IS NULL")

    # If there is exactly one configured source for a type, align legacy rows to that instance name.
    await db.execute(
        "UPDATE snapshots "
        "SET source_name = (SELECT MIN(name) FROM sources WHERE type = snapshots.source) "
        "WHERE (source_name = source OR source_name = '' OR source_name IS NULL) "
        "  AND (SELECT COUNT(*) FROM sources WHERE type = snapshots.source) = 1"
    )


async def _migrate_transactions_source_name_and_trade_side(db: aiosqlite.Connection) -> None:
    """Add transaction instance metadata, backfill source names, and dedupe by tx_id."""
    cursor = await db.execute("PRAGMA table_info(transactions)")
    columns = {row[1] for row in await cursor.fetchall()}
    if "source_name" not in columns:
        await db.execute("ALTER TABLE transactions ADD COLUMN source_name TEXT NOT NULL DEFAULT ''")
    if "trade_side" not in columns:
        await db.execute("ALTER TABLE transactions ADD COLUMN trade_side TEXT NOT NULL DEFAULT ''")

    await db.execute("CREATE INDEX IF NOT EXISTS idx_transactions_source_name_date ON transactions(source_name, date)")

    await db.execute("UPDATE transactions SET source_name = source WHERE source_name = '' OR source_name IS NULL")
    await db.execute(
        "UPDATE transactions "
        "SET source_name = (SELECT MIN(name) FROM sources WHERE type = transactions.source) "
        "WHERE (source_name = source OR source_name = '' OR source_name IS NULL) "
        "  AND (SELECT COUNT(*) FROM sources WHERE type = transactions.source) = 1"
    )

    await db.execute(
        "DELETE FROM transactions "
        "WHERE tx_id != '' AND id NOT IN ("
        "  SELECT MIN(id) FROM transactions WHERE tx_id != '' GROUP BY source_name, tx_id"
        ")"
    )
    await db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_source_name_tx_id_unique "
        "ON transactions(source_name, tx_id) WHERE tx_id != ''"
    )


async def init_db(path: Path, *, key_hex: str | None = None) -> None:
    """Create database and all tables if they don't exist.

    When *key_hex* is provided the database is opened via SQLCipher.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if key_hex is not None:
        from pfm.db.encryption import init_encrypted_db

        await init_encrypted_db(path, key_hex)
    else:
        async with aiosqlite.connect(str(path)) as db:
            await db.executescript(SCHEMA_SQL)
            await _migrate_snapshots_price(db)
            await _migrate_snapshots_apy(db)
            await _migrate_snapshots_source_name(db)
            await _migrate_transactions_source_name_and_trade_side(db)
            await db.execute("DROP TABLE IF EXISTS raw_responses")
            await db.commit()
