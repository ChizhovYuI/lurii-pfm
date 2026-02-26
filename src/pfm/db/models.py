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
class Snapshot:
    """A point-in-time balance for a single asset from a single source."""

    date: date
    source: str
    asset: str
    amount: Decimal
    usd_value: Decimal
    raw_json: str = ""
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
class RawResponse:
    """Raw API response stored for auditability."""

    date: date
    source: str
    endpoint: str
    response_body: str
    id: int | None = None
    created_at: datetime | None = None


@dataclass(slots=True)
class CollectorResult:
    """Result summary from a collector run."""

    source: str
    snapshots_count: int = 0
    transactions_count: int = 0
    errors: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0


SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    source TEXT NOT NULL,
    asset TEXT NOT NULL,
    amount TEXT NOT NULL,
    usd_value TEXT NOT NULL,
    raw_json TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_snapshots_date ON snapshots(date);
CREATE INDEX IF NOT EXISTS idx_snapshots_source ON snapshots(source);

CREATE TABLE IF NOT EXISTS transactions (
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

CREATE TABLE IF NOT EXISTS raw_responses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    source TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    response_body TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS analytics_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_analytics_cache_date ON analytics_cache(date);
"""


async def init_db(path: Path) -> None:
    """Create database and all tables if they don't exist."""
    path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(str(path)) as db:
        await db.executescript(SCHEMA_SQL)
        await db.commit()
