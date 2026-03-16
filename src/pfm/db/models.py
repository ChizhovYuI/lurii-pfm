"""Database models and schema definition."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import date, datetime
    from pathlib import Path


class TransactionType(enum.StrEnum):
    """Normalized transaction types across all sources."""

    DEPOSIT = "deposit"
    WITHDRAWAL = "withdrawal"
    SPEND = "spend"
    TRADE = "trade"
    YIELD = "yield"
    FEE = "fee"
    TRANSFER = "transfer"
    UNKNOWN = "unknown"


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


@dataclass(frozen=True, slots=True)
class TransactionCategory:
    """A category definition for transactions."""

    tx_type: str
    category: str
    display_name: str
    sort_order: int = 0
    id: int | None = None
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class TransactionMetadata:
    """Per-transaction metadata overlay."""

    transaction_id: int
    category: str | None = None
    category_source: str = "auto"
    category_confidence: float | None = None
    type_override: str | None = None
    is_internal_transfer: bool = False
    transfer_pair_id: int | None = None
    transfer_detected_by: str | None = None
    reviewed: bool = False
    notes: str = ""
    updated_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class CategoryRule:
    """A compound rule for auto-categorizing transactions.

    Condition 1 (required): type_match — which tx_type(s) this rule applies to.
    Condition 2 (optional): field_name + field_operator + field_value — raw_json field match.
    Source filter (optional): source — restrict to a specific source.
    """

    type_match: str
    result_category: str
    type_operator: str = "eq"
    field_name: str = ""
    field_operator: str = ""
    field_value: str = ""
    source: str = "*"
    priority: int = 300
    builtin: bool = False
    deleted: bool = False
    id: int | None = None
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class TypeRule:
    """A rule for resolving transaction type from raw_json fields.

    Mirrors CategoryRule but produces a TransactionType instead of a category.
    """

    source: str = "*"
    field_name: str = ""
    field_operator: str = "eq"
    field_value: str = ""
    result_type: str = ""
    priority: int = 100
    builtin: bool = False
    deleted: bool = False
    id: int | None = None
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class UserCategoryChoice:
    """Records a manual category selection for learning."""

    transaction_id: int
    source: str
    effective_type: str
    chosen_category: str
    field_snapshot: str = ""
    previous_category: str = ""
    id: int | None = None
    created_at: datetime | None = None


def effective_type(tx: Transaction, meta: TransactionMetadata | None) -> str:
    """Return the effective transaction type, respecting manual overrides."""
    if meta and meta.type_override:
        return meta.type_override
    return tx.tx_type.value


SYNC_MARKER_ASSET = "__SYNC__"
SYNC_MARKER_RAW_JSON = '{"sync_marker": true}'


def is_sync_marker_asset(asset: str) -> bool:
    """Return whether an asset ticker is an internal sync marker row."""
    return asset == SYNC_MARKER_ASSET


def is_sync_marker_snapshot(snapshot: Snapshot) -> bool:
    """Return whether a snapshot is an internal sync marker row."""
    return is_sync_marker_asset(snapshot.asset)


def make_sync_marker_snapshot(*, snapshot_date: date, source: str, source_name: str) -> Snapshot:
    """Create a zero-value marker snapshot to record a successful sync day."""
    return Snapshot(
        date=snapshot_date,
        source=source,
        source_name=source_name or source,
        asset=SYNC_MARKER_ASSET,
        amount=Decimal(0),
        usd_value=Decimal(0),
        price=Decimal(0),
        apy=Decimal(0),
        raw_json=SYNC_MARKER_RAW_JSON,
    )


async def init_db(path: Path, *, key_hex: str | None = None) -> None:
    """Upgrade the database schema to the latest Alembic revision."""
    from pfm.db.migrations.runner import run_migrations

    await run_migrations(path, key_hex=key_hex)
