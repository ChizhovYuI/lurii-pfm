"""Transaction grouping for display: partial fills, trade pairs, internal transfers."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from pfm.db.models import TransactionType

if TYPE_CHECKING:
    from datetime import date

    from pfm.db.models import Transaction, TransactionMetadata

_MS_THRESHOLD = 1e12


@dataclass(frozen=True, slots=True)
class TransactionGroup:
    """A group of related transactions collapsed into a single display row."""

    group_type: str  # "trade_pair" | "partial_fill" | "internal_transfer"
    display_date: date
    display_tx_type: str
    display_usd_value: Decimal
    child_ids: list[int]
    from_source: str
    to_source: str
    from_asset: str
    to_asset: str
    from_amount: Decimal
    to_amount: Decimal


# ── Timestamp extraction ──────────────────────────────────────────────

_TIMESTAMP_KEYS = ("created_at", "dateTime", "datetime", "timestamp", "time", "ts")

_DATETIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y%m%d;%H%M%S",  # IBKR Flex format
)


def _parse_time_str(val: str, tx_date: date) -> int | None:
    """Parse a time string into epoch milliseconds."""
    # Full datetime formats.
    for fmt in _DATETIME_FORMATS:
        try:
            dt = datetime.strptime(val, fmt)  # noqa: DTZ007
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    # HH:MM only (e.g. KBank) — combine with tx.date in Bangkok timezone.
    stripped = val.strip()
    if len(stripped) >= 5 and stripped[2] == ":":  # noqa: PLR2004
        try:
            dt = datetime.strptime(stripped[:5], "%H:%M")  # noqa: DTZ007
            combined = datetime(
                tx_date.year,
                tx_date.month,
                tx_date.day,
                dt.hour,
                dt.minute,
                tzinfo=ZoneInfo("Asia/Bangkok"),
            )
            return int(combined.timestamp() * 1000)
        except ValueError:
            pass
    return None


def _extract_timestamp_ms(tx: Transaction) -> int | None:
    """Extract a sub-day timestamp from raw_json for clustering."""
    if not tx.raw_json:
        return None
    try:
        parsed = json.loads(tx.raw_json)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(parsed, dict):
        return None
    for key in _TIMESTAMP_KEYS:
        val = parsed.get(key)
        if val is None:
            continue
        if isinstance(val, int | float):
            # Assume milliseconds if > _MS_THRESHOLD, seconds otherwise.
            return int(val) if val > _MS_THRESHOLD else int(val * 1000)
        if isinstance(val, str):
            result = _parse_time_str(val, tx.date)
            if result is not None:
                return result
    return None


# ── Clustering ────────────────────────────────────────────────────────

_CLUSTER_WINDOW_MS = 60_000  # 60 seconds


def _cluster_by_timestamp(txs: list[Transaction]) -> list[list[Transaction]]:
    """Cluster transactions within a time window. Falls back to one cluster if no timestamps."""
    with_ts: list[tuple[int, Transaction]] = []
    without_ts: list[Transaction] = []

    for tx in txs:
        ts = _extract_timestamp_ms(tx)
        if ts is not None:
            with_ts.append((ts, tx))
        else:
            without_ts.append(tx)

    if not with_ts:
        return [txs] if txs else []

    with_ts.sort(key=lambda pair: pair[0])
    clusters: list[list[Transaction]] = []
    current_cluster: list[Transaction] = [with_ts[0][1]]
    current_start = with_ts[0][0]

    for ts, tx in with_ts[1:]:
        if ts - current_start <= _CLUSTER_WINDOW_MS:
            current_cluster.append(tx)
        else:
            clusters.append(current_cluster)
            current_cluster = [tx]
            current_start = ts
    clusters.append(current_cluster)

    # Transactions without timestamps go into their own singles.
    clusters.extend([tx] for tx in without_ts)

    return clusters


# ── Amount matching ───────────────────────────────────────────────────

_USD_TOLERANCE = Decimal("0.10")  # 10%


def _usd_totals_close(a: Decimal, b: Decimal) -> bool:
    """Check if two USD totals are within tolerance."""
    if a == 0 and b == 0:
        return True
    larger = max(abs(a), abs(b))
    if larger == 0:
        return True
    return abs(a - b) / larger <= _USD_TOLERANCE


# ── Pass 1: Internal transfers ────────────────────────────────────────


def _group_internal_transfers(
    items: list[tuple[Transaction, TransactionMetadata | None]],
) -> tuple[list[TransactionGroup], set[int]]:
    """Group transactions linked via transfer_pair_id."""
    groups: list[TransactionGroup] = []
    consumed: set[int] = set()

    # Build lookup by transaction ID.
    by_id: dict[int, tuple[Transaction, TransactionMetadata | None]] = {}
    for tx, meta in items:
        if tx.id is not None:
            by_id[tx.id] = (tx, meta)

    for tx, meta in items:
        if tx.id is None or tx.id in consumed:
            continue
        if meta is None or not meta.is_internal_transfer or meta.transfer_pair_id is None:
            continue
        pair_id = meta.transfer_pair_id
        if pair_id in consumed:
            continue

        pair = by_id.get(pair_id)
        if pair is None:
            continue

        pair_tx, _ = pair

        # Determine withdrawal (from) and deposit (to) sides.
        outflow_types = frozenset({TransactionType.WITHDRAWAL, TransactionType.TRANSFER})
        if tx.tx_type in outflow_types:
            from_tx, to_tx = tx, pair_tx
        else:
            from_tx, to_tx = pair_tx, tx

        from_source = from_tx.source_name or from_tx.source
        to_source = to_tx.source_name or to_tx.source

        groups.append(
            TransactionGroup(
                group_type="internal_transfer",
                display_date=tx.date,
                display_tx_type="transfer",
                display_usd_value=max(tx.usd_value, pair_tx.usd_value),
                child_ids=sorted([tx.id, pair_id]),
                from_source=from_source,
                to_source=to_source,
                from_asset=from_tx.asset,
                to_asset=to_tx.asset,
                from_amount=abs(from_tx.amount),
                to_amount=abs(to_tx.amount),
            )
        )
        consumed.add(tx.id)
        consumed.add(pair_id)

    return groups, consumed


# ── Pass 2: Trade pairs ──────────────────────────────────────────────


def _try_pair_cluster(
    cluster: list[Transaction],
    source: str,
    dt: date,
) -> TransactionGroup | None:
    """Try to pair a cluster of trades into a from/to group."""
    if len(cluster) < 2:  # noqa: PLR2004
        return None

    # Split by asset.
    by_asset: dict[str, list[Transaction]] = {}
    for tx in cluster:
        by_asset.setdefault(tx.asset, []).append(tx)

    if len(by_asset) < 2:  # noqa: PLR2004
        return None

    # Sort assets by total USD to find the two dominant sides.
    asset_totals: list[tuple[str, Decimal, list[Transaction]]] = []
    for asset, txs in by_asset.items():
        total = sum((abs(t.usd_value) for t in txs), Decimal(0))
        asset_totals.append((asset, total, txs))
    asset_totals.sort(key=lambda x: x[1], reverse=True)

    if len(asset_totals) < 2:  # noqa: PLR2004
        return None

    # Take top 2 by USD value.
    a_asset, a_usd, a_txs = asset_totals[0]
    b_asset, b_usd, b_txs = asset_totals[1]

    if not _usd_totals_close(a_usd, b_usd):
        return None

    # Determine sell/buy sides using trade_side field.
    a_is_sell = any(t.trade_side == "sell" for t in a_txs)
    b_is_sell = any(t.trade_side == "sell" for t in b_txs)

    if a_is_sell and not b_is_sell:
        from_asset, from_txs, to_asset, to_txs = a_asset, a_txs, b_asset, b_txs
    elif b_is_sell and not a_is_sell:
        from_asset, from_txs, to_asset, to_txs = b_asset, b_txs, a_asset, a_txs
    # Alphabetical fallback.
    elif a_asset <= b_asset:
        from_asset, from_txs, to_asset, to_txs = a_asset, a_txs, b_asset, b_txs
    else:
        from_asset, from_txs, to_asset, to_txs = b_asset, b_txs, a_asset, a_txs

    all_txs = from_txs + to_txs
    child_ids = sorted(t.id for t in all_txs if t.id is not None)

    return TransactionGroup(
        group_type="trade_pair",
        display_date=dt,
        display_tx_type="trade",
        display_usd_value=max(a_usd, b_usd),
        child_ids=child_ids,
        from_source=source,
        to_source=source,
        from_asset=from_asset,
        to_asset=to_asset,
        from_amount=sum((abs(t.amount) for t in from_txs), Decimal(0)),
        to_amount=sum((abs(t.amount) for t in to_txs), Decimal(0)),
    )


def _group_trade_pairs(
    items: list[tuple[Transaction, TransactionMetadata | None]],
    consumed: set[int],
) -> tuple[list[TransactionGroup], set[int]]:
    """Group multi-asset trade clusters into from/to pairs."""
    groups: list[TransactionGroup] = []

    # Bucket remaining trades by (source_name, date).
    buckets: dict[tuple[str, date], list[Transaction]] = {}
    for tx, _ in items:
        if tx.id is None or tx.id in consumed:
            continue
        if tx.tx_type != TransactionType.TRADE:
            continue
        key = (tx.source_name or tx.source, tx.date)
        buckets.setdefault(key, []).append(tx)

    for (source_name, dt), bucket_txs in buckets.items():
        if len(bucket_txs) < 2:  # noqa: PLR2004
            continue

        for cluster in _cluster_by_timestamp(bucket_txs):
            group = _try_pair_cluster(cluster, source_name, dt)
            if group is None:
                continue
            groups.append(group)
            for cid in group.child_ids:
                consumed.add(cid)

    return groups, consumed


# ── Pass 3: Partial fills ─────────────────────────────────────────────


def _group_partial_fills(
    items: list[tuple[Transaction, TransactionMetadata | None]],
    consumed: set[int],
) -> tuple[list[TransactionGroup], set[int]]:
    """Group remaining same (source, date, asset, tx_type) clusters."""
    groups: list[TransactionGroup] = []

    buckets: dict[tuple[str, date, str, str], list[Transaction]] = {}
    for tx, _ in items:
        if tx.id is None or tx.id in consumed:
            continue
        if tx.tx_type in (TransactionType.SPEND, TransactionType.TRANSFER):
            continue
        key: tuple[str, date, str, str] = (tx.source_name or tx.source, tx.date, tx.asset, tx.tx_type.value)
        buckets.setdefault(key, []).append(tx)

    for (source, dt, asset, tx_type), bucket_txs in buckets.items():
        if len(bucket_txs) < 2:  # noqa: PLR2004
            continue

        child_ids = sorted(t.id for t in bucket_txs if t.id is not None)
        total_amount = sum((abs(t.amount) for t in bucket_txs), Decimal(0))
        total_usd = sum((abs(t.usd_value) for t in bucket_txs), Decimal(0))

        groups.append(
            TransactionGroup(
                group_type="partial_fill",
                display_date=dt,
                display_tx_type=tx_type,
                display_usd_value=total_usd,
                child_ids=child_ids,
                from_source=source,
                to_source=source,
                from_asset=asset,
                to_asset=asset,
                from_amount=total_amount,
                to_amount=total_amount,
            )
        )
        for cid in child_ids:
            consumed.add(cid)

    return groups, consumed


# ── Public API ────────────────────────────────────────────────────────


@dataclass(slots=True)
class GroupingResult:
    """Result of grouping transactions."""

    groups: list[TransactionGroup] = field(default_factory=list)
    ungrouped: list[tuple[Transaction, TransactionMetadata | None]] = field(default_factory=list)
    total_ungrouped: int = 0


def group_transactions(
    items: list[tuple[Transaction, TransactionMetadata | None]],
) -> GroupingResult:
    """Group transactions into display rows.

    Three passes (earlier passes consume transactions):
    1. Internal transfers (via transfer_pair_id)
    2. Trade pairs (multi-asset clusters within 60s)
    3. Partial fills (same source/date/asset/type)
    """
    transfer_groups, consumed = _group_internal_transfers(items)
    trade_groups, consumed = _group_trade_pairs(items, consumed)
    fill_groups, consumed = _group_partial_fills(items, consumed)

    ungrouped = [(tx, meta) for tx, meta in items if tx.id is not None and tx.id not in consumed]

    return GroupingResult(
        groups=transfer_groups + trade_groups + fill_groups,
        ungrouped=ungrouped,
        total_ungrouped=len(items),
    )
