"""PnL analytics over portfolio snapshots."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING

from pfm.db.models import TransactionType, is_sync_marker_snapshot

if TYPE_CHECKING:
    from pfm.db.models import Snapshot
    from pfm.db.repository import Repository

_HUNDRED = Decimal(100)


class PnlPeriod(StrEnum):
    """Supported PnL periods."""

    ONE_WEEK = "1w"
    MONTH_TO_DATE = "mtd"
    ONE_MONTH = "1m"
    THREE_MONTHS = "3m"
    YEAR_TO_DATE = "ytd"
    ONE_YEAR = "1y"
    ALL = "all"
    THIRTY_DAYS = "30d"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    ALL_TIME = "all_time"


@dataclass(frozen=True, slots=True)
class AssetPnl:
    """Per-asset PnL row."""

    asset: str
    start_value: Decimal
    end_value: Decimal
    absolute_change: Decimal
    percentage_change: Decimal
    cost_basis_value: Decimal | None = None


@dataclass(frozen=True, slots=True)
class PnlResult:
    """Portfolio PnL summary for a period."""

    start_date: date | None
    end_date: date | None
    start_value: Decimal
    end_value: Decimal
    absolute_change: Decimal
    percentage_change: Decimal
    by_asset: list[AssetPnl] = field(default_factory=list)
    top_gainers: list[AssetPnl] = field(default_factory=list)
    top_losers: list[AssetPnl] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


async def compute_pnl(repo: Repository, as_of: date, period: PnlPeriod) -> PnlResult:
    """Compute period PnL from available snapshots."""
    return await _compute_pnl(repo, as_of, period, require_exact_window=False)


async def compute_pnl_exact(repo: Repository, as_of: date, period: PnlPeriod) -> PnlResult:
    """Compute period PnL only when exact start and end dates both exist."""
    return await _compute_pnl(repo, as_of, period, require_exact_window=True)


async def _compute_pnl(repo: Repository, as_of: date, period: PnlPeriod, *, require_exact_window: bool) -> PnlResult:
    """Compute period PnL from available snapshots."""
    dates = await _get_available_dates(repo, as_of)
    if not dates:
        return _empty_pnl_result("No snapshots available on or before requested date.")

    notes: list[str] = []
    if require_exact_window:
        if as_of not in dates:
            return _empty_pnl_result(f"No snapshot on {as_of.isoformat()}; PnL is unavailable for the selected range.")
        end_date = as_of
    else:
        end_date = dates[-1]
        if end_date != as_of:
            notes.append(f"No snapshot on {as_of.isoformat()}; using {end_date.isoformat()} as end date.")

    target_start = _target_start_date(end_date, period, dates[0])
    if require_exact_window:
        if target_start not in dates:
            return _empty_pnl_result(
                f"No snapshot on {target_start.isoformat()}; PnL is unavailable for the selected range."
            )
        start_date = target_start
    else:
        start_date = _latest_on_or_before(dates, target_start)
        if start_date != target_start:
            notes.append(f"Requested start {target_start.isoformat()} not found; using {start_date.isoformat()}.")

    start_snapshots = await repo.get_snapshots_resolved(start_date)
    end_snapshots = await repo.get_snapshots_resolved(end_date)
    start_values = _aggregate_usd_by_asset(start_snapshots)
    end_values = _aggregate_usd_by_asset(end_snapshots)
    end_amounts = _aggregate_amount_by_asset(end_snapshots)
    cost_basis_avg = await _compute_average_cost_basis(repo, end_date)

    start_total = sum(start_values.values(), Decimal(0))
    end_total = sum(end_values.values(), Decimal(0))
    absolute_change = end_total - start_total
    percentage_change = _percentage(absolute_change, start_total)

    asset_rows: list[AssetPnl] = []
    for asset in sorted(set(start_values) | set(end_values)):
        start_value = start_values.get(asset, Decimal(0))
        end_value = end_values.get(asset, Decimal(0))
        change = end_value - start_value
        avg_cost = cost_basis_avg.get(asset)
        cost_basis_value = None
        if avg_cost is not None:
            cost_basis_value = avg_cost * end_amounts.get(asset, Decimal(0))
        asset_rows.append(
            AssetPnl(
                asset=asset,
                start_value=start_value,
                end_value=end_value,
                absolute_change=change,
                percentage_change=_percentage(change, start_value),
                cost_basis_value=cost_basis_value,
            )
        )

    top_gainers = [
        row for row in sorted(asset_rows, key=lambda r: r.absolute_change, reverse=True) if row.absolute_change > 0
    ][:5]
    top_losers = [row for row in sorted(asset_rows, key=lambda r: r.absolute_change) if row.absolute_change < 0][:5]

    return PnlResult(
        start_date=start_date,
        end_date=end_date,
        start_value=start_total,
        end_value=end_total,
        absolute_change=absolute_change,
        percentage_change=percentage_change,
        by_asset=asset_rows,
        top_gainers=top_gainers,
        top_losers=top_losers,
        notes=notes,
    )


def _empty_pnl_result(note: str) -> PnlResult:
    return PnlResult(
        start_date=None,
        end_date=None,
        start_value=Decimal(0),
        end_value=Decimal(0),
        absolute_change=Decimal(0),
        percentage_change=Decimal(0),
        by_asset=[],
        notes=[note],
    )


async def _get_available_dates(repo: Repository, as_of: date) -> list[date]:
    snapshots = await repo.get_snapshots_for_range(date.min, as_of)
    return sorted({s.date for s in snapshots})


def _target_start_date(end_date: date, period: PnlPeriod, earliest: date) -> date:
    start_dates = {
        PnlPeriod.ONE_WEEK: end_date - timedelta(days=6),
        PnlPeriod.MONTH_TO_DATE: date(end_date.year, end_date.month, 1),
        PnlPeriod.ONE_MONTH: end_date - timedelta(days=29),
        PnlPeriod.THREE_MONTHS: end_date - timedelta(days=89),
        PnlPeriod.YEAR_TO_DATE: date(end_date.year, 1, 1),
        PnlPeriod.ONE_YEAR: end_date - timedelta(days=364),
        PnlPeriod.ALL: earliest,
        PnlPeriod.THIRTY_DAYS: end_date - timedelta(days=30),
        PnlPeriod.DAILY: end_date - timedelta(days=1),
        PnlPeriod.WEEKLY: end_date - timedelta(days=7),
        PnlPeriod.MONTHLY: date(end_date.year, end_date.month, 1),
    }
    return start_dates.get(period, earliest)


def _latest_on_or_before(dates: list[date], target: date) -> date:
    candidates = [d for d in dates if d <= target]
    if candidates:
        return candidates[-1]
    return dates[0]


def _aggregate_usd_by_asset(snapshots: list[Snapshot]) -> dict[str, Decimal]:
    by_asset: dict[str, Decimal] = {}
    for snap in snapshots:
        if is_sync_marker_snapshot(snap):
            continue
        by_asset[snap.asset] = by_asset.get(snap.asset, Decimal(0)) + snap.usd_value
    return by_asset


def _aggregate_amount_by_asset(snapshots: list[Snapshot]) -> dict[str, Decimal]:
    by_asset: dict[str, Decimal] = {}
    for snap in snapshots:
        if is_sync_marker_snapshot(snap):
            continue
        by_asset[snap.asset] = by_asset.get(snap.asset, Decimal(0)) + snap.amount
    return by_asset


def _percentage(change: Decimal, base: Decimal) -> Decimal:
    if base == 0:
        return Decimal(0)
    return (change / base) * _HUNDRED


async def _compute_average_cost_basis(repo: Repository, as_of: date) -> dict[str, Decimal]:
    """Compute average cost basis (USD per unit) from transaction history."""
    txs = await repo.get_transactions(end=as_of)
    txs_sorted = sorted(txs, key=lambda tx: tx.date)
    position_qty: dict[str, Decimal] = {}
    avg_cost: dict[str, Decimal] = {}

    for tx in txs_sorted:
        if tx.amount <= 0 or tx.asset == "":
            continue

        current_qty = position_qty.get(tx.asset, Decimal(0))
        current_avg = avg_cost.get(tx.asset, Decimal(0))
        trade_side = tx.trade_side.lower()

        if tx.tx_type == TransactionType.TRADE and trade_side == "sell":
            new_qty = current_qty - tx.amount
            position_qty[tx.asset] = new_qty if new_qty > 0 else Decimal(0)
            continue

        if tx.tx_type in {
            TransactionType.DEPOSIT,
            TransactionType.TRADE,
            TransactionType.YIELD,
        }:
            if tx.usd_value <= 0:
                position_qty[tx.asset] = current_qty + tx.amount
                continue
            new_qty = current_qty + tx.amount
            if new_qty > 0:
                avg_cost[tx.asset] = ((current_qty * current_avg) + tx.usd_value) / new_qty
                position_qty[tx.asset] = new_qty
        elif tx.tx_type in {TransactionType.WITHDRAWAL, TransactionType.FEE}:
            new_qty = current_qty - tx.amount
            position_qty[tx.asset] = new_qty if new_qty > 0 else Decimal(0)

    return {asset: cost for asset, cost in avg_cost.items() if position_qty.get(asset, Decimal(0)) > 0}
