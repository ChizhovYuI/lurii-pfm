"""Yield analytics from snapshot and transaction history."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING

from pfm.db.models import TransactionType

if TYPE_CHECKING:
    from pfm.db.models import Snapshot, Transaction
    from pfm.db.repository import Repository

_HUNDRED = Decimal(100)


@dataclass(frozen=True, slots=True)
class YieldResult:
    """Yield summary for a source+asset over a period."""

    source: str
    asset: str
    start_date: date | None
    end_date: date | None
    principal_estimate: Decimal
    current_value: Decimal
    yield_amount: Decimal
    yield_percentage: Decimal
    annualized_rate: Decimal
    notes: list[str] = field(default_factory=list)


async def compute_yield(
    repo: Repository,
    source: str,
    asset: str,
    start: date,
    end: date,
) -> YieldResult:
    """Compute yield using balance snapshots and contribution-adjusted principal."""
    if end < start:
        msg = "end date must be on or after start date"
        raise ValueError(msg)

    asset_upper = asset.upper()
    notes: list[str] = []

    snapshots = await repo.get_snapshots_for_range(date.min, end)
    relevant = [s for s in snapshots if s.source == source and s.asset.upper() == asset_upper]
    if not relevant:
        return YieldResult(
            source=source,
            asset=asset_upper,
            start_date=None,
            end_date=None,
            principal_estimate=Decimal(0),
            current_value=Decimal(0),
            yield_amount=Decimal(0),
            yield_percentage=Decimal(0),
            annualized_rate=Decimal(0),
            notes=["No snapshots found for requested source/asset."],
        )

    by_date = _group_snapshot_values(relevant)
    available_dates = sorted(by_date.keys())

    end_date = _latest_on_or_before(available_dates, end)
    if end_date != end:
        notes.append(f"No snapshot on {end.isoformat()}; using {end_date.isoformat()} as end date.")

    start_date = _latest_on_or_before(available_dates, start)
    if start_date != start:
        notes.append(f"No snapshot on {start.isoformat()}; using {start_date.isoformat()} as start date.")

    start_value = by_date[start_date]
    current_value = by_date[end_date]

    txs = await repo.get_transactions(source=source, start=start_date, end=end_date)
    principal_estimate = start_value + _net_contributions_usd(txs, asset_upper)
    if principal_estimate < 0:
        principal_estimate = Decimal(0)
        notes.append("Principal estimate went below zero; clamped to zero.")

    yield_amount = current_value - principal_estimate
    yield_percentage = _percentage(yield_amount, principal_estimate)
    annualized_rate = _annualized_rate(principal_estimate, current_value, start_date, end_date)

    return YieldResult(
        source=source,
        asset=asset_upper,
        start_date=start_date,
        end_date=end_date,
        principal_estimate=principal_estimate,
        current_value=current_value,
        yield_amount=yield_amount,
        yield_percentage=yield_percentage,
        annualized_rate=annualized_rate,
        notes=notes,
    )


def _group_snapshot_values(snapshots: list[Snapshot]) -> dict[date, Decimal]:
    grouped: dict[date, Decimal] = {}
    for snap in snapshots:
        grouped[snap.date] = grouped.get(snap.date, Decimal(0)) + snap.usd_value
    return grouped


def _latest_on_or_before(dates: list[date], target: date) -> date:
    candidates = [d for d in dates if d <= target]
    if candidates:
        return candidates[-1]
    return dates[0]


def _net_contributions_usd(txs: list[Transaction], asset: str) -> Decimal:
    """Estimate net principal contributions for the tracked asset."""
    total = Decimal(0)
    for tx in txs:
        if tx.asset.upper() != asset:
            continue
        if tx.usd_value <= 0:
            continue

        if tx.tx_type in {TransactionType.DEPOSIT, TransactionType.TRANSFER, TransactionType.TRADE}:
            total += tx.usd_value
        elif tx.tx_type in {TransactionType.WITHDRAWAL, TransactionType.FEE}:
            total -= tx.usd_value
    return total


def _percentage(change: Decimal, base: Decimal) -> Decimal:
    if base <= 0:
        return Decimal(0)
    return (change / base) * _HUNDRED


def _annualized_rate(principal: Decimal, current: Decimal, start: date, end: date) -> Decimal:
    days = (end - start).days
    if days <= 0 or principal <= 0 or current <= 0:
        return Decimal(0)

    ratio = float(current / principal)
    annualized = ((ratio ** (365.0 / days)) - 1.0) * 100.0
    return Decimal(str(annualized))
