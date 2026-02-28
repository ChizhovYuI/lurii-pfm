"""Portfolio-level analytics derived from daily snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import date

    from pfm.db.models import Snapshot
    from pfm.db.repository import Repository

_HUNDRED = Decimal(100)

_FIAT_ASSETS: frozenset[str] = frozenset(
    {
        "USD",
        "THB",
        "GBP",
        "EUR",
        "JPY",
        "CHF",
        "CAD",
        "AUD",
        "NZD",
        "SGD",
        "HKD",
    }
)
_CRYPTO_SOURCES: frozenset[str] = frozenset({"okx", "binance", "binance_th", "bybit", "lobstr"})
_FIAT_SOURCES: frozenset[str] = frozenset({"wise", "kbank"})
_STOCK_SOURCES: frozenset[str] = frozenset({"ibkr"})
_DEFI_SOURCES: frozenset[str] = frozenset({"blend"})


@dataclass(frozen=True, slots=True)
class AssetAllocation:
    """Allocation row for a single asset aggregated across sources."""

    asset: str
    sources: tuple[str, ...]
    amount: Decimal
    usd_value: Decimal
    price: Decimal
    percentage: Decimal


@dataclass(frozen=True, slots=True)
class BucketAllocation:
    """Allocation row for grouped buckets (source/category)."""

    bucket: str
    usd_value: Decimal
    percentage: Decimal


@dataclass(frozen=True, slots=True)
class CurrencyExposure:
    """Exposure row for fiat currencies as a share of portfolio value."""

    currency: str
    usd_value: Decimal
    percentage: Decimal


@dataclass(frozen=True, slots=True)
class RiskMetrics:
    """Portfolio concentration metrics."""

    concentration_percentage: Decimal
    top_5_assets: list[AssetAllocation]
    hhi_index: Decimal


async def compute_net_worth(repo: Repository, snapshot_date: date) -> Decimal:
    """Compute total USD value of all snapshots for a date."""
    snapshots = await repo.get_snapshots_by_date(snapshot_date)
    return sum((s.usd_value for s in snapshots), Decimal(0))


async def compute_allocation_by_asset(repo: Repository, snapshot_date: date) -> list[AssetAllocation]:
    """Compute per-asset allocation with sources list and cached price."""
    snapshots = await repo.get_snapshots_by_date(snapshot_date)
    total_usd = _sum_usd(snapshots)
    by_asset: dict[str, tuple[Decimal, Decimal, set[str]]] = {}

    for snap in snapshots:
        amount, usd_value, sources = by_asset.get(snap.asset, (Decimal(0), Decimal(0), set()))
        sources.add(snap.source)
        by_asset[snap.asset] = (amount + snap.amount, usd_value + snap.usd_value, sources)

    # Build price lookup from prices table
    prices = await repo.get_prices_by_date(snapshot_date)
    price_map: dict[str, Decimal] = {}
    for p in prices:
        if p.currency == "USD":
            price_map[p.asset.upper()] = p.price

    rows = [
        AssetAllocation(
            asset=asset,
            sources=tuple(sorted(sources)),
            amount=amount,
            usd_value=usd_value,
            price=price_map.get(asset.upper(), usd_value / amount if amount else Decimal(0)),
            percentage=_percentage(usd_value, total_usd),
        )
        for asset, (amount, usd_value, sources) in by_asset.items()
    ]
    rows.sort(key=lambda r: r.usd_value, reverse=True)
    return rows


async def compute_allocation_by_source(repo: Repository, snapshot_date: date) -> list[BucketAllocation]:
    """Compute per-source allocation as share of total portfolio value."""
    snapshots = await repo.get_snapshots_by_date(snapshot_date)
    total_usd = _sum_usd(snapshots)
    by_source: dict[str, Decimal] = {}

    for snap in snapshots:
        by_source[snap.source] = by_source.get(snap.source, Decimal(0)) + snap.usd_value

    rows = [
        BucketAllocation(
            bucket=source,
            usd_value=usd_value,
            percentage=_percentage(usd_value, total_usd),
        )
        for source, usd_value in by_source.items()
    ]
    rows.sort(key=lambda r: r.usd_value, reverse=True)
    return rows


async def compute_allocation_by_category(repo: Repository, snapshot_date: date) -> list[BucketAllocation]:
    """Compute allocation across category buckets: crypto/fiat/stocks/DeFi."""
    snapshots = await repo.get_snapshots_by_date(snapshot_date)
    total_usd = _sum_usd(snapshots)
    by_category: dict[str, Decimal] = {}

    for snap in snapshots:
        category = _category_for_snapshot(snap)
        by_category[category] = by_category.get(category, Decimal(0)) + snap.usd_value

    rows = [
        BucketAllocation(
            bucket=category,
            usd_value=usd_value,
            percentage=_percentage(usd_value, total_usd),
        )
        for category, usd_value in by_category.items()
    ]
    rows.sort(key=lambda r: r.usd_value, reverse=True)
    return rows


async def compute_currency_exposure(repo: Repository, snapshot_date: date) -> list[CurrencyExposure]:
    """Compute fiat currency exposure as share of total portfolio value."""
    snapshots = await repo.get_snapshots_by_date(snapshot_date)
    total_usd = _sum_usd(snapshots)
    by_currency: dict[str, Decimal] = {}

    for snap in snapshots:
        asset = snap.asset.upper()
        if asset in _FIAT_ASSETS:
            by_currency[asset] = by_currency.get(asset, Decimal(0)) + snap.usd_value

    rows = [
        CurrencyExposure(
            currency=currency,
            usd_value=usd_value,
            percentage=_percentage(usd_value, total_usd),
        )
        for currency, usd_value in by_currency.items()
    ]
    rows.sort(key=lambda r: r.usd_value, reverse=True)
    return rows


async def compute_risk_metrics(repo: Repository, snapshot_date: date) -> RiskMetrics:
    """Compute concentration percentage, top 5 assets, and HHI index."""
    by_asset = await compute_allocation_by_asset(repo, snapshot_date)
    if not by_asset:
        return RiskMetrics(
            concentration_percentage=Decimal(0),
            top_5_assets=[],
            hhi_index=Decimal(0),
        )

    concentration = by_asset[0].percentage
    top_5 = by_asset[:5]
    hhi = sum(((row.percentage / _HUNDRED) ** 2 for row in by_asset), Decimal(0))
    return RiskMetrics(
        concentration_percentage=concentration,
        top_5_assets=top_5,
        hhi_index=hhi,
    )


def _sum_usd(snapshots: list[Snapshot]) -> Decimal:
    return sum((s.usd_value for s in snapshots), Decimal(0))


def _percentage(value: Decimal, total: Decimal) -> Decimal:
    if total == 0:
        return Decimal(0)
    return (value / total) * _HUNDRED


def _category_for_snapshot(snap: Snapshot) -> str:
    source = snap.source.lower()
    asset = snap.asset.upper()

    if source in _DEFI_SOURCES:
        return "DeFi"
    if source in _FIAT_SOURCES:
        return "fiat"
    if source in _STOCK_SOURCES:
        return "fiat" if asset in _FIAT_ASSETS else "stocks"
    if asset in _FIAT_ASSETS:
        return "fiat"
    if source in _CRYPTO_SOURCES:
        return "crypto"
    return "crypto"
