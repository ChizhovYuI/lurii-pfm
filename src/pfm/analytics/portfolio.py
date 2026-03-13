"""Portfolio-level analytics derived from daily snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from pfm.db.models import is_sync_marker_snapshot

if TYPE_CHECKING:
    from datetime import date

    from pfm.db.models import Snapshot, Source
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
_CRYPTO_SOURCES: frozenset[str] = frozenset(
    {"okx", "binance", "binance_th", "bybit", "coinex", "mexc", "mexc_earn", "lobstr", "rabby"}
)
_FIAT_SOURCES: frozenset[str] = frozenset({"wise", "kbank"})
_STOCK_SOURCES: frozenset[str] = frozenset({"ibkr", "trading212"})
_DEFI_SOURCES: frozenset[str] = frozenset({"blend", "yo", "bitget_wallet"})
_DEPOSIT_SOURCES: frozenset[str] = frozenset({"emcd"})


@dataclass(frozen=True, slots=True)
class AssetAllocation:
    """Allocation row for a single (asset, asset_type) pair across sources."""

    asset: str
    asset_type: str
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


def is_fiat_asset(asset: str) -> bool:
    """Return whether an asset ticker should be treated as fiat."""
    return asset.upper() in _FIAT_ASSETS


async def compute_net_worth(repo: Repository, snapshot_date: date) -> Decimal:
    """Compute total USD value of all snapshots for a date."""
    snapshots = await repo.get_snapshots_resolved(snapshot_date)
    return sum((s.usd_value for s in snapshots), Decimal(0))


async def compute_allocation_by_asset(repo: Repository, snapshot_date: date) -> list[AssetAllocation]:
    """Compute per-(asset, asset_type) allocation with sources list and cached price."""
    snapshots = [snap for snap in await repo.get_snapshots_resolved(snapshot_date) if not is_sync_marker_snapshot(snap)]
    total_usd = _sum_usd(snapshots)
    by_key: dict[tuple[str, str], tuple[Decimal, Decimal, set[str]]] = {}

    for snap in snapshots:
        a_type = _asset_type(snap.source, snap.asset)
        key = (snap.asset, a_type)
        amount, usd_value, sources = by_key.get(key, (Decimal(0), Decimal(0), set()))
        sources.add(snap.source)
        by_key[key] = (amount + snap.amount, usd_value + snap.usd_value, sources)

    # Build price lookup from prices table
    prices = await repo.get_prices_by_date(snapshot_date)
    price_map: dict[str, Decimal] = {}
    for p in prices:
        if p.currency == "USD":
            price_map[p.asset.upper()] = p.price

    rows = [
        AssetAllocation(
            asset=asset,
            asset_type=a_type,
            sources=tuple(sorted(sources)),
            amount=amount,
            usd_value=usd_value,
            price=price_map.get(asset.upper(), usd_value / amount if amount else Decimal(0)),
            percentage=_percentage(usd_value, total_usd),
        )
        for (asset, a_type), (amount, usd_value, sources) in by_key.items()
    ]
    rows.sort(key=lambda r: r.usd_value, reverse=True)
    return rows


async def compute_allocation_by_source(repo: Repository, snapshot_date: date) -> list[BucketAllocation]:
    """Compute per-source allocation as share of total portfolio value."""
    snapshots = [snap for snap in await repo.get_snapshots_resolved(snapshot_date) if not is_sync_marker_snapshot(snap)]
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
    snapshots = [snap for snap in await repo.get_snapshots_resolved(snapshot_date) if not is_sync_marker_snapshot(snap)]
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
    snapshots = [snap for snap in await repo.get_snapshots_resolved(snapshot_date) if not is_sync_marker_snapshot(snap)]
    total_usd = _sum_usd(snapshots)
    by_currency: dict[str, Decimal] = {}

    for snap in snapshots:
        asset = snap.asset.upper()
        if is_fiat_asset(asset):
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


_KBANK_STALE_DAYS = 3


def compute_data_warnings(
    snapshots: list[Snapshot],
    enabled_sources: list[Source],
    analysis_date: date,
) -> list[str]:
    """Generate warnings about unsynced sources and stale KBank statements."""
    source_dates: dict[str, date] = {}
    for snap in snapshots:
        source_name = snap.source_name or snap.source
        if source_name not in source_dates or snap.date > source_dates[source_name]:
            source_dates[source_name] = snap.date

    warnings: list[str] = []
    for source in enabled_sources:
        latest_date = source_dates.get(source.name)
        if latest_date is None:
            warnings.append(f"No snapshot data for source: {source.name}")
            continue

        if latest_date < analysis_date:
            warnings.append(f"Source not synced today: {source.name} (latest {latest_date.isoformat()})")

        if source.type == "kbank":
            age_days = (analysis_date - latest_date).days
            if age_days > _KBANK_STALE_DAYS:
                warnings.append(
                    f"KBank statement is outdated: {source.name} ({latest_date.isoformat()}, {age_days} days old)"
                )

    return warnings


def _sum_usd(snapshots: list[Snapshot]) -> Decimal:
    return sum((s.usd_value for s in snapshots), Decimal(0))


def _percentage(value: Decimal, total: Decimal) -> Decimal:
    if total == 0:
        return Decimal(0)
    return (value / total) * _HUNDRED


def _asset_type(source: str, asset: str) -> str:
    """Classify an asset by its source and ticker."""
    src = source.lower()
    tkr = asset.upper()
    result = "other"
    if src in _DEPOSIT_SOURCES:
        result = "deposit"
    elif src in _DEFI_SOURCES:
        result = "defi"
    elif src in _FIAT_SOURCES:
        result = "fiat"
    elif src in _STOCK_SOURCES:
        result = "fiat" if tkr in _FIAT_ASSETS else "stocks"
    elif tkr in _FIAT_ASSETS:
        result = "fiat"
    elif src in _CRYPTO_SOURCES:
        result = "crypto"
    return result


def _category_for_snapshot(snap: Snapshot) -> str:
    source = snap.source.lower()
    asset = snap.asset.upper()
    category = "crypto"

    if source in _DEPOSIT_SOURCES:
        category = "deposit"
    elif source in _DEFI_SOURCES:
        category = "DeFi"
    elif source in _FIAT_SOURCES:
        category = "fiat"
    elif source in _STOCK_SOURCES:
        category = "fiat" if asset in _FIAT_ASSETS else "stocks"
    elif asset in _FIAT_ASSETS:
        category = "fiat"
    elif source in _CRYPTO_SOURCES:
        category = "crypto"
    return category
