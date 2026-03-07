"""Tests for portfolio analytics."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pfm.analytics.portfolio import (
    compute_allocation_by_asset,
    compute_allocation_by_category,
    compute_allocation_by_source,
    compute_currency_exposure,
    compute_data_warnings,
    compute_net_worth,
    compute_risk_metrics,
)
from pfm.db.models import Price, Snapshot


async def test_compute_net_worth(repo):
    target_date = date(2024, 1, 15)
    await repo.save_snapshots(
        [
            Snapshot(target_date, "wise", "GBP", Decimal(100), Decimal(125)),
            Snapshot(target_date, "lobstr", "XLM", Decimal(50), Decimal(5)),
            Snapshot(target_date, "ibkr", "AAPL", Decimal(1), Decimal(180)),
        ]
    )

    net_worth = await compute_net_worth(repo, target_date)
    assert net_worth == Decimal(310)


async def test_compute_allocation_by_asset(repo):
    target_date = date(2024, 1, 15)
    await repo.save_snapshots(
        [
            Snapshot(target_date, "lobstr", "USDC", Decimal(100), Decimal(100)),
            Snapshot(target_date, "okx", "USDC", Decimal(50), Decimal(50)),
            Snapshot(target_date, "wise", "GBP", Decimal(200), Decimal(250)),
        ]
    )

    rows = await compute_allocation_by_asset(repo, target_date)
    # Grouped by (asset, asset_type); USDC(crypto) aggregated across lobstr+okx
    assert [(row.asset, row.asset_type) for row in rows] == [("GBP", "fiat"), ("USDC", "crypto")]
    assert rows[0].usd_value == Decimal(250)
    assert rows[0].sources == ("wise",)
    assert rows[1].amount == Decimal(150)
    assert rows[1].sources == ("lobstr", "okx")
    assert rows[0].percentage == Decimal("62.5")
    assert rows[1].percentage == Decimal("37.5")
    # No prices seeded — falls back to usd_value / amount
    assert rows[0].price == Decimal("1.25")  # 250 / 200
    assert rows[1].price == Decimal(1)  # 150 / 150


async def test_compute_allocation_by_asset_with_prices(repo):
    target_date = date(2024, 1, 15)
    await repo.save_snapshots(
        [
            Snapshot(target_date, "okx", "BTC", Decimal("0.5"), Decimal(32500)),
            Snapshot(target_date, "wise", "GBP", Decimal(1000), Decimal(1260)),
        ]
    )
    await repo.save_prices(
        [
            Price(target_date, "BTC", "USD", Decimal(65000)),
            Price(target_date, "GBP", "USD", Decimal("1.26")),
        ]
    )

    rows = await compute_allocation_by_asset(repo, target_date)
    assert rows[0].asset == "BTC"
    assert rows[0].price == Decimal(65000)
    assert rows[1].asset == "GBP"
    assert rows[1].price == Decimal("1.26")


async def test_compute_allocation_by_source(repo):
    target_date = date(2024, 1, 15)
    await repo.save_snapshots(
        [
            Snapshot(target_date, "wise", "GBP", Decimal(100), Decimal(125)),
            Snapshot(target_date, "wise", "EUR", Decimal(50), Decimal(55)),
            Snapshot(target_date, "lobstr", "XLM", Decimal(50), Decimal(5)),
        ]
    )

    rows = await compute_allocation_by_source(repo, target_date)
    assert [row.bucket for row in rows] == ["wise", "lobstr"]
    assert rows[0].usd_value == Decimal(180)
    assert rows[1].percentage == Decimal("2.702702702702702702702702703")


async def test_compute_allocation_by_category(repo):
    target_date = date(2024, 1, 15)
    await repo.save_snapshots(
        [
            Snapshot(target_date, "wise", "GBP", Decimal(100), Decimal(125)),
            Snapshot(target_date, "ibkr", "AAPL", Decimal(1), Decimal(200)),
            Snapshot(target_date, "ibkr", "USD", Decimal(20), Decimal(20)),
            Snapshot(target_date, "blend", "USDC", Decimal(300), Decimal(300)),
            Snapshot(target_date, "okx", "BTC", Decimal("0.1"), Decimal(5000)),
        ]
    )

    rows = await compute_allocation_by_category(repo, target_date)
    assert [row.bucket for row in rows] == ["crypto", "DeFi", "stocks", "fiat"]
    by_bucket = {row.bucket: row.usd_value for row in rows}
    assert by_bucket["crypto"] == Decimal(5000)
    assert by_bucket["DeFi"] == Decimal(300)
    assert by_bucket["stocks"] == Decimal(200)
    assert by_bucket["fiat"] == Decimal(145)


async def test_compute_currency_exposure(repo):
    target_date = date(2024, 1, 15)
    await repo.save_snapshots(
        [
            Snapshot(target_date, "wise", "GBP", Decimal(100), Decimal(125)),
            Snapshot(target_date, "kbank", "THB", Decimal(3000), Decimal(90)),
            Snapshot(target_date, "ibkr", "AAPL", Decimal(1), Decimal(200)),
        ]
    )

    rows = await compute_currency_exposure(repo, target_date)
    assert [row.currency for row in rows] == ["GBP", "THB"]
    assert rows[0].percentage == Decimal("30.12048192771084337349397590")
    assert rows[1].percentage == Decimal("21.68674698795180722891566265")


async def test_compute_risk_metrics(repo):
    target_date = date(2024, 1, 15)
    await repo.save_snapshots(
        [
            Snapshot(target_date, "okx", "BTC", Decimal("0.1"), Decimal(5000)),
            Snapshot(target_date, "wise", "GBP", Decimal(100), Decimal(125)),
            Snapshot(target_date, "ibkr", "AAPL", Decimal(1), Decimal(200)),
            Snapshot(target_date, "lobstr", "USDC", Decimal(300), Decimal(300)),
            Snapshot(target_date, "blend", "USDC", Decimal(400), Decimal(400)),
            Snapshot(target_date, "lobstr", "XLM", Decimal(50), Decimal(5)),
        ]
    )

    metrics = await compute_risk_metrics(repo, target_date)
    # 6 rows: BTC/crypto, USDC/defi, USDC/crypto, AAPL/stocks, GBP/fiat, XLM/crypto
    assert metrics.concentration_percentage == Decimal("82.91873963515754560530679934")
    assert len(metrics.top_5_assets) == 5
    assert metrics.top_5_assets[0].asset == "BTC"
    assert metrics.top_5_assets[0].sources == ("okx",)
    assert metrics.top_5_assets[0].asset_type == "crypto"
    total = Decimal(6030)
    hhi = sum(
        (v / total) ** 2 for v in [Decimal(5000), Decimal(400), Decimal(300), Decimal(200), Decimal(125), Decimal(5)]
    )
    assert metrics.hhi_index == hhi


async def test_empty_portfolio(repo):
    target_date = date(2024, 1, 15)

    assert await compute_net_worth(repo, target_date) == Decimal(0)
    assert await compute_allocation_by_asset(repo, target_date) == []
    assert await compute_allocation_by_source(repo, target_date) == []
    assert await compute_allocation_by_category(repo, target_date) == []
    assert await compute_currency_exposure(repo, target_date) == []
    metrics = await compute_risk_metrics(repo, target_date)
    assert metrics.concentration_percentage == Decimal(0)
    assert metrics.top_5_assets == []
    assert metrics.hhi_index == Decimal(0)


async def test_compute_allocation_by_category_unknown_source_fallbacks(repo):
    target_date = date(2024, 1, 15)
    await repo.save_snapshots(
        [
            Snapshot(target_date, "manual", "EUR", Decimal(100), Decimal(110)),
            Snapshot(target_date, "manual", "GOLD", Decimal(1), Decimal(90)),
        ]
    )

    rows = await compute_allocation_by_category(repo, target_date)
    by_bucket = {row.bucket: row.usd_value for row in rows}
    assert by_bucket["fiat"] == Decimal(110)
    # Unknown non-fiat assets currently map to crypto fallback bucket.
    assert by_bucket["crypto"] == Decimal(90)


# ── Data Warnings ────────────────────────────────────────────────────


def test_data_warnings_missing_source():
    snapshots = [
        Snapshot(date(2024, 1, 15), "okx", "BTC", Decimal(1), Decimal(45000)),
    ]
    enabled = {"okx", "kbank", "wise"}
    warnings = compute_data_warnings(snapshots, enabled, date(2024, 1, 15))
    assert "No snapshot data for source: kbank" in warnings
    assert "No snapshot data for source: wise" in warnings
    assert not any("okx" in w for w in warnings)


def test_data_warnings_kbank_outdated():
    snapshots = [
        Snapshot(date(2024, 1, 15), "okx", "BTC", Decimal(1), Decimal(45000)),
        Snapshot(date(2023, 12, 1), "kbank", "THB", Decimal(1000), Decimal(28)),
    ]
    enabled = {"okx", "kbank"}
    warnings = compute_data_warnings(snapshots, enabled, date(2024, 1, 15))
    assert any("KBank statement is outdated" in w for w in warnings)


def test_data_warnings_kbank_fresh():
    snapshots = [
        Snapshot(date(2024, 1, 15), "okx", "BTC", Decimal(1), Decimal(45000)),
        Snapshot(date(2024, 1, 14), "kbank", "THB", Decimal(1000), Decimal(28)),
    ]
    enabled = {"okx", "kbank"}
    warnings = compute_data_warnings(snapshots, enabled, date(2024, 1, 15))
    assert not any("KBank" in w for w in warnings)


def test_data_warnings_no_issues():
    snapshots = [
        Snapshot(date(2024, 1, 15), "okx", "BTC", Decimal(1), Decimal(45000)),
        Snapshot(date(2024, 1, 15), "wise", "GBP", Decimal(100), Decimal(125)),
    ]
    enabled = {"okx", "wise"}
    warnings = compute_data_warnings(snapshots, enabled, date(2024, 1, 15))
    assert warnings == []


def test_data_warnings_no_enabled_sources():
    """When no enabled sources are configured, skip missing-source check."""
    snapshots = [
        Snapshot(date(2024, 1, 15), "okx", "BTC", Decimal(1), Decimal(45000)),
    ]
    warnings = compute_data_warnings(snapshots, set(), date(2024, 1, 15))
    assert warnings == []
