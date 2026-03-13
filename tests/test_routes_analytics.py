"""Tests for the analytics REST endpoints."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from pfm.db.models import Snapshot, init_db
from pfm.db.repository import Repository
from pfm.db.source_store import SourceStore
from pfm.server.app import create_app


@pytest.fixture
async def db_path(tmp_path):
    path = tmp_path / "test.db"
    await init_db(path)
    return path


@pytest.fixture
async def db_with_snapshots(db_path):
    """Seed snapshots (analytics are computed live)."""
    async with Repository(db_path) as repo:
        for d in [date(2024, 1, 1), date(2024, 1, 7)]:
            await repo.save_snapshots(
                [
                    Snapshot(date=d, source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(40000)),
                ]
            )
    return db_path


@pytest.fixture
async def client(aiohttp_client, db_with_snapshots):
    app = create_app(db_with_snapshots)
    return await aiohttp_client(app)


@pytest.fixture
async def empty_client(aiohttp_client, db_path):
    app = create_app(db_path)
    return await aiohttp_client(app)


async def test_analytics_allocation(client):
    resp = await client.get("/api/v1/analytics/allocation")
    assert resp.status == 200
    data = await resp.json()
    assert len(data["by_asset"]) == 1
    assert data["by_asset"][0]["asset"] == "BTC"
    assert data["risk_metrics"]["concentration_percentage"] == "100"
    assert data["warnings"] == []


async def test_analytics_allocation_no_data(empty_client):
    resp = await empty_client.get("/api/v1/analytics/allocation")
    assert resp.status == 404


async def test_analytics_pnl_30d(db_path, aiohttp_client):
    async with Repository(db_path) as repo:
        await repo.save_snapshots(
            [
                Snapshot(date=date(2024, 1, 1), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(40000)),
                Snapshot(
                    date=date(2024, 1, 31), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(45000)
                ),
            ]
        )

    app = create_app(db_path)
    client = await aiohttp_client(app)

    resp = await client.get("/api/v1/analytics/pnl?period=30d")
    assert resp.status == 200
    data = await resp.json()
    assert data["date"] == "2024-01-31"
    assert data["period"] == "30d"
    assert data["pnl"]["start_date"] == "2024-01-01"
    assert data["pnl"]["end_date"] == "2024-01-31"
    assert data["pnl"]["absolute_change"] == "5000"
    assert data["pnl"]["percentage_change"] == "12.5"
    assert data["pnl"]["top_gainers"][0]["asset"] == "BTC"


async def test_analytics_pnl_returns_404_when_exact_start_snapshot_is_missing(db_path, aiohttp_client):
    async with Repository(db_path) as repo:
        await repo.save_snapshots(
            [
                Snapshot(date=date(2024, 1, 1), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(40000)),
                Snapshot(
                    date=date(2024, 1, 31), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(45000)
                ),
            ]
        )

    app = create_app(db_path)
    client = await aiohttp_client(app)

    resp = await client.get("/api/v1/analytics/pnl?period=1m")
    assert resp.status == 404
    data = await resp.json()
    assert "PnL is unavailable" in data["error"]


@pytest.mark.parametrize(
    ("period", "expected_start_date", "expected_absolute_change"),
    [
        ("1w", "2024-12-25", "500"),
        ("mtd", "2024-12-01", "1100"),
        ("1m", "2024-12-02", "1000"),
        ("3m", "2024-10-03", "2000"),
        ("ytd", "2024-01-01", "5000"),
        ("1y", "2024-01-02", "4900"),
        ("all", "2024-01-01", "5000"),
    ],
)
async def test_analytics_pnl_dashboard_ranges(
    db_path, aiohttp_client, period, expected_start_date, expected_absolute_change
):
    async with Repository(db_path) as repo:
        await repo.save_snapshots(
            [
                Snapshot(date=date(2024, 1, 1), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(40000)),
                Snapshot(date=date(2024, 1, 2), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(40100)),
                Snapshot(
                    date=date(2024, 10, 3), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(43000)
                ),
                Snapshot(
                    date=date(2024, 12, 1), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(43900)
                ),
                Snapshot(
                    date=date(2024, 12, 2), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(44000)
                ),
                Snapshot(
                    date=date(2024, 12, 25), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(44500)
                ),
                Snapshot(
                    date=date(2024, 12, 31), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(45000)
                ),
            ]
        )

    app = create_app(db_path)
    client = await aiohttp_client(app)

    resp = await client.get(f"/api/v1/analytics/pnl?period={period}")
    assert resp.status == 200
    data = await resp.json()

    assert data["date"] == "2024-12-31"
    assert data["period"] == period
    assert data["pnl"]["start_date"] == expected_start_date
    assert data["pnl"]["end_date"] == "2024-12-31"
    assert data["pnl"]["absolute_change"] == expected_absolute_change


async def test_analytics_pnl_no_data(empty_client):
    resp = await empty_client.get("/api/v1/analytics/pnl?period=30d")
    assert resp.status == 404


async def test_analytics_pnl_missing_period(client):
    resp = await client.get("/api/v1/analytics/pnl")
    assert resp.status == 400


async def test_analytics_pnl_invalid_period(client):
    resp = await client.get("/api/v1/analytics/pnl?period=banana")
    assert resp.status == 400


async def test_analytics_allocation_warns_about_unsynced_sources_and_outdated_kbank(db_path, aiohttp_client):
    store = SourceStore(db_path)
    await store.add("okx-main", "okx", {"api_key": "key", "api_secret": "secret", "passphrase": "pass"})
    await store.add("wise-main", "wise", {"api_token": "test-token-123456"})
    await store.add("cash-main", "cash", {"fiat_currencies": "USD"})
    await store.add(
        "kbank-main",
        "kbank",
        {"gmail_address": "user@example.com", "gmail_app_password": "app-pass", "pdf_password": "01011990"},
    )

    async with Repository(db_path) as repo:
        await repo.save_snapshots(
            [
                Snapshot(
                    date=date(2024, 1, 15),
                    source="okx",
                    source_name="okx-main",
                    asset="BTC",
                    amount=Decimal(1),
                    usd_value=Decimal(45000),
                ),
                Snapshot(
                    date=date(2024, 1, 14),
                    source="wise",
                    source_name="wise-main",
                    asset="USD",
                    amount=Decimal(250),
                    usd_value=Decimal(250),
                ),
                Snapshot(
                    date=date(2024, 1, 14),
                    source="cash",
                    source_name="cash-main",
                    asset="USD",
                    amount=Decimal(500),
                    usd_value=Decimal(500),
                ),
                Snapshot(
                    date=date(2024, 1, 10),
                    source="kbank",
                    source_name="kbank-main",
                    asset="THB",
                    amount=Decimal(1000),
                    usd_value=Decimal(28),
                ),
            ]
        )

    app = create_app(db_path)
    client = await aiohttp_client(app)

    resp = await client.get("/api/v1/analytics/allocation")
    assert resp.status == 200
    data = await resp.json()

    assert "Source not synced today: wise-main (latest 2024-01-14)" in data["warnings"]
    assert not any("cash-main" in warning for warning in data["warnings"])
    assert not any("Source not synced today: kbank-main" in warning for warning in data["warnings"])
    assert any("KBank statement is outdated" in warning for warning in data["warnings"])


async def test_analytics_exposure(client):
    resp = await client.get("/api/v1/analytics/exposure")
    assert resp.status == 200
    data = await resp.json()
    # BTC is not fiat, so no currency exposure
    assert isinstance(data["exposure"], list)


async def test_analytics_exposure_no_data(empty_client):
    resp = await empty_client.get("/api/v1/analytics/exposure")
    assert resp.status == 404


async def test_analytics_source_movers_returns_top_gainers_and_reducers(db_path, aiohttp_client):
    async with Repository(db_path) as repo:
        await repo.save_snapshots(
            [
                Snapshot(date=date(2024, 1, 6), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(100)),
                Snapshot(date=date(2024, 1, 6), source="bybit", asset="ETH", amount=Decimal(1), usd_value=Decimal(80)),
                Snapshot(date=date(2024, 1, 6), source="wise", asset="USD", amount=Decimal(1), usd_value=Decimal(60)),
                Snapshot(
                    date=date(2024, 1, 6), source="trading212", asset="VOO", amount=Decimal(1), usd_value=Decimal(40)
                ),
                Snapshot(date=date(2024, 1, 7), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(160)),
                Snapshot(date=date(2024, 1, 7), source="bybit", asset="ETH", amount=Decimal(1), usd_value=Decimal(50)),
                Snapshot(date=date(2024, 1, 7), source="wise", asset="USD", amount=Decimal(1), usd_value=Decimal(90)),
                Snapshot(date=date(2024, 1, 7), source="rabby", asset="USDC", amount=Decimal(1), usd_value=Decimal(25)),
            ]
        )

    app = create_app(db_path)
    client = await aiohttp_client(app)

    resp = await client.get("/api/v1/analytics/source-movers")
    assert resp.status == 200
    data = await resp.json()

    assert data["date"] == "2024-01-07"
    assert data["previous_date"] == "2024-01-06"
    assert data["gainers"] == [
        {
            "source": "okx",
            "absolute_change": "60",
            "current_usd_value": "160",
            "previous_usd_value": "100",
        },
        {
            "source": "wise",
            "absolute_change": "30",
            "current_usd_value": "90",
            "previous_usd_value": "60",
        },
    ]
    assert data["reducers"] == [
        {
            "source": "bybit",
            "absolute_change": "-30",
            "current_usd_value": "50",
            "previous_usd_value": "80",
        }
    ]


async def test_analytics_source_movers_carries_forward_stale_sources(db_path, aiohttp_client):
    async with Repository(db_path) as repo:
        await repo.save_snapshots(
            [
                Snapshot(date=date(2024, 1, 5), source="bybit", asset="ETH", amount=Decimal(1), usd_value=Decimal(50)),
                Snapshot(date=date(2024, 1, 6), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(100)),
                Snapshot(date=date(2024, 1, 7), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(140)),
            ]
        )

    app = create_app(db_path)
    client = await aiohttp_client(app)

    resp = await client.get("/api/v1/analytics/source-movers")
    assert resp.status == 200
    data = await resp.json()

    assert data["gainers"] == [
        {
            "source": "okx",
            "absolute_change": "40",
            "current_usd_value": "140",
            "previous_usd_value": "100",
        }
    ]
    assert data["reducers"] == []


async def test_analytics_source_movers_without_previous_day_returns_empty_lists(db_path, aiohttp_client):
    async with Repository(db_path) as repo:
        await repo.save_snapshots(
            [
                Snapshot(date=date(2024, 1, 7), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(100)),
            ]
        )

    app = create_app(db_path)
    client = await aiohttp_client(app)

    resp = await client.get("/api/v1/analytics/source-movers")
    assert resp.status == 200
    data = await resp.json()

    assert data["date"] == "2024-01-07"
    assert data["previous_date"] is None
    assert data["gainers"] == []
    assert data["reducers"] == []


async def test_analytics_source_movers_no_data(empty_client):
    resp = await empty_client.get("/api/v1/analytics/source-movers")
    assert resp.status == 404


async def test_analytics_yield_missing_params(client):
    resp = await client.get("/api/v1/analytics/yield")
    assert resp.status == 400
