"""Tests for the portfolio REST endpoints."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from pfm.db.models import Snapshot, init_db, make_sync_marker_snapshot
from pfm.db.repository import Repository
from pfm.db.source_store import SourceStore
from pfm.server.app import create_app


@pytest.fixture
async def db_path(tmp_path):
    path = tmp_path / "test.db"
    await init_db(path)
    return path


@pytest.fixture
async def db_with_data(db_path):
    """Seed snapshots (analytics computed live)."""
    async with Repository(db_path) as repo:
        snaps = [
            Snapshot(date=date(2024, 1, 7), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(40000)),
            Snapshot(date=date(2024, 1, 7), source="wise", asset="USD", amount=Decimal(5000), usd_value=Decimal(5000)),
        ]
        await repo.save_snapshots(snaps)
    return db_path


@pytest.fixture
async def client(aiohttp_client, db_with_data):
    app = create_app(db_with_data)
    return await aiohttp_client(app)


@pytest.fixture
async def empty_client(aiohttp_client, db_path):
    app = create_app(db_path)
    return await aiohttp_client(app)


async def test_portfolio_summary(client):
    resp = await client.get("/api/v1/portfolio/summary")
    assert resp.status == 200
    data = await resp.json()
    assert data["date"] == "2024-01-07"
    assert data["net_worth"]["usd"] == "45000"
    assert len(data["holdings"]) == 2
    sources = {row["source"] for row in data["holdings"]}
    assert sources == {"okx", "wise"}
    # All holdings should include apy field
    for holding in data["holdings"]:
        assert "apy" in holding
        assert holding["apy"] == "0"


async def test_portfolio_summary_no_data(empty_client):
    resp = await empty_client.get("/api/v1/portfolio/summary")
    assert resp.status == 404


async def test_portfolio_summary_warns_about_unsynced_sources(db_path, aiohttp_client):
    store = SourceStore(db_path)
    await store.add("okx-main", "okx", {"api_key": "key", "api_secret": "secret", "passphrase": "pass"})
    await store.add("cash-main", "cash", {"fiat_currencies": "USD"})

    async with Repository(db_path) as repo:
        await repo.save_snapshots(
            [
                Snapshot(
                    date=date(2024, 1, 7),
                    source="okx",
                    source_name="okx-main",
                    asset="BTC",
                    amount=Decimal(1),
                    usd_value=Decimal(40000),
                ),
                Snapshot(
                    date=date(2024, 1, 6),
                    source="cash",
                    source_name="cash-main",
                    asset="USD",
                    amount=Decimal(100),
                    usd_value=Decimal(100),
                    price=Decimal(1),
                ),
            ]
        )

    app = create_app(db_path)
    client = await aiohttp_client(app)

    resp = await client.get("/api/v1/portfolio/summary")
    assert resp.status == 200
    data = await resp.json()
    assert "Source not synced today: cash-main (latest 2024-01-06)" in data["warnings"]


async def test_portfolio_summary_hides_sync_marker_holdings_but_preserves_freshness(db_path, aiohttp_client):
    store = SourceStore(db_path)
    await store.add("wise-main", "wise", {"api_token": "test-token-123456"})

    async with Repository(db_path) as repo:
        await repo.save_snapshots(
            [
                make_sync_marker_snapshot(
                    snapshot_date=date(2024, 1, 7),
                    source="wise",
                    source_name="wise-main",
                )
            ]
        )

    app = create_app(db_path)
    client = await aiohttp_client(app)

    resp = await client.get("/api/v1/portfolio/summary")
    assert resp.status == 200
    data = await resp.json()
    assert data["date"] == "2024-01-07"
    assert data["net_worth"]["usd"] == "0"
    assert data["holdings"] == []
    assert data["warnings"] == []


async def test_portfolio_net_worth_history_returns_daily_points(db_path, aiohttp_client):
    async with Repository(db_path) as repo:
        await repo.save_snapshots(
            [
                Snapshot(
                    date=date(2024, 1, 5), source="wise", asset="USD", amount=Decimal(1000), usd_value=Decimal(1000)
                ),
                Snapshot(
                    date=date(2024, 1, 6), source="wise", asset="USD", amount=Decimal(1200), usd_value=Decimal(1200)
                ),
                Snapshot(
                    date=date(2024, 1, 7), source="wise", asset="USD", amount=Decimal(1300), usd_value=Decimal(1300)
                ),
            ]
        )

    app = create_app(db_path)
    client = await aiohttp_client(app)

    history_resp = await client.get("/api/v1/portfolio/net-worth-history?days=30")
    assert history_resp.status == 200
    history = await history_resp.json()

    summary_resp = await client.get("/api/v1/portfolio/summary")
    assert summary_resp.status == 200
    summary = await summary_resp.json()

    assert history["start_date"] == "2024-01-05"
    assert history["end_date"] == summary["date"] == "2024-01-07"
    assert history["currency"] == "usd"
    assert history["points"] == [
        {"date": "2024-01-05", "usd_value": "1000"},
        {"date": "2024-01-06", "usd_value": "1200"},
        {"date": "2024-01-07", "usd_value": "1300"},
    ]


async def test_portfolio_net_worth_history_carries_forward_stale_sources(db_path, aiohttp_client):
    async with Repository(db_path) as repo:
        await repo.save_snapshots(
            [
                Snapshot(
                    date=date(2024, 1, 1), source="wise", asset="USD", amount=Decimal(100), usd_value=Decimal(100)
                ),
                Snapshot(
                    date=date(2024, 1, 3), source="okx", asset="BTC", amount=Decimal("0.01"), usd_value=Decimal(400)
                ),
            ]
        )

    app = create_app(db_path)
    client = await aiohttp_client(app)

    resp = await client.get("/api/v1/portfolio/net-worth-history?days=3")
    assert resp.status == 200
    data = await resp.json()

    assert data["points"] == [
        {"date": "2024-01-01", "usd_value": "100"},
        {"date": "2024-01-02", "usd_value": "100"},
        {"date": "2024-01-03", "usd_value": "500"},
    ]


async def test_portfolio_net_worth_history_returns_partial_window_for_short_history(db_path, aiohttp_client):
    async with Repository(db_path) as repo:
        await repo.save_snapshots(
            [
                Snapshot(
                    date=date(2024, 1, 6), source="wise", asset="USD", amount=Decimal(900), usd_value=Decimal(900)
                ),
                Snapshot(
                    date=date(2024, 1, 7), source="wise", asset="USD", amount=Decimal(1000), usd_value=Decimal(1000)
                ),
            ]
        )

    app = create_app(db_path)
    client = await aiohttp_client(app)

    resp = await client.get("/api/v1/portfolio/net-worth-history?days=30")
    assert resp.status == 200
    data = await resp.json()

    assert data["start_date"] == "2024-01-06"
    assert data["end_date"] == "2024-01-07"
    assert data["points"] == [
        {"date": "2024-01-06", "usd_value": "900"},
        {"date": "2024-01-07", "usd_value": "1000"},
    ]


async def test_portfolio_net_worth_history_no_data(empty_client):
    resp = await empty_client.get("/api/v1/portfolio/net-worth-history?days=30")
    assert resp.status == 404


@pytest.mark.parametrize("days_value", ["0", "-5", "abc"])
async def test_portfolio_net_worth_history_invalid_days(client, days_value):
    resp = await client.get(f"/api/v1/portfolio/net-worth-history?days={days_value}")
    assert resp.status == 400
    data = await resp.json()
    assert "positive integer" in data["error"]


async def test_portfolio_snapshots(client):
    resp = await client.get("/api/v1/portfolio/snapshots?start=2024-01-01&end=2024-01-31")
    assert resp.status == 200
    data = await resp.json()
    assert len(data) == 2
    assert data[0]["asset"] in ("BTC", "USD")


async def test_portfolio_snapshots_missing_params(client):
    resp = await client.get("/api/v1/portfolio/snapshots")
    assert resp.status == 400


async def test_portfolio_holdings(client):
    resp = await client.get("/api/v1/portfolio/holdings")
    assert resp.status == 200
    data = await resp.json()
    assert data["date"] == "2024-01-07"
    assert len(data["holdings"]) == 2


async def test_portfolio_holdings_empty(empty_client):
    resp = await empty_client.get("/api/v1/portfolio/holdings")
    assert resp.status == 404


async def test_portfolio_summary_uses_last_filled_cash_snapshot(db_path, aiohttp_client):
    store = SourceStore(db_path)
    await store.add("cash", "cash", {"fiat_currencies": "USD"})

    async with Repository(db_path) as repo:
        await repo.save_snapshots(
            [
                Snapshot(
                    date=date(2024, 1, 6),
                    source="cash",
                    source_name="cash",
                    asset="USD",
                    amount=Decimal(100),
                    usd_value=Decimal(100),
                    price=Decimal(1),
                ),
                Snapshot(
                    date=date(2024, 1, 7),
                    source="okx",
                    asset="BTC",
                    amount=Decimal(1),
                    usd_value=Decimal(40000),
                ),
            ]
        )

    app = create_app(db_path)
    client = await aiohttp_client(app)

    resp = await client.get("/api/v1/portfolio/summary")
    assert resp.status == 200
    data = await resp.json()
    assert data["date"] == "2024-01-07"
    assert data["net_worth"]["usd"] == "40100"
    assert any(
        row["source"] == "cash" and row["asset"] == "USD" and row["usd_value"] == "100" for row in data["holdings"]
    )
