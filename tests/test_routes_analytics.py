"""Tests for the analytics REST endpoints."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from pfm.db.models import Snapshot, init_db
from pfm.db.repository import Repository
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


async def test_analytics_pnl_live(client):
    resp = await client.get("/api/v1/analytics/pnl?period=weekly")
    assert resp.status == 200
    data = await resp.json()
    assert data["period"] == "weekly"
    assert "absolute_change" in data["pnl"]


async def test_analytics_pnl_no_data(empty_client):
    resp = await empty_client.get("/api/v1/analytics/pnl")
    assert resp.status == 404


async def test_analytics_allocation(client):
    resp = await client.get("/api/v1/analytics/allocation")
    assert resp.status == 200
    data = await resp.json()
    assert len(data["by_asset"]) == 1
    assert data["by_asset"][0]["asset"] == "BTC"


async def test_analytics_allocation_no_data(empty_client):
    resp = await empty_client.get("/api/v1/analytics/allocation")
    assert resp.status == 404


async def test_analytics_exposure(client):
    resp = await client.get("/api/v1/analytics/exposure")
    assert resp.status == 200
    data = await resp.json()
    # BTC is not fiat, so no currency exposure
    assert isinstance(data["exposure"], list)


async def test_analytics_exposure_no_data(empty_client):
    resp = await empty_client.get("/api/v1/analytics/exposure")
    assert resp.status == 404


async def test_analytics_yield_missing_params(client):
    resp = await client.get("/api/v1/analytics/yield")
    assert resp.status == 400
