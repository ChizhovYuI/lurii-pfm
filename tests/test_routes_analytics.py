"""Tests for the analytics REST endpoints."""

from __future__ import annotations

import json
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
async def db_with_analytics(db_path):
    """Seed snapshots and cached analytics."""
    async with Repository(db_path) as repo:
        # Two dates worth of snapshots for PnL
        for d in [date(2024, 1, 1), date(2024, 1, 7)]:
            await repo.save_snapshots(
                [
                    Snapshot(date=d, source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(40000)),
                ]
            )
        await repo.save_analytics_metric(
            date(2024, 1, 7),
            "pnl",
            json.dumps(
                {
                    "weekly": {
                        "start_date": "2024-01-01",
                        "end_date": "2024-01-07",
                        "absolute_change": "500",
                        "percentage_change": "1.25",
                    },
                }
            ),
        )
        await repo.save_analytics_metric(
            date(2024, 1, 7),
            "allocation_by_asset",
            json.dumps([{"asset": "BTC", "usd_value": "40000", "percentage": "100"}]),
        )
        await repo.save_analytics_metric(
            date(2024, 1, 7),
            "allocation_by_source",
            json.dumps([{"source": "okx", "usd_value": "40000", "percentage": "100"}]),
        )
        await repo.save_analytics_metric(
            date(2024, 1, 7),
            "allocation_by_category",
            json.dumps([{"category": "crypto", "usd_value": "40000", "percentage": "100"}]),
        )
        await repo.save_analytics_metric(
            date(2024, 1, 7),
            "currency_exposure",
            json.dumps([{"currency": "USD", "usd_value": "40000", "percentage": "100"}]),
        )
    return db_path


@pytest.fixture
async def client(aiohttp_client, db_with_analytics):
    app = create_app(db_with_analytics)
    return await aiohttp_client(app)


@pytest.fixture
async def empty_client(aiohttp_client, db_path):
    app = create_app(db_path)
    return await aiohttp_client(app)


async def test_analytics_pnl_cached(client):
    resp = await client.get("/api/v1/analytics/pnl?period=weekly")
    assert resp.status == 200
    data = await resp.json()
    assert data["period"] == "weekly"
    assert data["pnl"]["absolute_change"] == "500"


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
    assert len(data["exposure"]) == 1


async def test_analytics_exposure_no_data(empty_client):
    resp = await empty_client.get("/api/v1/analytics/exposure")
    assert resp.status == 404


async def test_analytics_yield_missing_params(client):
    resp = await client.get("/api/v1/analytics/yield")
    assert resp.status == 400
