"""Tests for the portfolio REST endpoints."""

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
async def db_with_data(db_path):
    """Seed snapshots and analytics cache."""
    async with Repository(db_path) as repo:
        snaps = [
            Snapshot(date=date(2024, 1, 7), source="okx", asset="BTC", amount=Decimal(1), usd_value=Decimal(40000)),
            Snapshot(date=date(2024, 1, 7), source="wise", asset="USD", amount=Decimal(5000), usd_value=Decimal(5000)),
        ]
        await repo.save_snapshots(snaps)
        await repo.save_analytics_metric(
            date(2024, 1, 7),
            "net_worth",
            json.dumps({"usd": "45000"}),
        )
        await repo.save_analytics_metric(
            date(2024, 1, 7),
            "allocation_by_asset",
            json.dumps(
                [
                    {"asset": "BTC", "usd_value": "40000", "percentage": "88.89"},
                    {"asset": "USD", "usd_value": "5000", "percentage": "11.11"},
                ]
            ),
        )
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
    assert len(data["allocation"]) == 2


async def test_portfolio_summary_no_data(empty_client):
    resp = await empty_client.get("/api/v1/portfolio/summary")
    assert resp.status == 404


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
