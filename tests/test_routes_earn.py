"""Tests for the earn REST endpoints."""

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
async def db_with_data(db_path):
    """Seed one earning and one non-earning snapshot."""
    async with Repository(db_path) as repo:
        snaps = [
            Snapshot(
                date=date(2024, 1, 7),
                source="okx",
                asset="USDT",
                amount=Decimal(10000),
                usd_value=Decimal(10000),
                price=Decimal(1),
                apy=Decimal("0.1049"),
            ),
            Snapshot(
                date=date(2024, 1, 7),
                source="wise",
                asset="USD",
                amount=Decimal(5000),
                usd_value=Decimal(5000),
                price=Decimal(1),
                apy=Decimal(0),
            ),
        ]
        await repo.save_snapshots(snaps)
    return db_path


@pytest.fixture
async def db_no_earning(db_path):
    """Seed snapshots with apy=0 only."""
    async with Repository(db_path) as repo:
        snaps = [
            Snapshot(
                date=date(2024, 1, 7),
                source="wise",
                asset="USD",
                amount=Decimal(5000),
                usd_value=Decimal(5000),
                price=Decimal(1),
                apy=Decimal(0),
            ),
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


@pytest.fixture
async def no_earning_client(aiohttp_client, db_no_earning):
    app = create_app(db_no_earning)
    return await aiohttp_client(app)


async def test_earn_summary(client):
    resp = await client.get("/api/v1/earn/summary")
    assert resp.status == 200
    data = await resp.json()
    assert data["date"] == "2024-01-07"
    assert data["total_usd_value"] == "10000"
    assert data["weighted_avg_apy"] == "0.1049"
    assert len(data["positions"]) == 1
    pos = data["positions"][0]
    assert pos["source"] == "okx"
    assert pos["asset"] == "USDT"
    assert pos["asset_type"] == "crypto"
    assert pos["apy"] == "0.1049"


async def test_earn_summary_no_data(empty_client):
    resp = await empty_client.get("/api/v1/earn/summary")
    assert resp.status == 404


async def test_earn_summary_no_earning(no_earning_client):
    resp = await no_earning_client.get("/api/v1/earn/summary")
    assert resp.status == 200
    data = await resp.json()
    assert data["date"] == "2024-01-07"
    assert data["total_usd_value"] == "0"
    assert data["weighted_avg_apy"] == "0"
    assert data["positions"] == []
