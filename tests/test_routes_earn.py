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
    assert isinstance(pos["id"], int)
    assert pos["source"] == "okx"
    assert pos["asset"] == "USDT"
    assert pos["asset_type"] == "crypto"
    assert pos["apy"] == "0.1049"


async def test_earn_summary_preserves_distinct_rows_for_same_source_and_asset(db_path, aiohttp_client):
    async with Repository(db_path) as repo:
        snaps = [
            Snapshot(
                date=date(2024, 1, 7),
                source="mexc_earn",
                source_name="mexc-earn-main",
                asset="USDC",
                amount=Decimal("262.79260253"),
                usd_value=Decimal("262.79260253"),
                price=Decimal(1),
                apy=Decimal("0.12"),
            ),
            Snapshot(
                date=date(2024, 1, 7),
                source="mexc_earn",
                source_name="mexc-earn-main",
                asset="USDC",
                amount=Decimal("5039.448835"),
                usd_value=Decimal("5039.448835"),
                price=Decimal(1),
                apy=Decimal("0.15"),
            ),
            Snapshot(
                date=date(2024, 1, 7),
                source="mexc_earn",
                source_name="mexc-earn-main",
                asset="USDT",
                amount=Decimal("300.0064994"),
                usd_value=Decimal("300.0064994"),
                price=Decimal(1),
                apy=Decimal("0.15"),
            ),
        ]
        await repo.save_snapshots(snaps)

    app = create_app(db_path)
    client = await aiohttp_client(app)

    resp = await client.get("/api/v1/earn/summary")
    assert resp.status == 200
    data = await resp.json()

    positions = data["positions"]
    usdc_positions = [pos for pos in positions if pos["source"] == "mexc_earn" and pos["asset"] == "USDC"]
    assert len(usdc_positions) == 2
    assert {pos["amount"] for pos in usdc_positions} == {"262.79260253", "5039.448835"}
    assert len({pos["id"] for pos in usdc_positions}) == 2


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


async def test_earn_summary_includes_coinex_financial_when_apy_zero(db_path, aiohttp_client):
    async with Repository(db_path) as repo:
        snaps = [
            Snapshot(
                date=date(2024, 1, 7),
                source="coinex",
                source_name="coinex",
                asset="USDT",
                amount=Decimal(1000),
                usd_value=Decimal(1000),
                price=Decimal(1),
                apy=Decimal(0),
                raw_json='{"account_type":"financial","row":{"ccy":"USDT"}}',
            ),
            Snapshot(
                date=date(2024, 1, 7),
                source="coinex",
                source_name="coinex",
                asset="USDC",
                amount=Decimal(500),
                usd_value=Decimal(500),
                price=Decimal(1),
                apy=Decimal(0),
                raw_json='{"account_type":"spot","row":{"ccy":"USDC"}}',
            ),
        ]
        await repo.save_snapshots(snaps)

    app = create_app(db_path)
    client = await aiohttp_client(app)
    resp = await client.get("/api/v1/earn/summary")
    assert resp.status == 200
    data = await resp.json()
    assert data["total_usd_value"] == "1000"
    assert data["weighted_avg_apy"] == "0"
    assert len(data["positions"]) == 1
    assert data["positions"][0]["source"] == "coinex"
    assert data["positions"][0]["asset"] == "USDT"


async def test_earn_summary_exposes_settlement_and_category(db_path, aiohttp_client):
    """Positions with settlement_at and category in raw_json expose them in the API."""
    import json

    async with Repository(db_path) as repo:
        snaps = [
            Snapshot(
                date=date(2024, 1, 7),
                source="bybit",
                source_name="bybit-main",
                asset="USDT",
                amount=Decimal("1053.6253"),
                usd_value=Decimal("1053.6253"),
                price=Decimal(1),
                apy=Decimal("1.7936"),
                raw_json=json.dumps(
                    {
                        "account_type": "earn",
                        "category": "Dual Asset",
                        "settlement_at": "2026-03-28T07:59:00Z",
                        "row": {"coin": "USDT", "equity": "1053.6253"},
                    }
                ),
            ),
        ]
        await repo.save_snapshots(snaps)

    app = create_app(db_path)
    client = await aiohttp_client(app)

    resp = await client.get("/api/v1/earn/summary")
    assert resp.status == 200
    data = await resp.json()
    pos = data["positions"][0]
    assert pos["settlement_at"] == "2026-03-28T07:59:00Z"
    assert pos["earn_category"] == "Dual Asset"


async def test_earn_history_returns_daily_points_and_latest_anchor(db_path, aiohttp_client):
    async with Repository(db_path) as repo:
        await repo.save_snapshots(
            [
                Snapshot(
                    date=date(2024, 1, 5),
                    source="okx",
                    asset="USDT",
                    amount=Decimal(10000),
                    usd_value=Decimal(10000),
                    price=Decimal(1),
                    apy=Decimal("0.10"),
                ),
                Snapshot(
                    date=date(2024, 1, 7),
                    source="okx",
                    asset="USDT",
                    amount=Decimal(12000),
                    usd_value=Decimal(12000),
                    price=Decimal(1),
                    apy=Decimal("0.12"),
                ),
            ]
        )

    app = create_app(db_path)
    client = await aiohttp_client(app)

    resp = await client.get("/api/v1/earn/history?days=3")
    assert resp.status == 200

    data = await resp.json()
    assert data["start_date"] == "2024-01-05"
    assert data["end_date"] == "2024-01-07"
    assert data["points"] == [
        {"date": "2024-01-05", "total_usd_value": "10000", "weighted_avg_apy": "0.1"},
        {"date": "2024-01-06", "total_usd_value": "10000", "weighted_avg_apy": "0.1"},
        {"date": "2024-01-07", "total_usd_value": "12000", "weighted_avg_apy": "0.12"},
    ]


async def test_earn_history_carries_forward_resolved_positions_and_computes_weighted_apy(db_path, aiohttp_client):
    async with Repository(db_path) as repo:
        await repo.save_snapshots(
            [
                Snapshot(
                    date=date(2024, 1, 5),
                    source="okx",
                    asset="USDT",
                    amount=Decimal(100),
                    usd_value=Decimal(100),
                    price=Decimal(1),
                    apy=Decimal("0.10"),
                ),
                Snapshot(
                    date=date(2024, 1, 7),
                    source="aave",
                    asset="USDC",
                    amount=Decimal(50),
                    usd_value=Decimal(50),
                    price=Decimal(1),
                    apy=Decimal("0.20"),
                ),
            ]
        )

    app = create_app(db_path)
    client = await aiohttp_client(app)

    resp = await client.get("/api/v1/earn/history?days=1")
    assert resp.status == 200

    data = await resp.json()
    assert data["start_date"] == "2024-01-07"
    assert data["end_date"] == "2024-01-07"
    assert data["points"] == [
        {
            "date": "2024-01-07",
            "total_usd_value": "150",
            "weighted_avg_apy": "0.1333333333333333333333333333",
        }
    ]


async def test_earn_history_returns_partial_range_when_less_than_requested_history_exists(client):
    resp = await client.get("/api/v1/earn/history?days=30")
    assert resp.status == 200

    data = await resp.json()
    assert data["start_date"] == "2024-01-07"
    assert data["end_date"] == "2024-01-07"
    assert data["points"] == [{"date": "2024-01-07", "total_usd_value": "10000", "weighted_avg_apy": "0.1049"}]


async def test_earn_history_returns_zero_for_days_without_earn_positions(db_path, aiohttp_client):
    async with Repository(db_path) as repo:
        await repo.save_snapshots(
            [
                Snapshot(
                    date=date(2024, 1, 5),
                    source="wise",
                    asset="USD",
                    amount=Decimal(5000),
                    usd_value=Decimal(5000),
                    price=Decimal(1),
                    apy=Decimal(0),
                ),
                Snapshot(
                    date=date(2024, 1, 7),
                    source="okx",
                    asset="USDT",
                    amount=Decimal(1000),
                    usd_value=Decimal(1000),
                    price=Decimal(1),
                    apy=Decimal("0.08"),
                ),
            ]
        )

    app = create_app(db_path)
    client = await aiohttp_client(app)

    resp = await client.get("/api/v1/earn/history?days=3")
    assert resp.status == 200

    data = await resp.json()
    assert data["points"] == [
        {"date": "2024-01-05", "total_usd_value": "0", "weighted_avg_apy": "0"},
        {"date": "2024-01-06", "total_usd_value": "0", "weighted_avg_apy": "0"},
        {"date": "2024-01-07", "total_usd_value": "1000", "weighted_avg_apy": "0.08"},
    ]


async def test_earn_history_no_data(empty_client):
    resp = await empty_client.get("/api/v1/earn/history?days=30")
    assert resp.status == 404


@pytest.mark.parametrize("days_value", ["0", "-3", "abc"])
async def test_earn_history_rejects_invalid_days(client, days_value):
    resp = await client.get(f"/api/v1/earn/history?days={days_value}")
    assert resp.status == 400
