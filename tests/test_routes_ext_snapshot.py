"""Tests for extension snapshot ingest endpoint."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from pfm.db.models import init_db
from pfm.db.repository import Repository
from pfm.db.source_store import SourceStore
from pfm.server.app import create_app


@pytest.fixture
async def db_path(tmp_path):
    path = tmp_path / "test.db"
    await init_db(path)
    return path


@pytest.fixture
async def client(aiohttp_client, db_path):
    app = create_app(db_path)
    return await aiohttp_client(app)


def _payload() -> dict[str, object]:
    return {
        "schemaVersion": "lurii.extension.snapshot.v1",
        "capturedAt": "2026-03-03T11:36:53.000Z",
        "source": {"type": "mexc_earn", "uid": "65064080"},
        "snapshot": {
            "assets": [
                {
                    "symbol": "USDC",
                    "amount": 300,
                    "usdValue": 299.96,
                    "quotedAprPercent": 10,
                }
            ]
        },
    }


async def test_ext_snapshot_ingest_saves_rows(client, db_path):
    store = SourceStore(db_path)
    await store.add("mexc-earn-main", "mexc_earn", {"uid": "65064080"})

    resp = await client.post("/api/v1/ext/snapshot?source_type=mexc_earn&uid=65064080", json=_payload())
    assert resp.status == 200
    data = await resp.json()
    assert data["saved"] == 1
    assert data["source_name"] == "mexc-earn-main"
    assert data["source_type"] == "mexc_earn"
    assert data["uid"] == "65064080"

    async with Repository(db_path) as repo:
        snapshots = await repo.get_snapshots_by_date(date(2026, 3, 3))

    assert len(snapshots) == 1
    snap = snapshots[0]
    assert snap.source == "mexc_earn"
    assert snap.source_name == "mexc-earn-main"
    assert snap.asset == "USDC"
    assert snap.amount == Decimal(300)
    assert snap.usd_value == Decimal("299.96")
    assert snap.apy == Decimal("0.1")


async def test_ext_snapshot_ingest_no_matching_source_returns_404(client):
    resp = await client.post("/api/v1/ext/snapshot?source_type=mexc_earn&uid=65064080", json=_payload())
    assert resp.status == 404
    data = await resp.json()
    assert "No enabled source found" in data["error"]


async def test_ext_snapshot_ingest_multiple_matching_sources_returns_409(client, db_path):
    store = SourceStore(db_path)
    await store.add("mexc-earn-a", "mexc_earn", {"uid": "65064080"})
    await store.add("mexc-earn-b", "mexc_earn", {"uid": "65064080"})

    resp = await client.post("/api/v1/ext/snapshot?source_type=mexc_earn&uid=65064080", json=_payload())
    assert resp.status == 409
    data = await resp.json()
    assert "Multiple enabled sources match" in data["error"]
    assert set(data["matches"]) == {"mexc-earn-a", "mexc-earn-b"}


async def test_ext_snapshot_ingest_requires_uid(client):
    body = _payload()
    body["source"] = {"type": "mexc_earn"}
    resp = await client.post("/api/v1/ext/snapshot?source_type=mexc_earn", json=body)
    assert resp.status == 400
    data = await resp.json()
    assert data["error"] == "uid is required"
