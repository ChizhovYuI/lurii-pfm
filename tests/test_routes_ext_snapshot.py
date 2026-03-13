"""Tests for extension snapshot ingest endpoint."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

import pytest
from aiohttp import WSMsgType

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


def _payload(
    *,
    source_type: str = "mexc_earn",
    uid: str = "65064080",
    assets: list[dict[str, object]] | None = None,
    captured_at: str = "2026-03-03T11:36:53.000Z",
) -> dict[str, object]:
    return {
        "schemaVersion": "lurii.extension.snapshot.v1",
        "capturedAt": captured_at,
        "source": {"type": source_type, "uid": uid},
        "snapshot": {
            "assets": assets
            or [
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


async def test_ext_snapshot_ingest_broadcasts_snapshot_updated(client, db_path):
    store = SourceStore(db_path)
    await store.add("mexc-earn-main", "mexc_earn", {"uid": "65064080"})

    async with client.ws_connect("/api/v1/ws") as ws:
        resp = await client.post("/api/v1/ext/snapshot?source_type=mexc_earn&uid=65064080", json=_payload())
        assert resp.status == 200

        msg = await ws.receive()
        assert msg.type == WSMsgType.TEXT
        assert json.loads(msg.data) == {"type": "snapshot_updated"}


async def test_ext_snapshot_ingest_replaces_same_day_rows_for_mexc_earn(client, db_path):
    store = SourceStore(db_path)
    await store.add("mexc-earn-main", "mexc_earn", {"uid": "65064080"})

    first_payload = _payload(
        assets=[
            {
                "symbol": "USDC",
                "amount": 300,
                "usdValue": 299.96,
                "quotedAprPercent": 10,
            },
            {
                "symbol": "USDT",
                "amount": 200,
                "usdValue": 200,
                "quotedAprPercent": 12,
            },
        ]
    )
    second_payload = _payload(
        assets=[
            {
                "symbol": "USDC",
                "amount": 310,
                "usdValue": 309.50,
                "quotedAprPercent": 9,
            }
        ]
    )

    first_resp = await client.post("/api/v1/ext/snapshot?source_type=mexc_earn&uid=65064080", json=first_payload)
    second_resp = await client.post("/api/v1/ext/snapshot?source_type=mexc_earn&uid=65064080", json=second_payload)

    assert first_resp.status == 200
    assert second_resp.status == 200

    async with Repository(db_path) as repo:
        snapshots = await repo.get_snapshots_by_date(date(2026, 3, 3))

    assert len(snapshots) == 1
    snap = snapshots[0]
    assert snap.source == "mexc_earn"
    assert snap.source_name == "mexc-earn-main"
    assert snap.asset == "USDC"
    assert snap.amount == Decimal(310)
    assert snap.usd_value == Decimal("309.50")
    assert snap.apy == Decimal("0.09")
    assert all(saved_snap.asset != "USDT" for saved_snap in snapshots)


async def test_ext_snapshot_ingest_prefers_effective_apr_percent_for_mexc_earn(client, db_path):
    store = SourceStore(db_path)
    await store.add("mexc-earn-main", "mexc_earn", {"uid": "65064080"})

    payload = _payload(
        assets=[
            {
                "symbol": "USDT",
                "amount": 300.86335046,
                "usdValue": 300.86335046,
                "quotedAprPercent": 25,
                "effectiveAprPercent": "15.0107418",
                "financialType": "FIXED",
            }
        ]
    )

    resp = await client.post("/api/v1/ext/snapshot?source_type=mexc_earn&uid=65064080", json=payload)
    assert resp.status == 200

    async with Repository(db_path) as repo:
        snapshots = await repo.get_snapshots_by_date(date(2026, 3, 3))

    assert len(snapshots) == 1
    assert snapshots[0].apy == Decimal("0.150107418")


async def test_ext_snapshot_ingest_replaces_same_day_rows_for_emcd(client, db_path):
    store = SourceStore(db_path)
    await store.add("emcd-main", "emcd", {"email": "miner@example.com"})

    first_payload = _payload(
        source_type="emcd",
        uid="miner@example.com",
        assets=[
            {
                "symbol": "BTC",
                "amount": 0.01,
                "usdValue": 700,
                "quotedAprPercent": 0,
            },
            {
                "symbol": "USDT",
                "amount": 50,
                "usdValue": 50,
                "quotedAprPercent": 0,
            },
        ],
    )
    second_payload = _payload(
        source_type="emcd",
        uid="miner@example.com",
        assets=[
            {
                "symbol": "BTC",
                "amount": 0.02,
                "usdValue": 1400,
                "quotedAprPercent": 0,
            }
        ],
    )

    first_resp = await client.post("/api/v1/ext/snapshot?source_type=emcd&uid=miner@example.com", json=first_payload)
    second_resp = await client.post("/api/v1/ext/snapshot?source_type=emcd&uid=miner@example.com", json=second_payload)

    assert first_resp.status == 200
    assert second_resp.status == 200

    async with Repository(db_path) as repo:
        snapshots = await repo.get_snapshots_by_date(date(2026, 3, 3))

    assert len(snapshots) == 1
    snap = snapshots[0]
    assert snap.source == "emcd"
    assert snap.source_name == "emcd-main"
    assert snap.asset == "BTC"
    assert snap.amount == Decimal("0.02")
    assert snap.usd_value == Decimal(1400)
    assert all(saved_snap.asset != "USDT" for saved_snap in snapshots)
