"""Tests for APY rules REST endpoints."""

from __future__ import annotations

import pytest

from pfm.db.models import init_db
from pfm.db.source_store import SourceStore
from pfm.server.app import create_app

_VALID_RULE = {
    "protocol": "aave",
    "coin": "usdc",
    "type": "base",
    "limits": [
        {"from_amount": "0", "to_amount": "5000", "apy": "0.10"},
        {"from_amount": "5000", "to_amount": None, "apy": "0.0297"},
    ],
    "started_at": "2024-01-01",
    "finished_at": "2025-12-31",
}


@pytest.fixture
async def db_path(tmp_path):
    path = tmp_path / "test.db"
    await init_db(path)
    return path


@pytest.fixture
async def db_with_source(db_path):
    store = SourceStore(db_path)
    await store.add("bitget-live", "bitget_wallet", {"wallet_address": "0x" + "a" * 40})
    return db_path


@pytest.fixture
async def client(aiohttp_client, db_with_source):
    app = create_app(db_with_source)
    return await aiohttp_client(app)


@pytest.fixture
async def client_no_source(aiohttp_client, db_path):
    app = create_app(db_path)
    return await aiohttp_client(app)


async def test_list_empty(client):
    resp = await client.get("/api/v1/sources/bitget-live/apy-rules")
    assert resp.status == 200
    data = await resp.json()
    assert data == []


async def test_add_rule(client):
    resp = await client.post("/api/v1/sources/bitget-live/apy-rules", json=_VALID_RULE)
    assert resp.status == 201
    data = await resp.json()
    assert len(data) == 1
    assert data[0]["protocol"] == "aave"
    assert data[0]["coin"] == "usdc"
    assert data[0]["type"] == "base"
    assert "id" in data[0]


async def test_add_rule_invalid(client):
    resp = await client.post(
        "/api/v1/sources/bitget-live/apy-rules",
        json={**_VALID_RULE, "protocol": "compound"},
    )
    assert resp.status == 400


async def test_update_rule(client):
    resp = await client.post("/api/v1/sources/bitget-live/apy-rules", json=_VALID_RULE)
    data = await resp.json()
    rule_id = data[0]["id"]

    updated = {**_VALID_RULE, "type": "bonus"}
    resp = await client.put(f"/api/v1/sources/bitget-live/apy-rules/{rule_id}", json=updated)
    assert resp.status == 200
    data = await resp.json()
    assert data[0]["type"] == "bonus"


async def test_update_rule_not_found(client):
    resp = await client.put(
        "/api/v1/sources/bitget-live/apy-rules/nonexistent",
        json=_VALID_RULE,
    )
    assert resp.status == 404


async def test_delete_rule(client):
    resp = await client.post("/api/v1/sources/bitget-live/apy-rules", json=_VALID_RULE)
    data = await resp.json()
    rule_id = data[0]["id"]

    resp = await client.delete(f"/api/v1/sources/bitget-live/apy-rules/{rule_id}")
    assert resp.status == 200
    data = await resp.json()
    assert data == []


async def test_delete_rule_not_found(client):
    resp = await client.delete("/api/v1/sources/bitget-live/apy-rules/nonexistent")
    assert resp.status == 404


async def test_source_not_found(client_no_source):
    resp = await client_no_source.get("/api/v1/sources/nonexistent/apy-rules")
    assert resp.status == 404


async def test_wrong_source_type(client_no_source):
    """APY rules rejected for non-bitget_wallet sources."""
    store = SourceStore(client_no_source.app["db_path"])
    await store.add("wise-main", "wise", {"api_token": "test-token"})

    resp = await client_no_source.get("/api/v1/sources/wise-main/apy-rules")
    assert resp.status == 400
    data = await resp.json()
    assert "not supported" in data["error"]
