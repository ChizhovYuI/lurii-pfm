"""Tests for the sources REST endpoints."""

from __future__ import annotations

import aiosqlite
import pytest

from pfm.collectors import COLLECTOR_REGISTRY
from pfm.collectors.base import BaseCollector
from pfm.db.models import init_db
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


@pytest.fixture
async def db_with_source(db_path):
    """Seed a wise source in the database."""
    store = SourceStore(db_path)
    await store.add("wise-main", "wise", {"api_token": "test-token-123456"})
    return db_path


@pytest.fixture
async def client_with_source(aiohttp_client, db_with_source):
    app = create_app(db_with_source)
    return await aiohttp_client(app)


async def test_list_source_types(client):
    resp = await client.get("/api/v1/source-types")
    assert resp.status == 200
    data = await resp.json()
    # Should return all configured source types
    assert len(data) == 17
    expected_types = {
        "okx",
        "binance",
        "binance_th",
        "bybit",
        "mexc",
        "mexc_earn",
        "bitget_wallet",
        "lobstr",
        "blend",
        "wise",
        "kbank",
        "ibkr",
        "rabby",
        "revolut",
        "trading212",
        "emcd",
        "yo",
    }
    assert set(data.keys()) == expected_types
    # Each type has fields list and supported_apy_rules
    for type_info in data.values():
        assert "fields" in type_info
        assert "supported_apy_rules" in type_info
        assert isinstance(type_info["supported_apy_rules"], list)
        fields = type_info["fields"]
        assert isinstance(fields, list)
        assert len(fields) > 0
        for field in fields:
            assert "name" in field
            assert "prompt" in field
            assert "required" in field
            assert "secret" in field
            assert "tip" in field
            assert isinstance(field["required"], bool)
            assert isinstance(field["secret"], bool)
    # Only bitget_wallet has APY rules config
    apy_rules = data["bitget_wallet"]["supported_apy_rules"]
    assert len(apy_rules) == 1
    assert apy_rules[0]["protocol"] == "aave"
    assert apy_rules[0]["coins"] == ["usdc", "usdt"]
    assert data["okx"]["supported_apy_rules"] == []


async def test_list_source_types_wise_fields(client):
    resp = await client.get("/api/v1/source-types")
    data = await resp.json()
    wise_fields = data["wise"]["fields"]
    assert len(wise_fields) == 1
    assert wise_fields[0]["name"] == "api_token"
    assert wise_fields[0]["required"] is True
    assert wise_fields[0]["secret"] is True
    assert "wise.com" in wise_fields[0]["tip"]


async def test_list_source_types_mexc_earn_uid_field(client):
    resp = await client.get("/api/v1/source-types")
    data = await resp.json()
    fields = {field["name"]: field for field in data["mexc_earn"]["fields"]}
    assert "uid" in fields
    assert fields["uid"]["required"] is True
    assert fields["uid"]["secret"] is False


async def test_list_source_types_trading212_fields(client):
    resp = await client.get("/api/v1/source-types")
    data = await resp.json()
    fields = {field["name"]: field for field in data["trading212"]["fields"]}
    assert set(fields) == {"api_key", "api_secret"}
    assert fields["api_key"]["secret"] is True
    assert fields["api_secret"]["secret"] is True


async def test_list_sources_empty(client):
    resp = await client.get("/api/v1/sources")
    assert resp.status == 200
    data = await resp.json()
    assert data == []


async def test_add_source(client):
    resp = await client.post(
        "/api/v1/sources",
        json={
            "name": "wise-main",
            "type": "wise",
            "credentials": {"api_token": "my-secret-token-1234"},
        },
    )
    assert resp.status == 201
    data = await resp.json()
    assert data["name"] == "wise-main"
    assert data["type"] == "wise"
    assert "..." in data["credentials"]["api_token"]  # masked


async def test_add_source_duplicate(client):
    await client.post(
        "/api/v1/sources",
        json={
            "name": "wise-main",
            "type": "wise",
            "credentials": {"api_token": "token"},
        },
    )
    resp = await client.post(
        "/api/v1/sources",
        json={
            "name": "wise-main",
            "type": "wise",
            "credentials": {"api_token": "token"},
        },
    )
    assert resp.status == 409


async def test_add_source_invalid_type(client):
    resp = await client.post(
        "/api/v1/sources",
        json={
            "name": "bad",
            "type": "nonexistent",
            "credentials": {},
        },
    )
    assert resp.status == 400


async def test_get_source(client_with_source):
    resp = await client_with_source.get("/api/v1/sources/wise-main")
    assert resp.status == 200
    data = await resp.json()
    assert data["name"] == "wise-main"
    assert "***" in data["credentials"]["api_token"] or "..." in data["credentials"]["api_token"]


async def test_get_source_not_found(client):
    resp = await client.get("/api/v1/sources/nonexistent")
    assert resp.status == 404


async def test_delete_source(client_with_source):
    resp = await client_with_source.delete("/api/v1/sources/wise-main")
    assert resp.status == 200
    data = await resp.json()
    assert data["deleted"] is True

    # Verify it's gone
    resp = await client_with_source.get("/api/v1/sources/wise-main")
    assert resp.status == 404


async def test_delete_source_not_found(client):
    resp = await client.delete("/api/v1/sources/nonexistent")
    assert resp.status == 404


async def test_update_source(client_with_source):
    resp = await client_with_source.patch(
        "/api/v1/sources/wise-main",
        json={
            "enabled": False,
        },
    )
    assert resp.status == 200
    data = await resp.json()
    assert data["enabled"] is False


async def test_list_sources_after_add(client_with_source):
    resp = await client_with_source.get("/api/v1/sources")
    assert resp.status == 200
    data = await resp.json()
    assert len(data) == 1
    assert data[0]["name"] == "wise-main"


class _FakeWiseValidationCollector(BaseCollector):
    source_name = "wise"
    last_api_token = ""
    last_cache_db_path = "unexpected"

    def __init__(self, pricing, *, api_token: str) -> None:
        super().__init__(pricing)
        type(self).last_api_token = api_token
        type(self).last_cache_db_path = str(pricing._cache_db_path)

    async def fetch_raw_balances(self):
        return []

    async def fetch_transactions(self, since=None):
        return []


async def test_validate_source_connection_success_without_persisting(client, monkeypatch, db_path):
    monkeypatch.setitem(COLLECTOR_REGISTRY, "wise", _FakeWiseValidationCollector)

    resp = await client.post(
        "/api/v1/source-connections/validate",
        json={
            "type": "wise",
            "credentials": {"api_token": "validate-only-token"},
        },
    )

    assert resp.status == 200
    data = await resp.json()
    assert data == {"ok": True, "message": "Connection successful."}
    assert _FakeWiseValidationCollector.last_api_token == "validate-only-token"
    assert _FakeWiseValidationCollector.last_cache_db_path == "None"

    store = SourceStore(db_path)
    assert await store.list_all() == []

    async with aiosqlite.connect(str(db_path)) as db:
        sources_count = (await (await db.execute("SELECT COUNT(*) FROM sources")).fetchone())[0]
        snapshots_count = (await (await db.execute("SELECT COUNT(*) FROM snapshots")).fetchone())[0]
        transactions_count = (await (await db.execute("SELECT COUNT(*) FROM transactions")).fetchone())[0]
        prices_count = (await (await db.execute("SELECT COUNT(*) FROM prices")).fetchone())[0]

    assert sources_count == 0
    assert snapshots_count == 0
    assert transactions_count == 0
    assert prices_count == 0


async def test_validate_source_connection_reuses_saved_secret(client_with_source, monkeypatch):
    monkeypatch.setitem(COLLECTOR_REGISTRY, "wise", _FakeWiseValidationCollector)

    resp = await client_with_source.post(
        "/api/v1/source-connections/validate",
        json={
            "name": "wise-main",
            "credentials": {},
        },
    )

    assert resp.status == 200
    assert _FakeWiseValidationCollector.last_api_token == "test-token-123456"


async def test_validate_source_connection_invalid_credentials(client):
    resp = await client.post(
        "/api/v1/source-connections/validate",
        json={
            "type": "wise",
            "credentials": {},
        },
    )

    assert resp.status == 400
    data = await resp.json()
    assert "Missing required field: api_token" in data["error"]


async def test_validate_source_connection_unknown_source(client):
    resp = await client.post(
        "/api/v1/source-connections/validate",
        json={
            "name": "unknown-source",
            "credentials": {},
        },
    )

    assert resp.status == 404
    data = await resp.json()
    assert "not found" in data["error"]
