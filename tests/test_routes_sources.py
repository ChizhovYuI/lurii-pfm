"""Tests for the sources REST endpoints."""

from __future__ import annotations

import pytest

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
