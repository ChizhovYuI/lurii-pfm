"""Tests for internal coordination endpoints."""

from __future__ import annotations

import asyncio

import pytest

from pfm.db.models import init_db
from pfm.server.app import create_app
from pfm.server.state import get_broadcaster


@pytest.fixture
async def db_path(tmp_path):
    path = tmp_path / "test.db"
    await init_db(path)
    return path


@pytest.fixture
async def client(aiohttp_client, db_path):
    app = create_app(db_path)
    return await aiohttp_client(app)


async def test_broadcast_event_invalid_type(client):
    resp = await client.post("/api/v1/internal/broadcast", json={"type": "evil"})
    assert resp.status == 400
    data = await resp.json()
    assert "snapshot_updated" in data["error"]


async def test_broadcast_event_invalid_body(client):
    resp = await client.post("/api/v1/internal/broadcast", data="not json")
    assert resp.status == 400


async def test_broadcast_event_missing_type(client):
    resp = await client.post("/api/v1/internal/broadcast", json={})
    assert resp.status == 400


async def test_broadcast_event_dispatches_to_subscribers(client):
    import json as _json

    received: list[str] = []

    class _FakeWS:
        closed = False

        async def send_str(self, payload):
            received.append(payload)

    fake = _FakeWS()
    broadcaster = get_broadcaster(client.app)
    broadcaster.register(fake)  # type: ignore[arg-type]

    try:
        resp = await client.post("/api/v1/internal/broadcast", json={"type": "snapshot_updated"})
        assert resp.status == 200
        data = await resp.json()
        assert data == {"broadcast": True, "type": "snapshot_updated"}

        await asyncio.sleep(0)
        assert [_json.loads(p) for p in received] == [{"type": "snapshot_updated"}]
    finally:
        broadcaster.unregister(fake)  # type: ignore[arg-type]
