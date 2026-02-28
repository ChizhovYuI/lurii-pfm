"""Tests for WebSocket support and EventBroadcaster."""

from __future__ import annotations

import json

import pytest
from aiohttp import WSMsgType

from pfm.db.models import init_db
from pfm.server.app import create_app
from pfm.server.ws import EventBroadcaster


@pytest.fixture
async def db_path(tmp_path):
    path = tmp_path / "test.db"
    await init_db(path)
    return path


@pytest.fixture
async def client(aiohttp_client, db_path):
    app = create_app(db_path)
    return await aiohttp_client(app)


class TestEventBroadcaster:
    def test_initial_state(self):
        b = EventBroadcaster()
        assert b.client_count == 0

    async def test_close_empty(self):
        b = EventBroadcaster()
        await b.close()
        assert b.client_count == 0


async def test_websocket_connect(client):
    """Test that a WebSocket client can connect and receive events."""
    async with client.ws_connect("/api/v1/ws") as ws:
        # The connection should be established
        broadcaster = client.app["broadcaster"]
        assert broadcaster.client_count == 1

        # Broadcast an event
        await broadcaster.broadcast({"type": "test", "data": "hello"})

        msg = await ws.receive()
        assert msg.type == WSMsgType.TEXT
        data = json.loads(msg.data)
        assert data["type"] == "test"
        assert data["data"] == "hello"

    # After disconnect, client count should decrease
    # (may take a moment for cleanup)
