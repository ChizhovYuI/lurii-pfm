"""Tests for the collection REST endpoint."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pfm.db.models import CollectorResult, init_db
from pfm.db.source_store import SourceStore
from pfm.server.app import create_app
from pfm.server.routes.collect import start_collection_task
from pfm.server.state import get_runtime_state


@pytest.fixture
async def db_path(tmp_path):
    path = tmp_path / "test.db"
    await init_db(path)
    return path


@pytest.fixture
async def db_with_source(db_path):
    store = SourceStore(db_path)
    await store.add("wise-main", "wise", {"api_token": "test-token-123456"})
    return db_path


@pytest.fixture
async def client(aiohttp_client, db_with_source):
    app = create_app(db_with_source)
    return await aiohttp_client(app)


@pytest.fixture
async def empty_client(aiohttp_client, db_path):
    app = create_app(db_path)
    return await aiohttp_client(app)


async def test_collect_status_idle(client):
    """Test that collect status returns collecting=false when idle."""
    resp = await client.get("/api/v1/collect/status")
    assert resp.status == 200
    data = await resp.json()
    assert data == {"collecting": False}


async def test_collect_status_during_collection(client):
    """Test that collect status returns collecting=true during collection."""
    get_runtime_state(client.app).collecting = True
    resp = await client.get("/api/v1/collect/status")
    assert resp.status == 200
    data = await resp.json()
    assert data == {"collecting": True}
    get_runtime_state(client.app).collecting = False


async def test_start_collection_task_sets_collecting_and_creates_task(client):
    async_mock = AsyncMock()
    with patch("pfm.server.routes.collect._run_collection", async_mock):
        started = start_collection_task(client.app, "wise-main")

        assert started is True
        state = get_runtime_state(client.app)
        assert state.collecting is True
        task = state.collection_task
        assert task is not None
        await asyncio.wait_for(task, timeout=5.0)
        async_mock.assert_awaited_once_with(client.app, "wise-main")


async def test_start_collection_task_returns_false_when_busy(client):
    get_runtime_state(client.app).collecting = True

    with patch("pfm.server.routes.collect._run_collection", AsyncMock()) as async_mock:
        started = start_collection_task(client.app, "wise-main")

    assert started is False
    async_mock.assert_not_called()


async def test_collect_returns_202(client):
    """Test that collection trigger returns 202 immediately."""
    mock_cls = MagicMock()
    mock_instance = MagicMock()
    mock_instance.collect = AsyncMock(
        return_value=CollectorResult(
            source="wise-main",
            snapshots_count=1,
            snapshots_usd_total=Decimal(1000),
            transactions_count=0,
        ),
    )
    mock_cls.return_value = mock_instance

    with patch("pfm.collectors.COLLECTOR_REGISTRY", {"wise": mock_cls}):
        resp = await client.post("/api/v1/collect", json={})
        assert resp.status == 202
        data = await resp.json()
        assert data["status"] == "started"

        # Wait for background task to complete
        task = get_runtime_state(client.app).collection_task
        if task:
            await asyncio.wait_for(task, timeout=5.0)


async def test_collect_rejects_concurrent(client):
    """Test that concurrent collection requests are rejected with 409."""
    get_runtime_state(client.app).collecting = True
    resp = await client.post("/api/v1/collect", json={})
    assert resp.status == 409
    get_runtime_state(client.app).collecting = False


async def test_collect_with_source_filter(client):
    """Test collection with a specific source name."""
    mock_cls = MagicMock()
    mock_instance = MagicMock()
    mock_instance.collect = AsyncMock(
        return_value=CollectorResult(
            source="wise-main",
            snapshots_count=1,
            snapshots_usd_total=Decimal(100),
        ),
    )
    mock_cls.return_value = mock_instance

    with patch("pfm.collectors.COLLECTOR_REGISTRY", {"wise": mock_cls}):
        resp = await client.post("/api/v1/collect", json={"source": "wise-main"})
        assert resp.status == 202

        task = get_runtime_state(client.app).collection_task
        if task:
            await asyncio.wait_for(task, timeout=5.0)
