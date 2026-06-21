"""Tests for the USD-valuation background job and status endpoint."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from pfm.db.models import CollectorResult, init_db
from pfm.server.app import create_app
from pfm.server.routes.backfill import maybe_start_valuation, start_valuation_task
from pfm.server.state import get_runtime_state


@pytest.fixture
async def db_path(tmp_path):
    path = tmp_path / "test.db"
    await init_db(path)
    return path


@pytest.fixture
async def client(aiohttp_client, db_path):
    app = create_app(db_path)
    return await aiohttp_client(app)


async def test_backfill_status_idle(client):
    resp = await client.get("/api/v1/backfill/status")
    assert resp.status == 200
    assert await resp.json() == {"valuing": False}


async def test_backfill_status_during_valuation(client):
    get_runtime_state(client.app).valuing = True
    resp = await client.get("/api/v1/backfill/status")
    assert resp.status == 200
    assert await resp.json() == {"valuing": True}
    get_runtime_state(client.app).valuing = False


async def test_start_valuation_task_sets_flag_and_creates_task(client):
    async_mock = AsyncMock()
    with patch("pfm.server.routes.backfill._run_valuation", async_mock):
        started = start_valuation_task(client.app)

        assert started is True
        state = get_runtime_state(client.app)
        assert state.valuing is True
        task = state.valuation_task
        assert task is not None
        await asyncio.wait_for(task, timeout=5.0)
        async_mock.assert_awaited_once_with(client.app)


async def test_start_valuation_task_returns_false_when_busy(client):
    get_runtime_state(client.app).valuing = True

    with patch("pfm.server.routes.backfill._run_valuation", AsyncMock()) as async_mock:
        started = start_valuation_task(client.app)

    assert started is False
    async_mock.assert_not_called()
    get_runtime_state(client.app).valuing = False


async def test_maybe_start_valuation_skips_when_no_new_transactions(client):
    results = [CollectorResult(source="wise-main", snapshots_count=1, transactions_count=0)]
    with patch("pfm.server.routes.backfill.start_valuation_task") as spawn:
        assert maybe_start_valuation(client.app, results) is False
        spawn.assert_not_called()


async def test_maybe_start_valuation_starts_when_new_transactions(client):
    results = [
        CollectorResult(source="wise-main", transactions_count=0),
        CollectorResult(source="okx-main", transactions_count=3),
    ]
    with patch("pfm.server.routes.backfill.start_valuation_task", return_value=True) as spawn:
        assert maybe_start_valuation(client.app, results) is True
        spawn.assert_called_once_with(client.app)


async def test_run_valuation_broadcasts_lifecycle_events(client):
    state = get_runtime_state(client.app)
    state.valuing = True
    events: list[dict[str, object]] = []

    async def _capture(event: dict[str, object]) -> None:
        events.append(event)

    summary = {"scanned": 5, "updated": 3, "no_price": 1, "unique_lookups": 2}
    with (
        patch.object(state.broadcaster, "broadcast", side_effect=_capture),
        patch(
            "pfm.server.routes.backfill.forward_fill_recent",
            AsyncMock(return_value=summary),
        ),
    ):
        from pfm.server.routes.backfill import _run_valuation

        await _run_valuation(client.app)

    types = [e["type"] for e in events]
    assert types == ["backfill_started", "backfill_completed"]
    assert events[-1]["valued"] == 3
    assert events[-1]["scanned"] == 5
    assert state.valuing is False
    assert state.valuation_task is None


async def test_run_valuation_broadcasts_failure_and_resets_state(client):
    state = get_runtime_state(client.app)
    state.valuing = True
    events: list[dict[str, object]] = []

    async def _capture(event: dict[str, object]) -> None:
        events.append(event)

    with (
        patch.object(state.broadcaster, "broadcast", side_effect=_capture),
        patch(
            "pfm.server.routes.backfill.forward_fill_recent",
            AsyncMock(side_effect=RuntimeError("coingecko down")),
        ),
    ):
        from pfm.server.routes.backfill import _run_valuation

        await _run_valuation(client.app)

    types = [e["type"] for e in events]
    assert types == ["backfill_started", "backfill_failed"]
    assert "coingecko down" in str(events[-1]["error"])
    # The flag must always reset, or the indicator/guard would stick forever.
    assert state.valuing is False
    assert state.valuation_task is None


async def test_run_valuation_forwards_progress_events(client):
    state = get_runtime_state(client.app)
    state.valuing = True
    events: list[dict[str, object]] = []

    async def _capture(event: dict[str, object]) -> None:
        events.append(event)

    async def _fake_backfill(repo, pricing, *, on_progress=None):
        if on_progress is not None:
            await on_progress(50, 200, 44)
        return {"scanned": 200, "updated": 44, "no_price": 0, "unique_lookups": 5}

    with (
        patch.object(state.broadcaster, "broadcast", side_effect=_capture),
        patch("pfm.server.routes.backfill.forward_fill_recent", _fake_backfill),
    ):
        from pfm.server.routes.backfill import _run_valuation

        await _run_valuation(client.app)

    progress = [e for e in events if e["type"] == "backfill_progress"]
    assert progress == [{"type": "backfill_progress", "current": 50, "total": 200, "valued": 44}]


async def test_run_valuation_resets_valuing_when_getter_raises(client):
    """A getter raising before the work must not wedge the valuing guard True."""
    state = get_runtime_state(client.app)
    state.valuing = True

    with patch("pfm.server.routes.backfill.get_repo", side_effect=RuntimeError("db locked")):
        from pfm.server.routes.backfill import _run_valuation

        await _run_valuation(client.app)

    assert state.valuing is False
    assert state.valuation_task is None


async def test_start_valuation_task_returns_false_when_shutting_down(client):
    state = get_runtime_state(client.app)
    state.shutting_down = True

    with patch("pfm.server.routes.backfill._run_valuation", AsyncMock()) as async_mock:
        started = start_valuation_task(client.app)

    assert started is False
    assert state.valuing is False
    async_mock.assert_not_called()
    state.shutting_down = False


async def test_repository_write_holds_write_lock(repo):
    """Leaf writes must commit while holding the shared write lock (serialization)."""
    held: list[bool] = []
    original_commit = repo.connection.commit

    async def _spy_commit() -> None:
        held.append(repo.write_lock.locked())
        await original_commit()

    assert not repo.write_lock.locked()
    with patch.object(repo.connection, "commit", _spy_commit):
        # No matching row, but the UPDATE still executes + commits under the lock.
        await repo.update_transaction_usd_values([(999_999, Decimal(1))])

    assert held == [True]
    assert not repo.write_lock.locked()
