"""Tests for the updates endpoints."""

from __future__ import annotations

import asyncio
from pathlib import Path as RealPath
from unittest.mock import AsyncMock, patch

import pytest

from pfm import __version__
from pfm.db.models import init_db
from pfm.server.app import create_app
from pfm.server.routes import updates as updates_mod


@pytest.fixture
async def db_path(tmp_path):
    path = tmp_path / "test.db"
    await init_db(path)
    return path


@pytest.fixture
async def client(aiohttp_client, db_path):
    app = create_app(db_path)
    return await aiohttp_client(app)


@pytest.fixture(autouse=True)
def _clear_cache():
    """Reset module-level update caches between tests."""
    updates_mod._cache["data"] = None
    updates_mod._cache["ts"] = 0.0
    updates_mod._set_cached_install_state(updates_mod._default_install_state())


async def _wait_for_status(db_path, expected_status: str):
    for _ in range(200):
        state = await updates_mod._load_install_state(db_path)
        if state["status"] == expected_status:
            return state
        await asyncio.sleep(0.01)
    pytest.fail(f"Timed out waiting for install status {expected_status!r}")


async def test_fetch_latest_tag_parses_tag(client):
    import httpx

    mock_resp = AsyncMock(spec=httpx.Response)
    mock_resp.status_code = 200
    mock_resp.raise_for_status = AsyncMock()
    mock_resp.json.return_value = {"tag_name": "v1.2.3"}

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get.return_value = mock_resp

    with patch("pfm.server.routes.updates.httpx.AsyncClient", return_value=mock_client):
        result = await updates_mod._fetch_latest_tag("ChizhovYuI/lurii-pfm")

    assert result == "1.2.3"


async def test_fetch_latest_tag_returns_none_on_404(client):
    import httpx

    mock_resp = AsyncMock(spec=httpx.Response)
    mock_resp.status_code = 404

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get.return_value = mock_resp

    with patch("pfm.server.routes.updates.httpx.AsyncClient", return_value=mock_client):
        result = await updates_mod._fetch_latest_tag("ChizhovYuI/nonexistent")

    assert result is None


async def test_check_updates_returns_versions(client):
    with patch.object(updates_mod, "_fetch_latest_tag", new_callable=AsyncMock) as mock:
        mock.side_effect = lambda repo: "0.15.0" if "pfm" in repo else "1.9"
        resp = await client.get("/api/v1/updates")

    assert resp.status == 200
    data = await resp.json()
    assert data["pfm"]["current"] == __version__
    assert data["pfm"]["latest"] == "0.15.0"
    assert data["app"]["latest"] == "1.9"
    assert data["restart_pending"] is False


async def test_check_updates_caches_result(client):
    with patch.object(updates_mod, "_fetch_latest_tag", new_callable=AsyncMock) as mock:
        mock.return_value = __version__
        await client.get("/api/v1/updates")
        await client.get("/api/v1/updates")

    # Second call should use cache — only 2 calls total (pfm + app).
    assert mock.call_count == 2


async def test_install_unknown_target_returns_400(client):
    resp = await client.post("/api/v1/updates/install", json={"target": "bogus"})
    assert resp.status == 400


async def test_install_no_body_defaults_to_all(client, db_path):
    with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock:
        proc = AsyncMock()
        proc.communicate.return_value = (b"ok", b"")
        proc.returncode = 0
        mock.return_value = proc

        with patch.object(updates_mod, "_get_updates", new_callable=AsyncMock) as mock_updates:
            mock_updates.return_value = {
                "pfm": {"current": __version__, "latest": __version__, "update_available": False},
                "app": {"latest": "2.9"},
            }
            resp = await client.post("/api/v1/updates/install")
            assert resp.status == 202
            await _wait_for_status(db_path, "installed")


async def test_install_returns_202(client, db_path):
    with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock:
        proc = AsyncMock()
        proc.communicate.return_value = (b"ok", b"")
        proc.returncode = 0
        mock.return_value = proc

        with patch.object(updates_mod, "_get_updates", new_callable=AsyncMock) as mock_updates:
            mock_updates.return_value = {
                "pfm": {"current": __version__, "latest": "0.20.0", "update_available": False},
                "app": {"latest": "2.9"},
            }
            resp = await client.post("/api/v1/updates/install", json={"target": "pfm"})
            state = await _wait_for_status(db_path, "installed")

    assert resp.status == 202
    data = await resp.json()
    assert data["status"] == "started"
    assert state["target"] == "pfm"


async def test_install_status_returns_persisted_state(client, db_path):
    await updates_mod._save_install_state(
        db_path,
        {
            "status": "installed",
            "progress": 1.0,
            "message": "Updates installed",
            "target": "all",
            "installed_versions": {"app": "2.9"},
            "updated_at": "2026-03-08T00:00:00+00:00",
        },
    )
    updates_mod._set_cached_install_state(updates_mod._default_install_state())

    resp = await client.get("/api/v1/updates/status")

    assert resp.status == 200
    data = await resp.json()
    assert data["status"] == "installed"
    assert data["progress"] == 1.0
    assert data["installed_versions"] == {"app": "2.9"}


async def test_install_conflict_when_already_installing(client, db_path):
    await updates_mod._save_install_state(
        db_path,
        {
            "status": "installing",
            "progress": 0.33,
            "message": "Running brew update...",
            "target": "all",
            "installed_versions": {},
            "updated_at": "2026-03-08T00:00:00+00:00",
        },
    )

    resp = await client.post("/api/v1/updates/install", json={"target": "pfm"})

    assert resp.status == 409


async def test_restart_returns_404_when_no_plist(client, db_path):
    await updates_mod._save_install_state(
        db_path,
        {
            "status": "installed",
            "progress": 1.0,
            "message": "Updates installed",
            "target": "all",
            "installed_versions": {"app": "2.9"},
            "updated_at": "2026-03-08T00:00:00+00:00",
        },
    )

    with patch("pfm.server.routes.updates.Path.home") as mock_home:
        mock_home.return_value = RealPath("/nonexistent")
        resp = await client.post("/api/v1/updates/restart")

    assert resp.status == 404
    state = await updates_mod._load_install_state(db_path)
    assert state["status"] == "installed"


async def test_restart_resets_install_state(client, db_path):
    await updates_mod._save_install_state(
        db_path,
        {
            "status": "installed",
            "progress": 1.0,
            "message": "Updates installed",
            "target": "all",
            "installed_versions": {"app": "2.9"},
            "updated_at": "2026-03-08T00:00:00+00:00",
        },
    )

    with (
        patch("pfm.server.routes.updates.Path.home") as mock_home,
        patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec,
    ):
        tmp = RealPath("/tmp/test_launchagent")
        la_dir = tmp / "Library/LaunchAgents"
        la_dir.mkdir(parents=True, exist_ok=True)
        plist = la_dir / "finance.lurii.pfm.plist"
        plist.write_text("<plist/>")
        mock_home.return_value = tmp

        proc = AsyncMock()
        proc.communicate.return_value = (b"", b"")
        proc.returncode = 0
        mock_exec.return_value = proc

        resp = await client.post("/api/v1/updates/restart")

    assert resp.status == 200
    state = await updates_mod._load_install_state(db_path)
    assert state["status"] == "idle"
    assert state["progress"] == 0.0


async def test_check_updates_restart_pending_when_installed(client, db_path):
    await updates_mod._save_install_state(
        db_path,
        {
            "status": "installed",
            "progress": 1.0,
            "message": "Updates installed",
            "target": "all",
            "installed_versions": {"app": "2.9"},
            "updated_at": "2026-03-08T00:00:00+00:00",
        },
    )
    updates_mod._set_cached_install_state(updates_mod._default_install_state())

    with patch.object(updates_mod, "_fetch_latest_tag", new_callable=AsyncMock) as mock:
        mock.return_value = __version__
        resp = await client.get("/api/v1/updates")

    data = await resp.json()
    assert data["restart_pending"] is True


async def test_reconcile_interrupted_install_state_marks_error(db_path):
    await updates_mod._save_install_state(
        db_path,
        {
            "status": "installing",
            "progress": 0.66,
            "message": "Upgrading packages...",
            "target": "all",
            "installed_versions": {},
            "updated_at": "2026-03-08T00:00:00+00:00",
        },
    )

    state = await updates_mod.reconcile_interrupted_install_state(db_path)

    assert state["status"] == "error"
    assert state["progress"] == 0.0
    assert "interrupted" in state["message"].lower()


async def test_startup_reconciles_interrupted_install_state(aiohttp_client, db_path):
    await updates_mod._save_install_state(
        db_path,
        {
            "status": "installing",
            "progress": 0.66,
            "message": "Upgrading packages...",
            "target": "all",
            "installed_versions": {},
            "updated_at": "2026-03-08T00:00:00+00:00",
        },
    )

    app = create_app(db_path)
    client = await aiohttp_client(app)
    resp = await client.get("/api/v1/updates/status")

    assert resp.status == 200
    data = await resp.json()
    assert data["status"] == "error"
    assert "interrupted" in data["message"].lower()


async def test_install_flow_persists_progress_and_versions(client, db_path, monkeypatch):
    seen_states: list[tuple[str, float, str]] = []

    async def fake_exec(*cmd: str) -> int:
        state = await updates_mod._load_install_state(db_path)
        seen_states.append((" ".join(cmd), state["progress"], state["status"]))
        return 0

    mock_updates = AsyncMock(
        return_value={
            "pfm": {"current": __version__, "latest": "0.20.0", "update_available": False},
            "app": {"latest": "2.9"},
        }
    )

    monkeypatch.setattr(updates_mod, "_exec", fake_exec)
    monkeypatch.setattr(updates_mod, "_get_updates", mock_updates)

    resp = await client.post("/api/v1/updates/install", json={"target": "all"})

    assert resp.status == 202
    state = await _wait_for_status(db_path, "installed")
    assert state["progress"] == 1.0
    assert state["installed_versions"] == {"pfm": "0.20.0", "app": "2.9"}
    assert any(progress == 0.33 for _, progress, _ in seen_states)
    assert any(progress == 0.66 for _, progress, _ in seen_states)
