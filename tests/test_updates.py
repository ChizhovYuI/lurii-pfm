"""Tests for the updates endpoint."""

from __future__ import annotations

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
    """Reset the module-level cache between tests."""
    updates_mod._cache["data"] = None
    updates_mod._cache["ts"] = 0.0


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


async def test_install_no_body_defaults_to_all(client):
    with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock:
        proc = AsyncMock()
        proc.communicate.return_value = (b"ok", b"")
        proc.returncode = 0
        mock.return_value = proc
        resp = await client.post("/api/v1/updates/install")

    assert resp.status == 202


async def test_install_returns_202(client):
    with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock:
        proc = AsyncMock()
        proc.communicate.return_value = (b"ok", b"")
        proc.returncode = 0
        mock.return_value = proc
        resp = await client.post("/api/v1/updates/install", json={"target": "pfm"})

    assert resp.status == 202
    data = await resp.json()
    assert data["status"] == "started"
