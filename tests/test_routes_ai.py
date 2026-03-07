"""Tests for AI provider validation endpoints."""

from __future__ import annotations

import aiosqlite
import httpx
import pytest

from pfm.ai.base import CommentaryResult, LLMProvider, ProviderName
from pfm.ai.providers.registry import PROVIDER_REGISTRY
from pfm.db.ai_store import AIProviderStore
from pfm.db.models import init_db
from pfm.server.app import create_app


class _FakeGeminiValidationProvider(LLMProvider):
    name = "gemini"
    last_api_key = ""
    last_model = ""
    validation_error: Exception | None = None
    close_calls = 0

    def __init__(self, *, api_key: str, model: str | None = None) -> None:
        type(self).last_api_key = api_key
        type(self).last_model = model or ""

    async def validate_connection(self) -> None:
        if type(self).validation_error is not None:
            raise type(self).validation_error

    async def generate_commentary(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        max_output_tokens: int = 4096,
    ) -> CommentaryResult:
        return CommentaryResult(text="", model=None)

    async def close(self) -> None:
        type(self).close_calls += 1


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
def reset_fake_provider(monkeypatch):
    _FakeGeminiValidationProvider.last_api_key = ""
    _FakeGeminiValidationProvider.last_model = ""
    _FakeGeminiValidationProvider.validation_error = None
    _FakeGeminiValidationProvider.close_calls = 0
    monkeypatch.setitem(PROVIDER_REGISTRY, ProviderName.gemini, _FakeGeminiValidationProvider)


async def test_validate_ai_provider_success_with_saved_secret(client, db_path):
    store = AIProviderStore(db_path)
    await store.add("gemini", api_key="saved-gemini-key", model="gemini-2.5-flash", active=False)

    resp = await client.post("/api/v1/ai/providers/gemini/validate", json={})

    assert resp.status == 200
    data = await resp.json()
    assert data == {"ok": True, "message": "Connection successful."}
    assert _FakeGeminiValidationProvider.last_api_key == "saved-gemini-key"
    assert _FakeGeminiValidationProvider.last_model == "gemini-2.5-flash"
    assert _FakeGeminiValidationProvider.close_calls == 1

    async with aiosqlite.connect(str(db_path)) as db:
        row = await (
            await db.execute("SELECT api_key, model, base_url, active FROM ai_providers WHERE type = 'gemini'")
        ).fetchone()

    assert row == ("saved-gemini-key", "gemini-2.5-flash", "", 0)


async def test_validate_ai_provider_invalid_input(client, db_path):
    resp = await client.post("/api/v1/ai/providers/gemini/validate", json={})

    assert resp.status == 400
    data = await resp.json()
    assert "Missing required field: api_key" in data["error"]

    async with aiosqlite.connect(str(db_path)) as db:
        count = (await (await db.execute("SELECT COUNT(*) FROM ai_providers")).fetchone())[0]

    assert count == 0


async def test_validate_ai_provider_unreachable_returns_503(client, db_path):
    _FakeGeminiValidationProvider.validation_error = httpx.ConnectError("connection refused")

    resp = await client.post(
        "/api/v1/ai/providers/gemini/validate",
        json={"api_key": "temporary-key"},
    )

    assert resp.status == 503
    data = await resp.json()
    assert "Unable to reach service" in data["error"]
    assert _FakeGeminiValidationProvider.close_calls == 1

    async with aiosqlite.connect(str(db_path)) as db:
        count = (await (await db.execute("SELECT COUNT(*) FROM ai_providers")).fetchone())[0]

    assert count == 0
