"""Tests for AI routes and provider validation endpoints."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from unittest.mock import patch

import aiosqlite
import httpx
import pytest

from pfm.ai.base import CommentaryResult, LLMProvider, ProviderName
from pfm.ai.prompts import REPORT_PROMPT_VERSION
from pfm.ai.providers.registry import PROVIDER_REGISTRY
from pfm.db.ai_report_memory_store import AIReportMemoryStore, hash_ai_report_memory
from pfm.db.ai_store import AIProviderStore
from pfm.db.models import Snapshot, init_db
from pfm.server.app import create_app
from pfm.server.state import get_repo, get_runtime_state


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


async def _seed_snapshot(repo, snapshot_date: date) -> None:
    await repo.save_snapshots(
        [
            Snapshot(
                date=snapshot_date,
                source="wise",
                source_name="wise-main",
                asset="USD",
                amount=Decimal(100),
                usd_value=Decimal(100),
            )
        ]
    )


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


async def test_get_ai_commentary_recovers_sections_from_cached_text_when_sections_missing(client):
    repo = get_repo(client.app)
    snapshot_date = date(2024, 1, 15)
    await _seed_snapshot(repo, snapshot_date)

    truncated = (
        '[{"title": "Market Context", "description": "BTC at **$95k**."}, '
        '{"title": "Risk Alerts", "description": "High con'
    )
    await repo.save_analytics_metric(
        snapshot_date,
        "ai_commentary",
        json.dumps({"text": truncated, "model": "gemini-2.5-flash"}),
    )

    resp = await client.get("/api/v1/ai/commentary")

    assert resp.status == 200
    data = await resp.json()
    assert data["date"] == "2024-01-15"
    assert data["model"] == "gemini-2.5-flash"
    assert data["text"] == "## Market Context\n\nBTC at **$95k**.\n\n## Risk Alerts\n\nHigh con"
    assert data["sections"] == [
        {"title": "Market Context", "description": "BTC at **$95k**."},
        {"title": "Risk Alerts", "description": "High con"},
    ]
    assert data["stale"] is False
    assert data["stale_reason"] is None
    assert "generation_meta" not in data


async def test_get_ai_commentary_does_not_invent_sections_for_plain_text(client):
    repo = get_repo(client.app)
    snapshot_date = date(2024, 1, 16)
    await _seed_snapshot(repo, snapshot_date)

    text = "### Summary\n\n- BTC is strong\n- Reduce concentration"
    await repo.save_analytics_metric(
        snapshot_date,
        "ai_commentary",
        json.dumps({"text": text, "model": "gemini-2.5-flash"}),
    )

    resp = await client.get("/api/v1/ai/commentary")

    assert resp.status == 200
    data = await resp.json()
    assert data["text"] == text
    assert data["sections"] == []
    assert data["stale"] is False


async def test_get_ai_commentary_normalizes_inline_unordered_lists_in_sections(client):
    repo = get_repo(client.app)
    snapshot_date = date(2024, 1, 16)
    await _seed_snapshot(repo, snapshot_date)

    await repo.save_analytics_metric(
        snapshot_date,
        "ai_commentary",
        json.dumps(
            {
                "text": "ignored",
                "model": "gemini-2.5-flash",
                "sections": [
                    {
                        "title": "Market Context",
                        "description": "Sentence. - External flows - Internal conversions - Residual effects",
                    }
                ],
            }
        ),
    )

    resp = await client.get("/api/v1/ai/commentary")

    assert resp.status == 200
    data = await resp.json()
    assert data["sections"] == [
        {
            "title": "Market Context",
            "description": "Sentence.\n\n- External flows\n- Internal conversions\n- Residual effects",
        }
    ]
    assert data["text"] == (
        "## Market Context\n\nSentence.\n\n- External flows\n- Internal conversions\n- Residual effects"
    )


async def test_get_ai_commentary_normalizes_inline_numbered_lists_in_sections(client):
    repo = get_repo(client.app)
    snapshot_date = date(2024, 1, 16)
    await _seed_snapshot(repo, snapshot_date)

    await repo.save_analytics_metric(
        snapshot_date,
        "ai_commentary",
        json.dumps(
            {
                "text": "ignored",
                "model": "gemini-2.5-flash",
                "sections": [
                    {
                        "title": "Actionable Recommendations for Next 7 Days",
                        "description": "Intro: 1. First 2. Second 3. Third",
                    }
                ],
            }
        ),
    )

    resp = await client.get("/api/v1/ai/commentary")

    assert resp.status == 200
    data = await resp.json()
    assert data["sections"] == [
        {
            "title": "Actionable Recommendations for Next 7 Days",
            "description": "Intro:\n\n1. First\n2. Second\n3. Third",
        }
    ]
    assert data["text"] == ("## Actionable Recommendations for Next 7 Days\n\nIntro:\n\n1. First\n2. Second\n3. Third")


async def test_get_ai_commentary_marks_stale_when_memory_hash_changes(client):
    repo = get_repo(client.app)
    snapshot_date = date(2024, 1, 17)
    await _seed_snapshot(repo, snapshot_date)
    await AIReportMemoryStore(client.app["db_path"]).set("## Location & Expenses\nThailand")

    await repo.save_analytics_metric(
        snapshot_date,
        "ai_commentary",
        json.dumps(
            {
                "text": "Market Context\nAll good.",
                "sections": [{"title": "Market Context", "description": "All good."}],
                "model": "gemini-2.5-flash",
                "prompt_version": REPORT_PROMPT_VERSION,
                "memory_hash": hash_ai_report_memory("## Location & Expenses\nUK"),
            }
        ),
    )

    resp = await client.get("/api/v1/ai/commentary")

    assert resp.status == 200
    data = await resp.json()
    assert data["stale"] is True
    assert data["stale_reason"] == "AI report was generated before the report memory was updated."


async def test_get_ai_commentary_returns_generation_meta_when_present(client):
    repo = get_repo(client.app)
    snapshot_date = date(2024, 1, 18)
    await _seed_snapshot(repo, snapshot_date)

    await repo.save_analytics_metric(
        snapshot_date,
        "ai_commentary",
        json.dumps(
            {
                "text": "## Market Context\n\nGenerated body.",
                "model": "deepseek-chat",
                "sections": [{"title": "Market Context", "description": "Generated body."}],
                "generation_meta": {
                    "strategy": "deepseek_json_single_shot",
                    "provider": "deepseek",
                    "model": "deepseek-chat",
                    "status": "generated",
                    "finish_reason": "stop",
                    "attempts": 1,
                },
            }
        ),
    )

    resp = await client.get("/api/v1/ai/commentary")

    assert resp.status == 200
    data = await resp.json()
    assert data["generation_meta"]["provider"] == "deepseek"
    assert data["generation_meta"]["finish_reason"] == "stop"


async def test_commentary_status_returns_progress_fields(client):
    state = get_runtime_state(client.app)
    state.generating_commentary = True
    state.commentary_completed_sections = 2
    state.commentary_total_sections = 5
    state.commentary_current_section = "Risk Alerts"
    state.commentary_strategy = "section_by_section"
    state.commentary_last_error = "temporary problem"

    resp = await client.get("/api/v1/ai/commentary/status")

    assert resp.status == 200
    assert await resp.json() == {
        "generating": True,
        "completed_sections": 2,
        "total_sections": 5,
        "current_section": "Risk Alerts",
        "strategy": "section_by_section",
        "last_error": "temporary problem",
    }


async def test_failed_generation_does_not_overwrite_previous_cached_report(client):
    repo = get_repo(client.app)
    snapshot_date = date(2024, 1, 19)
    await _seed_snapshot(repo, snapshot_date)
    await repo.save_analytics_metric(
        snapshot_date,
        "ai_commentary",
        json.dumps(
            {
                "text": "## Market Context\n\nPrevious successful report.",
                "model": "deepseek-chat",
                "sections": [{"title": "Market Context", "description": "Previous successful report."}],
            }
        ),
    )

    async def _fake_generate(*_args, **_kwargs) -> CommentaryResult:
        return CommentaryResult(
            text="",
            model="deepseek-chat",
            provider="deepseek",
            error="JSON output was invalid after retry.",
            generation_meta={
                "strategy": "deepseek_json_single_shot",
                "provider": "deepseek",
                "model": "deepseek-chat",
                "status": "failed",
                "finish_reason": "stop",
                "attempts": 2,
                "reason": "invalid_json",
            },
        )

    with patch("pfm.ai.generate_commentary_with_model", new=_fake_generate):
        await client.post("/api/v1/ai/commentary")
        await get_runtime_state(client.app).commentary_task

    resp = await client.get("/api/v1/ai/commentary")
    assert resp.status == 200
    data = await resp.json()
    assert data["text"] == "## Market Context\n\nPrevious successful report."
    assert data["sections"] == [{"title": "Market Context", "description": "Previous successful report."}]

    status_resp = await client.get("/api/v1/ai/commentary/status")
    assert status_resp.status == 200
    status_data = await status_resp.json()
    assert status_data["last_error"] == "JSON output was invalid after retry."


async def test_gemini_generation_start_sets_single_shot_status(client, db_path):
    repo = get_repo(client.app)
    snapshot_date = date(2024, 1, 20)
    await _seed_snapshot(repo, snapshot_date)
    await AIProviderStore(db_path).add("gemini", api_key="gem-key", model="gemini-2.5-flash", active=True)

    async def _fake_generate(*_args, **_kwargs) -> CommentaryResult:
        return CommentaryResult(
            text="## Market Context\n\nGemini weekly report.",
            model="gemini-2.5-flash",
            provider="gemini",
            sections=(*(),),
        )

    with patch("pfm.ai.generate_commentary_with_model", new=_fake_generate):
        resp = await client.post("/api/v1/ai/commentary")
        assert resp.status == 202
        status_resp = await client.get("/api/v1/ai/commentary/status")
        status_data = await status_resp.json()
        assert status_data["strategy"] == "gemini_json_single_shot"
        assert status_data["total_sections"] == 1
        assert status_data["current_section"] == "Weekly Report"
        await get_runtime_state(client.app).commentary_task


async def test_failed_gemini_generation_does_not_overwrite_previous_cached_report(client):
    repo = get_repo(client.app)
    snapshot_date = date(2024, 1, 21)
    await _seed_snapshot(repo, snapshot_date)
    await repo.save_analytics_metric(
        snapshot_date,
        "ai_commentary",
        json.dumps(
            {
                "text": "## Market Context\n\nPrevious successful Gemini report.",
                "model": "gemini-2.5-flash",
                "sections": [{"title": "Market Context", "description": "Previous successful Gemini report."}],
            }
        ),
    )

    async def _fake_generate(*_args, **_kwargs) -> CommentaryResult:
        return CommentaryResult(
            text="",
            model="gemini-2.5-flash",
            provider="gemini",
            error="JSON output was invalid after retry.",
            generation_meta={
                "strategy": "gemini_json_single_shot",
                "provider": "gemini",
                "model": "gemini-2.5-flash",
                "status": "failed",
                "finish_reason": "STOP",
                "attempts": 2,
                "reason": "invalid_json",
            },
        )

    with patch("pfm.ai.generate_commentary_with_model", new=_fake_generate):
        await client.post("/api/v1/ai/commentary")
        await get_runtime_state(client.app).commentary_task

    resp = await client.get("/api/v1/ai/commentary")
    data = await resp.json()
    assert data["text"] == "## Market Context\n\nPrevious successful Gemini report."
    assert data["sections"] == [{"title": "Market Context", "description": "Previous successful Gemini report."}]

    status_resp = await client.get("/api/v1/ai/commentary/status")
    status_data = await status_resp.json()
    assert status_data["last_error"] == "JSON output was invalid after retry."
