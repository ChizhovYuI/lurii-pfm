"""Tests for read-only connection validation helpers."""

from __future__ import annotations

import aiosqlite
import httpx
import pytest
from google.genai import errors as genai_errors
from openai import APIConnectionError, APIStatusError

from pfm.ai.base import CommentaryResult, LLMProvider, ProviderName
from pfm.ai.providers.registry import PROVIDER_REGISTRY
from pfm.collectors import COLLECTOR_REGISTRY
from pfm.collectors.base import BaseCollector
from pfm.db.ai_store import AIProviderStore
from pfm.db.models import init_db
from pfm.db.source_store import SourceStore
from pfm.server.connection_validation import (
    ConnectionValidationError,
    _build_provider_kwargs,
    _extract_http_message,
    _extract_payload_message,
    _find_message,
    _load_saved_provider_fields,
    _map_ai_error,
    _map_source_error,
    _normalize_string_dict,
    _resolve_provider_class,
    validate_ai_provider_connection,
    validate_source_connection,
)


class _FakeWiseValidationCollector(BaseCollector):
    source_name = "wise"
    last_api_token = ""
    close_calls = 0
    validation_error = None

    def __init__(self, pricing, *, api_token: str) -> None:
        super().__init__(pricing)
        type(self).last_api_token = api_token

    async def fetch_raw_balances(self):
        return []

    async def validate_connection(self) -> None:
        if type(self).validation_error is not None:
            raise type(self).validation_error

    async def fetch_transactions(self, since=None):
        return []

    async def close(self) -> None:
        type(self).close_calls += 1


class _FakeGeminiValidationProvider(LLMProvider):
    name = "gemini"
    last_api_key = ""
    last_model = ""
    validation_error = None
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


@pytest.fixture(autouse=True)
def reset_validation_fakes(monkeypatch):
    _FakeWiseValidationCollector.last_api_token = ""
    _FakeWiseValidationCollector.close_calls = 0
    _FakeWiseValidationCollector.validation_error = None
    _FakeGeminiValidationProvider.last_api_key = ""
    _FakeGeminiValidationProvider.last_model = ""
    _FakeGeminiValidationProvider.close_calls = 0
    _FakeGeminiValidationProvider.validation_error = None
    monkeypatch.setitem(COLLECTOR_REGISTRY, "wise", _FakeWiseValidationCollector)
    monkeypatch.setitem(PROVIDER_REGISTRY, ProviderName.gemini, _FakeGeminiValidationProvider)


async def test_validate_source_connection_requires_type_when_name_missing(db_path):
    with pytest.raises(ConnectionValidationError, match="type is required"):
        await validate_source_connection(db_path, source_name=None, source_type=None, credentials={})


async def test_validate_source_connection_rejects_unknown_type(db_path):
    with pytest.raises(ConnectionValidationError, match="Unknown source type"):
        await validate_source_connection(
            db_path,
            source_name=None,
            source_type="not-real",
            credentials={"token": "value"},
        )


async def test_validate_source_connection_requires_registered_collector(db_path, monkeypatch):
    monkeypatch.delitem(COLLECTOR_REGISTRY, "wise", raising=False)

    with pytest.raises(ConnectionValidationError, match="is not registered") as exc_info:
        await validate_source_connection(
            db_path,
            source_name=None,
            source_type="wise",
            credentials={"api_token": "validate-only-token"},
        )

    assert exc_info.value.status_code == 500


async def test_validate_source_connection_merges_saved_credentials_and_closes_collector(db_path):
    store = SourceStore(db_path)
    await store.add("wise-main", "wise", {"api_token": "saved-token"})

    message = await validate_source_connection(
        db_path,
        source_name="wise-main",
        source_type=None,
        credentials={},
    )

    assert message == "Connection successful."
    assert _FakeWiseValidationCollector.last_api_token == "saved-token"
    assert _FakeWiseValidationCollector.close_calls == 1


async def test_validate_source_connection_maps_http_errors(db_path):
    request = httpx.Request("GET", "https://example.com/validate")
    response = httpx.Response(401, request=request, json={"error": "bad token"})
    _FakeWiseValidationCollector.validation_error = httpx.HTTPStatusError(
        "unauthorized",
        request=request,
        response=response,
    )

    with pytest.raises(ConnectionValidationError, match="bad token") as exc_info:
        await validate_source_connection(
            db_path,
            source_name=None,
            source_type="wise",
            credentials={"api_token": "bad-token"},
        )

    assert exc_info.value.status_code == 400
    assert _FakeWiseValidationCollector.close_calls == 1


async def test_validate_ai_provider_connection_merges_saved_fields(db_path):
    store = AIProviderStore(db_path)
    await store.add("gemini", api_key="saved-key", model="gemini-2.5-flash", active=False)

    message = await validate_ai_provider_connection(
        db_path,
        provider_type="gemini",
        fields={},
    )

    assert message == "Connection successful."
    assert _FakeGeminiValidationProvider.last_api_key == "saved-key"
    assert _FakeGeminiValidationProvider.last_model == "gemini-2.5-flash"
    assert _FakeGeminiValidationProvider.close_calls == 1


async def test_validate_ai_provider_connection_rejects_unknown_provider(db_path):
    with pytest.raises(ConnectionValidationError, match="Unknown AI provider") as exc_info:
        await validate_ai_provider_connection(
            db_path,
            provider_type="made-up",
            fields={"api_key": "value"},
        )

    assert exc_info.value.status_code == 400


async def test_validate_ai_provider_connection_uses_saved_secret_with_override(db_path):
    store = AIProviderStore(db_path)
    await store.add("gemini", api_key="saved-key", model="gemini-2.5-pro", active=False)

    message = await validate_ai_provider_connection(
        db_path,
        provider_type="gemini",
        fields={"model": "gemini-2.5-flash"},
    )

    assert message == "Connection successful."
    assert _FakeGeminiValidationProvider.last_api_key == "saved-key"
    assert _FakeGeminiValidationProvider.last_model == "gemini-2.5-flash"


async def test_load_saved_provider_fields_handles_missing_table(tmp_path):
    db_path = tmp_path / "missing-table.db"

    fields = await _load_saved_provider_fields(db_path, "gemini")

    assert fields == {}


async def test_load_saved_provider_fields_returns_saved_values(db_path):
    async with aiosqlite.connect(str(db_path)) as db:
        await db.execute(
            "INSERT INTO ai_providers(type, api_key, model, base_url, active) VALUES (?, ?, ?, ?, ?)",
            ("gemini", "saved-key", "gemini-2.5-pro", "https://example.com", 0),
        )
        await db.commit()

    fields = await _load_saved_provider_fields(db_path, "gemini")

    assert fields == {
        "api_key": "saved-key",
        "model": "gemini-2.5-pro",
        "base_url": "https://example.com",
    }


def test_build_provider_kwargs_requires_missing_required_fields():
    kwargs = _build_provider_kwargs(_FakeGeminiValidationProvider, {"api_key": " key ", "model": " gemini "})
    assert kwargs == {"api_key": "key", "model": "gemini"}

    with pytest.raises(ConnectionValidationError, match="Missing required field: api_key"):
        _build_provider_kwargs(_FakeGeminiValidationProvider, {})


def test_resolve_provider_class_rejects_unregistered_provider(monkeypatch):
    monkeypatch.delitem(PROVIDER_REGISTRY, ProviderName.gemini, raising=False)

    with pytest.raises(ConnectionValidationError, match="is not registered"):
        _resolve_provider_class("gemini")


def test_normalize_string_dict_rejects_invalid_shapes():
    assert _normalize_string_dict(None) == {}
    assert _normalize_string_dict({"api_key": "  key  ", "model": None}) == {"api_key": "key", "model": ""}

    with pytest.raises(ConnectionValidationError, match="JSON object"):
        _normalize_string_dict(["not", "a", "dict"])

    with pytest.raises(ConnectionValidationError, match="field names must be strings"):
        _normalize_string_dict({1: "value"})


def test_map_source_error_covers_transport_value_and_unexpected():
    request = httpx.Request("GET", "https://example.com")
    response = httpx.Response(503, request=request, text="upstream unavailable")
    http_error = httpx.HTTPStatusError("failed", request=request, response=response)
    mapped_http = _map_source_error(http_error)
    assert mapped_http.message == "upstream unavailable"
    assert mapped_http.status_code == 503

    mapped_transport = _map_source_error(httpx.ConnectError("connection refused", request=request))
    assert "Unable to reach service" in mapped_transport.message
    assert mapped_transport.status_code == 503

    mapped_value = _map_source_error(ValueError("bad payload"))
    assert mapped_value.message == "bad payload"
    assert mapped_value.status_code == 400

    mapped_unexpected = _map_source_error(RuntimeError("boom"))
    assert mapped_unexpected.message == "Unexpected validation error."
    assert mapped_unexpected.status_code == 500


def test_map_ai_error_covers_openai_genai_and_passthrough():
    request = httpx.Request("GET", "https://example.com")
    response = httpx.Response(401, request=request, json={"error": {"message": "bad auth"}})

    passthrough = ConnectionValidationError("keep me", 400)
    assert _map_ai_error(passthrough) is passthrough

    connection_error = APIConnectionError(message="connection refused", request=request)
    mapped_connection = _map_ai_error(connection_error)
    assert "Unable to reach service" in mapped_connection.message
    assert mapped_connection.status_code == 503

    status_error = APIStatusError("bad auth", response=response, body=None)
    mapped_status = _map_ai_error(status_error)
    assert mapped_status.message == "bad auth"
    assert mapped_status.status_code == 400

    genai_error = genai_errors.APIError(
        503,
        {"error": {"message": "provider down"}},
        response=httpx.Response(503, request=request, json={"error": {"message": "provider down"}}),
    )
    mapped_genai = _map_ai_error(genai_error)
    assert mapped_genai.message == "provider down"
    assert mapped_genai.status_code == 503

    mapped_type = _map_ai_error(TypeError("missing config"))
    assert mapped_type.message == "missing config"
    assert mapped_type.status_code == 400

    mapped_unexpected = _map_ai_error(RuntimeError("boom"))
    assert mapped_unexpected.message == "Unexpected validation error."
    assert mapped_unexpected.status_code == 500


def test_extract_payload_messages_from_json_and_text():
    request = httpx.Request("GET", "https://example.com")
    nested_response = httpx.Response(
        400,
        request=request,
        json={"error": [{"detail": "first"}, {"message": "second"}]},
    )
    assert _extract_payload_message(nested_response) == "first"
    assert _extract_http_message(nested_response) == "first"

    text_response = httpx.Response(500, request=request, text="plain text failure")
    assert _extract_payload_message(text_response) == "plain text failure"

    long_response = httpx.Response(500, request=request, text="x" * 201)
    assert _extract_payload_message(long_response) is None
    assert _extract_http_message(long_response) == "Service returned 500 Internal Server Error."


def test_find_message_supports_strings_dicts_and_lists():
    assert _find_message("  hello  ") == "hello"
    assert _find_message({"error_description": "bad"}) == "bad"
    assert _find_message([{"detail": ""}, {"message": "fallback"}]) == "fallback"
    assert _find_message({"nested": "ignored"}) is None
