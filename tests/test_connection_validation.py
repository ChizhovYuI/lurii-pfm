"""Tests for read-only connection validation helpers."""

from __future__ import annotations

import httpx
import pytest

from pfm.collectors import COLLECTOR_REGISTRY
from pfm.collectors.base import BaseCollector
from pfm.db.models import init_db
from pfm.db.source_store import SourceStore
from pfm.server.connection_validation import (
    ConnectionValidationError,
    _extract_http_message,
    _extract_payload_message,
    _find_message,
    _map_source_error,
    _normalize_string_dict,
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
    monkeypatch.setitem(COLLECTOR_REGISTRY, "wise", _FakeWiseValidationCollector)


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
