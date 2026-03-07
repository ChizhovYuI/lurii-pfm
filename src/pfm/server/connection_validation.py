"""Read-only connection validation for sources and AI providers."""

from __future__ import annotations

import inspect
import json
import logging
from typing import TYPE_CHECKING, Any

import aiosqlite
import httpx
from google.genai import errors as genai_errors
from openai import APIConnectionError, APIStatusError

from pfm.ai.base import LLMProvider, ProviderName
from pfm.ai.providers.registry import PROVIDER_REGISTRY
from pfm.collectors import COLLECTOR_REGISTRY
from pfm.collectors._retry import is_dns_resolution_error
from pfm.config import get_settings
from pfm.db.source_store import SourceNotFoundError, SourceStore
from pfm.pricing.coingecko import PricingService
from pfm.source_types import SOURCE_TYPES, validate_credentials

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

_BAD_REQUEST = 400
_NOT_FOUND = 404
_INTERNAL_SERVER_ERROR = 500
_SERVICE_UNAVAILABLE = 503
_SUCCESS_MESSAGE = "Connection successful."
_NETWORK_HINT = "Service access appears restricted from your current network or region. Try a VPN and retry."
_MAX_INLINE_ERROR_LENGTH = 200


class ConnectionValidationError(Exception):
    """A user-facing validation error with an associated HTTP status."""

    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code


async def validate_source_connection(
    db_path: str | Path,
    *,
    source_name: str | None,
    source_type: str | None,
    credentials: dict[str, Any] | None,
) -> str:
    """Validate source credentials without persisting anything."""
    merged_credentials = _normalize_string_dict(credentials)

    if source_name:
        store = SourceStore(db_path)
        try:
            source = await store.get(source_name)
        except SourceNotFoundError as exc:
            msg = f"Source {source_name!r} not found"
            raise ConnectionValidationError(msg, _NOT_FOUND) from exc

        source_type = source.type
        stored_credentials = _normalize_string_dict(json.loads(source.credentials))
        merged_credentials = {**stored_credentials, **merged_credentials}

    if not source_type:
        msg = "type is required when name is not provided"
        raise ConnectionValidationError(msg, _BAD_REQUEST)
    if source_type not in SOURCE_TYPES:
        msg = f"Unknown source type: {source_type!r}"
        raise ConnectionValidationError(msg, _BAD_REQUEST)

    errors = validate_credentials(source_type, merged_credentials)
    if errors:
        raise ConnectionValidationError("; ".join(errors), _BAD_REQUEST)

    collector_cls = COLLECTOR_REGISTRY.get(source_type)
    if collector_cls is None:
        msg = f"Collector for source type {source_type!r} is not registered"
        raise ConnectionValidationError(msg, _INTERNAL_SERVER_ERROR)

    settings = get_settings()
    pricing = PricingService(api_key=settings.coingecko_api_key, cache_db_path=None)
    collector = None
    try:
        collector = collector_cls(pricing, **merged_credentials)
        await collector.validate_connection()
    except Exception as exc:
        raise _map_source_error(exc) from exc
    else:
        return _SUCCESS_MESSAGE
    finally:
        if collector is not None:
            await collector.close()
        await pricing.close()


async def validate_ai_provider_connection(
    db_path: str | Path,
    *,
    provider_type: str,
    fields: dict[str, Any] | None,
) -> str:
    """Validate AI provider configuration without persisting anything."""
    cls = _resolve_provider_class(provider_type)
    merged_fields = {**(await _load_saved_provider_fields(db_path, provider_type)), **_normalize_string_dict(fields)}
    kwargs = _build_provider_kwargs(cls, merged_fields)

    provider = None
    try:
        provider = cls(**kwargs)
        await provider.validate_connection()
    except Exception as exc:
        raise _map_ai_error(exc) from exc
    else:
        return _SUCCESS_MESSAGE
    finally:
        if provider is not None:
            await provider.close()


def _resolve_provider_class(provider_type: str) -> type[LLMProvider]:
    try:
        provider_name = ProviderName(provider_type)
    except ValueError as exc:
        msg = f"Unknown AI provider: {provider_type!r}"
        raise ConnectionValidationError(msg, _BAD_REQUEST) from exc

    cls = PROVIDER_REGISTRY.get(provider_name)
    if cls is None:
        msg = f"AI provider {provider_type!r} is not registered"
        raise ConnectionValidationError(msg, _BAD_REQUEST)
    return cls


def _build_provider_kwargs(cls: type[LLMProvider], values: dict[str, str]) -> dict[str, str]:
    sig = inspect.signature(cls.__init__)
    kwargs: dict[str, str] = {}
    missing: list[str] = []

    for name, param in sig.parameters.items():
        if name == "self" or name in {"client", "raw_client", "openai_client", "http_client"}:
            continue
        if param.kind is inspect.Parameter.VAR_KEYWORD:
            continue

        value = values.get(name, "").strip()
        if value:
            kwargs[name] = value
            continue

        if param.default is inspect.Parameter.empty:
            missing.append(name)

    if missing:
        msg = "; ".join(f"Missing required field: {name}" for name in missing)
        raise ConnectionValidationError(msg, _BAD_REQUEST)

    return kwargs


async def _load_saved_provider_fields(db_path: str | Path, provider_type: str) -> dict[str, str]:
    try:
        async with aiosqlite.connect(str(db_path)) as db:
            row = await (
                await db.execute(
                    "SELECT api_key, model, base_url FROM ai_providers WHERE type = ?",
                    (provider_type,),
                )
            ).fetchone()
    except aiosqlite.Error:
        logger.debug("AI provider lookup skipped during validation.", exc_info=True)
        return {}

    if row is None:
        return {}

    return {
        "api_key": str(row[0] or ""),
        "model": str(row[1] or ""),
        "base_url": str(row[2] or ""),
    }


def _normalize_string_dict(raw: object | None) -> dict[str, str]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        msg = "Request field must be a JSON object"
        raise ConnectionValidationError(msg, _BAD_REQUEST)

    normalized: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(key, str):
            msg = "All field names must be strings"
            raise ConnectionValidationError(msg, _BAD_REQUEST)
        normalized[key] = "" if value is None else str(value).strip()
    return normalized


def _map_source_error(exc: Exception) -> ConnectionValidationError:
    if isinstance(exc, ConnectionValidationError):
        return exc
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        return ConnectionValidationError(_extract_http_message(exc.response), _map_upstream_status(status_code))
    if isinstance(exc, httpx.TransportError | OSError):
        return ConnectionValidationError(_transport_message(exc), _SERVICE_UNAVAILABLE)
    if isinstance(exc, TypeError | ValueError):
        return ConnectionValidationError(str(exc), _BAD_REQUEST)
    logger.exception("Unexpected source validation failure")
    return ConnectionValidationError("Unexpected validation error.", _INTERNAL_SERVER_ERROR)


def _map_ai_error(exc: Exception) -> ConnectionValidationError:
    if isinstance(exc, ConnectionValidationError):
        return exc
    if isinstance(exc, APIConnectionError):
        return ConnectionValidationError(_transport_message(exc), _SERVICE_UNAVAILABLE)
    if isinstance(exc, APIStatusError):
        upstream_status = getattr(exc, "status_code", _INTERNAL_SERVER_ERROR) or _INTERNAL_SERVER_ERROR
        response = getattr(exc, "response", None)
        upstream_message = _extract_http_message(response) if response is not None else str(exc)
        return ConnectionValidationError(upstream_message, _map_upstream_status(upstream_status))

    message: str
    status_code: int
    if isinstance(exc, genai_errors.APIError):
        status_code = getattr(exc, "code", _INTERNAL_SERVER_ERROR) or _INTERNAL_SERVER_ERROR
        message = _extract_genai_message(exc)
    elif isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        message = _extract_http_message(exc.response)
    elif isinstance(exc, httpx.TransportError | OSError):
        status_code = _SERVICE_UNAVAILABLE
        message = _transport_message(exc)
    elif isinstance(exc, TypeError | ValueError):
        status_code = _BAD_REQUEST
        message = str(exc)
    else:
        logger.exception("Unexpected AI validation failure")
        status_code = _INTERNAL_SERVER_ERROR
        message = "Unexpected validation error."

    final_status = (
        status_code
        if status_code in {_BAD_REQUEST, _SERVICE_UNAVAILABLE, _INTERNAL_SERVER_ERROR}
        else _map_upstream_status(status_code)
    )
    return ConnectionValidationError(message, final_status)


def _map_upstream_status(status_code: int) -> int:
    return _BAD_REQUEST if _BAD_REQUEST <= status_code < _INTERNAL_SERVER_ERROR else _SERVICE_UNAVAILABLE


def _transport_message(exc: Exception) -> str:
    if is_dns_resolution_error(exc):
        return _NETWORK_HINT
    detail = str(exc).strip()
    if detail:
        return f"Unable to reach service: {detail}"
    return "Unable to reach service."


def _extract_http_message(response: httpx.Response) -> str:
    detail = _extract_payload_message(response)
    if detail:
        return detail
    return f"Service returned {response.status_code} {response.reason_phrase}."


def _extract_payload_message(response: httpx.Response) -> str | None:
    try:
        payload = response.json()
    except ValueError:
        payload = None

    detail = _find_message(payload)
    if detail:
        return detail

    text = response.text.strip()
    if text and len(text) <= _MAX_INLINE_ERROR_LENGTH:
        return text
    return None


def _extract_genai_message(exc: genai_errors.APIError) -> str:
    response = getattr(exc, "response", None)
    if response is not None:
        message = _extract_payload_message(response)
        if message:
            return message
    detail = str(exc).strip()
    return detail or "AI provider request failed."


def _find_message(payload: object) -> str | None:
    if isinstance(payload, str):
        detail = payload.strip()
        return detail or None
    if isinstance(payload, dict):
        for key in ("error_description", "error", "message", "msg", "detail"):
            value = payload.get(key)
            nested_detail = _find_message(value)
            if nested_detail:
                return nested_detail
    if isinstance(payload, list):
        for item in payload:
            nested_detail = _find_message(item)
            if nested_detail:
                return nested_detail
    return None
