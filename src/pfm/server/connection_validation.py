"""Read-only connection validation for sources."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import httpx

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
