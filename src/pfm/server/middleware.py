"""Server middlewares for local-only access and error handling."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from aiohttp import web

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)

_MAX_LOG_BODY = 1024

_LOCAL_ADDRS = frozenset({"127.0.0.1", "::1"})

# Paths that remain accessible when the database is locked.
_UNLOCKED_PATHS = frozenset({"/api/v1/health", "/api/v1/unlock", "/api/v1/encryption/status"})


@web.middleware
async def local_only_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    """Reject requests not originating from localhost."""
    if request.remote not in _LOCAL_ADDRS:
        raise web.HTTPForbidden(text="Access restricted to localhost")
    return await handler(request)


@web.middleware
async def db_locked_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    """Return 423 Locked for data endpoints when DB is locked."""
    if request.app.get("db_locked") and request.path not in _UNLOCKED_PATHS:
        return web.json_response({"error": "Database is locked"}, status=423)
    return await handler(request)


@web.middleware
async def api_logging_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    """Log API request and response details."""
    method = request.method
    path = request.path

    req_body = ""
    if method in {"POST", "PUT", "PATCH"}:
        raw = await request.read()
        req_body = raw[:_MAX_LOG_BODY].decode("utf-8", errors="replace") if raw else ""

    start = time.monotonic()
    response = await handler(request)
    duration_ms = (time.monotonic() - start) * 1000

    resp_body = ""
    if hasattr(response, "body") and response.body:
        resp_body = response.body[:_MAX_LOG_BODY].decode("utf-8", errors="replace")

    logger.info(
        "%s %s %d (%.0fms) | req=%s | resp=%s",
        method,
        path,
        response.status,
        duration_ms,
        req_body or "-",
        resp_body or "-",
    )
    return response


@web.middleware
async def error_handling_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    """Catch unhandled exceptions and return JSON 500."""
    try:
        return await handler(request)
    except web.HTTPException:
        raise
    except Exception:
        logger.exception("Unhandled error processing %s %s", request.method, request.path)
        return web.json_response({"error": "Internal server error"}, status=500)
