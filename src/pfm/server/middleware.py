"""Server middlewares for local-only access and error handling."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from aiohttp import web

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)

_LOCAL_ADDRS = frozenset({"127.0.0.1", "::1"})


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
