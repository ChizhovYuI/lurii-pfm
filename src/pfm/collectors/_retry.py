"""Retry and rate limiting utilities for collectors."""

from __future__ import annotations

import asyncio
import functools
import logging
import socket
import time
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

import httpx

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

logger = logging.getLogger(__name__)

P = ParamSpec("P")
T = TypeVar("T")

# Default retryable exceptions
RETRYABLE_EXCEPTIONS: tuple[type[Exception], ...] = (
    httpx.TransportError,
    httpx.TimeoutException,
)


def _is_dns_resolution_error(exc: Exception) -> bool:
    """Return True when an exception chain contains a DNS resolution failure."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        if isinstance(current, socket.gaierror):
            return True
        seen.add(id(current))
        next_exc = current.__cause__ or current.__context__
        current = next_exc if isinstance(next_exc, BaseException) else None
    return False


def retry(
    max_attempts: int = 3,
    backoff_base: float = 2.0,
    retryable: tuple[type[Exception], ...] = RETRYABLE_EXCEPTIONS,
) -> Callable[[Callable[P, Coroutine[Any, Any, T]]], Callable[P, Coroutine[Any, Any, T]]]:
    """Decorator for async functions with exponential backoff retry."""

    def decorator(func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, Coroutine[Any, Any, T]]:
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            last_exc: Exception | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except retryable as exc:
                    last_exc = exc
                    if _is_dns_resolution_error(exc):
                        logger.warning("DNS resolution failed for %s: %s. Not retrying.", func.__name__, exc)
                        raise
                    if attempt < max_attempts:
                        delay = backoff_base**attempt
                        logger.warning(
                            "Attempt %d/%d failed for %s: %s. Retrying in %.1fs",
                            attempt,
                            max_attempts,
                            func.__name__,
                            exc,
                            delay,
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.warning(
                            "All %d attempts failed for %s: %s",
                            max_attempts,
                            func.__name__,
                            exc,
                        )
            raise last_exc  # type: ignore[misc]

        return wrapper

    return decorator


class RateLimiter:
    """Simple token-bucket rate limiter."""

    def __init__(self, requests_per_minute: float) -> None:
        if requests_per_minute <= 0:
            msg = "requests_per_minute must be > 0"
            raise ValueError(msg)

        self._min_interval = 60.0 / requests_per_minute
        self._last_request: float = 0.0

    async def acquire(self) -> None:
        """Wait if needed to respect rate limit."""
        now = time.monotonic()
        elapsed = now - self._last_request
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)
        self._last_request = time.monotonic()
