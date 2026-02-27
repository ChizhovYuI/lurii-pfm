"""Retry and rate limiting utilities for collectors."""

from __future__ import annotations

import asyncio
import functools
import logging
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

    def __init__(
        self,
        requests_per_minute: float | None = None,
        *,
        requests_per_second: float | None = None,
    ) -> None:
        if requests_per_minute is not None and requests_per_second is not None:
            msg = "Provide either requests_per_minute or requests_per_second, not both."
            raise ValueError(msg)

        if requests_per_second is None:
            if requests_per_minute is None:
                msg = "requests_per_minute is required when requests_per_second is not provided."
                raise ValueError(msg)
            if requests_per_minute <= 0:
                msg = "requests_per_minute must be > 0"
                raise ValueError(msg)
            requests_per_second = requests_per_minute / 60.0
        elif requests_per_second <= 0:
            msg = "requests_per_second must be > 0"
            raise ValueError(msg)

        self._min_interval = 1.0 / requests_per_second
        self._last_request: float = 0.0

    async def acquire(self) -> None:
        """Wait if needed to respect rate limit."""
        now = time.monotonic()
        elapsed = now - self._last_request
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)
        self._last_request = time.monotonic()
