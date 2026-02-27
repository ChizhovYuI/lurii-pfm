"""Tests for retry and rate limiting utilities."""

import time

import httpx
import pytest

from pfm.collectors._retry import RateLimiter, retry


async def test_retry_succeeds_first_attempt():
    call_count = 0

    @retry(max_attempts=3)
    async def always_ok() -> str:
        nonlocal call_count
        call_count += 1
        return "ok"

    result = await always_ok()
    assert result == "ok"
    assert call_count == 1


async def test_retry_retries_on_transport_error():
    call_count = 0

    @retry(max_attempts=3, backoff_base=0.01)
    async def fail_twice() -> str:
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise httpx.TransportError("connection error")
        return "recovered"

    result = await fail_twice()
    assert result == "recovered"
    assert call_count == 3


async def test_retry_raises_after_max_attempts():
    call_count = 0

    @retry(max_attempts=2, backoff_base=0.01)
    async def always_fail() -> str:
        nonlocal call_count
        call_count += 1
        raise httpx.TimeoutException("timeout")

    with pytest.raises(httpx.TimeoutException, match="timeout"):
        await always_fail()

    assert call_count == 2


async def test_retry_does_not_retry_non_retryable():
    call_count = 0

    @retry(max_attempts=3)
    async def value_error() -> str:
        nonlocal call_count
        call_count += 1
        msg = "bad value"
        raise ValueError(msg)

    with pytest.raises(ValueError, match="bad value"):
        await value_error()

    assert call_count == 1


async def test_retry_custom_retryable():
    call_count = 0

    @retry(max_attempts=2, backoff_base=0.01, retryable=(ValueError,))
    async def custom_fail() -> str:
        nonlocal call_count
        call_count += 1
        msg = "custom"
        raise ValueError(msg)

    with pytest.raises(ValueError, match="custom"):
        await custom_fail()

    assert call_count == 2


async def test_rate_limiter_allows_first_request():
    limiter = RateLimiter(requests_per_minute=600.0)
    start = time.monotonic()
    await limiter.acquire()
    elapsed = time.monotonic() - start
    assert elapsed < 0.2


async def test_rate_limiter_throttles():
    limiter = RateLimiter(requests_per_minute=1200.0)  # 50ms interval
    await limiter.acquire()
    start = time.monotonic()
    await limiter.acquire()
    elapsed = time.monotonic() - start
    # Should wait ~50ms
    assert elapsed >= 0.04


def test_rate_limiter_rejects_invalid_args():
    with pytest.raises(ValueError, match="requests_per_minute must be > 0"):
        RateLimiter(requests_per_minute=0)
