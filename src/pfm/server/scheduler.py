"""Daily collection scheduler — runs as a background asyncio task."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, time, timedelta
from typing import TYPE_CHECKING

from pfm.server.state import get_runtime_state

if TYPE_CHECKING:
    from aiohttp import web

logger = logging.getLogger(__name__)

_TARGET_TIME = time(0, 5, tzinfo=UTC)  # 00:05 UTC
_INTERNET_CHECK_HOST = "api.coingecko.com"
_INTERNET_RETRY_DELAY = 300  # 5 minutes
_INTERNET_MAX_RETRIES = 12  # 1 hour total


def _seconds_until(target: time) -> float:
    """Return seconds from now until the next occurrence of *target* (UTC)."""
    now = datetime.now(tz=UTC)
    today_target = datetime.combine(now.date(), target)
    if today_target <= now:
        today_target += timedelta(days=1)
    return (today_target - now).total_seconds()


async def _check_internet() -> bool:
    """Return True if internet is reachable (HTTP HEAD probe)."""
    import httpx

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.head(f"https://{_INTERNET_CHECK_HOST}/api/v3/ping")
            return resp.status_code < 500  # noqa: PLR2004
    except (httpx.TransportError, OSError):
        return False


async def _wait_for_internet() -> bool:
    """Wait up to 1 hour for internet. Return True if connected, False if timed out."""
    for attempt in range(_INTERNET_MAX_RETRIES):
        if await _check_internet():
            if attempt > 0:
                logger.info("Internet restored after %d retries", attempt)
            return True
        logger.warning("No internet — retry %d/%d in %ds", attempt + 1, _INTERNET_MAX_RETRIES, _INTERNET_RETRY_DELAY)
        await asyncio.sleep(_INTERNET_RETRY_DELAY)
    return False


async def run_daily_collector(app: web.Application) -> None:
    """Sleep until 00:05, trigger collection, repeat."""
    from pfm.server.routes.collect import _run_collection

    logger.info("Scheduler started — daily collection at %s UTC", _TARGET_TIME.strftime("%H:%M"))
    state = get_runtime_state(app)

    try:
        while True:
            delay = _seconds_until(_TARGET_TIME)
            logger.info("Next collection in %.0f s", delay)
            await asyncio.sleep(delay)

            if state.collecting:
                logger.warning("Scheduled collection skipped — already running")
                continue

            if not await _wait_for_internet():
                logger.error("No internet after %d retries — skipping today's collection", _INTERNET_MAX_RETRIES)
                continue

            logger.info("Scheduled daily collection started")
            state.collecting = True
            try:
                await _run_collection(app, source_name=None)
            except Exception:
                # _run_collection handles its own errors; this catches
                # failures before its try/finally (e.g. import errors).
                logger.exception("Scheduled collection failed")
                state.collecting = False
    except asyncio.CancelledError:
        logger.info("Scheduler stopped")
