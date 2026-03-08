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


def _seconds_until(target: time) -> float:
    """Return seconds from now until the next occurrence of *target* (UTC)."""
    now = datetime.now(tz=UTC)
    today_target = datetime.combine(now.date(), target)
    if today_target <= now:
        today_target += timedelta(days=1)
    return (today_target - now).total_seconds()


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
