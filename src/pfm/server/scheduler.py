"""Daily collection scheduler — runs as a background asyncio task."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aiohttp import web

logger = logging.getLogger(__name__)

_TARGET_TIME = time(0, 5)  # 00:05 local


def _seconds_until(target: time) -> float:
    """Return seconds from now until the next occurrence of *target* today/tomorrow."""
    now = datetime.now()  # noqa: DTZ005 — intentionally wall-clock local
    today_target = datetime.combine(now.date(), target)
    if today_target <= now:
        today_target += timedelta(days=1)
    return (today_target - now).total_seconds()


async def run_daily_collector(app: web.Application) -> None:
    """Sleep until 00:05, trigger collection, repeat."""
    from pfm.server.routes.collect import _run_collection

    logger.info("Scheduler started — daily collection at %s", _TARGET_TIME)

    try:
        while True:
            delay = _seconds_until(_TARGET_TIME)
            logger.info("Next collection in %.0f s", delay)
            await asyncio.sleep(delay)

            if app["collecting"]:
                logger.warning("Scheduled collection skipped — already running")
                continue

            logger.info("Scheduled daily collection started")
            app["collecting"] = True
            try:
                await _run_collection(app, source_name=None)
            except Exception:
                # _run_collection handles its own errors; this catches
                # failures before its try/finally (e.g. import errors).
                logger.exception("Scheduled collection failed")
                app["collecting"] = False
    except asyncio.CancelledError:
        logger.info("Scheduler stopped")
