"""USD-value backfill (valuation) background job and status endpoint.

Collection finishes as soon as snapshots and categorization are done. Valuing
freshly imported zero-USD rows is a slow, CoinGecko-bound step (serialized at
~2.1s/request), so it runs here as a detached background job that streams
``backfill_*`` events to the UI instead of blocking the collection banner.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from aiohttp import web

from pfm.analytics.usd_value_backfill import forward_fill_recent
from pfm.db.models import has_new_transactions
from pfm.server.state import get_broadcaster, get_pricing, get_repo, get_runtime_state

if TYPE_CHECKING:
    from pfm.db.models import CollectorResult

logger = logging.getLogger(__name__)

routes = web.RouteTableDef()


def maybe_start_valuation(app: web.Application, results: list[CollectorResult]) -> bool:
    """Start a valuation job when a collection imported new transactions."""
    if not has_new_transactions(results):
        return False
    return start_valuation_task(app)


def start_valuation_task(app: web.Application) -> bool:
    """Start a background USD-valuation job if none is active.

    Returns ``False`` (no-op) when a valuation is already running, so overlapping
    collects (e.g. a manual trigger during the 07:05 scheduled run) never spawn a
    second concurrent backfill — and when the daemon is shutting down, so we never
    spawn work against a repository that ``_on_cleanup`` is closing.
    """
    state = get_runtime_state(app)
    if state.valuing or state.shutting_down:
        return False

    state.valuing = True
    task = asyncio.create_task(_run_valuation(app))
    state.valuation_task = task
    return True


@routes.get("/api/v1/backfill/status")
async def backfill_status(request: web.Request) -> web.Response:
    """Return current valuation state."""
    return web.json_response({"valuing": get_runtime_state(request.app).valuing})


async def _run_valuation(app: web.Application) -> None:
    """Background task: value zero-USD rows, broadcasting progress events."""
    state = get_runtime_state(app)
    broadcaster = get_broadcaster(app)

    try:
        # repo/pricing are fetched inside the try so a getter raise (DB locked /
        # mid-shutdown) still hits the finally that clears state.valuing — a leak
        # there would wedge the valuing guard True and disable valuation forever.
        repo = get_repo(app)
        pricing = get_pricing(app)

        await broadcaster.broadcast({"type": "backfill_started"})

        async def _on_progress(scanned: int, total: int, valued: int) -> None:
            await broadcaster.broadcast(
                {"type": "backfill_progress", "current": scanned, "total": total, "valued": valued}
            )

        summary = await forward_fill_recent(repo, pricing, on_progress=_on_progress)
        logger.info("Background valuation complete: %s", summary)
        await broadcaster.broadcast(
            {"type": "backfill_completed", "valued": summary["updated"], "scanned": summary["scanned"]}
        )
    except Exception as exc:
        # Non-fatal: collection already succeeded; valuation drains over later collects.
        logger.exception("Background valuation failed")
        await broadcaster.broadcast({"type": "backfill_failed", "error": str(exc)})
    finally:
        state.valuing = False
        state.valuation_task = None
