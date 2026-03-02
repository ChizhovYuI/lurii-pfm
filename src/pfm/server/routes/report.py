"""Report notification REST endpoint."""

from __future__ import annotations

import logging

from aiohttp import web

from pfm.server.serializers import parse_cached_ai_commentary

logger = logging.getLogger(__name__)

routes = web.RouteTableDef()


@routes.post("/api/v1/report/notify")
async def send_report_notify(request: web.Request) -> web.Response:
    """Format and send the Telegram report for the latest analytics."""
    from pfm.reporting import format_weekly_report, is_telegram_configured, send_report
    from pfm.server.analytics_helper import build_analytics_summary

    db_path = request.app["db_path"]
    repo = request.app["repo"]

    if not await is_telegram_configured(db_path=db_path):
        return web.json_response({"error": "Telegram is not configured"}, status=400)

    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    report_date = max(s.date for s in latest)
    analytics = await build_analytics_summary(repo, report_date, db_path=db_path)

    metrics = await repo.get_analytics_metrics_by_date(report_date)
    commentary = parse_cached_ai_commentary(metrics.get("ai_commentary"))
    if not commentary:
        commentary = "AI commentary is not cached. Run 'pfm comment' to generate."

    report_payload = format_weekly_report(analytics, commentary)
    sent = await send_report(report_payload, db_path=db_path)

    if sent:
        return web.json_response({"sent": True})
    return web.json_response({"error": "Failed to send report"}, status=500)
