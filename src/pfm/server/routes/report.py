"""Report notification REST endpoint."""

from __future__ import annotations

import logging

from aiohttp import web

from pfm.server.serializers import parse_cached_ai_commentary, parse_net_worth_usd

logger = logging.getLogger(__name__)

routes = web.RouteTableDef()


@routes.post("/api/v1/report/notify")
async def send_report_notify(request: web.Request) -> web.Response:
    """Format and send the Telegram report for the latest analytics."""
    from pfm.ai import AnalyticsSummary
    from pfm.reporting import format_weekly_report, is_telegram_configured, send_report

    db_path = request.app["db_path"]
    repo = request.app["repo"]

    if not await is_telegram_configured(db_path=db_path):
        return web.json_response({"error": "Telegram is not configured"}, status=400)

    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    report_date = latest[0].date
    metrics = await repo.get_analytics_metrics_by_date(report_date)

    required = (
        "net_worth",
        "allocation_by_asset",
        "allocation_by_source",
        "allocation_by_category",
        "currency_exposure",
        "risk_metrics",
        "pnl",
        "weekly_pnl_by_asset",
    )
    missing = [m for m in required if m not in metrics]
    if missing:
        return web.json_response(
            {"error": f"Missing analytics: {', '.join(missing)}"},
            status=400,
        )

    analytics = AnalyticsSummary(
        as_of_date=report_date,
        net_worth_usd=parse_net_worth_usd(metrics["net_worth"]),
        allocation_by_asset=metrics["allocation_by_asset"],
        allocation_by_source=metrics["allocation_by_source"],
        allocation_by_category=metrics["allocation_by_category"],
        currency_exposure=metrics["currency_exposure"],
        risk_metrics=metrics["risk_metrics"],
        pnl=metrics["pnl"],
        weekly_pnl_by_asset=metrics["weekly_pnl_by_asset"],
    )

    commentary = parse_cached_ai_commentary(metrics.get("ai_commentary"))
    if not commentary:
        commentary = "AI commentary is not cached. Run 'pfm comment' to generate."

    report_payload = format_weekly_report(analytics, commentary)
    sent = await send_report(report_payload)

    if sent:
        return web.json_response({"sent": True})
    return web.json_response({"error": "Failed to send report"}, status=500)
