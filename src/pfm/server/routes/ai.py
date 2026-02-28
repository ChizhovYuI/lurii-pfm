"""AI commentary and provider configuration REST endpoints."""

from __future__ import annotations

import json
import logging
from typing import Any

from aiohttp import web

from pfm.db.ai_store import AIProviderStore
from pfm.server.serializers import mask_secret

logger = logging.getLogger(__name__)

routes = web.RouteTableDef()


@routes.get("/api/v1/ai/commentary")
async def get_commentary(request: web.Request) -> web.Response:
    """Read cached AI commentary for the latest snapshot date."""
    repo = request.app["repo"]
    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    analysis_date = latest[0].date
    metrics = await repo.get_analytics_metrics_by_date(analysis_date)
    raw = metrics.get("ai_commentary")
    if raw is None:
        return web.json_response({"error": "No AI commentary cached"}, status=404)

    parsed = json.loads(raw)
    return web.json_response(
        {
            "date": analysis_date.isoformat(),
            "text": parsed.get("text", ""),
            "model": parsed.get("model"),
        }
    )


@routes.post("/api/v1/ai/commentary")
async def generate_commentary(request: web.Request) -> web.Response:
    """Generate AI commentary from latest analytics and cache it."""
    from pfm.ai import AnalyticsSummary, generate_commentary_with_model

    repo = request.app["repo"]
    db_path = request.app["db_path"]

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
            {"error": f"Missing analytics metrics: {', '.join(missing)}. Run analyze first."},
            status=400,
        )

    from pfm.server.serializers import parse_net_worth_usd

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

    result = await generate_commentary_with_model(analytics, db_path=db_path)

    metric_payload: dict[str, str] = {"text": result.text}
    if result.model:
        metric_payload["model"] = result.model

    await repo.save_analytics_metric(
        report_date,
        "ai_commentary",
        json.dumps(metric_payload),
    )

    return web.json_response(
        {
            "date": report_date.isoformat(),
            "text": result.text,
            "model": result.model,
        }
    )


# ── Legacy single-provider endpoints (backward compat) ──────────────


@routes.get("/api/v1/ai/config")
async def get_ai_config(request: web.Request) -> web.Response:
    """Return current active AI provider configuration (secrets masked)."""
    store = AIProviderStore(request.app["db_path"])
    config = await store.get_active()
    if config is None:
        return web.json_response({"configured": False})

    return web.json_response(
        {
            "configured": True,
            "provider": config.type,
            "model": config.model,
            "base_url": config.base_url,
            "api_key": mask_secret(config.api_key) if config.api_key else "",
        }
    )


@routes.put("/api/v1/ai/config")
async def update_ai_config(request: web.Request) -> web.Response:
    """Update AI provider configuration (sets as active)."""
    body: dict[str, Any] = await request.json()
    provider = body.get("provider")
    if not provider:
        return web.json_response({"error": "provider is required"}, status=400)

    store = AIProviderStore(request.app["db_path"])
    config = await store.add(
        provider,
        api_key=body.get("api_key", ""),
        model=body.get("model", ""),
        base_url=body.get("base_url", ""),
        activate=True,
    )

    return web.json_response(
        {
            "configured": True,
            "provider": config.type,
            "model": config.model,
            "base_url": config.base_url,
            "api_key": mask_secret(config.api_key) if config.api_key else "",
        }
    )


# ── Multi-provider endpoints ────────────────────────────────────────


@routes.get("/api/v1/ai/providers")
async def list_providers(request: web.Request) -> web.Response:
    """List all configured AI providers (secrets masked)."""
    store = AIProviderStore(request.app["db_path"])
    providers = await store.list_all()
    return web.json_response(
        {
            "providers": [
                {
                    "type": p.type,
                    "model": p.model,
                    "base_url": p.base_url,
                    "api_key": mask_secret(p.api_key) if p.api_key else "",
                    "active": p.active,
                }
                for p in providers
            ]
        }
    )


@routes.post("/api/v1/ai/providers/deactivate")
async def deactivate_provider(request: web.Request) -> web.Response:
    """Clear the active provider."""
    store = AIProviderStore(request.app["db_path"])
    changed = await store.deactivate()
    return web.json_response({"deactivated": changed})


@routes.put("/api/v1/ai/providers/{type}")
async def upsert_provider(request: web.Request) -> web.Response:
    """Add or update a provider configuration."""
    provider_type = request.match_info["type"]
    body: dict[str, Any] = await request.json()

    store = AIProviderStore(request.app["db_path"])
    config = await store.add(
        provider_type,
        api_key=body.get("api_key", ""),
        model=body.get("model", ""),
        base_url=body.get("base_url", ""),
        activate=body.get("activate", False),
    )

    return web.json_response(
        {
            "type": config.type,
            "model": config.model,
            "base_url": config.base_url,
            "api_key": mask_secret(config.api_key) if config.api_key else "",
            "active": config.active,
        }
    )


@routes.delete("/api/v1/ai/providers/{type}")
async def remove_provider(request: web.Request) -> web.Response:
    """Remove a provider configuration."""
    provider_type = request.match_info["type"]
    store = AIProviderStore(request.app["db_path"])
    deleted = await store.remove(provider_type)
    if not deleted:
        return web.json_response({"error": f"Provider '{provider_type}' not found"}, status=404)
    return web.json_response({"deleted": True})


@routes.post("/api/v1/ai/providers/{type}/activate")
async def activate_provider(request: web.Request) -> web.Response:
    """Set a provider as the active one."""
    provider_type = request.match_info["type"]
    store = AIProviderStore(request.app["db_path"])
    try:
        config = await store.activate(provider_type)
    except ValueError as exc:
        return web.json_response({"error": str(exc)}, status=404)

    return web.json_response(
        {
            "type": config.type,
            "model": config.model,
            "base_url": config.base_url,
            "api_key": mask_secret(config.api_key) if config.api_key else "",
            "active": config.active,
        }
    )
