"""AI commentary and provider configuration REST endpoints."""

from __future__ import annotations

import dataclasses
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

    analysis_date = max(s.date for s in latest)
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
    """Generate AI commentary from live-computed analytics and cache it."""
    from pfm.ai import generate_commentary_with_model
    from pfm.server.analytics_helper import build_analytics_summary

    repo = request.app["repo"]
    db_path = request.app["db_path"]

    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    report_date = max(s.date for s in latest)
    analytics = await build_analytics_summary(repo, report_date, db_path=db_path)

    result = await generate_commentary_with_model(analytics, db_path=db_path)

    metric_payload: dict[str, str] = {"text": result.text}
    if result.model:
        metric_payload["model"] = result.model

    await repo.save_analytics_metric(
        report_date,
        "ai_commentary",
        json.dumps(metric_payload),
    )

    response_data: dict[str, Any] = {
        "date": report_date.isoformat(),
        "text": result.text,
        "model": result.model,
    }
    if result.error:
        response_data["error"] = result.error

    return web.json_response(response_data)


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
    """Update AI provider configuration with merge semantics.

    Only fields present in the request body are updated; existing fields
    not included in the request are preserved.
    """
    body: dict[str, Any] = await request.json()
    provider = body.get("provider")
    if not provider:
        return web.json_response({"error": "provider is required"}, status=400)

    store = AIProviderStore(request.app["db_path"])
    existing = await store.get(provider)

    kwargs: dict[str, Any] = {k: v for k, v in dataclasses.asdict(existing).items() if k != "type"} if existing else {}
    for k, v in body.items():
        if k == "provider":
            continue
        kwargs[k] = v
    kwargs.setdefault("active", True)

    config = await store.add(provider, **kwargs)

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
    """Add or update a provider configuration with merge semantics."""
    provider_type = request.match_info["type"]
    body: dict[str, Any] = await request.json()

    store = AIProviderStore(request.app["db_path"])
    existing = await store.get(provider_type)

    kwargs: dict[str, Any] = {k: v for k, v in dataclasses.asdict(existing).items() if k != "type"} if existing else {}
    kwargs.update(body)

    config = await store.add(provider_type, **kwargs)

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
