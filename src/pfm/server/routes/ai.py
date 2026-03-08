"""AI commentary and provider configuration REST endpoints."""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
from typing import TYPE_CHECKING, Any

from aiohttp import web

from pfm.ai.base import flatten_sections
from pfm.ai.commentary_parser import parse_commentary_sections
from pfm.ai.prompts import REPORT_PROMPT_VERSION, REPORT_SECTION_SPECS
from pfm.db.ai_report_memory_store import AIReportMemoryStore, hash_ai_report_memory
from pfm.db.ai_store import AIProviderStore
from pfm.server.connection_validation import (
    ConnectionValidationError,
)
from pfm.server.connection_validation import (
    validate_ai_provider_connection as run_ai_provider_validation,
)
from pfm.server.serializers import mask_secret
from pfm.server.state import get_broadcaster, get_repo, get_runtime_state

if TYPE_CHECKING:
    from datetime import date

    from pfm.ai.base import CommentaryResult
    from pfm.db.repository import Repository

logger = logging.getLogger(__name__)

routes = web.RouteTableDef()


@routes.get("/api/v1/ai/commentary")
async def get_commentary(request: web.Request) -> web.Response:
    """Read cached AI commentary for the latest snapshot date."""
    repo = get_repo(request.app)
    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    analysis_date = max(s.date for s in latest)
    metrics = await repo.get_analytics_metrics_by_date(analysis_date)
    raw = metrics.get("ai_commentary")
    if raw is None:
        return web.json_response({"error": "No AI commentary cached"}, status=404)

    parsed = json.loads(raw)
    text = parsed.get("text", "")
    sections = parsed.get("sections", [])
    if (not isinstance(sections, list) or not sections) and isinstance(text, str):
        recovered = parse_commentary_sections(text)
        if recovered:
            text = flatten_sections(recovered)
            sections = [{"title": section.title, "description": section.description} for section in recovered]

    current_memory = await AIReportMemoryStore(request.app["db_path"]).get()
    current_memory_hash = hash_ai_report_memory(current_memory)
    cached_memory_hash = parsed.get("memory_hash", "")
    prompt_version = parsed.get("prompt_version")
    stale = bool(prompt_version == REPORT_PROMPT_VERSION and cached_memory_hash != current_memory_hash)
    stale_reason = "AI report was generated before the report memory was updated." if stale else None

    payload = {
        "date": analysis_date.isoformat(),
        "text": text,
        "model": parsed.get("model"),
        "sections": sections if isinstance(sections, list) else [],
        "stale": stale,
        "stale_reason": stale_reason,
    }
    if isinstance(parsed.get("generation_meta"), dict):
        payload["generation_meta"] = parsed["generation_meta"]

    return web.json_response(payload)


@routes.get("/api/v1/ai/commentary/status")
async def commentary_status(request: web.Request) -> web.Response:
    """Return whether AI commentary generation is in progress."""
    state = get_runtime_state(request.app)
    return web.json_response(
        {
            "generating": state.generating_commentary,
            "completed_sections": state.commentary_completed_sections,
            "total_sections": state.commentary_total_sections,
            "current_section": state.commentary_current_section,
            "strategy": state.commentary_strategy,
            "last_error": state.commentary_last_error,
        }
    )


@routes.post("/api/v1/ai/commentary")
async def generate_commentary(request: web.Request) -> web.Response:
    """Spawn background AI commentary generation. Returns 202 immediately."""
    state = get_runtime_state(request.app)
    if state.generating_commentary:
        return web.json_response({"error": "Commentary generation already in progress"}, status=409)

    repo = get_repo(request.app)
    latest = await repo.get_latest_snapshots()
    if not latest:
        return web.json_response({"error": "No snapshots available"}, status=404)

    active_provider = await AIProviderStore(request.app["db_path"]).get_active()
    strategy = _commentary_strategy_for_provider(active_provider)
    is_single_shot = strategy != "section_by_section"
    state.generating_commentary = True
    state.commentary_completed_sections = 0
    state.commentary_total_sections = 1 if is_single_shot else len(REPORT_SECTION_SPECS)
    state.commentary_current_section = (
        "Weekly Report" if is_single_shot else (REPORT_SECTION_SPECS[0].title if REPORT_SECTION_SPECS else None)
    )
    state.commentary_strategy = strategy
    state.commentary_last_error = None
    task = asyncio.create_task(_run_commentary(request.app))
    state.commentary_task = task

    return web.json_response({"status": "started"}, status=202)


async def _run_commentary(app: web.Application) -> None:
    """Background task: generate AI commentary, cache it, broadcast result."""
    from pfm.ai import generate_commentary_with_model
    from pfm.server.analytics_helper import build_analytics_summary

    state = get_runtime_state(app)
    repo = get_repo(app)
    db_path = app["db_path"]
    broadcaster = get_broadcaster(app)

    try:
        await broadcaster.broadcast(
            {
                "type": "commentary_started",
                "completed_sections": 0,
                "total_sections": state.commentary_total_sections,
                "current_section": state.commentary_current_section,
                "strategy": state.commentary_strategy,
            }
        )

        latest = await repo.get_latest_snapshots()
        report_date = max(s.date for s in latest)

        analytics = await build_analytics_summary(repo, report_date, db_path=db_path)
        memory = await AIReportMemoryStore(db_path).get()

        async def _report_progress(completed_sections: int, total_sections: int, current_section: str) -> None:
            state.commentary_completed_sections = completed_sections
            state.commentary_total_sections = total_sections
            state.commentary_current_section = current_section
            await broadcaster.broadcast(
                {
                    "type": "commentary_progress",
                    "completed_sections": completed_sections,
                    "total_sections": total_sections,
                    "current_section": current_section,
                    "strategy": state.commentary_strategy,
                }
            )

        result = await generate_commentary_with_model(
            analytics,
            db_path=db_path,
            progress_callback=_report_progress,
            investor_memory=memory,
        )
        await _handle_commentary_result(
            app=app,
            report_date=report_date,
            memory=memory,
            result=result,
        )

    except Exception as exc:
        logger.exception("AI commentary generation failed")
        state.commentary_last_error = str(exc)
        await broadcaster.broadcast(
            {
                "type": "commentary_failed",
                "error": str(exc),
                "strategy": state.commentary_strategy,
                "last_error": state.commentary_last_error,
            }
        )
    finally:
        state.generating_commentary = False
        state.commentary_task = None
        state.commentary_completed_sections = 0
        state.commentary_total_sections = 0
        state.commentary_current_section = None
        if state.commentary_last_error is None:
            state.commentary_strategy = None


async def _handle_commentary_result(
    *,
    app: web.Application,
    report_date: date,
    memory: str,
    result: CommentaryResult,
) -> None:
    repo = get_repo(app)
    broadcaster = get_broadcaster(app)
    state = get_runtime_state(app)
    generation_meta = result.generation_meta if isinstance(result.generation_meta, dict) else {}
    strategy_value = generation_meta.get("strategy")
    strategy = strategy_value if isinstance(strategy_value, str) else None
    state.commentary_strategy = strategy

    if not result.sections:
        state.commentary_last_error = result.error or "AI commentary generation failed."
        await broadcaster.broadcast(
            {
                "type": "commentary_failed",
                "strategy": strategy,
                "error": state.commentary_last_error,
                "reason": generation_meta.get("reason"),
                "last_error": state.commentary_last_error,
            }
        )
        return

    sections_dicts = [{"title": s.title, "description": s.description} for s in result.sections]
    await _save_commentary_metric(repo, report_date, memory, result, sections_dicts)
    state.commentary_last_error = None

    event: dict[str, Any] = {
        "type": "commentary_completed",
        "date": report_date.isoformat(),
        "text": result.text,
        "model": result.model,
        "sections": sections_dicts,
        "strategy": strategy,
    }
    if result.error:
        event["error"] = result.error
    if isinstance(result.generation_meta, dict):
        event["generation_meta"] = result.generation_meta

    await broadcaster.broadcast(event)


async def _save_commentary_metric(
    repo: Repository,
    report_date: date,
    memory: str,
    result: CommentaryResult,
    sections_dicts: list[dict[str, str]],
) -> None:
    metric_payload: dict[str, Any] = {"text": result.text}
    if result.model:
        metric_payload["model"] = result.model
    if sections_dicts:
        metric_payload["sections"] = sections_dicts
    if isinstance(result.generation_meta, dict):
        metric_payload["generation_meta"] = result.generation_meta
    metric_payload["prompt_version"] = REPORT_PROMPT_VERSION
    metric_payload["memory_hash"] = hash_ai_report_memory(memory)

    await repo.save_analytics_metric(
        report_date,
        "ai_commentary",
        json.dumps(metric_payload),
    )


def _commentary_strategy_for_provider(provider: object | None) -> str:
    if provider is None:
        return "section_by_section"
    provider_type = getattr(provider, "type", None)
    model = (getattr(provider, "model", None) or "").strip()
    if provider_type == "deepseek" and model == "deepseek-chat":
        return "deepseek_json_single_shot"
    if provider_type == "gemini":
        return "gemini_json_single_shot"
    return "section_by_section"


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


@routes.post("/api/v1/ai/providers/{type}/validate")
async def validate_provider(request: web.Request) -> web.Response:
    """Validate a provider configuration without saving it."""
    provider_type = request.match_info["type"]
    body: dict[str, Any] = await request.json()

    try:
        message = await run_ai_provider_validation(
            str(request.app["db_path"]),
            provider_type=provider_type,
            fields=body,
        )
    except ConnectionValidationError as exc:
        return web.json_response({"error": exc.message}, status=exc.status_code)

    return web.json_response({"ok": True, "message": message})


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
