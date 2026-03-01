"""Application settings REST endpoints."""

from __future__ import annotations

import inspect
from typing import Any

import aiosqlite
from aiohttp import web

from pfm.ai.providers.registry import PROVIDER_REGISTRY
from pfm.db.ai_store import AIProviderStore
from pfm.server.serializers import mask_secret

routes = web.RouteTableDef()

_SECRET_KEYS: frozenset[str] = frozenset(
    {
        "telegram_bot_token",
        "gemini_api_key",
        "ai_provider_api_key",
    }
)

# Fields that every provider *could* have; order matters for UI rendering.
_PROVIDER_FIELDS = ("api_key", "model", "base_url")

# Fields whose values must be masked in API responses.
_SECRET_FIELDS: frozenset[str] = frozenset({"api_key"})


def _build_ai_providers_meta() -> list[dict[str, Any]]:
    """Build provider descriptors from the registry for UI rendering."""
    providers: list[dict[str, Any]] = []
    for name, cls in sorted(PROVIDER_REGISTRY.items(), key=lambda kv: str(kv[0])):
        sig = inspect.signature(cls.__init__)
        params = sig.parameters

        fields: list[dict[str, Any]] = []
        for field_name in _PROVIDER_FIELDS:
            if field_name not in params:
                continue
            p = params[field_name]
            required = p.default is inspect.Parameter.empty
            field_info: dict[str, Any] = {
                "name": field_name,
                "required": required,
                "secret": field_name in _SECRET_FIELDS,
            }
            # Expose class-level defaults (e.g. default_model, default_base_url)
            default_attr = f"default_{field_name}"
            cls_default = getattr(cls, default_attr, None)
            if cls_default:
                field_info["default"] = cls_default
            # Expose model options list if available
            if field_name == "model":
                models = getattr(cls, "models", None)
                if models:
                    field_info["options"] = list(models)
            fields.append(field_info)

        providers.append({"type": str(name), "fields": fields})
    return providers


# Cache since provider registry is static after import
_AI_PROVIDERS_META: list[dict[str, Any]] | None = None


def _get_ai_providers_meta() -> list[dict[str, Any]]:
    global _AI_PROVIDERS_META  # noqa: PLW0603
    if _AI_PROVIDERS_META is None:
        _AI_PROVIDERS_META = _build_ai_providers_meta()
    return _AI_PROVIDERS_META


@routes.get("/api/v1/settings")
async def get_settings(request: web.Request) -> web.Response:
    """Read all settings including AI provider configurations."""
    db_path = request.app["db_path"]

    async with aiosqlite.connect(str(db_path)) as db:
        rows = await (await db.execute("SELECT key, value FROM app_settings")).fetchall()
    settings_dict: dict[str, Any] = {
        str(row[0]): mask_secret(str(row[1])) if str(row[0]) in _SECRET_KEYS else str(row[1]) for row in rows
    }

    # AI providers: configured instances + static metadata
    store = AIProviderStore(db_path)
    configured = await store.list_all()

    # Build a {type: fields_meta} lookup from provider metadata
    meta = _get_ai_providers_meta()
    fields_by_type: dict[str, list[dict[str, Any]]] = {m["type"]: m["fields"] for m in meta}

    providers_list: list[dict[str, Any]] = []
    for p in configured:
        entry: dict[str, Any] = {"type": p.type, "active": p.active}
        for field_meta in fields_by_type.get(p.type, []):
            name = field_meta["name"]
            value = getattr(p, name, "")
            is_secret = field_meta.get("secret", False)
            if is_secret:
                entry[name] = mask_secret(value) if value else ""
            else:
                entry[name] = value
        providers_list.append(entry)

    settings_dict["ai_providers"] = providers_list

    # All available provider types (for the "add provider" combo box)
    settings_dict["ai_providers_available"] = _get_ai_providers_meta()

    return web.json_response(settings_dict)


@routes.put("/api/v1/settings")
async def update_settings(request: web.Request) -> web.Response:
    """Update key-value settings in app_settings table."""
    db_path = request.app["db_path"]
    body: dict[str, Any] = await request.json()

    if not body:
        return web.json_response({"error": "Request body must be a non-empty JSON object"}, status=400)

    async with aiosqlite.connect(str(db_path)) as db:
        for key, value in body.items():
            await db.execute(
                "INSERT INTO app_settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, "
                "updated_at = datetime('now')",
                (key, str(value)),
            )
        await db.commit()

    return web.json_response({"updated": True})
