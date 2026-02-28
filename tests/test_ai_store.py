"""Tests for AIProviderStore (multi-provider AI configuration)."""

from __future__ import annotations

import pytest

from pfm.db.ai_store import AIProviderStore
from pfm.db.gemini_store import GeminiStore
from pfm.db.models import AIProvider, init_db


@pytest.fixture
async def db_path(tmp_path):
    path = tmp_path / "test.db"
    await init_db(path)
    return path


# ── CRUD: add / get / list_all / upsert ─────────────────────────────


async def test_add_and_get(db_path):
    store = AIProviderStore(db_path)
    result = await store.add("ollama", model="llama3.1:8b")
    assert result.type == "ollama"
    assert result.model == "llama3.1:8b"
    assert result.api_key == ""
    assert result.active is False

    loaded = await store.get("ollama")
    assert loaded is not None
    assert loaded.type == "ollama"
    assert loaded.model == "llama3.1:8b"


async def test_add_with_all_fields(db_path):
    store = AIProviderStore(db_path)
    result = await store.add(
        "openrouter",
        api_key="or-key",
        model="anthropic/claude-sonnet-4",
        base_url="https://openrouter.ai/api",
    )
    assert result.type == "openrouter"
    assert result.api_key == "or-key"
    assert result.model == "anthropic/claude-sonnet-4"
    assert result.base_url == "https://openrouter.ai/api"


async def test_add_upserts(db_path):
    store = AIProviderStore(db_path)
    await store.add("gemini", api_key="key1")
    await store.add("gemini", api_key="key2", model="gemini-pro")

    loaded = await store.get("gemini")
    assert loaded is not None
    assert loaded.api_key == "key2"
    assert loaded.model == "gemini-pro"


async def test_get_returns_none_when_empty(db_path):
    store = AIProviderStore(db_path)
    assert await store.get("gemini") is None


async def test_list_all_empty(db_path):
    store = AIProviderStore(db_path)
    assert await store.list_all() == []


async def test_list_all_multiple(db_path):
    store = AIProviderStore(db_path)
    await store.add("gemini", api_key="gk")
    await store.add("ollama", model="llama3.1:8b")
    await store.add("openrouter", api_key="or-key")

    providers = await store.list_all()
    assert len(providers) == 3
    types = [p.type for p in providers]
    assert types == ["gemini", "ollama", "openrouter"]  # ordered by type


async def test_add_empty_type_raises(db_path):
    store = AIProviderStore(db_path)
    with pytest.raises(ValueError, match="cannot be empty"):
        await store.add("")


# ── Activate / deactivate ────────────────────────────────────────────


async def test_activate(db_path):
    store = AIProviderStore(db_path)
    await store.add("gemini", api_key="gk")
    await store.add("ollama", model="llama")

    result = await store.activate("gemini")
    assert result.active is True

    active = await store.get_active()
    assert active is not None
    assert active.type == "gemini"


async def test_activate_switches(db_path):
    store = AIProviderStore(db_path)
    await store.add("gemini", api_key="gk", activate=True)
    await store.add("ollama", model="llama")

    await store.activate("ollama")

    gemini = await store.get("gemini")
    assert gemini is not None
    assert gemini.active is False

    ollama = await store.get("ollama")
    assert ollama is not None
    assert ollama.active is True


async def test_activate_unconfigured_raises(db_path):
    store = AIProviderStore(db_path)
    with pytest.raises(ValueError, match="not configured"):
        await store.activate("nonexistent")


async def test_get_active_none_when_empty(db_path):
    store = AIProviderStore(db_path)
    assert await store.get_active() is None


async def test_get_active_none_when_no_active(db_path):
    store = AIProviderStore(db_path)
    await store.add("gemini", api_key="gk")  # not activated
    assert await store.get_active() is None


async def test_deactivate(db_path):
    store = AIProviderStore(db_path)
    await store.add("gemini", api_key="gk", activate=True)

    changed = await store.deactivate()
    assert changed is True
    assert await store.get_active() is None


async def test_deactivate_when_none_active(db_path):
    store = AIProviderStore(db_path)
    await store.add("gemini", api_key="gk")
    changed = await store.deactivate()
    assert changed is False


async def test_add_with_activate(db_path):
    store = AIProviderStore(db_path)
    await store.add("gemini", api_key="gk", activate=True)
    await store.add("ollama", model="llama", activate=True)

    # ollama should be active, gemini deactivated
    gemini = await store.get("gemini")
    assert gemini is not None
    assert gemini.active is False

    active = await store.get_active()
    assert active is not None
    assert active.type == "ollama"


# ── Remove ───────────────────────────────────────────────────────────


async def test_remove_existing(db_path):
    store = AIProviderStore(db_path)
    await store.add("gemini", api_key="gk")

    deleted = await store.remove("gemini")
    assert deleted is True
    assert await store.get("gemini") is None


async def test_remove_nonexistent(db_path):
    store = AIProviderStore(db_path)
    deleted = await store.remove("nonexistent")
    assert deleted is False


# ── Migration ────────────────────────────────────────────────────────


async def test_migrate_from_ai_provider_keys(db_path):
    """Migrate legacy ai_provider* app_settings keys."""
    import aiosqlite

    async with aiosqlite.connect(str(db_path)) as db:
        for key, value in [
            ("ai_provider", "openrouter"),
            ("ai_provider_api_key", "or-secret"),
            ("ai_provider_model", "anthropic/claude-sonnet-4"),
            ("ai_provider_base_url", "https://openrouter.ai/api"),
        ]:
            await db.execute(
                "INSERT INTO app_settings (key, value) VALUES (?, ?)",
                (key, value),
            )
        await db.commit()

    store = AIProviderStore(db_path)
    migrated = await store.migrate_from_legacy()
    assert migrated is True

    active = await store.get_active()
    assert active is not None
    assert active.type == "openrouter"
    assert active.api_key == "or-secret"
    assert active.model == "anthropic/claude-sonnet-4"
    assert active.base_url == "https://openrouter.ai/api"


async def test_migrate_from_gemini_api_key(db_path):
    """Migrate legacy gemini_api_key when no ai_provider* keys exist."""
    gemini_store = GeminiStore(db_path)
    await gemini_store.set("legacy-gemini-key")

    store = AIProviderStore(db_path)
    migrated = await store.migrate_from_legacy()
    assert migrated is True

    active = await store.get_active()
    assert active is not None
    assert active.type == "gemini"
    assert active.api_key == "legacy-gemini-key"


async def test_migrate_idempotent(db_path):
    """Second migration is a no-op if providers already exist."""
    gemini_store = GeminiStore(db_path)
    await gemini_store.set("legacy-key")

    store = AIProviderStore(db_path)
    assert await store.migrate_from_legacy() is True
    assert await store.migrate_from_legacy() is False


async def test_migrate_noop_when_no_legacy(db_path):
    """No migration when there's nothing to migrate."""
    store = AIProviderStore(db_path)
    assert await store.migrate_from_legacy() is False


# ── Backward compat alias ───────────────────────────────────────────


def test_aiconfig_alias():
    from pfm.db.ai_store import AIConfig

    assert AIConfig is AIProvider


def test_aistore_alias():
    from pfm.db.ai_store import AIStore

    assert AIStore is AIProviderStore
