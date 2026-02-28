"""Tests for AI settings store."""

from __future__ import annotations

import pytest

from pfm.db.ai_store import AIStore
from pfm.db.gemini_store import GeminiStore
from pfm.db.models import init_db


@pytest.fixture
async def db_path(tmp_path):
    path = tmp_path / "test.db"
    await init_db(path)
    return path


async def test_set_get_cycle(db_path):
    store = AIStore(db_path)
    config = await store.set(provider="ollama", model="llama3.1:8b")
    assert config.provider == "ollama"
    assert config.model == "llama3.1:8b"
    assert config.api_key == ""

    loaded = await store.get()
    assert loaded is not None
    assert loaded.provider == "ollama"
    assert loaded.model == "llama3.1:8b"


async def test_set_with_all_fields(db_path):
    store = AIStore(db_path)
    config = await store.set(
        provider="openrouter",
        api_key="or-key",
        model="anthropic/claude-sonnet-4",
        base_url="https://openrouter.ai/api",
    )
    assert config.provider == "openrouter"
    assert config.api_key == "or-key"
    assert config.model == "anthropic/claude-sonnet-4"
    assert config.base_url == "https://openrouter.ai/api"


async def test_set_upserts(db_path):
    store = AIStore(db_path)
    await store.set(provider="gemini", api_key="key1")
    await store.set(provider="ollama", model="llama3.1:8b")

    loaded = await store.get()
    assert loaded is not None
    assert loaded.provider == "ollama"


async def test_get_returns_none_when_empty(db_path):
    store = AIStore(db_path)
    assert await store.get() is None


async def test_clear(db_path):
    store = AIStore(db_path)
    await store.set(provider="gemini", api_key="key")

    deleted = await store.clear()
    assert deleted is True
    assert await store.get() is None


async def test_clear_when_empty(db_path):
    store = AIStore(db_path)
    deleted = await store.clear()
    assert deleted is False


async def test_set_empty_provider_raises(db_path):
    store = AIStore(db_path)
    with pytest.raises(ValueError, match="cannot be empty"):
        await store.set(provider="")


async def test_migrate_from_gemini(db_path):
    # Set up legacy key
    gemini_store = GeminiStore(db_path)
    await gemini_store.set("legacy-gemini-key")

    store = AIStore(db_path)
    migrated = await store.migrate_from_gemini()
    assert migrated is True

    config = await store.get()
    assert config is not None
    assert config.provider == "gemini"
    assert config.api_key == "legacy-gemini-key"


async def test_migrate_from_gemini_idempotent(db_path):
    gemini_store = GeminiStore(db_path)
    await gemini_store.set("legacy-key")

    store = AIStore(db_path)
    assert await store.migrate_from_gemini() is True
    # Second call should be no-op since config already exists
    assert await store.migrate_from_gemini() is False


async def test_migrate_from_gemini_skips_when_no_legacy_key(db_path):
    store = AIStore(db_path)
    assert await store.migrate_from_gemini() is False


async def test_migrate_from_gemini_skips_when_new_config_exists(db_path):
    gemini_store = GeminiStore(db_path)
    await gemini_store.set("legacy-key")

    store = AIStore(db_path)
    await store.set(provider="ollama")
    assert await store.migrate_from_gemini() is False
