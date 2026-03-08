"""Shared test fixtures."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from pfm.config import Settings
from pfm.db.repository import Repository
from pfm.pricing.coingecko import PricingService

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


@pytest.fixture
def test_settings() -> Settings:
    """Settings with dummy values for testing."""
    return Settings(
        database_path=Path(":memory:"),
        telegram_bot_token="test-token",  # type: ignore[arg-type]
        gemini_api_key="test-key",  # type: ignore[arg-type]
    )


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncGenerator[Repository]:
    """In-memory repository for testing."""
    db_path = tmp_path / "test.db"
    async with Repository(db_path) as r:
        yield r


@pytest.fixture
def pricing() -> PricingService:
    """Pricing service for testing (uses cache, no real API calls)."""
    svc = PricingService()
    svc._coins_by_symbol = {}
    return svc


@pytest.fixture(autouse=True)
def _no_daemon(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prevent tests from proxying to a running daemon."""
    monkeypatch.setattr("pfm.server.client.is_daemon_reachable", lambda *_a, **_kw: False)


@pytest.fixture(autouse=True)
def _restore_logging_state():
    """Keep global logging mutations from leaking between tests."""
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    original_disabled = logging.root.manager.disable
    try:
        yield
    finally:
        root.handlers.clear()
        for handler in original_handlers:
            root.addHandler(handler)
        root.setLevel(original_level)
        logging.disable(original_disabled)
