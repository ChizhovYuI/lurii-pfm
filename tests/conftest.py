"""Shared test fixtures."""

from __future__ import annotations

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
        anthropic_api_key="test-key",  # type: ignore[arg-type]
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
    return PricingService()
