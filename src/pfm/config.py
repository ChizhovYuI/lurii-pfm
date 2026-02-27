"""Application settings loaded from .env file."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All application settings. Loaded from .env file at project root."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── Database ──────────────────────────────────────────────────────
    database_path: Path = Path("data/pfm.db")

    # ── Telegram ──────────────────────────────────────────────────────
    telegram_bot_token: SecretStr = SecretStr("")
    telegram_chat_id: str = ""

    # ── Gemini API ────────────────────────────────────────────────────
    gemini_api_key: SecretStr = SecretStr("")

    # ── CoinGecko ─────────────────────────────────────────────────────
    coingecko_api_key: str = ""  # optional, free tier works without

    # ── Logging ───────────────────────────────────────────────────────
    log_level: str = "INFO"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached application settings."""
    return Settings()
