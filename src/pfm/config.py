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

    # ── OKX ───────────────────────────────────────────────────────────
    okx_api_key: SecretStr = SecretStr("")
    okx_api_secret: SecretStr = SecretStr("")
    okx_passphrase: SecretStr = SecretStr("")

    # ── Binance (global) ──────────────────────────────────────────────
    binance_api_key: SecretStr = SecretStr("")
    binance_api_secret: SecretStr = SecretStr("")

    # ── Binance TH ────────────────────────────────────────────────────
    binance_th_api_key: SecretStr = SecretStr("")
    binance_th_api_secret: SecretStr = SecretStr("")

    # ── Bybit ─────────────────────────────────────────────────────────
    bybit_api_key: SecretStr = SecretStr("")
    bybit_api_secret: SecretStr = SecretStr("")

    # ── Uphold ────────────────────────────────────────────────────────
    uphold_pat: SecretStr = SecretStr("")

    # ── Stellar / Lobstr / Blend ──────────────────────────────────────
    stellar_public_address: str = ""
    blend_pool_contract_id: str = ""
    soroban_rpc_url: str = "https://soroban-rpc.mainnet.stellar.gateway.fm"

    # ── Wise ──────────────────────────────────────────────────────────
    wise_api_token: SecretStr = SecretStr("")

    # ── IBKR ──────────────────────────────────────────────────────────
    ibkr_flex_token: SecretStr = SecretStr("")
    ibkr_flex_query_id: str = ""

    # ── Telegram ──────────────────────────────────────────────────────
    telegram_bot_token: SecretStr = SecretStr("")
    telegram_chat_id: str = ""

    # ── Claude API ────────────────────────────────────────────────────
    anthropic_api_key: SecretStr = SecretStr("")

    # ── CoinGecko ─────────────────────────────────────────────────────
    coingecko_api_key: str = ""  # optional, free tier works without

    # ── Logging ───────────────────────────────────────────────────────
    log_level: str = "INFO"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached application settings."""
    return Settings()
