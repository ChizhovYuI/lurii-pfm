"""Telegram push reporting client."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import httpx

from pfm.config import get_settings
from pfm.db.telegram_store import TelegramCredentials, TelegramStore

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org"
TELEGRAM_MESSAGE_LIMIT = 4096


@dataclass(frozen=True, slots=True)
class WeeklyReport:
    """Formatted report payload ready to send."""

    text: str


async def send_message(  # noqa: PLR0913
    chat_id: str | None,
    text: str,
    parse_mode: str | None = "HTML",
    *,
    bot_token: str | None = None,
    db_path: str | Path | None = None,
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Send a message (split into Telegram-safe chunks if needed)."""
    creds = await resolve_telegram_credentials(
        chat_id=chat_id,
        bot_token=bot_token,
        db_path=db_path,
    )
    if creds is None:
        logger.info("Telegram is not configured; skipping send.")
        return False

    chunks = _split_message(text, TELEGRAM_MESSAGE_LIMIT)
    endpoint = f"{TELEGRAM_API_BASE}/bot{creds.bot_token}/sendMessage"
    owns_client = client is None
    http_client = client if client is not None else httpx.AsyncClient(timeout=20.0)
    try:
        for chunk in chunks:
            payload: dict[str, str] = {
                "chat_id": creds.chat_id,
                "text": chunk,
            }
            if parse_mode:
                payload["parse_mode"] = parse_mode

            resp = await http_client.post(endpoint, json=payload)
            resp.raise_for_status()
            try:
                body = resp.json()
            except ValueError as exc:
                logger.warning("Telegram API returned invalid JSON response: %s", exc)
                return False
            if not body.get("ok", False):
                logger.warning("Telegram API returned ok=false: %s", body)
                return False
    except httpx.HTTPError as exc:
        logger.warning("Telegram API request failed: %s", exc)
        return False
    finally:
        if owns_client:
            await http_client.aclose()

    return True


async def send_report(
    report: WeeklyReport,
    *,
    chat_id: str | None = None,
    bot_token: str | None = None,
    db_path: str | Path | None = None,
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Send a formatted weekly report to Telegram."""
    return await send_message(
        chat_id,
        report.text,
        parse_mode="HTML",
        bot_token=bot_token,
        db_path=db_path,
        client=client,
    )


async def send_error_alert(
    errors: list[str],
    *,
    chat_id: str | None = None,
    bot_token: str | None = None,
    db_path: str | Path | None = None,
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Send pipeline error alerts to Telegram."""
    if not errors:
        return True

    lines = ["PFM pipeline errors detected:"] + [f"- {error}" for error in errors]
    return await send_message(
        chat_id,
        "\n".join(lines),
        parse_mode=None,
        bot_token=bot_token,
        db_path=db_path,
        client=client,
    )


async def is_telegram_configured(*, db_path: str | Path | None = None) -> bool:
    """Return True when Telegram credentials are available."""
    creds = await resolve_telegram_credentials(db_path=db_path)
    return creds is not None


async def resolve_telegram_credentials(
    *,
    chat_id: str | None = None,
    bot_token: str | None = None,
    db_path: str | Path | None = None,
) -> TelegramCredentials | None:
    """Resolve Telegram credentials from explicit params, then DB."""
    if bot_token and chat_id:
        return TelegramCredentials(bot_token=bot_token, chat_id=chat_id)

    settings = get_settings()
    store = TelegramStore(db_path if db_path is not None else settings.database_path)
    stored = await store.get()
    if stored is not None:
        resolved_bot_token = bot_token or stored.bot_token
        resolved_chat_id = chat_id or stored.chat_id
        if resolved_bot_token and resolved_chat_id:
            return TelegramCredentials(bot_token=resolved_bot_token, chat_id=resolved_chat_id)

    return None


def _split_message(text: str, limit: int) -> list[str]:
    """Split text into chunks up to `limit`, preferring newline boundaries."""
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    remaining = text
    while len(remaining) > limit:
        split_at = remaining.rfind("\n", 0, limit + 1)
        if split_at <= 0:
            split_at = limit
        chunk = remaining[:split_at].strip()
        if chunk:
            chunks.append(chunk)
        remaining = remaining[split_at:].lstrip("\n")
    if remaining.strip():
        chunks.append(remaining.strip())
    return chunks
