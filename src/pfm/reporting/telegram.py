"""Telegram push reporting client."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

from pfm.config import get_settings

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org"
TELEGRAM_MESSAGE_LIMIT = 4096


@dataclass(frozen=True, slots=True)
class WeeklyReport:
    """Formatted report payload ready to send."""

    text: str


async def send_message(
    chat_id: str,
    text: str,
    parse_mode: str | None = "HTML",
    *,
    bot_token: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Send a message (split into Telegram-safe chunks if needed)."""
    settings = get_settings()
    token = bot_token if bot_token is not None else settings.telegram_bot_token.get_secret_value()
    if not token:
        logger.warning("Telegram bot token is not configured.")
        return False
    if not chat_id:
        logger.warning("Telegram chat ID is not configured.")
        return False

    chunks = _split_message(text, TELEGRAM_MESSAGE_LIMIT)
    endpoint = f"{TELEGRAM_API_BASE}/bot{token}/sendMessage"
    owns_client = client is None
    http_client = client if client is not None else httpx.AsyncClient(timeout=20.0)
    try:
        for chunk in chunks:
            payload: dict[str, str] = {
                "chat_id": chat_id,
                "text": chunk,
            }
            if parse_mode:
                payload["parse_mode"] = parse_mode

            resp = await http_client.post(endpoint, json=payload)
            resp.raise_for_status()
            body = resp.json()
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
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Send a formatted weekly report to Telegram."""
    settings = get_settings()
    destination_chat_id = chat_id if chat_id is not None else settings.telegram_chat_id
    return await send_message(
        destination_chat_id,
        report.text,
        parse_mode="HTML",
        bot_token=bot_token,
        client=client,
    )


async def send_error_alert(
    errors: list[str],
    *,
    chat_id: str | None = None,
    bot_token: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> bool:
    """Send pipeline error alerts to Telegram."""
    if not errors:
        return True

    settings = get_settings()
    destination_chat_id = chat_id if chat_id is not None else settings.telegram_chat_id
    lines = ["PFM pipeline errors detected:"] + [f"- {error}" for error in errors]
    return await send_message(
        destination_chat_id,
        "\n".join(lines),
        parse_mode=None,
        bot_token=bot_token,
        client=client,
    )


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
