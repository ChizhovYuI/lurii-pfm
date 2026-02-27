"""Telegram push reporting client."""

from __future__ import annotations

import html
import logging
import re
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
_HTTP_BAD_REQUEST = 400
_HTTP_UNAUTHORIZED = 401
_HTTP_FORBIDDEN = 403
_HTTP_NOT_FOUND = 404

_HTML_TAG_RE = re.compile(r"<[^>]+>")


@dataclass(frozen=True, slots=True)
class WeeklyReport:
    """Formatted report payload ready to send."""

    text: str


@dataclass(frozen=True, slots=True)
class _ChunkSendResult:
    ok: bool
    status_code: int | None = None
    description: str | None = None


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
            sent = await _send_chunk(
                http_client,
                endpoint=endpoint,
                chat_id=creds.chat_id,
                chunk=chunk,
                parse_mode=parse_mode,
            )
            if not sent:
                return False
    except httpx.HTTPError as exc:
        logger.warning("Telegram API transport error (%s): %s", type(exc).__name__, exc)
        return False
    finally:
        if owns_client:
            await http_client.aclose()

    return True


async def _send_chunk(
    client: httpx.AsyncClient,
    *,
    endpoint: str,
    chat_id: str,
    chunk: str,
    parse_mode: str | None,
) -> bool:
    first_attempt = await _post_chunk(client, endpoint=endpoint, chat_id=chat_id, chunk=chunk, parse_mode=parse_mode)
    if first_attempt.ok:
        return True

    if parse_mode is None or first_attempt.status_code != _HTTP_BAD_REQUEST:
        _log_status_failure(first_attempt.status_code, first_attempt.description)
        return False

    plain_chunk = _html_to_plain_text(chunk)
    logger.warning(
        "Telegram rejected formatted chunk (400%s); retrying the same chunk as plain text.",
        f": {first_attempt.description}" if first_attempt.description else "",
    )
    plain_attempt = await _post_chunk(client, endpoint=endpoint, chat_id=chat_id, chunk=plain_chunk, parse_mode=None)
    if plain_attempt.ok:
        return True

    _log_status_failure(plain_attempt.status_code, plain_attempt.description)
    return False


async def _post_chunk(
    client: httpx.AsyncClient,
    *,
    endpoint: str,
    chat_id: str,
    chunk: str,
    parse_mode: str | None,
) -> _ChunkSendResult:
    payload: dict[str, str] = {
        "chat_id": chat_id,
        "text": chunk,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode

    resp = await client.post(endpoint, json=payload)
    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError:
        return _ChunkSendResult(
            ok=False,
            status_code=resp.status_code,
            description=_extract_error_description(resp),
        )

    try:
        body = resp.json()
    except ValueError as exc:
        logger.warning("Telegram API returned invalid JSON response: %s", exc)
        return _ChunkSendResult(ok=False)
    if not body.get("ok", False):
        logger.warning("Telegram API returned ok=false: %s", body)
        return _ChunkSendResult(ok=False, description=str(body.get("description", "")))
    return _ChunkSendResult(ok=True)


def _html_to_plain_text(text: str) -> str:
    plain = text.replace("<br>", "\n")
    plain = _HTML_TAG_RE.sub("", plain)
    return html.unescape(plain)


def _extract_error_description(response: httpx.Response) -> str | None:
    try:
        body = response.json()
    except ValueError:
        return None
    if not isinstance(body, dict):
        return None
    description = body.get("description")
    if isinstance(description, str) and description.strip():
        return description.strip()
    return None


def _log_status_failure(status_code: int | None, description: str | None) -> None:
    if status_code == _HTTP_NOT_FOUND:
        logger.warning("Telegram API returned 404. Bot token is likely invalid.")
        return
    if status_code == _HTTP_UNAUTHORIZED:
        logger.warning("Telegram API returned 401 Unauthorized. Check bot token.")
        return
    if status_code == _HTTP_FORBIDDEN:
        logger.warning("Telegram API returned 403 Forbidden. Bot may be blocked or missing chat access.")
        return
    if status_code == _HTTP_BAD_REQUEST:
        if description:
            logger.warning("Telegram API returned 400 Bad Request: %s", description)
        else:
            logger.warning("Telegram API returned 400 Bad Request. Check chat ID and message format.")
        return
    if status_code is not None:
        logger.warning("Telegram API request failed with HTTP %d.", status_code)
        return
    if description:
        logger.warning("Telegram message send failed: %s", description)
        return
    logger.warning("Telegram message send failed.")


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
