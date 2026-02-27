"""Gemini API client for portfolio commentary."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

import httpx

from pfm.ai.prompts import WEEKLY_REPORT_SYSTEM_PROMPT, AnalyticsSummary, render_weekly_report_user_prompt
from pfm.config import get_settings
from pfm.db.gemini_store import GeminiStore

logger = logging.getLogger(__name__)

GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"
GEMINI_MODEL = "gemini-2.0-flash"
GEMINI_MAX_OUTPUT_TOKENS = 1024
GEMINI_MAX_RETRIES = 3
GEMINI_BASE_BACKOFF_SECONDS = 2.0
GEMINI_MAX_BACKOFF_SECONDS = 30.0
HTTP_TOO_MANY_REQUESTS = 429
FALLBACK_COMMENTARY = (
    "AI commentary is currently unavailable. " "Review net worth trend, concentration risk, and PnL changes manually."
)


async def generate_commentary(
    analytics: AnalyticsSummary,
    *,
    api_key: str | None = None,
    db_path: str | Path | None = None,
    client: httpx.AsyncClient | None = None,
) -> str:
    """Generate weekly portfolio commentary using Gemini."""
    resolved_api_key = await resolve_gemini_api_key(api_key=api_key, db_path=db_path)
    if not resolved_api_key:
        logger.warning("Gemini API key is not configured; returning fallback commentary.")
        return FALLBACK_COMMENTARY

    prompt = render_weekly_report_user_prompt(analytics)
    payload: dict[str, object] = {
        "system_instruction": {"parts": [{"text": WEEKLY_REPORT_SYSTEM_PROMPT}]},
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"maxOutputTokens": GEMINI_MAX_OUTPUT_TOKENS},
    }
    endpoint = f"{GEMINI_API_BASE}/models/{GEMINI_MODEL}:generateContent"

    owns_client = client is None
    http_client = client if client is not None else httpx.AsyncClient(timeout=30.0)
    try:
        body = await _request_commentary_body(http_client, endpoint, resolved_api_key, payload)
    finally:
        if owns_client:
            await http_client.aclose()

    if body is None:
        return FALLBACK_COMMENTARY

    _log_token_usage(body)
    text = _extract_text(body)
    if text:
        return text

    logger.warning("Gemini returned empty text response; using fallback commentary.")
    return FALLBACK_COMMENTARY


def _retry_delay_seconds(response: httpx.Response, attempt: int) -> float:
    retry_after = response.headers.get("Retry-After", "").strip()
    if retry_after:
        try:
            parsed = float(retry_after)
        except ValueError:
            parsed = 0.0
        if parsed > 0:
            return float(min(parsed, GEMINI_MAX_BACKOFF_SECONDS))

    backoff = GEMINI_BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
    return float(min(backoff, GEMINI_MAX_BACKOFF_SECONDS))


async def _request_commentary_body(
    client: httpx.AsyncClient,
    endpoint: str,
    api_key: str,
    payload: Mapping[str, object],
) -> dict[str, Any] | None:
    for attempt in range(1, GEMINI_MAX_RETRIES + 1):
        try:
            response = await client.post(endpoint, params={"key": api_key}, json=payload)
            response.raise_for_status()
            body = response.json()
            if isinstance(body, dict):
                return body
            logger.warning("Gemini API returned non-object JSON body; using fallback commentary.")
            break
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status == HTTP_TOO_MANY_REQUESTS and attempt < GEMINI_MAX_RETRIES:
                retry_delay = _retry_delay_seconds(exc.response, attempt)
                logger.warning(
                    "Gemini rate limited (HTTP 429). Retrying in %.1fs (%d/%d).",
                    retry_delay,
                    attempt,
                    GEMINI_MAX_RETRIES,
                )
                await asyncio.sleep(retry_delay)
                continue
            if status == HTTP_TOO_MANY_REQUESTS:
                logger.warning("Gemini rate limited (HTTP 429). Using fallback commentary.")
            else:
                logger.warning("Gemini API request failed with HTTP %d. Using fallback commentary.", status)
            break
        except httpx.HTTPError as exc:
            logger.warning("Gemini API transport error (%s).", type(exc).__name__)
            logger.debug("Gemini transport error details: %s", exc)
            break
        except ValueError as exc:
            logger.warning("Gemini API returned invalid JSON: %s", exc)
            break
        except Exception:  # pragma: no cover - defensive guardrail
            logger.exception("Unexpected Gemini API error")
            break
    else:
        logger.warning("Gemini commentary request failed after retries. Using fallback commentary.")
    return None


async def resolve_gemini_api_key(
    *,
    api_key: str | None = None,
    db_path: str | Path | None = None,
) -> str | None:
    """Resolve Gemini API key from explicit param, DB config, then environment."""
    if api_key is not None and api_key.strip():
        return api_key.strip()

    settings = get_settings()
    settings_db_path = getattr(settings, "database_path", None)
    if db_path is not None:
        target_db_path: str | Path = db_path
    elif isinstance(settings_db_path, str | Path):
        target_db_path = settings_db_path
    else:
        target_db_path = Path("data/pfm.db")
    store = GeminiStore(target_db_path)
    try:
        stored = await store.get()
    except Exception:  # pragma: no cover - defensive guardrail
        logger.exception("Failed to load Gemini API key from DB settings.")
    else:
        if stored is not None:
            return stored.api_key

    env_value = settings.gemini_api_key.get_secret_value().strip()
    if env_value:
        return env_value
    return None


def _extract_text(body: dict[str, Any]) -> str:
    candidates = body.get("candidates", [])
    if not isinstance(candidates, list):
        return ""

    parts: list[str] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        content = candidate.get("content", {})
        if not isinstance(content, dict):
            continue
        content_parts = content.get("parts", [])
        if not isinstance(content_parts, list):
            continue
        for part in content_parts:
            if not isinstance(part, dict):
                continue
            text = part.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())

    return "\n".join(parts).strip()


def _log_token_usage(body: dict[str, Any]) -> None:
    usage = body.get("usageMetadata", {})
    if not isinstance(usage, dict):
        return

    logger.info(
        "gemini_usage model=%s prompt_tokens=%s candidates_tokens=%s total_tokens=%s",
        GEMINI_MODEL,
        usage.get("promptTokenCount"),
        usage.get("candidatesTokenCount"),
        usage.get("totalTokenCount"),
    )
