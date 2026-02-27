"""Gemini API client for portfolio commentary."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import httpx

from pfm.ai.prompts import WEEKLY_REPORT_SYSTEM_PROMPT, AnalyticsSummary, render_weekly_report_user_prompt
from pfm.config import get_settings
from pfm.db.gemini_store import GeminiStore

logger = logging.getLogger(__name__)

GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"
GEMINI_MODEL = "gemini-2.0-flash"
GEMINI_MAX_OUTPUT_TOKENS = 1024
FALLBACK_COMMENTARY = (
    "AI commentary is currently unavailable. " "Review net worth trend, concentration risk, and PnL changes manually."
)


async def generate_commentary(  # noqa: PLR0911
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
    payload = {
        "system_instruction": {"parts": [{"text": WEEKLY_REPORT_SYSTEM_PROMPT}]},
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"maxOutputTokens": GEMINI_MAX_OUTPUT_TOKENS},
    }
    endpoint = f"{GEMINI_API_BASE}/models/{GEMINI_MODEL}:generateContent"

    owns_client = client is None
    http_client = client if client is not None else httpx.AsyncClient(timeout=30.0)
    try:
        response = await http_client.post(endpoint, params={"key": resolved_api_key}, json=payload)
        response.raise_for_status()
        body: dict[str, Any] = response.json()
    except httpx.HTTPStatusError as exc:
        logger.warning("Gemini API request failed with HTTP %d.", exc.response.status_code)
        return FALLBACK_COMMENTARY
    except httpx.HTTPError as exc:
        logger.warning("Gemini API transport error (%s).", type(exc).__name__)
        logger.debug("Gemini transport error details: %s", exc)
        return FALLBACK_COMMENTARY
    except ValueError as exc:
        logger.warning("Gemini API returned invalid JSON: %s", exc)
        return FALLBACK_COMMENTARY
    except Exception:  # pragma: no cover - defensive guardrail
        logger.exception("Unexpected Gemini API error")
        return FALLBACK_COMMENTARY
    finally:
        if owns_client:
            await http_client.aclose()

    _log_token_usage(body)
    text = _extract_text(body)
    if text:
        return text

    logger.warning("Gemini returned empty text response; using fallback commentary.")
    return FALLBACK_COMMENTARY


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
