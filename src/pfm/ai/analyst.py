"""Gemini API client for portfolio commentary."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Mapping
from pathlib import Path

from google import genai
from google.genai import errors

from pfm.ai.prompts import WEEKLY_REPORT_SYSTEM_PROMPT, AnalyticsSummary, render_weekly_report_user_prompt
from pfm.config import get_settings
from pfm.db.gemini_store import GeminiStore

logger = logging.getLogger(__name__)

GEMINI_MODEL = "gemini-2.5-flash-lite"
GEMINI_MAX_OUTPUT_TOKENS = 1024
GEMINI_MAX_RETRIES = 3
GEMINI_BASE_BACKOFF_SECONDS = 2.0
GEMINI_MAX_BACKOFF_SECONDS = 120.0
GEMINI_TOKEN_ESTIMATE_CHARS = 4
GEMINI_RATE_LIMIT_STATE_FILE = Path("data/gemini_last_request_at.txt")
HTTP_TOO_MANY_REQUESTS = 429
FALLBACK_COMMENTARY = (
    "AI commentary is currently unavailable. " "Review net worth trend, concentration risk, and PnL changes manually."
)


async def generate_commentary(
    analytics: AnalyticsSummary,
    *,
    api_key: str | None = None,
    db_path: str | Path | None = None,
    client: genai.Client | None = None,
) -> str:
    """Generate weekly portfolio commentary using Gemini."""
    resolved_api_key = await resolve_gemini_api_key(api_key=api_key, db_path=db_path)
    if not resolved_api_key:
        logger.warning("Gemini API key is not configured; returning fallback commentary.")
        return FALLBACK_COMMENTARY

    prompt = render_weekly_report_user_prompt(analytics)
    prompt_chars = len(prompt)
    prompt_tokens_est = _estimate_tokens(prompt_chars)
    logger.info(
        "gemini_input_size model=%s prompt_chars=%d prompt_tokens_est=%d max_output_tokens=%d",
        GEMINI_MODEL,
        prompt_chars,
        prompt_tokens_est,
        GEMINI_MAX_OUTPUT_TOKENS,
    )
    owns_client = client is None
    sdk_client = client if client is not None else genai.Client(api_key=resolved_api_key)
    try:
        response = await _request_commentary_response(
            sdk_client.aio.models,
            prompt,
            model=GEMINI_MODEL,
            input_size=(prompt_chars, prompt_tokens_est),
            enforce_local_rate_limit=owns_client,
        )
    finally:
        if owns_client:
            await sdk_client.aio.aclose()
            sdk_client.close()

    if response is None:
        return FALLBACK_COMMENTARY

    _log_token_usage(response)
    text = _extract_text(response)
    if text:
        return text

    logger.warning("Gemini returned empty text response; using fallback commentary.")
    return FALLBACK_COMMENTARY


def _retry_delay_seconds(retry_after: str | None, attempt: int, model: str) -> float:
    min_delay = _min_retry_delay_seconds(model, attempt)
    if retry_after:
        try:
            parsed = float(retry_after)
        except ValueError:
            parsed = 0.0
        if parsed > 0:
            return float(min(max(parsed, min_delay), GEMINI_MAX_BACKOFF_SECONDS))

    backoff = GEMINI_BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
    return float(min(max(backoff, min_delay), GEMINI_MAX_BACKOFF_SECONDS))


def _min_retry_delay_seconds(model: str, attempt: int) -> float:
    model_lower = model.lower()
    if "pro" in model_lower:
        base = 30.0
    elif "flash-lite" in model_lower:
        base = 5.0
    elif "flash" in model_lower:
        base = 7.0
    else:
        base = 10.0

    return float(min(base * (2 ** (attempt - 1)), GEMINI_MAX_BACKOFF_SECONDS))


async def _request_commentary_response(
    models: object,
    prompt: str,
    *,
    model: str,
    input_size: tuple[int, int],
    enforce_local_rate_limit: bool,
) -> object | None:
    prompt_chars, prompt_tokens_est = input_size
    config: Mapping[str, object] = {
        "system_instruction": WEEKLY_REPORT_SYSTEM_PROMPT,
        "max_output_tokens": GEMINI_MAX_OUTPUT_TOKENS,
    }
    generate_content = getattr(models, "generate_content", None)
    if generate_content is None:
        logger.warning("Gemini SDK client is missing generate_content; using fallback commentary.")
        return None

    for attempt in range(1, GEMINI_MAX_RETRIES + 1):
        try:
            if enforce_local_rate_limit:
                await _apply_local_rate_limit(model)
            response: object = await generate_content(
                model=model,
                contents=prompt,
                config=config,
            )
        except errors.APIError as exc:
            status = exc.code
            if status == HTTP_TOO_MANY_REQUESTS and attempt < GEMINI_MAX_RETRIES:
                retry_delay = _retry_delay_seconds(_extract_retry_after(exc), attempt, model)
                logger.warning(
                    "Gemini rate limited (HTTP 429). Retrying in %.1fs (%d/%d). input_chars=%d input_tokens_est=%d",
                    retry_delay,
                    attempt,
                    GEMINI_MAX_RETRIES,
                    prompt_chars,
                    prompt_tokens_est,
                )
                await asyncio.sleep(retry_delay)
                continue
            if status == HTTP_TOO_MANY_REQUESTS:
                recommended_wait = _min_retry_delay_seconds(model, GEMINI_MAX_RETRIES)
                logger.warning(
                    "Gemini rate limited (HTTP 429). Using fallback commentary. "
                    "Wait about %.0fs before retrying 'pfm comment'. input_chars=%d input_tokens_est=%d",
                    recommended_wait,
                    prompt_chars,
                    prompt_tokens_est,
                )
            else:
                logger.warning("Gemini API request failed with HTTP %d. Using fallback commentary.", status)
            break
        except Exception:  # pragma: no cover - defensive guardrail
            logger.exception("Unexpected Gemini API error")
            break
        else:
            return response
    else:
        logger.warning("Gemini commentary request failed after retries. Using fallback commentary.")
    return None


def _extract_retry_after(exc: errors.APIError) -> str | None:
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    value = headers.get("Retry-After")
    if value is None:
        return None
    retry_after = value.strip()
    return retry_after or None


async def _apply_local_rate_limit(model: str) -> None:
    min_interval = _min_retry_delay_seconds(model, 1)
    wait_seconds = _compute_local_wait_seconds(min_interval)
    if wait_seconds > 0:
        logger.info(
            "gemini_local_rate_limit_wait model=%s wait_seconds=%.1f",
            model,
            wait_seconds,
        )
        await asyncio.sleep(wait_seconds)
    _record_local_request_time()


def _compute_local_wait_seconds(min_interval: float) -> float:
    last_request_at = _read_last_request_time()
    if last_request_at is None:
        return 0.0
    now = time.time()
    wait_seconds = (last_request_at + min_interval) - now
    return max(wait_seconds, 0.0)


def _read_last_request_time() -> float | None:
    try:
        raw = GEMINI_RATE_LIMIT_STATE_FILE.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    except OSError:
        logger.debug("Failed reading Gemini local rate-limit state file.", exc_info=True)
        return None
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _record_local_request_time() -> None:
    try:
        GEMINI_RATE_LIMIT_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        GEMINI_RATE_LIMIT_STATE_FILE.write_text(str(time.time()), encoding="utf-8")
    except OSError:
        logger.debug("Failed writing Gemini local rate-limit state file.", exc_info=True)


def _estimate_tokens(text_chars: int) -> int:
    if text_chars <= 0:
        return 0
    return (text_chars + GEMINI_TOKEN_ESTIMATE_CHARS - 1) // GEMINI_TOKEN_ESTIMATE_CHARS


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


def _extract_text(body: object) -> str:
    text = _field_value(body, "text")
    if isinstance(text, str) and text.strip():
        return text.strip()

    candidates = _field_value(body, "candidates")
    if not isinstance(candidates, list):
        return ""

    parts: list[str] = []
    for candidate in candidates:
        content = _field_value(candidate, "content")
        if content is None:
            continue
        content_parts = _field_value(content, "parts")
        if not isinstance(content_parts, list):
            continue
        for part in content_parts:
            part_text = _field_value(part, "text")
            if isinstance(part_text, str) and part_text.strip():
                parts.append(part_text.strip())

    return "\n".join(parts).strip()


def _log_token_usage(body: object) -> None:
    usage = _field_value(body, "usage_metadata")
    if usage is None:
        usage = _field_value(body, "usageMetadata")
    if usage is None:
        return

    prompt_tokens = _field_value(usage, "prompt_token_count")
    if prompt_tokens is None:
        prompt_tokens = _field_value(usage, "promptTokenCount")
    candidates_tokens = _field_value(usage, "candidates_token_count")
    if candidates_tokens is None:
        candidates_tokens = _field_value(usage, "candidatesTokenCount")
    total_tokens = _field_value(usage, "total_token_count")
    if total_tokens is None:
        total_tokens = _field_value(usage, "totalTokenCount")

    logger.info(
        "gemini_usage model=%s prompt_tokens=%s candidates_tokens=%s total_tokens=%s",
        GEMINI_MODEL,
        prompt_tokens,
        candidates_tokens,
        total_tokens,
    )


def _field_value(obj: object, field_name: str) -> object | None:
    if isinstance(obj, Mapping):
        return obj.get(field_name)
    return getattr(obj, field_name, None)
