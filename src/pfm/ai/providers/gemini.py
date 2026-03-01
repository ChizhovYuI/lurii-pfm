"""Gemini LLM provider using the native google-genai SDK."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Mapping
from pathlib import Path

from google import genai
from google.genai import errors

from pfm.ai.base import CommentaryResult, LLMProvider
from pfm.ai.providers.registry import register_provider

logger = logging.getLogger(__name__)

GEMINI_MODELS: tuple[str, ...] = (
    "gemini-2.5-pro",
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
)
GEMINI_MAX_RETRIES = 3
GEMINI_BASE_BACKOFF_SECONDS = 2.0
GEMINI_MAX_BACKOFF_SECONDS = 120.0
GEMINI_TOKEN_ESTIMATE_CHARS = 4
GEMINI_RATE_LIMIT_STATE_FILE = Path("data/gemini_last_request_at.txt")
HTTP_TOO_MANY_REQUESTS = 429


@register_provider
class GeminiProvider(LLMProvider):
    """Gemini provider with model failover chain and rate limiting."""

    name = "gemini"
    default_model = "gemini-2.5-pro"
    models: tuple[str, ...] = GEMINI_MODELS

    def __init__(
        self,
        *,
        api_key: str,
        model: str | None = None,
        client: genai.Client | None = None,
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._owns_client = client is None
        self._client = client or genai.Client(api_key=api_key)

    @property
    def _models(self) -> tuple[str, ...]:
        if self._model:
            return (self._model,)
        return GEMINI_MODELS

    async def generate_commentary(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        max_output_tokens: int = 4096,
    ) -> CommentaryResult:
        """Generate commentary using Gemini with model failover."""
        prompt_chars = len(user_prompt)
        prompt_tokens_est = _estimate_tokens(prompt_chars)
        models = self._models
        logger.info(
            "gemini_input_size models=%s prompt_chars=%d prompt_tokens_est=%d max_output_tokens=%d",
            ",".join(models),
            prompt_chars,
            prompt_tokens_est,
            max_output_tokens,
        )

        for index, model in enumerate(models):
            response = await _request_commentary_response(
                self._client.aio.models,
                user_prompt,
                system_prompt=system_prompt,
                model=model,
                max_output_tokens=max_output_tokens,
                input_size=(prompt_chars, prompt_tokens_est),
                enforce_local_rate_limit=self._owns_client and index == 0,
            )
            if response is None:
                logger.warning("Gemini model %s failed. Trying next fallback model.", model)
                continue

            _log_token_usage(response, model=model)
            text = _extract_text(response)
            if text:
                return CommentaryResult(text=text, model=model)
            logger.warning("Gemini model %s returned empty text. Trying next fallback model.", model)

        return CommentaryResult(text="", model=None, error="All Gemini models failed (rate limited or error)")

    async def close(self) -> None:
        """Close the SDK client if owned."""
        if self._owns_client:
            await self._client.aio.aclose()
            self._client.close()


# -- private helpers (extracted from analyst.py) -------------------------------


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


async def _request_commentary_response(  # noqa: PLR0913
    models: object,
    prompt: str,
    *,
    system_prompt: str,
    model: str,
    max_output_tokens: int,
    input_size: tuple[int, int],
    enforce_local_rate_limit: bool,
) -> object | None:
    prompt_chars, prompt_tokens_est = input_size
    config: Mapping[str, object] = {
        "system_instruction": system_prompt,
        "max_output_tokens": max_output_tokens,
    }
    generate_content = getattr(models, "generate_content", None)
    if generate_content is None:
        logger.warning("Gemini SDK client is missing generate_content; using fallback commentary.")
        return None

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
        if status == HTTP_TOO_MANY_REQUESTS:
            logger.warning(
                "Gemini rate limited (HTTP 429). Switching to next model immediately. "
                "input_chars=%d input_tokens_est=%d",
                prompt_chars,
                prompt_tokens_est,
            )
            return None
        logger.warning("Gemini API request failed with HTTP %d. Using fallback commentary.", status)
        return None
    except Exception:  # pragma: no cover - defensive guardrail
        logger.exception("Unexpected Gemini API error")
        return None
    return response


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


def _log_token_usage(body: object, *, model: str) -> None:
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
        model,
        prompt_tokens,
        candidates_tokens,
        total_tokens,
    )


def _field_value(obj: object, field_name: str) -> object | None:
    if isinstance(obj, Mapping):
        return obj.get(field_name)
    return getattr(obj, field_name, None)
