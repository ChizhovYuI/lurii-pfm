"""Shared base for OpenAI-compatible LLM providers using raw chat completions."""

from __future__ import annotations

import logging
from typing import Any

from openai import APIError, AsyncOpenAI

from pfm.ai.base import FALLBACK_COMMENTARY, CommentaryResult, LLMProvider

logger = logging.getLogger(__name__)

_TIMEOUT_SECONDS = 120.0


class OpenAICompatibleProvider(LLMProvider):
    """Base for providers that expose ``/v1/chat/completions``."""

    name: str = ""
    default_model: str = ""
    default_base_url: str = ""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        model: str | None = None,
        base_url: str | None = None,
        client: object | None = None,
        raw_client: object | None = None,
    ) -> None:
        self._api_key = api_key or ""
        self._model = model or self.default_model
        self._base_url = (base_url or self.default_base_url).rstrip("/")
        self._owns_client = client is None and raw_client is None
        self._client: Any
        self._raw_client: Any
        if client is not None:
            self._client = client
            self._raw_client = raw_client if raw_client is not None else client
        else:
            self._raw_client = raw_client or AsyncOpenAI(
                api_key=self._api_key or "not-needed",
                base_url=f"{self._base_url}/v1",
                timeout=_TIMEOUT_SECONDS,
            )
            self._client = self._raw_client

    async def validate_connection(self) -> None:
        """Validate auth/base URL using the provider's /models endpoint."""
        await self._raw_client.models.list()

    async def generate_commentary(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        max_output_tokens: int = 4096,
    ) -> CommentaryResult:
        """Call ``/v1/chat/completions`` and return plain markdown text."""
        try:
            response = await self._client.chat.completions.create(
                model=self._model,
                max_tokens=max_output_tokens,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
        except APIError:
            error_msg = f"{self.name} API request failed"
            logger.warning("%s", error_msg, exc_info=True)
            return CommentaryResult(text=FALLBACK_COMMENTARY, model=None, error=error_msg, provider=self.name)

        text = _extract_openai_text(response)
        finish_reason = _extract_openai_finish_reason(response)
        if not text:
            error_msg = f"{self.name} API returned empty response"
            logger.warning("%s", error_msg)
            return CommentaryResult(
                text=FALLBACK_COMMENTARY,
                model=None,
                error=error_msg,
                provider=self.name,
                finish_reason=finish_reason,
            )

        return CommentaryResult(text=text, model=self._model, provider=self.name, finish_reason=finish_reason)

    async def close(self) -> None:
        """Close the underlying OpenAI client if owned."""
        if self._owns_client and hasattr(self._raw_client, "close"):
            await self._raw_client.close()


def _extract_openai_text(response: object) -> str:
    choices = _field_value(response, "choices")
    if not isinstance(choices, list) or not choices:
        return ""

    message = _field_value(choices[0], "message")
    content = _field_value(message, "content")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            text = _field_value(item, "text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
        return "\n".join(parts).strip()
    return ""


def _extract_openai_finish_reason(response: object) -> str | None:
    choices = _field_value(response, "choices")
    if not isinstance(choices, list) or not choices:
        return None

    finish_reason = _field_value(choices[0], "finish_reason")
    if isinstance(finish_reason, str) and finish_reason.strip():
        return finish_reason.strip()
    return None


def _field_value(body: object, field: str) -> object | None:
    if isinstance(body, dict):
        return body.get(field)
    return getattr(body, field, None)
