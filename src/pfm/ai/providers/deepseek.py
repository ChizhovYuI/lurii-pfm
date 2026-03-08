"""DeepSeek LLM provider with reasoning-aware response handling."""

from __future__ import annotations

import logging

from openai import APIError

from pfm.ai.base import FALLBACK_COMMENTARY, CommentaryResult
from pfm.ai.providers.openai_compat import OpenAICompatibleProvider, _field_value
from pfm.ai.providers.registry import register_provider

logger = logging.getLogger(__name__)


@register_provider
class DeepSeekProvider(OpenAICompatibleProvider):
    """DeepSeek via the native API, with support for reasoning metadata."""

    name = "deepseek"
    description = "DeepSeek — native API, deepseek-chat recommended for weekly reports"
    default_model = "deepseek-chat"
    default_base_url = "https://api.deepseek.com"
    models: tuple[tuple[str, str], ...] = (
        ("deepseek-chat", "recommended for weekly reports"),
        ("deepseek-reasoner", "advanced reasoning, slower and needs larger token budgets"),
    )

    def __init__(
        self,
        *,
        api_key: str,
        model: str | None = None,
        base_url: str | None = None,
        client: object | None = None,
        raw_client: object | None = None,
    ) -> None:
        super().__init__(
            api_key=api_key,
            model=model,
            base_url=base_url,
            client=client,
            raw_client=raw_client,
        )

    async def generate_commentary(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        max_output_tokens: int = 4096,
    ) -> CommentaryResult:
        """Call DeepSeek chat completions and preserve reasoning metadata."""
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

        text, reasoning_text, finish_reason = _extract_deepseek_parts(response)
        if not text:
            if reasoning_text:
                error_msg = "deepseek API returned no final answer before reasoning budget was exhausted"
                logger.warning(
                    "%s model=%s finish_reason=%s reasoning_chars=%d",
                    error_msg,
                    self._model,
                    finish_reason,
                    len(reasoning_text),
                )
            else:
                error_msg = "deepseek API returned empty response"
                logger.warning("%s model=%s finish_reason=%s", error_msg, self._model, finish_reason)
            return CommentaryResult(
                text=FALLBACK_COMMENTARY,
                model=None,
                error=error_msg,
                provider=self.name,
                finish_reason=finish_reason,
                reasoning_text=reasoning_text or None,
            )

        return CommentaryResult(
            text=text,
            model=self._model,
            provider=self.name,
            finish_reason=finish_reason,
            reasoning_text=reasoning_text or None,
        )

    async def generate_commentary_json(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        max_output_tokens: int = 4096,
    ) -> CommentaryResult:
        """Call DeepSeek JSON mode and return the raw JSON payload."""
        try:
            response = await self._client.chat.completions.create(
                model=self._model,
                max_tokens=max_output_tokens,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
        except APIError:
            error_msg = f"{self.name} API request failed"
            logger.warning("%s", error_msg, exc_info=True)
            return CommentaryResult(text="", model=None, error=error_msg, provider=self.name)

        text, reasoning_text, finish_reason = _extract_deepseek_parts(response)
        if not text:
            if reasoning_text:
                error_msg = "deepseek API returned no final answer before reasoning budget was exhausted"
                logger.warning(
                    "%s model=%s finish_reason=%s reasoning_chars=%d",
                    error_msg,
                    self._model,
                    finish_reason,
                    len(reasoning_text),
                )
            else:
                error_msg = "deepseek API returned empty JSON response"
                logger.warning("%s model=%s finish_reason=%s", error_msg, self._model, finish_reason)
            return CommentaryResult(
                text="",
                model=None,
                error=error_msg,
                provider=self.name,
                finish_reason=finish_reason,
                reasoning_text=reasoning_text or None,
            )

        return CommentaryResult(
            text=text,
            model=self._model,
            provider=self.name,
            finish_reason=finish_reason,
            reasoning_text=reasoning_text or None,
        )


def _extract_deepseek_parts(response: object) -> tuple[str, str, str | None]:
    choices = _field_value(response, "choices")
    if not isinstance(choices, list) or not choices:
        return "", "", None

    choice = choices[0]
    message = _field_value(choice, "message")
    finish_reason = _field_value(choice, "finish_reason")
    return (
        _extract_content_block(_field_value(message, "content")),
        _extract_content_block(_field_value(message, "reasoning_content")),
        str(finish_reason).strip() if finish_reason else None,
    )


def _extract_content_block(content: object | None) -> str:
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
