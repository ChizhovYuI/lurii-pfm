"""Shared base for OpenAI-compatible LLM providers (OpenRouter, Grok) using instructor."""

from __future__ import annotations

import logging
from typing import Any

import instructor
from instructor.core import InstructorRetryException
from openai import APIError, AsyncOpenAI
from pydantic import ValidationError

from pfm.ai.base import FALLBACK_COMMENTARY, CommentaryResult, LLMProvider, flatten_sections
from pfm.ai.schemas import CommentaryResponse

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
            self._raw_client = raw_client if raw_client is not None else getattr(client, "client", client)
        else:
            self._raw_client = raw_client or AsyncOpenAI(
                api_key=self._api_key or "not-needed",
                base_url=f"{self._base_url}/v1",
                timeout=_TIMEOUT_SECONDS,
            )
            self._client = instructor.from_openai(self._raw_client, mode=instructor.Mode.JSON)

    # -- public API ------------------------------------------------------------

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
        """Call instructor-patched ``/v1/chat/completions`` and return a :class:`CommentaryResult`."""
        try:
            response: CommentaryResponse = await self._client.chat.completions.create(
                model=self._model,
                max_tokens=max_output_tokens,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                response_model=CommentaryResponse,
            )
        except (APIError, ValidationError, InstructorRetryException):
            error_msg = f"{self.name} API request failed"
            logger.warning("%s", error_msg, exc_info=True)
            return CommentaryResult(text=FALLBACK_COMMENTARY, model=None, error=error_msg)

        sections = response.to_commentary_sections()
        flat_text = flatten_sections(sections)
        return CommentaryResult(text=flat_text, model=self._model, sections=sections)

    async def close(self) -> None:
        """Close the underlying OpenAI client if owned."""
        if self._owns_client:
            await self._raw_client.close()
