"""Ollama LLM provider (local, OpenAI-compatible)."""

from __future__ import annotations

import logging

import httpx

from pfm.ai.base import CommentaryResult
from pfm.ai.providers.openai_compat import OpenAICompatibleProvider
from pfm.ai.providers.registry import register_provider

logger = logging.getLogger(__name__)

_PULL_TIMEOUT_SECONDS = 600.0


@register_provider
class OllamaProvider(OpenAICompatibleProvider):
    """Ollama local LLM provider."""

    name = "ollama"
    default_model = "llama3.1:8b"
    default_base_url = "http://localhost:11434"

    def _build_headers(self) -> dict[str, str]:
        return {}

    async def generate_commentary(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        max_output_tokens: int = 4096,
    ) -> CommentaryResult:
        """Generate commentary, auto-pulling the model on first empty result."""
        result = await super().generate_commentary(
            system_prompt,
            user_prompt,
            max_output_tokens=max_output_tokens,
        )
        if result.text:
            return result

        logger.info("Ollama returned empty result; attempting model pull for %s.", self._model)
        pulled = await self._try_pull_model()
        if not pulled:
            return result

        return await super().generate_commentary(
            system_prompt,
            user_prompt,
            max_output_tokens=max_output_tokens,
        )

    async def _try_pull_model(self) -> bool:
        """Pull the model via Ollama's ``/api/pull`` endpoint."""
        url = f"{self._base_url}/api/pull"
        try:
            response = await self._client.post(
                url,
                json={"name": self._model},
                timeout=_PULL_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
        except (httpx.HTTPError, OSError):
            logger.warning("Ollama model pull failed for %s.", self._model, exc_info=True)
            return False
        else:
            logger.info("Ollama model %s pulled successfully.", self._model)
            return True

    async def _handle_error(self, exc: Exception) -> CommentaryResult | None:
        """On connection errors, try pulling the model then retry."""
        if not isinstance(exc, httpx.ConnectError | httpx.HTTPStatusError):
            return None
        logger.info("Ollama request error; attempting model pull: %s", exc)
        pulled = await self._try_pull_model()
        if pulled:
            return None  # let caller re-raise; actual retry is in generate_commentary
        return CommentaryResult(text="", model=self._model)
