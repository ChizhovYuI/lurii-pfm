"""Ollama LLM provider using native /api/chat endpoint."""

from __future__ import annotations

import logging

import httpx

from pfm.ai.base import CommentaryResult, LLMProvider
from pfm.ai.providers.registry import register_provider

logger = logging.getLogger(__name__)

_TIMEOUT_SECONDS = 120.0
_PULL_TIMEOUT_SECONDS = 600.0


@register_provider
class OllamaProvider(LLMProvider):
    """Ollama local LLM provider using the native ``/api/chat`` endpoint."""

    name = "ollama"
    default_model = "qwen3:14b"
    default_base_url = "http://localhost:11434"

    def __init__(
        self,
        *,
        model: str | None = None,
        base_url: str | None = None,
        client: httpx.AsyncClient | None = None,
        **_kwargs: object,
    ) -> None:
        self._model = model or self.default_model
        self._base_url = (base_url or self.default_base_url).rstrip("/")
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=_TIMEOUT_SECONDS)

    async def generate_commentary(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        max_output_tokens: int = 4096,
    ) -> CommentaryResult:
        """Generate commentary via Ollama's native ``/api/chat``, auto-pulling on failure."""
        result = await self._call_chat(system_prompt, user_prompt, max_output_tokens=max_output_tokens)
        if result.text:
            return result

        logger.info("Ollama returned empty result; attempting model pull for %s.", self._model)
        pulled = await self._try_pull_model()
        if not pulled:
            return result

        return await self._call_chat(system_prompt, user_prompt, max_output_tokens=max_output_tokens)

    async def _call_chat(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        max_output_tokens: int,
    ) -> CommentaryResult:
        """Send a chat request to Ollama's native API."""
        url = f"{self._base_url}/api/chat"
        payload: dict[str, object] = {
            "model": self._model,
            "stream": False,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "options": {"num_predict": max_output_tokens},
        }

        try:
            response = await self._client.post(url, json=payload)
            response.raise_for_status()
            body = response.json()
        except (httpx.HTTPError, OSError) as exc:
            logger.warning("Ollama API request failed: %s", exc)
            return CommentaryResult(text="", model=self._model)

        text = self._parse_response(body)
        return CommentaryResult(text=text, model=self._model)

    @staticmethod
    def _parse_response(body: dict[str, object]) -> str:
        """Extract text from Ollama's native chat response."""
        message = body.get("message")
        if not isinstance(message, dict):
            return ""
        content = message.get("content")
        return str(content).strip() if content else ""

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

    async def close(self) -> None:
        """Close the underlying HTTP client if owned."""
        if self._owns_client:
            await self._client.aclose()
