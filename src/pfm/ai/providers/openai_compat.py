"""Shared base for OpenAI-compatible LLM providers (Ollama, OpenRouter, Grok)."""

from __future__ import annotations

import logging

import httpx

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
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._api_key = api_key or ""
        self._model = model or self.default_model
        self._base_url = (base_url or self.default_base_url).rstrip("/")
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=_TIMEOUT_SECONDS)

    # -- hooks for subclasses --------------------------------------------------

    def _build_headers(self) -> dict[str, str]:
        """Return extra headers (e.g. Authorization). Override in subclass."""
        if self._api_key:
            return {"Authorization": f"Bearer {self._api_key}"}
        return {}

    def _parse_response(self, body: dict[str, object]) -> str:
        """Extract generated text from a chat-completions response."""
        choices = body.get("choices")
        if not isinstance(choices, list) or not choices:
            return ""
        first = choices[0]
        if not isinstance(first, dict):
            return ""
        message = first.get("message")
        if not isinstance(message, dict):
            return ""
        content = message.get("content")
        return str(content).strip() if content else ""

    # -- public API ------------------------------------------------------------

    async def generate_commentary(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        max_output_tokens: int = 4096,
    ) -> CommentaryResult:
        """Call ``/v1/chat/completions`` and return a :class:`CommentaryResult`."""
        url = f"{self._base_url}/v1/chat/completions"
        headers = {"Content-Type": "application/json", **self._build_headers()}
        payload: dict[str, object] = {
            "model": self._model,
            "max_tokens": max_output_tokens,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }

        try:
            response = await self._client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            body = response.json()
        except httpx.HTTPStatusError as exc:
            logger.warning("%s API error %d: %s", self.name, exc.response.status_code, exc)
            return CommentaryResult(text=FALLBACK_COMMENTARY, model=None)
        except (httpx.HTTPError, OSError) as exc:
            logger.warning("%s API request failed: %s", self.name, exc)
            return CommentaryResult(text=FALLBACK_COMMENTARY, model=None)

        text = self._parse_response(body)
        return CommentaryResult(text=text, model=self._model)

    async def close(self) -> None:
        """Close the underlying HTTP client if owned."""
        if self._owns_client:
            await self._client.aclose()
