"""Ollama LLM provider using instructor + OpenAI-compatible /v1 endpoint."""

from __future__ import annotations

import logging
from typing import Any

import httpx
import instructor
from instructor.core import InstructorRetryException
from openai import APIError, AsyncOpenAI
from pydantic import ValidationError

from pfm.ai.base import FALLBACK_COMMENTARY, CommentaryResult, LLMProvider, flatten_sections
from pfm.ai.providers.registry import register_provider
from pfm.ai.schemas import CommentaryResponse

logger = logging.getLogger(__name__)

_TIMEOUT_SECONDS = 120.0
_PULL_TIMEOUT_SECONDS = 600.0


@register_provider
class OllamaProvider(LLMProvider):
    """Ollama local LLM provider using instructor for structured output."""

    name = "ollama"
    description = "Ollama — local/private inference, no API key needed"
    default_model = "qwen3:14b"
    default_base_url = "http://localhost:11434"
    models: tuple[tuple[str, str], ...] = (
        ("qwen3:14b", "best for 16+ GB RAM"),
        ("llama3.1:8b", "best for 8 GB RAM"),
    )

    def __init__(
        self,
        *,
        model: str | None = None,
        base_url: str | None = None,
        openai_client: object | None = None,
        http_client: httpx.AsyncClient | None = None,
        **_kwargs: object,
    ) -> None:
        self._model = model or self.default_model
        self._base_url = (base_url or self.default_base_url).rstrip("/")

        self._owns_openai_client = openai_client is None
        self._client: Any
        if openai_client is not None:
            self._client = openai_client
        else:
            raw = AsyncOpenAI(
                api_key="ollama",
                base_url=f"{self._base_url}/v1",
                timeout=_TIMEOUT_SECONDS,
            )
            self._client = instructor.from_openai(raw, mode=instructor.Mode.JSON)

        self._owns_http_client = http_client is None
        self._http_client = http_client or httpx.AsyncClient(timeout=_TIMEOUT_SECONDS)

    async def generate_commentary(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        max_output_tokens: int = 4096,
    ) -> CommentaryResult:
        """Generate commentary via Ollama, auto-pulling on failure."""
        result = await self._call_chat(system_prompt, user_prompt, max_output_tokens=max_output_tokens)
        if result.text:
            return result

        logger.info("Ollama returned empty result; attempting model pull for %s.", self._model)
        pulled = await self._try_pull_model()
        if not pulled:
            return result

        return await self._call_chat(system_prompt, user_prompt, max_output_tokens=max_output_tokens)

    async def validate_connection(self) -> None:
        """Validate Ollama reachability and model availability."""
        url = f"{self._base_url}/api/tags"
        response = await self._http_client.get(url, timeout=_TIMEOUT_SECONDS)
        response.raise_for_status()
        body = response.json()
        models = body.get("models")
        if not isinstance(models, list):
            msg = "Ollama returned an invalid models list."
            raise TypeError(msg)

        installed = {str(model.get("name", "")) for model in models if isinstance(model, dict)}
        if self._model and self._model not in installed:
            msg = f"Ollama model '{self._model}' is not installed."
            raise ValueError(msg)

    async def _call_chat(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        max_output_tokens: int,
    ) -> CommentaryResult:
        """Send a structured chat request via instructor."""
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
        except APIError:
            # Server/connection error — model may not exist, allow auto-pull.
            logger.warning("Ollama API error", exc_info=True)
            return CommentaryResult(text="", model=self._model)
        except (ValidationError, InstructorRetryException):
            # Model responded but generated invalid JSON — no point pulling.
            logger.warning("Ollama structured output validation failed", exc_info=True)
            return CommentaryResult(text=FALLBACK_COMMENTARY, model=self._model, error="structured_output_failed")

        sections = response.to_commentary_sections()
        flat_text = flatten_sections(sections)
        return CommentaryResult(text=flat_text, model=self._model, sections=sections)

    async def _try_pull_model(self) -> bool:
        """Pull the model via Ollama's ``/api/pull`` endpoint."""
        url = f"{self._base_url}/api/pull"
        try:
            response = await self._http_client.post(
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
        """Close owned clients."""
        if self._owns_openai_client:
            await self._client.client.close()
        if self._owns_http_client:
            await self._http_client.aclose()


async def list_installed_models(base_url: str | None = None) -> list[str]:
    """Fetch installed model names from Ollama's ``/api/tags`` endpoint."""
    url = f"{(base_url or OllamaProvider.default_base_url).rstrip('/')}/api/tags"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            body = resp.json()
    except (httpx.HTTPError, OSError):
        return []
    models = body.get("models")
    if not isinstance(models, list):
        return []
    return [str(m["name"]) for m in models if isinstance(m, dict) and "name" in m]
