"""Base types and abstract interface for LLM providers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum


class ProviderName(StrEnum):
    """Supported LLM provider identifiers."""

    gemini = "gemini"
    ollama = "ollama"
    openrouter = "openrouter"
    grok = "grok"


FALLBACK_COMMENTARY = (
    "AI commentary is currently unavailable. Review net worth trend, concentration risk, and PnL changes manually."
)


@dataclass(frozen=True, slots=True)
class CommentaryResult:
    """Result of LLM commentary generation."""

    text: str
    model: str | None
    error: str | None = None


class LLMProvider(ABC):
    """Abstract base for all LLM providers."""

    @abstractmethod
    async def generate_commentary(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        max_output_tokens: int = 4096,
    ) -> CommentaryResult:
        """Generate commentary text from prompts."""

    @abstractmethod
    async def close(self) -> None:
        """Release provider resources."""
