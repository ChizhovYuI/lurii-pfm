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
class CommentarySection:
    """A single section of AI commentary."""

    title: str
    description: str


@dataclass(frozen=True, slots=True)
class CommentaryResult:
    """Result of LLM commentary generation."""

    text: str
    model: str | None
    sections: tuple[CommentarySection, ...] = ()
    error: str | None = None


def flatten_sections(sections: tuple[CommentarySection, ...]) -> str:
    """Convert structured sections into plain text for Telegram."""
    parts: list[str] = []
    for section in sections:
        parts.append(f"## {section.title}")
        parts.append("")
        parts.append(section.description)
        parts.append("")
    return "\n".join(parts).strip()


class LLMProvider(ABC):
    """Abstract base for all LLM providers."""

    @abstractmethod
    async def validate_connection(self) -> None:
        """Validate provider connectivity without generating commentary."""

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
