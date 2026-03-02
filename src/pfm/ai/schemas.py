"""Pydantic response models for structured LLM output via instructor."""

from __future__ import annotations

from pydantic import BaseModel, Field

from pfm.ai.base import CommentarySection


class ReportSection(BaseModel):
    """A single section of AI-generated commentary."""

    title: str = Field(description="Short section heading, plain text")
    description: str = Field(description="Section body in GitHub-flavored Markdown")


class CommentaryResponse(BaseModel):
    """Structured response from an LLM for portfolio commentary."""

    sections: list[ReportSection] = Field(min_length=1, max_length=10)

    def to_commentary_sections(self) -> tuple[CommentarySection, ...]:
        """Convert to the application's frozen dataclass format."""
        return tuple(CommentarySection(title=s.title, description=s.description) for s in self.sections)
