"""Pydantic models for structured LLM weekly report output."""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator, model_validator

from pfm.ai.base import CommentarySection

EXPECTED_WEEKLY_REPORT_TITLES = (
    "Market Context",
    "Portfolio Health Assessment",
    "Rebalancing Opportunities",
    "Risk Alerts",
    "Actionable Recommendations for Next 7 Days",
)


class ReportSection(BaseModel):
    """A single section of AI-generated commentary."""

    title: str = Field(description="Short section heading, plain text")
    description: str = Field(description="Section body in GitHub-flavored Markdown")


class CommentaryResponse(BaseModel):
    """Structured response from an LLM for portfolio commentary."""

    sections: list[ReportSection] = Field(min_length=5, max_length=5)

    @field_validator("sections")
    @classmethod
    def _validate_section_titles(cls, sections: list[ReportSection]) -> list[ReportSection]:
        titles = [section.title.strip() for section in sections]
        if titles != list(EXPECTED_WEEKLY_REPORT_TITLES):
            msg = "Weekly report sections must match the required titles and order."
            raise ValueError(msg)
        return sections

    @model_validator(mode="after")
    def _validate_non_empty_descriptions(self) -> CommentaryResponse:
        if any(not section.description.strip() for section in self.sections):
            msg = "Weekly report sections must have non-empty descriptions."
            raise ValueError(msg)
        return self

    def to_commentary_sections(self) -> tuple[CommentarySection, ...]:
        """Convert to the application's frozen dataclass format."""
        return tuple(CommentarySection(title=s.title, description=s.description) for s in self.sections)
