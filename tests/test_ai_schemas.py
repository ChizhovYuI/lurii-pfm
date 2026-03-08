"""Tests for AI Pydantic response schemas."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from pfm.ai.base import CommentarySection
from pfm.ai.schemas import CommentaryResponse, ReportSection


def _weekly_sections() -> list[ReportSection]:
    return [
        ReportSection(title="Market Context", description="BTC at **$95k**."),
        ReportSection(title="Portfolio Health Assessment", description="Diversification is acceptable."),
        ReportSection(title="Rebalancing Opportunities", description="- Trim concentration."),
        ReportSection(title="Risk Alerts", description="- High HHI."),
        ReportSection(
            title="Actionable Recommendations for Next 7 Days",
            description="1. Review risk.\n2. Check buffers.\n3. Rebalance carefully.",
        ),
    ]


def test_commentary_response_to_sections():
    response = CommentaryResponse(sections=_weekly_sections())
    sections = response.to_commentary_sections()

    assert len(sections) == 5
    assert sections[0] == CommentarySection(title="Market Context", description="BTC at **$95k**.")
    assert sections[3] == CommentarySection(title="Risk Alerts", description="- High HHI.")


def test_commentary_response_requires_exact_weekly_titles_and_order():
    bad_sections = _weekly_sections()
    bad_sections[0] = ReportSection(title="Summary", description="All good.")
    with pytest.raises(ValidationError):
        CommentaryResponse(sections=bad_sections)


def test_commentary_response_empty_sections_rejected():
    with pytest.raises(ValidationError):
        CommentaryResponse(sections=[])


def test_commentary_response_valid_sections_pass():
    response = CommentaryResponse(sections=_weekly_sections())
    assert len(response.sections) == 5
