"""Tests for AI Pydantic response schemas."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from pfm.ai.base import CommentarySection
from pfm.ai.schemas import CommentaryResponse, ReportSection


def test_commentary_response_to_sections():
    response = CommentaryResponse(
        sections=[
            ReportSection(title="Market Context", description="BTC at **$95k**."),
            ReportSection(title="Risk Alerts", description="High HHI."),
        ]
    )
    sections = response.to_commentary_sections()

    assert len(sections) == 2
    assert sections[0] == CommentarySection(title="Market Context", description="BTC at **$95k**.")
    assert sections[1] == CommentarySection(title="Risk Alerts", description="High HHI.")


def test_commentary_response_single_section():
    response = CommentaryResponse(sections=[ReportSection(title="Summary", description="All good.")])
    sections = response.to_commentary_sections()

    assert len(sections) == 1
    assert isinstance(sections, tuple)


def test_commentary_response_empty_sections_rejected():
    with pytest.raises(ValidationError):
        CommentaryResponse(sections=[])


def test_commentary_response_valid_sections_pass():
    response = CommentaryResponse(sections=[ReportSection(title="A", description="B")])
    assert len(response.sections) == 1
