"""Tests for OpenAI-compatible base provider using instructor."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from instructor.core import InstructorRetryException
from openai import APIError

from pfm.ai.base import FALLBACK_COMMENTARY
from pfm.ai.providers.openai_compat import OpenAICompatibleProvider
from pfm.ai.schemas import CommentaryResponse, ReportSection


class _TestProvider(OpenAICompatibleProvider):
    name = "test"
    default_model = "test-model"
    default_base_url = "http://localhost:9999"


def _mock_client(response=None, side_effect=None):
    """Create a mock instructor client."""
    client = MagicMock()
    create = AsyncMock(return_value=response, side_effect=side_effect)
    client.chat.completions.create = create
    return client


async def test_generate_commentary_success():
    response = CommentaryResponse(
        sections=[
            ReportSection(title="Market Context", description="BTC at **$95k**."),
            ReportSection(title="Risk Alerts", description="High HHI."),
        ]
    )
    client = _mock_client(response=response)
    provider = _TestProvider(client=client)

    result = await provider.generate_commentary("system", "user prompt")

    assert result.model == "test-model"
    assert len(result.sections) == 2
    assert result.sections[0].title == "Market Context"
    assert "BTC at **$95k**." in result.text

    call_kwargs = client.chat.completions.create.call_args
    assert call_kwargs.kwargs["model"] == "test-model"
    assert call_kwargs.kwargs["response_model"] is CommentaryResponse


async def test_generate_commentary_error_returns_fallback():
    err = APIError(message="API timeout", request=MagicMock(), body=None)
    client = _mock_client(side_effect=err)
    provider = _TestProvider(client=client)

    result = await provider.generate_commentary("sys", "usr")

    assert result.text == FALLBACK_COMMENTARY
    assert result.model is None
    assert result.error is not None


async def test_generate_commentary_custom_model():
    response = CommentaryResponse(sections=[ReportSection(title="Summary", description="OK.")])
    client = _mock_client(response=response)
    provider = _TestProvider(model="custom-model", client=client)

    result = await provider.generate_commentary("sys", "usr")

    assert result.model == "custom-model"
    call_kwargs = client.chat.completions.create.call_args
    assert call_kwargs.kwargs["model"] == "custom-model"


async def test_generate_commentary_retry_exhausted_returns_fallback():
    err = InstructorRetryException(
        n_attempts=3,
        total_usage=0,
        messages=[],
        last_completion=None,
    )
    client = _mock_client(side_effect=err)
    provider = _TestProvider(client=client)

    result = await provider.generate_commentary("sys", "usr")

    assert result.text == FALLBACK_COMMENTARY
    assert result.model is None
    assert result.error is not None


async def test_close_does_not_close_injected_client():
    client = _mock_client()
    provider = _TestProvider(client=client)

    await provider.close()
    # Injected client should not be closed (owns_client is False)
    assert not hasattr(client, "close") or not getattr(client.close, "called", False)
