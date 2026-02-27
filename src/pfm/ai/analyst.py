"""Claude API client for portfolio commentary."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from anthropic import APIError, AsyncAnthropic
from anthropic.types import TextBlock

from pfm.ai.prompts import WEEKLY_REPORT_SYSTEM_PROMPT, AnalyticsSummary, render_weekly_report_user_prompt
from pfm.config import get_settings

if TYPE_CHECKING:
    from anthropic.types import Message

logger = logging.getLogger(__name__)

CLAUDE_MODEL = "claude-sonnet-4-20250514"
CLAUDE_MAX_TOKENS = 1024
FALLBACK_COMMENTARY = (
    "AI commentary is currently unavailable. " "Review net worth trend, concentration risk, and PnL changes manually."
)


async def generate_commentary(
    analytics: AnalyticsSummary,
    *,
    client: AsyncAnthropic | None = None,
) -> str:
    """Generate weekly portfolio commentary using Claude."""
    settings = get_settings()
    api_key = settings.anthropic_api_key.get_secret_value()

    if client is None:
        if not api_key:
            logger.warning("Anthropic API key is not configured; returning fallback commentary.")
            return FALLBACK_COMMENTARY
        client = AsyncAnthropic(api_key=api_key)

    prompt = render_weekly_report_user_prompt(analytics)
    try:
        response = await client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=CLAUDE_MAX_TOKENS,
            system=WEEKLY_REPORT_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
    except APIError as exc:
        logger.warning("Claude API request failed: %s", exc)
        return FALLBACK_COMMENTARY

    _log_token_usage(response)
    text = _extract_text(response)
    if text:
        return text

    logger.warning("Claude returned empty text response; using fallback commentary.")
    return FALLBACK_COMMENTARY


def _extract_text(message: Message) -> str:
    text_blocks = [block.text for block in message.content if isinstance(block, TextBlock)]
    return "\n".join(part.strip() for part in text_blocks if part.strip()).strip()


def _log_token_usage(message: Message) -> None:
    usage = message.usage
    logger.info(
        "claude_usage model=%s input_tokens=%s output_tokens=%s "
        "cache_read_input_tokens=%s cache_creation_input_tokens=%s",
        message.model,
        usage.input_tokens,
        usage.output_tokens,
        usage.cache_read_input_tokens,
        usage.cache_creation_input_tokens,
    )
