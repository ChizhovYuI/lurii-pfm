"""AI commentary helpers."""

from pfm.ai.analyst import CLAUDE_MAX_TOKENS, CLAUDE_MODEL, FALLBACK_COMMENTARY, generate_commentary
from pfm.ai.prompts import (
    WEEKLY_REPORT_SYSTEM_PROMPT,
    WEEKLY_REPORT_USER_PROMPT_TEMPLATE,
    AnalyticsSummary,
    render_weekly_report_user_prompt,
)

__all__ = [
    "CLAUDE_MAX_TOKENS",
    "CLAUDE_MODEL",
    "FALLBACK_COMMENTARY",
    "WEEKLY_REPORT_SYSTEM_PROMPT",
    "WEEKLY_REPORT_USER_PROMPT_TEMPLATE",
    "AnalyticsSummary",
    "generate_commentary",
    "render_weekly_report_user_prompt",
]
