"""AI commentary helpers."""

from pfm.ai.analyst import FALLBACK_COMMENTARY, GEMINI_MAX_OUTPUT_TOKENS, GEMINI_MODEL, generate_commentary
from pfm.ai.prompts import (
    WEEKLY_REPORT_SYSTEM_PROMPT,
    WEEKLY_REPORT_USER_PROMPT_TEMPLATE,
    AnalyticsSummary,
    render_weekly_report_user_prompt,
)

__all__ = [
    "FALLBACK_COMMENTARY",
    "GEMINI_MAX_OUTPUT_TOKENS",
    "GEMINI_MODEL",
    "WEEKLY_REPORT_SYSTEM_PROMPT",
    "WEEKLY_REPORT_USER_PROMPT_TEMPLATE",
    "AnalyticsSummary",
    "generate_commentary",
    "render_weekly_report_user_prompt",
]
