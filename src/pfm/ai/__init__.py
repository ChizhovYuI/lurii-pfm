"""AI commentary helpers."""

from pfm.ai.analyst import (
    FALLBACK_COMMENTARY,
    GEMINI_MAX_OUTPUT_TOKENS,
    GEMINI_MODEL,
    CommentaryResult,
    generate_commentary,
    generate_commentary_with_model,
)
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
    "CommentaryResult",
    "generate_commentary",
    "generate_commentary_with_model",
    "render_weekly_report_user_prompt",
]
