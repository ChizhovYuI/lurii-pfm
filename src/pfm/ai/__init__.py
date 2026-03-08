"""AI commentary helpers."""

from pfm.ai.analyst import (
    GEMINI_MAX_OUTPUT_TOKENS,
    generate_commentary,
    generate_commentary_with_model,
)
from pfm.ai.base import (
    FALLBACK_COMMENTARY,
    CommentaryResult,
    CommentarySection,
    LLMProvider,
    ProviderName,
    flatten_sections,
)
from pfm.ai.prompts import (
    REPORT_PROMPT_VERSION,
    REPORT_SECTION_SPECS,
    WEEKLY_REPORT_SYSTEM_PROMPT,
    WEEKLY_REPORT_USER_PROMPT_TEMPLATE,
    AnalyticsSummary,
    render_report_section_prompt,
    render_weekly_report_user_prompt,
)

__all__ = [
    "FALLBACK_COMMENTARY",
    "GEMINI_MAX_OUTPUT_TOKENS",
    "REPORT_PROMPT_VERSION",
    "REPORT_SECTION_SPECS",
    "WEEKLY_REPORT_SYSTEM_PROMPT",
    "WEEKLY_REPORT_USER_PROMPT_TEMPLATE",
    "AnalyticsSummary",
    "CommentaryResult",
    "CommentarySection",
    "LLMProvider",
    "ProviderName",
    "flatten_sections",
    "generate_commentary",
    "generate_commentary_with_model",
    "render_report_section_prompt",
    "render_weekly_report_user_prompt",
]
