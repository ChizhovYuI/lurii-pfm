"""Helpers for exposing the production weekly report prompt contract."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from pfm.ai.prompts import (
    GEMINI_WEEKLY_REPORT_JSON_SYSTEM_PROMPT,
    REPORT_PROMPT_VERSION,
    REPORT_SECTION_SPECS,
    WEEKLY_REPORT_JSON_SYSTEM_PROMPT,
    WEEKLY_REPORT_SYSTEM_PROMPT,
    render_gemini_weekly_report_json_prompt,
    render_report_section_prompt,
    render_weekly_report_json_prompt,
)
from pfm.db.ai_report_memory_store import AIReportMemoryStore
from pfm.db.ai_store import AIProviderStore
from pfm.server.analytics_helper import build_analytics_summary

if TYPE_CHECKING:
    from datetime import date
    from pathlib import Path

    from pfm.db.repository import Repository

_PRIOR_SECTION_DESCRIPTION_CHAR_LIMIT = 300
_PRIOR_SECTIONS_TOTAL_CHAR_LIMIT = 1200


@dataclass(frozen=True, slots=True)
class WeeklyReportPromptSection:
    """Single section contract for external weekly-report generation."""

    index: int
    slug: str
    title: str
    purpose: str
    structure: str
    output_style: str
    base_user_prompt: str


@dataclass(frozen=True, slots=True)
class WeeklyReportPromptPack:
    """Full prompt pack for external AI weekly report generation."""

    kind: str
    prompt_version: int
    as_of_date: str
    workflow: str
    includes_memory: bool
    system_prompt: str
    investor_memory: str
    analytics_context: str
    execution_notes: tuple[str, ...]
    prior_sections_policy: dict[str, int]
    sections: tuple[WeeklyReportPromptSection, ...]


async def build_weekly_report_prompt_pack(
    repo: Repository,
    db_path: Path,
    as_of: date,
) -> dict[str, Any]:
    """Build the production weekly report prompt pack for external AI tools."""
    latest = await repo.get_latest_snapshots()
    if not latest:
        return {
            "kind": "weekly_report_prompt_pack",
            "prompt_version": REPORT_PROMPT_VERSION,
            "as_of_date": as_of.isoformat(),
            "error": "No snapshots available",
        }

    analytics = await build_analytics_summary(repo, as_of, db_path=db_path)
    investor_memory = await AIReportMemoryStore(db_path).get()
    active_provider = await AIProviderStore(db_path).get_active()
    workflow = _workflow_for_provider(active_provider)

    if workflow == "single_shot_json":
        is_deepseek = bool(active_provider and active_provider.type == "deepseek")
        system_prompt = WEEKLY_REPORT_JSON_SYSTEM_PROMPT if is_deepseek else GEMINI_WEEKLY_REPORT_JSON_SYSTEM_PROMPT
        prompt = (
            render_weekly_report_json_prompt(analytics, investor_memory=investor_memory)
            if is_deepseek
            else render_gemini_weekly_report_json_prompt(analytics, investor_memory=investor_memory)
        )
        return {
            "kind": "weekly_report_prompt_pack",
            "prompt_version": REPORT_PROMPT_VERSION,
            "as_of_date": as_of.isoformat(),
            "workflow": workflow,
            "includes_memory": True,
            "system_prompt": system_prompt,
            "investor_memory": investor_memory,
            "analytics_context": _extract_analytics_block(prompt),
            "execution_notes": (
                "Generate the full weekly report in one JSON object.",
                "Use the exact section titles and order required by the production backend.",
                "Return only a valid JSON object with a top-level sections array.",
                "Explain fiat balance changes using Fiat balance bridge and Internal "
                "conversions before FX or valuation explanations.",
            ),
            "prior_sections_policy": {},
            "sections": (),
        }

    sections = tuple(
        WeeklyReportPromptSection(
            index=index,
            slug=spec.slug,
            title=spec.title,
            purpose=spec.purpose,
            structure=spec.structure,
            output_style=spec.output_style,
            base_user_prompt=render_report_section_prompt(
                spec,
                analytics,
                investor_memory=investor_memory,
                prior_sections=(),
            ),
        )
        for index, spec in enumerate(REPORT_SECTION_SPECS, start=1)
    )
    analytics_context = render_report_section_prompt(
        REPORT_SECTION_SPECS[0],
        analytics,
        investor_memory=investor_memory,
        prior_sections=(),
    )
    analytics_block = _extract_analytics_block(analytics_context)

    pack = WeeklyReportPromptPack(
        kind="weekly_report_prompt_pack",
        prompt_version=REPORT_PROMPT_VERSION,
        as_of_date=as_of.isoformat(),
        workflow="section_by_section",
        includes_memory=True,
        system_prompt=WEEKLY_REPORT_SYSTEM_PROMPT,
        investor_memory=investor_memory,
        analytics_context=analytics_block,
        execution_notes=(
            "Generate sections in the listed order.",
            "Use the same system prompt for every section.",
            "For section 1, use the provided base_user_prompt as-is.",
            "For later sections, append prior generated sections under <prior_sections> "
            "using the same clipping rules as backend.",
            "Explain fiat balance changes using Fiat balance bridge and Internal conversions "
            "before FX or valuation explanations.",
        ),
        prior_sections_policy={
            "per_section_description_char_limit": _PRIOR_SECTION_DESCRIPTION_CHAR_LIMIT,
            "total_char_limit": _PRIOR_SECTIONS_TOTAL_CHAR_LIMIT,
        },
        sections=sections,
    )
    return asdict(pack)


def _workflow_for_provider(provider: object | None) -> str:
    if provider is None:
        return "section_by_section"
    provider_type = getattr(provider, "type", None)
    model = (getattr(provider, "model", None) or "").strip()
    if provider_type == "deepseek" and model == "deepseek-chat":
        return "single_shot_json"
    if provider_type == "gemini":
        return "single_shot_json"
    return "section_by_section"


def _extract_analytics_block(prompt: str) -> str:
    """Extract the analytics block from a rendered section prompt."""
    start_tag = "<analytics>"
    end_tag = "</analytics>"
    start = prompt.find(start_tag)
    end = prompt.find(end_tag)
    if start == -1 or end == -1 or end <= start:
        return ""
    return prompt[start + len(start_tag) : end].strip()
