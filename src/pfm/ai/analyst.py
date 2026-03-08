"""AI weekly commentary orchestrator."""

from __future__ import annotations

import inspect
import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from pydantic import ValidationError

from pfm.ai.base import FALLBACK_COMMENTARY, CommentaryResult, CommentarySection, ProviderName, flatten_sections
from pfm.ai.commentary_parser import (
    escape_newlines_in_json_strings,
    finalize_commentary_text,
    parse_commentary_sections,
)
from pfm.ai.prompts import (
    GEMINI_WEEKLY_REPORT_JSON_SYSTEM_PROMPT,
    REPORT_SECTION_SPECS,
    WEEKLY_REPORT_JSON_SYSTEM_PROMPT,
    WEEKLY_REPORT_SYSTEM_PROMPT,
    render_gemini_weekly_report_json_prompt,
    render_report_section_prompt,
    render_weekly_report_json_prompt,
)
from pfm.ai.providers.registry import PROVIDER_REGISTRY
from pfm.ai.schemas import CommentaryResponse
from pfm.config import get_settings
from pfm.db.ai_report_memory_store import AIReportMemoryStore
from pfm.db.ai_store import AIProviderStore

if TYPE_CHECKING:
    from collections.abc import Callable

    from pfm.ai.base import LLMProvider
    from pfm.ai.prompts import AnalyticsSummary, ReportSectionSpec
    from pfm.db.models import AIProvider

logger = logging.getLogger(__name__)

GEMINI_MAX_OUTPUT_TOKENS = 4096
_MIN_SECTION_TEXT_CHARS = 80
_MIN_CODE_FENCE_LINES = 2
_MAX_SINGLE_PARAGRAPH_CHARS = 280
_MAX_PARAGRAPH_CHARS = 280
_MAX_SENTENCES_PER_PARAGRAPH = 3
_MIN_STRUCTURED_BLOCKS = 2
_EXACT_TWO_BLOCKS = 2
_MIN_LIST_ITEMS = 2
_MAX_LIST_ITEMS = 4
_MIN_NUMBERED_ITEMS = 3
_MAX_NUMBERED_ITEMS = 5
_MAX_BULLET_ITEM_CHARS = 220
_DEEPSEEK_REASONER_MIN_OUTPUT_TOKENS = 6000
_DEEPSEEK_JSON_MAX_OUTPUT_TOKENS = 6000
_GEMINI_JSON_MAX_OUTPUT_TOKENS = 4096
_DATA_LIMITATION_MARKERS = (
    "insufficient data",
    "not enough data",
    "limited data",
    "data is missing",
    "data was not available",
    "no recent data",
)
_LIST_LINE_RE = re.compile(r"^\s*(?:[-*]|\d+\.)\s+")
_NEGATIVE_BALANCE_WORDS = (
    "drop",
    "dropped",
    "decline",
    "declined",
    "fell",
    "fall",
    "loss",
    "losses",
    "weakness",
    "weakened",
    "disappearance",
    "disappeared",
)
_CONVERSION_WORDS = (
    "redeploy",
    "redeployed",
    "conversion",
    "converted",
    "funded",
    "used to buy",
    "used to fund",
    "spent to buy",
    "spent on",
    "moved into",
    "reallocated",
    "rotated into",
    "purchase",
    "purchased",
    "bought",
)
_CONVERSION_VALIDATION_SECTIONS = {
    "Market Context",
    "Rebalancing Opportunities",
    "Risk Alerts",
}


class CommentaryProgressCallback(Protocol):
    """Callable used to report section-by-section commentary progress."""

    def __call__(self, completed_sections: int, total_sections: int, current_section: str) -> object: ...


@dataclass(frozen=True, slots=True)
class SectionInputContext:
    """Derived prompt facts used to validate section causality and structure."""

    redeployed_fiat_assets: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SectionGenerationDiagnostic:
    """Per-section generation status captured for debugging and API output."""

    title: str
    status: str
    reason: str
    finish_reason: str | None = None

    def as_dict(self) -> dict[str, str | None]:
        return {
            "title": self.title,
            "status": self.status,
            "reason": self.reason,
            "finish_reason": self.finish_reason,
        }


@dataclass(frozen=True, slots=True)
class SingleShotReportDiagnostic:
    """Whole-report diagnostic for single-shot JSON generation."""

    strategy: str
    provider: str | None
    model: str | None
    status: str
    finish_reason: str | None
    attempts: int
    reason: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "strategy": self.strategy,
            "provider": self.provider,
            "model": self.model,
            "status": self.status,
            "finish_reason": self.finish_reason,
            "attempts": self.attempts,
            "reason": self.reason,
        }


async def generate_commentary(
    analytics: AnalyticsSummary,
    *,
    db_path: str | Path | None = None,
) -> str:
    """Generate weekly portfolio commentary (text only)."""
    result = await generate_commentary_with_model(analytics, db_path=db_path)
    return result.text


async def generate_commentary_with_model(
    analytics: AnalyticsSummary,
    *,
    db_path: str | Path | None = None,
    progress_callback: CommentaryProgressCallback | None = None,
    investor_memory: str | None = None,
) -> CommentaryResult:
    """Generate weekly portfolio commentary with model info."""
    provider = await _resolve_provider(db_path)
    if provider is None:
        logger.warning("No AI provider configured; returning fallback commentary.")
        return CommentaryResult(text=FALLBACK_COMMENTARY, model=None)

    provider_name = _provider_name(provider)
    configured_model = _provider_model_name(provider)
    resolved_path = _resolve_db_path(db_path)
    if investor_memory is None:
        investor_memory = await AIReportMemoryStore(resolved_path).get()

    try:
        if _should_use_deepseek_json_mode(provider):
            return await _generate_single_shot_commentary(
                provider,
                analytics,
                investor_memory=investor_memory,
                progress_callback=progress_callback,
                strategy="deepseek_json_single_shot",
                system_prompt=WEEKLY_REPORT_JSON_SYSTEM_PROMPT,
                prompt_renderer=render_weekly_report_json_prompt,
                max_output_tokens=_DEEPSEEK_JSON_MAX_OUTPUT_TOKENS,
            )
        if _should_use_gemini_json_mode(provider):
            return await _generate_single_shot_commentary(
                provider,
                analytics,
                investor_memory=investor_memory,
                progress_callback=progress_callback,
                strategy="gemini_json_single_shot",
                system_prompt=GEMINI_WEEKLY_REPORT_JSON_SYSTEM_PROMPT,
                prompt_renderer=render_gemini_weekly_report_json_prompt,
                max_output_tokens=_GEMINI_JSON_MAX_OUTPUT_TOKENS,
            )

        sections, models, diagnostics = await _generate_sections(
            provider,
            analytics,
            investor_memory=investor_memory,
            progress_callback=progress_callback,
        )
    finally:
        await provider.close()

    generation_meta = _build_generation_meta(
        provider_name=provider_name,
        model=_summarize_models(models) or configured_model,
        diagnostics=diagnostics,
    )
    fallback_titles = tuple(diagnostic.title for diagnostic in diagnostics if diagnostic.status == "fallback")
    if not sections:
        logger.warning("All report sections failed; using fallback commentary.")
        return CommentaryResult(
            text=FALLBACK_COMMENTARY,
            model=None,
            error="All report sections fell back to the generic commentary.",
            provider=provider_name,
            generation_meta=generation_meta,
        )

    model = _summarize_models(models)
    error: str | None = None
    if fallback_titles:
        error = f"Some sections used fallback text: {', '.join(fallback_titles)}."
    return CommentaryResult(
        text=flatten_sections(sections),
        model=model,
        sections=sections,
        error=error,
        provider=provider_name,
        generation_meta=generation_meta,
    )


async def _generate_sections(
    provider: LLMProvider,
    analytics: AnalyticsSummary,
    *,
    investor_memory: str,
    progress_callback: CommentaryProgressCallback | None,
) -> tuple[tuple[CommentarySection, ...], tuple[str, ...], tuple[SectionGenerationDiagnostic, ...]]:
    generated_sections: list[CommentarySection] = []
    models: list[str] = []
    diagnostics: list[SectionGenerationDiagnostic] = []

    total_sections = len(REPORT_SECTION_SPECS)
    for spec in REPORT_SECTION_SPECS:
        await _emit_progress(progress_callback, len(generated_sections), total_sections, spec.title)
        section, model, diagnostic = await _generate_single_section(
            provider,
            spec,
            analytics,
            investor_memory=investor_memory,
            prior_sections=tuple(generated_sections),
        )
        generated_sections.append(section)
        if model:
            models.append(model)
        diagnostics.append(diagnostic)

    if diagnostics and all(diagnostic.status == "fallback" for diagnostic in diagnostics):
        return (), tuple(models), tuple(diagnostics)
    return tuple(generated_sections), tuple(models), tuple(diagnostics)


async def _generate_single_shot_commentary(  # noqa: PLR0913
    provider: LLMProvider,
    analytics: AnalyticsSummary,
    *,
    investor_memory: str,
    progress_callback: CommentaryProgressCallback | None,
    strategy: str,
    system_prompt: str,
    prompt_renderer: Callable[..., str],
    max_output_tokens: int,
) -> CommentaryResult:
    provider_name = _provider_name(provider)
    configured_model = _provider_model_name(provider)
    await _emit_progress(progress_callback, 0, 1, "Weekly Report")

    prompt = prompt_renderer(analytics, investor_memory=investor_memory)
    retry_prompt = (
        prompt_renderer(analytics, investor_memory=investor_memory)
        + "\n\n<retry_instruction>\n"
        + "Your previous answer was rejected.\n"
        + "Return one valid JSON object only.\n"
        + "Use the exact section titles and order already specified.\n"
        + "Do not include any wrapper text.\n"
        + "Do not leave blank lines between bullet items or numbered items.\n"
        + "Keep bullets to one sentence when possible.\n"
        + "Do not place bullets or numbered items inline after prose.\n"
        + "If fiat was redeployed into another asset, describe it as redeployed or converted.\n"
        + "</retry_instruction>"
    )

    last_reason = "validation_failed"
    last_finish_reason: str | None = None
    for attempt_number, attempt_prompt in enumerate((prompt, retry_prompt), start=1):
        logger.info(
            "weekly_report_json_request provider=%s model=%s attempt=%d max_output_tokens=%d",
            provider_name,
            configured_model,
            attempt_number,
            max_output_tokens,
        )
        result = await provider.generate_commentary_json(
            system_prompt,
            attempt_prompt,
            max_output_tokens=max_output_tokens,
        )
        last_finish_reason = result.finish_reason
        parsed_sections, reason = _parse_single_shot_sections(result, analytics)
        if parsed_sections is not None:
            logger.info(
                "weekly_report_json_generated provider=%s model=%s attempts=%d finish_reason=%s",
                provider_name,
                configured_model,
                attempt_number,
                result.finish_reason,
            )
            await _emit_progress(progress_callback, 1, 1, "Weekly Report")
            return CommentaryResult(
                text=flatten_sections(parsed_sections),
                model=result.model or configured_model,
                sections=parsed_sections,
                provider=provider_name,
                finish_reason=result.finish_reason,
                generation_meta=SingleShotReportDiagnostic(
                    strategy=strategy,
                    provider=provider_name,
                    model=result.model or configured_model,
                    status="generated",
                    finish_reason=result.finish_reason,
                    attempts=attempt_number,
                ).as_dict(),
            )

        last_reason = reason
        logger.warning(
            "weekly_report_json_retry provider=%s model=%s attempt=%d reason=%s finish_reason=%s",
            provider_name,
            configured_model,
            attempt_number,
            reason,
            result.finish_reason,
        )

    return CommentaryResult(
        text="",
        model=configured_model,
        error="JSON output was invalid after retry.",
        provider=provider_name,
        finish_reason=last_finish_reason,
        generation_meta=SingleShotReportDiagnostic(
            strategy=strategy,
            provider=provider_name,
            model=configured_model,
            status="failed",
            finish_reason=last_finish_reason,
            attempts=2,
            reason=last_reason,
        ).as_dict(),
    )


async def _generate_single_section(
    provider: LLMProvider,
    spec: ReportSectionSpec,
    analytics: AnalyticsSummary,
    *,
    investor_memory: str,
    prior_sections: tuple[CommentarySection, ...],
) -> tuple[CommentarySection, str | None, SectionGenerationDiagnostic]:
    context = _build_section_input_context(analytics)
    prompt = render_report_section_prompt(
        spec,
        analytics,
        investor_memory=investor_memory,
        prior_sections=prior_sections,
    )
    retry_prompt = (
        render_report_section_prompt(
            spec,
            analytics,
            investor_memory=investor_memory,
            prior_sections=(),
        )
        + "\n\n<retry_instruction>\n"
        + "Your previous answer was rejected because it either lacked paragraph or list structure or did not properly "
        + "distinguish conversions from valuation or FX changes.\n"
        + "Return only the markdown body, no JSON and no heading.\n"
        + "Return exactly 2 short paragraphs or 1 short paragraph plus bullets.\n"
        + "Do not leave blank lines between bullet items.\n"
        + "Keep bullets to one sentence when possible.\n"
        + "If fiat was redeployed into another asset, describe it as conversion or redeployment, not a fiat decline.\n"
        + "</retry_instruction>"
    )

    last_model: str | None = None
    attempt_failure_reason = "validation_failed"
    for attempt_number, attempt_prompt in enumerate((prompt, retry_prompt), start=1):
        max_output_tokens = _effective_max_output_tokens(provider, spec)
        logger.info(
            "report_section_request provider=%s model=%s title=%s attempt=%d max_output_tokens=%d",
            _provider_name(provider),
            _provider_model_name(provider),
            spec.title,
            attempt_number,
            max_output_tokens,
        )
        result = await provider.generate_commentary(
            WEEKLY_REPORT_SYSTEM_PROMPT,
            attempt_prompt,
            max_output_tokens=max_output_tokens,
        )
        if result.model:
            last_model = result.model
        body = _normalize_section_body(_sanitize_section_output(spec.title, result.text))
        failure_reason = _classify_attempt_failure(result, body)
        if _is_valid_section_body(body, spec, context):
            status = "retried" if attempt_number > 1 else "generated"
            logger.info(
                "report_section_generated provider=%s model=%s title=%s status=%s finish_reason=%s",
                _provider_name(provider),
                last_model,
                spec.title,
                status,
                result.finish_reason,
            )
            return (
                CommentarySection(title=spec.title, description=body),
                last_model,
                SectionGenerationDiagnostic(
                    title=spec.title,
                    status=status,
                    reason="ok" if status == "generated" else attempt_failure_reason,
                    finish_reason=result.finish_reason,
                ),
            )

        attempt_failure_reason = failure_reason
        log_event = "report_section_retry" if attempt_number == 1 else "report_section_rejected"
        logger.warning(
            "%s provider=%s model=%s title=%s attempt=%d reason=%s finish_reason=%s",
            log_event,
            _provider_name(provider),
            _provider_model_name(provider),
            spec.title,
            attempt_number,
            failure_reason,
            result.finish_reason,
        )

    logger.warning(
        "report_section_fallback provider=%s model=%s title=%s reason=%s",
        _provider_name(provider),
        _provider_model_name(provider),
        spec.title,
        attempt_failure_reason,
    )
    return (
        CommentarySection(title=spec.title, description=spec.fallback_text),
        last_model,
        SectionGenerationDiagnostic(
            title=spec.title,
            status="fallback",
            reason=attempt_failure_reason,
            finish_reason=result.finish_reason,
        ),
    )


async def _emit_progress(
    callback: CommentaryProgressCallback | None,
    completed_sections: int,
    total_sections: int,
    current_section: str,
) -> None:
    if callback is None:
        return
    result = callback(completed_sections, total_sections, current_section)
    if inspect.isawaitable(result):
        await result


def _sanitize_section_output(section_title: str, text: str) -> str:
    finalized = _strip_code_fences(finalize_commentary_text(text))
    if not finalized:
        return ""

    lines = finalized.splitlines()
    while lines and not lines[0].strip():
        lines.pop(0)

    if lines and _normalize_heading(lines[0]) == _normalize_heading(section_title):
        lines.pop(0)
        while lines and not lines[0].strip():
            lines.pop(0)

    return "\n".join(lines).strip()


def _parse_single_shot_sections(
    result: CommentaryResult,
    analytics: AnalyticsSummary,
) -> tuple[tuple[CommentarySection, ...] | None, str]:
    if result.error:
        return None, _classify_attempt_failure(result, "")

    raw = finalize_commentary_text(result.text).strip()
    if not raw:
        return None, _classify_attempt_failure(result, "")

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None, "invalid_json"

    try:
        response = CommentaryResponse.model_validate(payload)
    except ValidationError:
        return None, "invalid_schema"

    context = _build_section_input_context(analytics)
    normalized: list[CommentarySection] = []
    for expected_spec, section in zip(REPORT_SECTION_SPECS, response.to_commentary_sections(), strict=True):
        body = _normalize_section_body(_sanitize_section_output(expected_spec.title, section.description))
        if not _is_valid_section_body(body, expected_spec, context):
            return None, "validation_failed"
        normalized.append(CommentarySection(title=expected_spec.title, description=body))

    return tuple(normalized), "ok"


def _strip_code_fences(text: str) -> str:
    stripped = text.strip()
    if not stripped.startswith("```"):
        return stripped
    lines = stripped.splitlines()
    lines = lines[1:-1] if len(lines) >= _MIN_CODE_FENCE_LINES and lines[-1].strip().startswith("```") else lines[1:]
    return "\n".join(lines).strip()


def _normalize_section_body(text: str) -> str:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not normalized:
        return ""
    lines = [_normalize_markdown_line(line) for line in normalized.splitlines()]

    blocks: list[list[str]] = []
    current: list[str] = []
    for line in lines:
        if line.strip():
            current.append(line)
            continue
        if current:
            blocks.append(current)
            current = []
    if current:
        blocks.append(current)

    merged_blocks: list[list[str]] = []
    for block in blocks:
        if merged_blocks and _block_is_list_lines(merged_blocks[-1]) and _block_is_list_lines(block):
            merged_blocks[-1].extend(block)
        else:
            merged_blocks.append(block)

    normalized_blocks: list[str] = []
    for block in merged_blocks:
        stripped_lines = [line.strip() for line in block if line.strip()]
        if _block_is_list_lines(stripped_lines):
            normalized_blocks.append("\n".join(stripped_lines))
        else:
            normalized_blocks.append(" ".join(stripped_lines))

    return "\n\n".join(normalized_blocks).strip()


def _normalize_heading(text: str) -> str:
    normalized = re.sub(r"^[#>\-\s\d\.\)\(]+", "", text.strip().lower())
    return normalized.rstrip(":").strip()


def _is_valid_section_body(text: str, spec: ReportSectionSpec, context: SectionInputContext) -> bool:
    del spec, context
    stripped = text.strip()
    if not stripped or stripped == FALLBACK_COMMENTARY:
        return False
    if stripped.startswith(("[", "{")):
        return False
    compact_len = len(re.sub(r"\s+", "", stripped))
    if compact_len < _MIN_SECTION_TEXT_CHARS:
        lowered = stripped.lower()
        return any(marker in lowered for marker in _DATA_LIMITATION_MARKERS)
    return True


def _has_readable_structure(text: str, structure: str) -> bool:
    blocks = [block.strip() for block in re.split(r"\n\s*\n", text.strip()) if block.strip()]
    if not blocks:
        return False
    list_lines = [line for line in text.splitlines() if _LIST_LINE_RE.match(line)]
    numbered_lines = [line for line in text.splitlines() if re.match(r"^\s*\d+\.\s+", line)]
    long_single_block = len(blocks) == 1 and len(blocks[0]) > _MAX_SINGLE_PARAGRAPH_CHARS
    if long_single_block:
        return False

    if structure == "two_paragraphs":
        is_valid = len(blocks) == _EXACT_TWO_BLOCKS and not list_lines
    elif structure == "two_paragraphs_or_bullets":
        is_valid = (len(blocks) == _EXACT_TWO_BLOCKS and not list_lines) or _valid_list_block(
            list_lines, _MIN_LIST_ITEMS, _MAX_LIST_ITEMS
        )
    elif structure == "paragraph_then_bullets":
        is_valid = (
            len(blocks) == _EXACT_TWO_BLOCKS
            and not _LIST_LINE_RE.match(blocks[0].splitlines()[0])
            and _valid_list_block(blocks[1].splitlines(), _MIN_LIST_ITEMS, _MAX_LIST_ITEMS)
        )
    elif structure == "bullets_only":
        is_valid = len(blocks) == 1 and _valid_list_block(list_lines, _MIN_LIST_ITEMS, 3)
    elif structure == "numbered_list":
        is_valid = _valid_numbered_structure(blocks, numbered_lines)
    else:
        is_valid = len(blocks) >= _MIN_STRUCTURED_BLOCKS or _valid_list_block(
            list_lines,
            _MIN_LIST_ITEMS,
            _MAX_LIST_ITEMS,
        )
    if not is_valid:
        return False

    paragraph_blocks = [block for block in blocks if not _LIST_LINE_RE.match(block.splitlines()[0])]
    return all(_paragraph_is_readable(block) for block in paragraph_blocks)


def _paragraph_is_readable(block: str) -> bool:
    compact = re.sub(r"\s+", " ", block).strip()
    if not compact:
        return False
    if len(compact) > _MAX_PARAGRAPH_CHARS:
        return False
    return _sentence_count(compact) <= _MAX_SENTENCES_PER_PARAGRAPH


def _sentence_count(text: str) -> int:
    matches = re.findall(r"[.!?](?:\s|$)", text)
    return len(matches) if matches else 1


def _violates_conversion_reasoning(text: str, section_title: str, context: SectionInputContext) -> bool:
    if section_title not in _CONVERSION_VALIDATION_SECTIONS:
        return False
    if not context.redeployed_fiat_assets:
        return False
    lowered = text.lower()
    if any(word in lowered for word in _CONVERSION_WORDS):
        return False
    if not any(currency.lower() in lowered for currency in context.redeployed_fiat_assets):
        return False
    return any(word in lowered for word in _NEGATIVE_BALANCE_WORDS)


def _normalize_markdown_line(line: str) -> str:
    stripped = line.rstrip()
    bullet_match = re.match(r"^\s*[*•]\s+(.*)$", stripped)
    if bullet_match:
        return f"- {bullet_match.group(1).strip()}"
    numbered_match = re.match(r"^\s*(\d+)\.\s+(.*)$", stripped)
    if numbered_match:
        return f"{numbered_match.group(1)}. {numbered_match.group(2).strip()}"
    return stripped.strip()


def _block_is_list_lines(lines: list[str]) -> bool:
    return bool(lines) and all(_LIST_LINE_RE.match(line) for line in lines)


def _valid_list_block(lines: list[str], min_items: int, max_items: int) -> bool:
    if not (min_items <= len(lines) <= max_items):
        return False
    if not all(_LIST_LINE_RE.match(line) for line in lines):
        return False
    return all(len(re.sub(r"\s+", " ", line).strip()) <= _MAX_BULLET_ITEM_CHARS for line in lines)


def _valid_numbered_structure(blocks: list[str], numbered_lines: list[str]) -> bool:
    if not (_MIN_NUMBERED_ITEMS <= len(numbered_lines) <= _MAX_NUMBERED_ITEMS):
        return False
    if not _valid_list_block(numbered_lines, _MIN_NUMBERED_ITEMS, _MAX_NUMBERED_ITEMS):
        return False
    if len(blocks) == 1:
        return len(blocks[0].splitlines()) == len(numbered_lines)
    if len(blocks) != _EXACT_TWO_BLOCKS:
        return False
    intro = blocks[0]
    if _LIST_LINE_RE.match(intro.splitlines()[0]):
        return False
    return _paragraph_is_readable(intro) and len(blocks[1].splitlines()) == len(numbered_lines)


def _classify_attempt_failure(result: CommentaryResult, body: str) -> str:
    if result.reasoning_text and not body:
        return "empty_content_with_reasoning"
    if result.finish_reason == "length":
        return "length_truncated"
    if result.error:
        lowered = result.error.lower()
        if "json" in lowered:
            return "invalid_json"
        if "empty" in lowered:
            return "empty_content"
        return "provider_error"
    return "validation_failed"


def _should_use_deepseek_json_mode(provider: LLMProvider) -> bool:
    return _provider_name(provider) == ProviderName.deepseek and _provider_model_name(provider) == "deepseek-chat"


def _should_use_gemini_json_mode(provider: LLMProvider) -> bool:
    return _provider_name(provider) == ProviderName.gemini


def _effective_max_output_tokens(provider: LLMProvider, spec: ReportSectionSpec) -> int:
    provider_name = getattr(provider, "name", "")
    model_name = _provider_model_name(provider)
    if provider_name == "deepseek" and model_name == "deepseek-reasoner":
        return max(spec.max_output_tokens * 4, _DEEPSEEK_REASONER_MIN_OUTPUT_TOKENS)
    return min(spec.max_output_tokens, GEMINI_MAX_OUTPUT_TOKENS)


def _provider_model_name(provider: LLMProvider) -> str | None:
    model = getattr(provider, "_model", None)
    if isinstance(model, str) and model.strip():
        return model
    return None


def _provider_name(provider: LLMProvider) -> str | None:
    name = getattr(provider, "name", None)
    if isinstance(name, str) and name.strip():
        return name
    return provider.__class__.__name__


def _build_generation_meta(
    *,
    provider_name: str | None,
    model: str | None,
    diagnostics: tuple[SectionGenerationDiagnostic, ...],
) -> dict[str, object]:
    return {
        "strategy": "section_by_section",
        "provider": provider_name,
        "model": model,
        "sections": [diagnostic.as_dict() for diagnostic in diagnostics],
    }


def _build_section_input_context(analytics: AnalyticsSummary) -> SectionInputContext:
    try:
        parsed = json.loads(analytics.currency_flow_bridge) if analytics.currency_flow_bridge else []
    except json.JSONDecodeError:
        parsed = []
    if not isinstance(parsed, list):
        parsed = []

    redeployed: list[str] = []
    for row in parsed:
        if not isinstance(row, dict):
            continue
        delta_amount = _safe_decimal(row.get("delta_amount", "0"))
        trade_spend = _safe_decimal(row.get("explained_by_trade_spend", "0"))
        currency = str(row.get("currency", "")).upper()
        if currency and delta_amount < 0 and trade_spend > 0:
            redeployed.append(currency)
    return SectionInputContext(redeployed_fiat_assets=tuple(dict.fromkeys(redeployed)))


def _safe_decimal(value: object) -> float:
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return 0.0


def _summarize_models(models: tuple[str, ...]) -> str | None:
    if not models:
        return None
    if all(model == models[0] for model in models):
        return models[0]
    return "multiple"


async def _resolve_provider(db_path: str | Path | None) -> LLMProvider | None:
    """Build the active LLM provider from DB config or env fallback."""
    resolved_path = _resolve_db_path(db_path)
    store = AIProviderStore(resolved_path)

    try:
        await store.migrate_from_legacy()
    except (OSError, ValueError):  # pragma: no cover - defensive guardrail
        logger.debug("Legacy AI config migration skipped.", exc_info=True)

    config: AIProvider | None = None
    try:
        config = await store.get_active()
    except Exception:  # pragma: no cover - defensive guardrail
        logger.exception("Failed to load AI config from DB.")

    if config is not None:
        return _build_provider_from_config(config, PROVIDER_REGISTRY)

    settings = get_settings()
    env_key = settings.gemini_api_key.get_secret_value().strip()
    if env_key:
        gemini_cls = PROVIDER_REGISTRY.get(ProviderName.gemini)
        if gemini_cls is not None:
            return _build_provider(gemini_cls, api_key=env_key)

    return None


def _build_provider_from_config(
    config: AIProvider,
    registry: dict[ProviderName, type[LLMProvider]],
) -> LLMProvider | None:
    """Instantiate a provider from stored AI config."""
    try:
        provider_name = ProviderName(config.type)
    except ValueError:
        logger.warning("Unknown AI provider '%s'.", config.type)
        return None

    cls = registry.get(provider_name)
    if cls is None:
        logger.warning("Provider '%s' is not registered.", config.type)
        return None

    return _build_provider(
        cls,
        api_key=config.api_key or None,
        model=config.model or None,
        base_url=config.base_url or None,
    )


def _build_provider(
    cls: type[LLMProvider],
    *,
    api_key: str | None = None,
    model: str | None = None,
    base_url: str | None = None,
) -> LLMProvider:
    """Instantiate a provider class with applicable kwargs."""
    sig = inspect.signature(cls.__init__)
    kwargs: dict[str, object] = {}
    if "api_key" in sig.parameters and api_key is not None:
        kwargs["api_key"] = api_key
    if "model" in sig.parameters and model is not None:
        kwargs["model"] = model
    if "base_url" in sig.parameters and base_url is not None:
        kwargs["base_url"] = base_url
    return cls(**kwargs)


def _resolve_db_path(db_path: str | Path | None) -> str | Path:
    """Determine which DB path to use."""
    if db_path is not None:
        return db_path
    settings = get_settings()
    settings_db_path = getattr(settings, "database_path", None)
    if isinstance(settings_db_path, str | Path):
        return settings_db_path
    return Path("data/pfm.db")


def _finalize_commentary_text(text: str) -> str:
    return finalize_commentary_text(text)


def _escape_newlines_in_json_strings(text: str) -> str:
    return escape_newlines_in_json_strings(text)


def _parse_sections(text: str) -> tuple[CommentarySection, ...]:
    return parse_commentary_sections(text)
