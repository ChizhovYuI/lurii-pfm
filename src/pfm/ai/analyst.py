"""AI weekly commentary orchestrator."""

from __future__ import annotations

import inspect
import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from pfm.ai.base import FALLBACK_COMMENTARY, CommentaryResult, CommentarySection, ProviderName, flatten_sections
from pfm.ai.commentary_parser import (
    escape_newlines_in_json_strings,
    finalize_commentary_text,
    parse_commentary_sections,
)
from pfm.ai.prompts import (
    REPORT_SECTION_SPECS,
    WEEKLY_REPORT_SYSTEM_PROMPT,
    render_report_section_prompt,
)
from pfm.ai.providers.registry import PROVIDER_REGISTRY
from pfm.config import get_settings
from pfm.db.ai_report_memory_store import AIReportMemoryStore
from pfm.db.ai_store import AIProviderStore

if TYPE_CHECKING:
    from pfm.ai.base import LLMProvider
    from pfm.ai.prompts import AnalyticsSummary, ReportSectionSpec
    from pfm.db.models import AIProvider

logger = logging.getLogger(__name__)

GEMINI_MAX_OUTPUT_TOKENS = 4096
_MIN_SECTION_TEXT_CHARS = 80
_MIN_CODE_FENCE_LINES = 2
_MAX_SINGLE_PARAGRAPH_CHARS = 420
_MAX_PARAGRAPH_CHARS = 320
_MAX_SENTENCES_PER_PARAGRAPH = 3
_MIN_STRUCTURED_BLOCKS = 2
_MIN_LIST_ITEMS = 2
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

    resolved_path = _resolve_db_path(db_path)
    if investor_memory is None:
        investor_memory = await AIReportMemoryStore(resolved_path).get()

    try:
        sections, models, fallback_titles = await _generate_sections(
            provider,
            analytics,
            investor_memory=investor_memory,
            progress_callback=progress_callback,
        )
    finally:
        await provider.close()

    if not sections:
        logger.warning("All report sections failed; using fallback commentary.")
        return CommentaryResult(
            text=FALLBACK_COMMENTARY,
            model=None,
            error="All report sections fell back to the generic commentary.",
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
    )


async def _generate_sections(
    provider: LLMProvider,
    analytics: AnalyticsSummary,
    *,
    investor_memory: str,
    progress_callback: CommentaryProgressCallback | None,
) -> tuple[tuple[CommentarySection, ...], tuple[str, ...], tuple[str, ...]]:
    generated_sections: list[CommentarySection] = []
    models: list[str] = []
    fallback_titles: list[str] = []

    total_sections = len(REPORT_SECTION_SPECS)
    for spec in REPORT_SECTION_SPECS:
        await _emit_progress(progress_callback, len(generated_sections), total_sections, spec.title)
        section, model, used_fallback = await _generate_single_section(
            provider,
            spec,
            analytics,
            investor_memory=investor_memory,
            prior_sections=tuple(generated_sections),
        )
        generated_sections.append(section)
        if model:
            models.append(model)
        if used_fallback:
            fallback_titles.append(spec.title)

    if len(fallback_titles) == total_sections:
        return (), tuple(models), tuple(fallback_titles)
    return tuple(generated_sections), tuple(models), tuple(fallback_titles)


async def _generate_single_section(
    provider: LLMProvider,
    spec: ReportSectionSpec,
    analytics: AnalyticsSummary,
    *,
    investor_memory: str,
    prior_sections: tuple[CommentarySection, ...],
) -> tuple[CommentarySection, str | None, bool]:
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
        + "If fiat was redeployed into another asset, describe it as conversion or redeployment, not a fiat decline.\n"
        + "</retry_instruction>"
    )

    last_model: str | None = None
    for attempt_prompt in (prompt, retry_prompt):
        result = await provider.generate_commentary(
            WEEKLY_REPORT_SYSTEM_PROMPT,
            attempt_prompt,
            max_output_tokens=min(spec.max_output_tokens, GEMINI_MAX_OUTPUT_TOKENS),
        )
        if result.model:
            last_model = result.model
        body = _normalize_section_body(_sanitize_section_output(spec.title, result.text))
        if _is_valid_section_body(body, spec, context):
            return CommentarySection(title=spec.title, description=body), last_model, False

    return CommentarySection(title=spec.title, description=spec.fallback_text), last_model, True


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
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    normalized = re.sub(r"(?<!\n)\n(?=\s*(?:[-*]|\d+\.)\s+)", "\n\n", normalized)
    return normalized.strip()


def _normalize_heading(text: str) -> str:
    normalized = re.sub(r"^[#>\-\s\d\.\)\(]+", "", text.strip().lower())
    return normalized.rstrip(":").strip()


def _is_valid_section_body(text: str, spec: ReportSectionSpec, context: SectionInputContext) -> bool:
    stripped = text.strip()
    if not stripped or stripped == FALLBACK_COMMENTARY:
        return False
    if stripped.startswith(("[", "{")):
        return False
    compact_len = len(re.sub(r"\s+", "", stripped))
    if compact_len < _MIN_SECTION_TEXT_CHARS:
        lowered = stripped.lower()
        return any(marker in lowered for marker in _DATA_LIMITATION_MARKERS)

    if not _has_readable_structure(stripped, spec.structure):
        return False

    return not _violates_conversion_reasoning(stripped, spec.title, context)


def _has_readable_structure(text: str, structure: str) -> bool:
    blocks = [block.strip() for block in re.split(r"\n\s*\n", text.strip()) if block.strip()]
    if not blocks:
        return False
    list_lines = [line for line in text.splitlines() if _LIST_LINE_RE.match(line)]
    numbered_lines = [line for line in text.splitlines() if re.match(r"^\s*\d+\.\s+", line)]
    long_single_block = len(blocks) == 1 and len(blocks[0]) > _MAX_SINGLE_PARAGRAPH_CHARS
    if long_single_block:
        return False

    non_empty_lines = [line for line in text.splitlines() if line.strip()]
    starts_with_list = bool(_LIST_LINE_RE.match(blocks[0].splitlines()[0]))
    has_two_blocks = len(blocks) >= _MIN_STRUCTURED_BLOCKS
    has_list = len(list_lines) >= _MIN_LIST_ITEMS
    has_numbered_list = len(numbered_lines) >= _MIN_LIST_ITEMS

    if structure == "two_paragraphs":
        is_valid = has_two_blocks and not list_lines
    elif structure == "two_paragraphs_or_bullets":
        is_valid = has_two_blocks or has_list
    elif structure == "paragraph_then_bullets":
        is_valid = has_two_blocks and not starts_with_list and has_list
    elif structure == "bullets_only":
        is_valid = has_list and len(list_lines) == len(non_empty_lines)
    elif structure == "numbered_list":
        is_valid = has_numbered_list and (
            len(non_empty_lines) == len(numbered_lines)
            or (len(non_empty_lines) == len(numbered_lines) + 1 and not _LIST_LINE_RE.match(non_empty_lines[0]))
        )
    else:
        is_valid = has_two_blocks or has_list
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
