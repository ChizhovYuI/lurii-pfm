"""AI commentary orchestrator — thin layer delegating to pluggable providers."""

from __future__ import annotations

import inspect
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from pfm.ai.base import FALLBACK_COMMENTARY, CommentaryResult, ProviderName
from pfm.ai.prompts import WEEKLY_REPORT_SYSTEM_PROMPT, render_weekly_report_user_prompt
from pfm.ai.providers.registry import PROVIDER_REGISTRY
from pfm.config import get_settings
from pfm.db.ai_store import AIProviderStore

if TYPE_CHECKING:
    from pfm.ai.base import LLMProvider
    from pfm.ai.prompts import AnalyticsSummary
    from pfm.db.models import AIProvider

logger = logging.getLogger(__name__)

GEMINI_MAX_OUTPUT_TOKENS = 4096


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
) -> CommentaryResult:
    """Generate weekly portfolio commentary with model info."""
    provider = await _resolve_provider(db_path)
    if provider is None:
        logger.warning("No AI provider configured; returning fallback commentary.")
        return CommentaryResult(text=FALLBACK_COMMENTARY, model=None)

    prompt = render_weekly_report_user_prompt(analytics)
    try:
        result = await provider.generate_commentary(
            WEEKLY_REPORT_SYSTEM_PROMPT,
            prompt,
            max_output_tokens=GEMINI_MAX_OUTPUT_TOKENS,
        )
    finally:
        await provider.close()

    if result.text:
        return CommentaryResult(text=_finalize_commentary_text(result.text), model=result.model)

    logger.warning("Provider returned empty text; using fallback commentary.")
    return CommentaryResult(text=FALLBACK_COMMENTARY, model=None)


async def _resolve_provider(db_path: str | Path | None) -> LLMProvider | None:
    """Build the active LLM provider from DB config or env fallback."""
    resolved_path = _resolve_db_path(db_path)
    store = AIProviderStore(resolved_path)

    # Migrate legacy app_settings keys if needed
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

    # Env fallback for GEMINI_API_KEY
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
    """Normalize line endings and strip whitespace."""
    return text.replace("\r\n", "\n").replace("\r", "\n").strip()
