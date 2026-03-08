"""CLI entry point for pfm."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import sys
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING

import click
import httpx

from pfm import __version__
from pfm.ai.providers.ollama import OllamaProvider
from pfm.ai.providers.registry import get_provider_names
from pfm.config import get_settings
from pfm.db.ai_store import AIProviderStore
from pfm.db.gemini_store import GeminiStore
from pfm.db.models import CollectorResult, init_db
from pfm.db.source_store import (
    DuplicateSourceError,
    InvalidCredentialsError,
    SourceNotFoundError,
    SourceStore,
)
from pfm.db.telegram_store import TelegramStore
from pfm.server.serializers import (
    mask_secret as _mask,
)
from pfm.source_types import SOURCE_TYPES

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from pfm.ai import AnalyticsSummary
    from pfm.db.models import Source
    from pfm.db.repository import Repository

logger = logging.getLogger(__name__)
_COUNTRY_ACCESS_HINT_PATTERNS = (
    "service access appears restricted from your current network or region",
    "you don't have access from this country. use vpn or smth to handle this",
)


def _get_store() -> SourceStore:
    """Get a SourceStore using the configured database path."""
    settings = get_settings()
    return SourceStore(settings.database_path)


def _get_telegram_store() -> TelegramStore:
    """Get a TelegramStore using the configured database path."""
    settings = get_settings()
    return TelegramStore(settings.database_path)


def _get_gemini_store() -> GeminiStore:
    """Get a GeminiStore using the configured database path."""
    settings = get_settings()
    return GeminiStore(settings.database_path)


def _get_ai_provider_store() -> AIProviderStore:
    """Get an AIProviderStore using the configured database path."""
    settings = get_settings()
    return AIProviderStore(settings.database_path)


def _run[T](coro: Coroutine[object, object, T]) -> T:
    """Run an async coroutine synchronously."""
    return asyncio.run(coro)


def _ensure_db() -> None:
    """Ensure the database exists and has all tables."""
    settings = get_settings()
    asyncio.run(init_db(settings.database_path))


def _print_source_table(sources: list[Source]) -> None:
    """Print a formatted table of sources."""
    if not sources:
        click.echo("No sources configured. Run 'pfm source add' to add one.")
        return

    # Column widths
    name_w = max(len(s.name) for s in sources)
    name_w = max(name_w, 4)  # "NAME" header
    type_w = max(len(s.type) for s in sources)
    type_w = max(type_w, 4)  # "TYPE" header

    header = f"{'NAME':<{name_w}}  {'TYPE':<{type_w}}  {'ENABLED':<7}"
    click.echo(header)
    click.echo("-" * len(header))
    for s in sources:
        enabled = "yes" if s.enabled else "no"
        click.echo(f"{s.name:<{name_w}}  {s.type:<{type_w}}  {enabled:<7}")


# ── Main CLI group ────────────────────────────────────────────────────


@click.group()
@click.version_option(version=__version__, prog_name="pfm")
def cli() -> None:
    """pfm — Personal Financial Management."""


# ── Source management ─────────────────────────────────────────────────


@cli.group()
def source() -> None:
    """Manage data sources (add, list, show, delete, enable, disable)."""


@source.command("add")
def source_add() -> None:
    """Interactive wizard to add a new data source."""
    _ensure_db()

    # Step 1: pick source type
    type_names = sorted(SOURCE_TYPES.keys())
    click.echo("Available source types:")
    for i, name in enumerate(type_names, 1):
        click.echo(f"  {i}. {name}")

    choice = click.prompt(
        "\nSelect source type",
        type=click.IntRange(1, len(type_names)),
    )
    source_type = type_names[choice - 1]
    click.echo(f"\nAdding source of type: {source_type}")

    # Step 2: pick instance name
    default_name = source_type
    name = click.prompt("Instance name", default=default_name)

    # Step 3: fill credentials
    fields = SOURCE_TYPES[source_type]
    credentials: dict[str, str] = {}

    click.echo()
    for field in fields:
        prompt_text = field.prompt
        if field.default:
            prompt_text += f" [{field.default}]"

        if field.secret:
            value = click.prompt(prompt_text, hide_input=True, default=field.default or "")
        else:
            value = click.prompt(prompt_text, default=field.default or "")

        if value:
            credentials[field.name] = value

    # Step 4: save
    store = _get_store()
    try:
        result = _run(store.add(name, source_type, credentials))
    except DuplicateSourceError:
        click.echo(f"Error: source '{name}' already exists.", err=True)
        sys.exit(1)
    except InvalidCredentialsError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)

    click.echo(f"\nSource '{result.name}' ({result.type}) added successfully.")


@source.command("list")
def source_list() -> None:
    """List all configured sources."""
    from pfm.server.client import is_daemon_reachable, proxy_sources_list

    if is_daemon_reachable():
        data = _run(proxy_sources_list())
        if not data:
            click.echo("No sources configured. Run 'pfm source add' to add one.")
            return
        name_w = max(len(s["name"]) for s in data)
        name_w = max(name_w, 4)
        type_w = max(len(s["type"]) for s in data)
        type_w = max(type_w, 4)
        header = f"{'NAME':<{name_w}}  {'TYPE':<{type_w}}  {'ENABLED':<7}"
        click.echo(header)
        click.echo("-" * len(header))
        for s in data:
            enabled = "yes" if s["enabled"] else "no"
            click.echo(f"{s['name']:<{name_w}}  {s['type']:<{type_w}}  {enabled:<7}")
        return

    _ensure_db()
    store = _get_store()
    sources: list[Source] = _run(store.list_all())
    _print_source_table(sources)


@source.command("show")
@click.argument("name")
def source_show(name: str) -> None:
    """Show details for a source (secrets masked)."""
    _ensure_db()
    store = _get_store()
    try:
        src: Source = _run(store.get(name))
    except SourceNotFoundError:
        click.echo(f"Error: source '{name}' not found.", err=True)
        sys.exit(1)

    creds = json.loads(src.credentials)
    fields = SOURCE_TYPES.get(src.type, [])
    secret_names = {f.name for f in fields if f.secret}

    click.echo(f"Name:    {src.name}")
    click.echo(f"Type:    {src.type}")
    click.echo(f"Enabled: {'yes' if src.enabled else 'no'}")
    click.echo("Credentials:")
    for key, value in creds.items():
        display = _mask(value) if key in secret_names else value
        click.echo(f"  {key}: {display}")


@source.command("delete")
@click.argument("name")
def source_delete(name: str) -> None:
    """Delete a source (with confirmation)."""
    _ensure_db()

    if not click.confirm(f"Delete source '{name}'?"):
        click.echo("Cancelled.")
        return

    store = _get_store()
    try:
        _run(store.delete(name))
    except SourceNotFoundError:
        click.echo(f"Error: source '{name}' not found.", err=True)
        sys.exit(1)

    click.echo(f"Source '{name}' deleted.")


@source.command("enable")
@click.argument("name")
def source_enable(name: str) -> None:
    """Enable a source."""
    _ensure_db()
    store = _get_store()
    try:
        _run(store.update(name, enabled=True))
    except SourceNotFoundError:
        click.echo(f"Error: source '{name}' not found.", err=True)
        sys.exit(1)
    click.echo(f"Source '{name}' enabled.")


@source.command("disable")
@click.argument("name")
def source_disable(name: str) -> None:
    """Disable a source."""
    _ensure_db()
    store = _get_store()
    try:
        _run(store.update(name, enabled=False))
    except SourceNotFoundError:
        click.echo(f"Error: source '{name}' not found.", err=True)
        sys.exit(1)
    click.echo(f"Source '{name}' disabled.")


# ── Telegram config ───────────────────────────────────────────────────


@cli.group()
def gemini() -> None:
    """Manage Gemini API key for AI commentary."""


@gemini.command("set")
@click.option("--api-key", prompt=True, hide_input=True, help="Gemini API key.")
def gemini_set(api_key: str) -> None:
    """Set Gemini API key in DB settings."""
    _ensure_db()
    store = _get_gemini_store()
    try:
        config = _run(store.set(api_key))
    except ValueError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)

    click.echo("Gemini API key saved.")
    click.echo(f"API key: {_mask(config.api_key)}")


@gemini.command("show")
def gemini_show() -> None:
    """Show Gemini API configuration (key masked)."""
    _ensure_db()
    store = _get_gemini_store()
    config = _run(store.get())
    if config is None:
        click.echo("Gemini is not configured. Run 'pfm gemini set'.")
        return

    click.echo("Gemini configuration:")
    click.echo(f"API key: {_mask(config.api_key)}")


@gemini.command("clear")
def gemini_clear() -> None:
    """Delete Gemini API key from DB settings."""
    _ensure_db()
    if not click.confirm("Delete Gemini API key?"):
        click.echo("Cancelled.")
        return

    store = _get_gemini_store()
    deleted = _run(store.clear())
    if deleted:
        click.echo("Gemini API key removed.")
    else:
        click.echo("No Gemini API key was stored.")


# ── AI provider config ───────────────────────────────────────────────

_PROVIDERS_REQUIRING_API_KEY: frozenset[str] = frozenset({"gemini", "deepseek", "openrouter", "grok"})


_BYTES_GB = 1_000_000_000
_BYTES_MB = 1_000_000

_OPENROUTER_MODELS: list[tuple[str, str]] = [
    ("qwen/qwen3-235b-a22b-thinking-2507", "free, 235B MoE, reasoning"),
    ("arcee-ai/trinity-large-preview:free", "free, 400B MoE, creative"),
    ("google/gemini-2.5-flash-preview", "free, fast, 1M context"),
    ("anthropic/claude-sonnet-4", "paid, best quality"),
    ("openai/gpt-4.1-mini", "paid, fast, cheap"),
]

_DEEPSEEK_MODELS: list[tuple[str, str]] = [
    ("deepseek-chat", "recommended for weekly reports"),
    ("deepseek-reasoner", "advanced reasoning, slower and needs larger token budgets"),
]

_OLLAMA_MODEL_HINTS: dict[str, str] = {
    "llama3.1:8b": "best for 8 GB RAM",
    "qwen3:14b": "best for 16+ GB RAM",
}


def _format_bytes(size: int) -> str:
    """Format byte count as human-readable size (e.g. 4.6 GB)."""
    if size >= _BYTES_GB:
        return f"{size / 1_073_741_824:.1f} GB"
    if size >= _BYTES_MB:
        return f"{size / 1_048_576:.0f} MB"
    return f"{size} B"


def _pick_ollama_model(base_url: str) -> str:
    """Fetch available Ollama models and let the user pick one."""
    url = f"{base_url.rstrip('/')}/api/tags"
    try:
        response = httpx.get(url, timeout=5.0)
        response.raise_for_status()
        body = response.json()
    except httpx.ConnectError:
        click.echo("Could not connect to Ollama.", err=True)
        click.echo("  Install: brew install ollama", err=True)
        click.echo("  Run:     ollama serve", err=True)
        sys.exit(1)
    except (httpx.HTTPError, OSError, ValueError):
        click.echo("Could not reach Ollama to list models. Enter model name manually.")
        return str(click.prompt("Model"))

    models_list = body.get("models", [])
    if not models_list:
        click.echo("No models found in Ollama. Pull one first: ollama pull <model>")
        return str(click.prompt("Model"))

    names: list[str] = [m["name"] for m in models_list if isinstance(m, dict) and "name" in m]
    if not names:
        return str(click.prompt("Model"))

    default = OllamaProvider.default_model
    has_default = default in names

    click.echo("\nAvailable Ollama models:")
    for i, m in enumerate(models_list, 1):
        name = m.get("name", "?")
        size_bytes = m.get("size", 0) if isinstance(m, dict) else 0
        size_str = f"{_format_bytes(size_bytes)} RAM" if size_bytes else ""
        hint = _OLLAMA_MODEL_HINTS.get(name, "")
        suffix = f"  <- {hint}" if hint else ""
        click.echo(f"  {i}. {name:<30s} {size_str}{suffix}")

    if not has_default:
        hint = _OLLAMA_MODEL_HINTS.get(default, "")
        hint_str = f" ({hint})" if hint else ""
        click.echo(f"\nRecommended: {default}{hint_str} — install with: ollama pull {default}")

    choice: int = click.prompt(
        "\nSelect model",
        type=click.IntRange(1, len(names)),
    )
    return names[choice - 1]


def _pick_openrouter_model() -> str:
    """Show curated OpenRouter model list and let the user pick one."""
    click.echo("\nAvailable OpenRouter models:")
    for i, (model_id, label) in enumerate(_OPENROUTER_MODELS, 1):
        click.echo(f"  {i}. {model_id:<50s} {label}")

    choice: int = click.prompt(
        "\nSelect model",
        type=click.IntRange(1, len(_OPENROUTER_MODELS)),
    )
    return _OPENROUTER_MODELS[choice - 1][0]


def _pick_deepseek_model() -> str:
    """Show curated DeepSeek model list and let the user pick one."""
    click.echo("\nAvailable DeepSeek models:")
    for i, (model_id, label) in enumerate(_DEEPSEEK_MODELS, 1):
        click.echo(f"  {i}. {model_id:<25s} {label}")

    choice: int = click.prompt(
        "\nSelect model",
        type=click.IntRange(1, len(_DEEPSEEK_MODELS)),
        default=1,
        show_default=True,
    )
    return _DEEPSEEK_MODELS[choice - 1][0]


@cli.group("ai")
def ai_group() -> None:
    """Manage AI provider configuration."""


@ai_group.command("set")
@click.option(
    "--provider",
    "provider_name",
    required=True,
    type=click.Choice(["gemini", "deepseek", "ollama", "openrouter", "grok"]),
    help="LLM provider to use.",
)
@click.option("--api-key", default=None, help="API key (prompted if required).")
@click.option("--model", default="", help="Model override (optional).")
@click.option("--base-url", default="", help="Custom base URL (optional).")
def ai_set(provider_name: str, api_key: str | None, model: str, base_url: str) -> None:
    """Set the active AI provider."""
    _ensure_db()

    store = _get_ai_provider_store()
    existing = _run(store.get(provider_name))

    # Preserve existing API key when re-configuring the same provider
    if not api_key and existing:
        api_key = existing.api_key

    if provider_name in _PROVIDERS_REQUIRING_API_KEY and not api_key:
        api_key = click.prompt("API key", hide_input=True)

    if provider_name == "ollama" and not model:
        model = _pick_ollama_model(base_url or "http://localhost:11434")

    if provider_name == "deepseek" and not model:
        model = "deepseek-chat"

    if provider_name == "openrouter" and not model:
        model = _pick_openrouter_model()

    try:
        config = _run(
            store.add(
                provider_name,
                api_key=api_key or "",
                model=model,
                base_url=base_url or (existing.base_url if existing else ""),
                active=True,
            )
        )
    except ValueError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)

    click.echo(f"AI provider set to: {config.type}")
    if config.api_key:
        click.echo(f"API key: {_mask(config.api_key)}")
    if config.model:
        click.echo(f"Model: {config.model}")
    if config.base_url:
        click.echo(f"Base URL: {config.base_url}")


@ai_group.command("show")
def ai_show() -> None:
    """Show current active AI provider configuration (key masked)."""
    _ensure_db()
    store = _get_ai_provider_store()
    config = _run(store.get_active())
    if config is None:
        click.echo("AI provider is not configured. Run 'pfm ai set'.")
        return

    click.echo("AI configuration:")
    click.echo(f"Provider: {config.type}")
    if config.api_key:
        click.echo(f"API key:  {_mask(config.api_key)}")
    if config.model:
        click.echo(f"Model:    {config.model}")
    if config.base_url:
        click.echo(f"Base URL: {config.base_url}")


@ai_group.command("clear")
def ai_clear() -> None:
    """Deactivate the current AI provider (alias for 'ai deactivate')."""
    _ensure_db()
    if not click.confirm("Deactivate the current AI provider?"):
        click.echo("Cancelled.")
        return

    store = _get_ai_provider_store()
    changed = _run(store.deactivate())
    if changed:
        click.echo("AI provider deactivated.")
    else:
        click.echo("No AI provider was active.")


@ai_group.command("providers")
def ai_providers() -> None:
    """List registered AI provider implementations."""
    names = get_provider_names()
    if not names:
        click.echo("No providers registered.")
        return

    click.echo("Registered AI providers:")
    for name in names:
        click.echo(f"  {name}")


@ai_group.command("list")
def ai_list() -> None:
    """List all configured AI providers with active marker."""
    _ensure_db()
    store = _get_ai_provider_store()
    providers = _run(store.list_all())
    if not providers:
        click.echo("No AI providers configured. Run 'pfm ai set'.")
        return

    click.echo("Configured AI providers:")
    for p in providers:
        marker = " (active)" if p.active else ""
        click.echo(f"  {p.type}{marker}")
        if p.api_key:
            click.echo(f"    API key:  {_mask(p.api_key)}")
        if p.model:
            click.echo(f"    Model:    {p.model}")
        if p.base_url:
            click.echo(f"    Base URL: {p.base_url}")


@ai_group.command("activate")
@click.argument("provider_type")
def ai_activate(provider_type: str) -> None:
    """Set a configured provider as the active one."""
    _ensure_db()
    store = _get_ai_provider_store()
    try:
        config = _run(store.activate(provider_type))
    except ValueError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)
    click.echo(f"Activated AI provider: {config.type}")


@ai_group.command("deactivate")
def ai_deactivate() -> None:
    """Clear the active AI provider."""
    _ensure_db()
    store = _get_ai_provider_store()
    changed = _run(store.deactivate())
    if changed:
        click.echo("AI provider deactivated.")
    else:
        click.echo("No AI provider was active.")


@ai_group.command("remove")
@click.argument("provider_type")
def ai_remove(provider_type: str) -> None:
    """Remove a configured AI provider."""
    _ensure_db()
    if not click.confirm(f"Remove AI provider '{provider_type}'?"):
        click.echo("Cancelled.")
        return

    store = _get_ai_provider_store()
    deleted = _run(store.remove(provider_type))
    if deleted:
        click.echo(f"AI provider '{provider_type}' removed.")
    else:
        click.echo(f"AI provider '{provider_type}' not found.")


# ── Telegram config ───────────────────────────────────────────────────


@cli.group()
def telegram() -> None:
    """Manage Telegram bot credentials for reporting."""


@telegram.command("set")
@click.option("--bot-token", prompt=True, hide_input=True, help="Telegram bot token.")
@click.option("--chat-id", prompt=True, help="Telegram chat ID.")
def telegram_set(bot_token: str, chat_id: str) -> None:
    """Set Telegram bot token and chat ID in DB settings."""
    _ensure_db()
    store = _get_telegram_store()
    try:
        creds = _run(store.set(bot_token=bot_token, chat_id=chat_id))
    except ValueError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)

    click.echo("Telegram credentials saved.")
    click.echo(f"Bot token: {_mask(creds.bot_token)}")
    click.echo(f"Chat ID:   {creds.chat_id}")


@telegram.command("show")
def telegram_show() -> None:
    """Show Telegram configuration (token masked)."""
    _ensure_db()
    store = _get_telegram_store()
    creds = _run(store.get())
    if creds is None:
        click.echo("Telegram is not configured. Run 'pfm telegram set'.")
        return

    click.echo("Telegram configuration:")
    click.echo(f"Bot token: {_mask(creds.bot_token)}")
    click.echo(f"Chat ID:   {creds.chat_id}")


@telegram.command("clear")
def telegram_clear() -> None:
    """Delete Telegram credentials from DB settings."""
    _ensure_db()
    if not click.confirm("Delete Telegram credentials?"):
        click.echo("Cancelled.")
        return

    store = _get_telegram_store()
    deleted = _run(store.clear())
    if deleted:
        click.echo("Telegram credentials removed.")
    else:
        click.echo("No Telegram credentials were stored.")


# ── Pipeline stubs ────────────────────────────────────────────────────


@cli.command()
@click.option("--source", "source_name", default=None, help="Run a single source by name.")
def collect(source_name: str | None) -> None:
    """Fetch balances and transactions from configured sources."""
    from pfm.server.client import is_daemon_reachable, proxy_collect

    if is_daemon_reachable():
        result = _run(proxy_collect(source_name))
        click.echo(f"Collection triggered via daemon: {result.get('status', 'unknown')}")
        return

    _ensure_db()
    results = _run(_collect_async(source_name))
    _print_collect_results(results)


async def _collect_async(source_name: str | None) -> list[CollectorResult]:
    """Run collection: parallel raw fetch, batch pricing, save snapshots."""
    from pfm.collectors.pipeline import run_parallel_pipeline
    from pfm.db.repository import Repository
    from pfm.pricing import PricingService
    from pfm.server.routes.collect import _build_collectors

    settings = get_settings()
    store = SourceStore(settings.database_path)

    if source_name:
        try:
            src = await store.get(source_name)
        except SourceNotFoundError:
            click.echo(f"Error: source '{source_name}' not found.", err=True)
            sys.exit(1)
        if not src.enabled:
            click.echo(f"Warning: source '{source_name}' is disabled. Running anyway.")
        sources_to_run = [src]
    else:
        sources_to_run = await store.list_enabled()
        if not sources_to_run:
            click.echo("No enabled sources. Run 'pfm source add' first.")
            return []

    pricing = PricingService(
        api_key=settings.coingecko_api_key,
        cache_db_path=settings.database_path,
    )

    try:
        async with Repository(settings.database_path) as repo:
            collectors = await _build_collectors(sources_to_run, pricing, settings.database_path)
            click.echo(f"Fetching raw balances from {len(collectors)} source(s) in parallel...")

            results = await run_parallel_pipeline(collectors, pricing, repo)

            for (src, collector), _result in zip(collectors, results, strict=True):
                click.echo(f"Processing: {src.name} ({src.type})...")
                _print_kbank_statement_freshness(
                    source_type=src.type,
                    collector=collector,
                    today=pricing.today(),
                )
    finally:
        await pricing.close()

    return results


def _print_collect_results(results: list[CollectorResult]) -> None:
    """Print a summary table of collection results."""
    if not results:
        return

    click.echo()
    click.echo("Collection complete:")
    click.echo(f"{'SOURCE':<20}  {'SNAPS':>5}  {'USD':>14}  {'TXNS':>5}  {'ERRORS':>6}  {'TIME':>7}")
    click.echo("-" * 72)

    total_snaps = 0
    total_usd = Decimal(0)
    total_txns = 0
    total_errors = 0
    for r in results:
        total_snaps += r.snapshots_count
        total_usd += r.snapshots_usd_total
        total_txns += r.transactions_count
        total_errors += len(r.errors)
        status = f"{r.duration_seconds:.1f}s"
        click.echo(
            f"{r.source:<20}  {r.snapshots_count:>5}  ${_fmt_money(r.snapshots_usd_total):>13}  "
            f"{r.transactions_count:>5}  {len(r.errors):>6}  {status:>7}",
        )
        for err in r.errors:
            _print_collect_error(err)

    click.echo("-" * 72)
    click.echo(f"{'TOTAL':<20}  {total_snaps:>5}  ${_fmt_money(total_usd):>13}  {total_txns:>5}  {total_errors:>6}")


def _print_kbank_statement_freshness(*, source_type: str, collector: object, today: date) -> None:
    """Print KBank statement date and staleness hint when available."""
    if source_type != "kbank":
        return

    statement_date = getattr(collector, "last_statement_date", None)
    if not isinstance(statement_date, date):
        click.secho("  KBank statement date: unavailable.", fg="yellow")
        click.secho(
            "    Hint: request a new statement from K PLUS and send it to your email.",
            fg="yellow",
        )
        return

    age_days = (today - statement_date).days
    click.secho(f"  KBank statement date: {statement_date.isoformat()} ({age_days}d ago)", fg="cyan")
    if age_days >= 3:  # noqa: PLR2004
        click.secho(
            f"    Statement is {age_days} days old (3+ days is stale).",
            fg="yellow",
        )
        click.secho(
            "    Request a new statement from K PLUS and send it to your email, then run collect again.",
            fg="yellow",
        )


def _print_collect_error(err: str) -> None:
    """Render collector errors with user-friendly formatting and color."""
    lowered = err.lower()
    if any(pattern in lowered for pattern in _COUNTRY_ACCESS_HINT_PATTERNS):
        match = re.match(r"Failed to fetch (\w+) from ([^:]+):", err)
        stage = match.group(1) if match else "data"
        source = match.group(2) if match else "source"
        click.secho(
            f"  ! {source}: cannot fetch {stage} because access looks geo-restricted.",
            fg="red",
            bold=True,
            err=True,
        )
        click.secho(
            "    Hint: connect a VPN (or run from a supported country) and retry.",
            fg="yellow",
            err=True,
        )
        return

    click.secho(f"  ! {err}", fg="red", err=True)


@cli.command()
def analyze() -> None:
    """Run analytics on the latest snapshot."""
    _ensure_db()
    _run(_analyze_async())


def _fmt_money(value: Decimal) -> str:
    """Format a decimal amount as money with 2 decimals and separators."""
    return f"{value.quantize(Decimal('0.01')):,}"


async def _analyze_async() -> None:
    """Compute analytics for the latest snapshot date and display results."""
    settings = get_settings()

    # Late imports to avoid circular dependencies and keep startup fast
    from pfm.analytics import (
        compute_allocation_by_asset,
        compute_net_worth,
    )
    from pfm.db.repository import Repository

    async with Repository(settings.database_path) as repo:
        latest = await repo.get_latest_snapshots()
        if not latest:
            click.echo("No snapshots found. Run 'pfm collect' first.")
            return

        analysis_date = max(s.date for s in latest)
        net_worth = await compute_net_worth(repo, analysis_date)
        alloc_asset = await compute_allocation_by_asset(repo, analysis_date)

    click.echo(f"Analytics date: {analysis_date.isoformat()}")
    click.echo(f"Net worth (USD): {_fmt_money(net_worth)}")
    click.echo("Top assets:")
    for asset_row in alloc_asset[:5]:
        click.echo(
            f"  {asset_row.asset}: ${_fmt_money(asset_row.usd_value)} "
            f"({asset_row.percentage.quantize(Decimal('0.01'))}%)"
        )
    click.echo("Analytics computed (on-the-fly, no caching).")


@cli.command("comment")
def comment_command() -> None:
    """Generate AI commentary for latest analytics, print it, and cache it."""
    _ensure_db()
    if not _run(_comment_async()):
        sys.exit(1)


@cli.command()
def report() -> None:
    """Generate and send the Telegram report."""
    _ensure_db()
    if not _run(_report_async()):
        sys.exit(1)


@cli.command()
def run() -> None:
    """Full pipeline: collect → analyze → report."""
    _ensure_db()
    if not _run(_run_pipeline_async()):
        sys.exit(1)


async def _report_async() -> bool:
    """Generate and send the Telegram report from cached analytics."""
    settings = get_settings()

    # Late imports to avoid circular dependencies and keep startup fast
    from pfm.db.repository import Repository
    from pfm.reporting import format_weekly_report, is_telegram_configured, send_report

    if not await is_telegram_configured(db_path=settings.database_path):
        click.echo("Telegram is not configured. Skipping report send.")
        return True

    async with Repository(settings.database_path) as repo:
        analytics = await _load_latest_analytics_summary(repo)

    if analytics is None:
        return False

    try:
        async with Repository(settings.database_path) as repo:
            metrics = await repo.get_analytics_metrics_by_date(analytics.as_of_date)

        commentary = _parse_cached_ai_commentary(metrics.get("ai_commentary"))
        commentary_model = _parse_cached_ai_commentary_model(metrics.get("ai_commentary"))
        if commentary:
            click.echo("Using cached AI commentary.")
            if commentary_model:
                click.echo(f"AI commentary model: {commentary_model}")
        else:
            commentary = (
                "AI commentary is not cached for this analysis date. Run 'pfm comment' to generate and store it."
            )
            click.echo("No cached AI commentary for this analysis date. Using fallback text.")

        report_payload = format_weekly_report(analytics, commentary)
        sent = await send_report(report_payload)
    except Exception as exc:  # pragma: no cover - defensive guardrail
        logger.exception("Unexpected report pipeline error")
        click.echo(f"Failed to generate/send report: {exc}", err=True)
        return False

    if sent:
        click.echo("Report sent to Telegram.")
        return True

    click.echo("Failed to send report to Telegram.", err=True)
    return False


async def _comment_async() -> bool:
    """Generate AI commentary for latest analytics and cache it by date."""
    settings = get_settings()

    # Late imports to avoid circular dependencies and keep startup fast
    from pfm.ai import generate_commentary_with_model
    from pfm.db.repository import Repository

    async with Repository(settings.database_path) as repo:
        analytics = await _load_latest_analytics_summary(repo)

    if analytics is None:
        return False

    try:
        result = await generate_commentary_with_model(analytics)
    except Exception as exc:  # pragma: no cover - defensive guardrail
        logger.exception("Unexpected AI commentary generation error")
        click.echo(f"Failed to generate AI commentary: {exc}", err=True)
        return False

    metric_payload: dict[str, object] = {"text": result.text}
    if result.model:
        metric_payload["model"] = result.model
    if result.sections:
        metric_payload["sections"] = [{"title": s.title, "description": s.description} for s in result.sections]
    if isinstance(result.generation_meta, dict):
        metric_payload["generation_meta"] = result.generation_meta

    async with Repository(settings.database_path) as repo:
        await repo.save_analytics_metric(
            analytics.as_of_date,
            "ai_commentary",
            json.dumps(metric_payload),
        )

    click.echo(f"AI commentary date: {analytics.as_of_date.isoformat()}")
    if result.model:
        click.echo(f"AI model: {result.model}")
    else:
        click.echo("AI model: fallback")
    click.echo("AI commentary:")
    click.echo(result.text)
    click.echo("AI commentary cached.")
    return True


async def _run_pipeline_async() -> bool:
    """Run collect → analyze → report and alert on collection errors."""
    # Late imports to avoid circular dependencies and keep startup fast
    from pfm.reporting import is_telegram_configured, send_error_alert

    collect_ok = True
    analyze_ok = True
    report_ok = False
    alert_errors: list[str] = []

    click.echo("Running: collect")
    try:
        collect_results = await _collect_async(None)
    except Exception as exc:  # pragma: no cover - defensive guardrail
        collect_ok = False
        collect_results = []
        alert_errors.append(f"collect stage failed: {exc}")
        logger.exception("Collect stage failed unexpectedly")

    collect_errors = [f"{r.source}: {error}" for r in collect_results for error in r.errors]
    alert_errors.extend(collect_errors)
    if collect_errors:
        click.echo(f"Collection completed with {len(collect_errors)} error(s).")

    click.echo("Running: analyze")
    try:
        await _analyze_async()
    except Exception as exc:  # pragma: no cover - defensive guardrail
        analyze_ok = False
        alert_errors.append(f"analyze stage failed: {exc}")
        logger.exception("Analyze stage failed unexpectedly")
        click.echo(f"Analyze failed: {exc}", err=True)

    click.echo("Running: report")
    try:
        report_ok = await _report_async()
    except Exception as exc:  # pragma: no cover - defensive guardrail
        report_ok = False
        alert_errors.append(f"report stage failed: {exc}")
        logger.exception("Report stage failed unexpectedly")
        click.echo(f"Report failed: {exc}", err=True)

    if alert_errors:
        settings = get_settings()
        if await is_telegram_configured(db_path=settings.database_path):
            if await send_error_alert(alert_errors):
                click.echo("Error alert sent to Telegram.")
            else:
                click.echo("Failed to send error alert to Telegram.", err=True)
        else:
            click.echo("Telegram is not configured. Skipping error alert.")

    success = collect_ok and analyze_ok and report_ok
    if success:
        click.echo("Pipeline finished successfully.")
    else:
        click.echo("Pipeline finished with errors.", err=True)
    return success


async def _load_latest_analytics_summary(repo: Repository) -> AnalyticsSummary | None:
    """Compute analytics live for the latest snapshot date."""
    from pfm.server.analytics_helper import build_analytics_summary

    latest = await repo.get_latest_snapshots()
    if not latest:
        click.echo("No snapshots found. Run 'pfm collect' first.")
        return None

    return await build_analytics_summary(repo, max(s.date for s in latest))


def _parse_cached_ai_commentary(raw_json: str | None) -> str | None:
    """Parse cached AI commentary metric text, if present."""
    if raw_json is None:
        return None

    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError:
        text = raw_json.strip()
        return text or None

    if isinstance(parsed, str):
        text = parsed.strip()
        return text or None

    if isinstance(parsed, dict):
        text_value = parsed.get("text")
        if isinstance(text_value, str):
            value = text_value.strip()
            return value or None

    return None


def _parse_cached_ai_commentary_model(raw_json: str | None) -> str | None:
    """Parse cached AI commentary metric model name, if present."""
    if raw_json is None:
        return None

    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError:
        return None

    if isinstance(parsed, dict):
        model_value = parsed.get("model")
        if isinstance(model_value, str):
            value = model_value.strip()
            return value or None

    return None


# ── Daemon management ────────────────────────────────────────────────


@cli.group()
def daemon() -> None:
    """Manage the background HTTP server."""


@daemon.command("start")
@click.option("--port", default=19274, show_default=True, help="Port to listen on.")
def daemon_start(port: int) -> None:
    """Start the daemon via launchd."""
    from pfm.server.daemon import install_plist, is_daemon_running, load_daemon

    running, pid = is_daemon_running()
    if running:
        click.echo(f"Daemon is already running (PID {pid}).")
        return

    install_plist(port)
    load_daemon()
    click.echo(f"Daemon started on port {port}.")


@daemon.command("stop")
def daemon_stop() -> None:
    """Stop the daemon via launchd."""
    from pfm.server.daemon import is_daemon_running, unload_daemon

    running, _pid = is_daemon_running()
    if not running:
        click.echo("Daemon is not running.")
        return

    unload_daemon()
    click.echo("Daemon stopped.")


@daemon.command("status")
def daemon_status() -> None:
    """Show daemon status and PID."""
    from pfm.server.daemon import is_daemon_running

    running, pid = is_daemon_running()
    if running:
        click.echo(f"Daemon is running (PID {pid}).")
    else:
        click.echo("Daemon is not running.")


@daemon.command("logs")
@click.option("-f", "--follow", is_flag=True, help="Follow log output.")
def daemon_logs(*, follow: bool) -> None:
    """Tail the daemon log file."""
    import subprocess

    from pfm.server.daemon import get_log_path

    log_path = get_log_path()
    if not log_path.exists():
        click.echo("No log file found.")
        return

    cmd = ["/usr/bin/tail"]
    if follow:
        cmd.append("-f")
    cmd.append(str(log_path))
    subprocess.run(cmd, check=False)  # noqa: S603


@cli.command("server", hidden=True)
@click.option("--port", default=19274, show_default=True, help="Port to listen on.")
def server_command(port: int) -> None:
    """Run the HTTP server directly (used by launchd)."""
    from pfm.server.run import run_server

    run_server(port=port)
