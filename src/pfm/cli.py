"""CLI entry point for pfm."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from typing import TYPE_CHECKING

import click

from pfm.collectors import COLLECTOR_REGISTRY
from pfm.config import get_settings
from pfm.db.models import CollectorResult, init_db
from pfm.db.source_store import (
    DuplicateSourceError,
    InvalidCredentialsError,
    SourceNotFoundError,
    SourceStore,
)
from pfm.source_types import SOURCE_TYPES

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from pfm.db.models import Source

logger = logging.getLogger(__name__)


def _mask(value: str) -> str:
    """Mask a secret value, showing first 3 and last 3 chars."""
    if len(value) <= 8:  # noqa: PLR2004
        return "***"
    return f"{value[:3]}...{value[-3:]}"


def _get_store() -> SourceStore:
    """Get a SourceStore using the configured database path."""
    settings = get_settings()
    return SourceStore(settings.database_path)


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


# ── Pipeline stubs ────────────────────────────────────────────────────


@cli.command()
@click.option("--source", "source_name", default=None, help="Run a single source by name.")
def collect(source_name: str | None) -> None:
    """Fetch balances and transactions from configured sources."""
    _ensure_db()
    results = _run(_collect_async(source_name))
    _print_collect_results(results)


async def _collect_async(source_name: str | None) -> list[CollectorResult]:
    """Run collection for enabled sources (or a single named source)."""
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

    # Late imports to avoid circular dependencies and keep startup fast
    from pfm.db.repository import Repository
    from pfm.pricing import PricingService

    pricing = PricingService(api_key=settings.coingecko_api_key)
    results: list[CollectorResult] = []

    try:
        async with Repository(settings.database_path) as repo:
            tasks = []
            for src in sources_to_run:
                collector_cls = COLLECTOR_REGISTRY.get(src.type)
                if collector_cls is None:
                    msg = f"No collector registered for type '{src.type}'"
                    logger.warning(msg)
                    click.echo(f"Skipping '{src.name}': {msg}", err=True)
                    continue

                creds = json.loads(src.credentials)
                collector = collector_cls(pricing, **creds)
                click.echo(f"Collecting: {src.name} ({src.type})...")
                tasks.append(collector.collect(repo))

            results = await asyncio.gather(*tasks)
    finally:
        await pricing.close()

    return list(results)


def _print_collect_results(results: list[CollectorResult]) -> None:
    """Print a summary table of collection results."""
    if not results:
        return

    click.echo()
    click.echo("Collection complete:")
    click.echo(f"{'SOURCE':<20}  {'SNAPS':>5}  {'TXNS':>5}  {'ERRORS':>6}  {'TIME':>7}")
    click.echo("-" * 55)

    total_snaps = 0
    total_txns = 0
    total_errors = 0
    for r in results:
        total_snaps += r.snapshots_count
        total_txns += r.transactions_count
        total_errors += len(r.errors)
        status = f"{r.duration_seconds:.1f}s"
        click.echo(
            f"{r.source:<20}  {r.snapshots_count:>5}  " f"{r.transactions_count:>5}  {len(r.errors):>6}  {status:>7}",
        )
        for err in r.errors:
            click.echo(f"  ! {err}", err=True)

    click.echo("-" * 55)
    click.echo(f"{'TOTAL':<20}  {total_snaps:>5}  {total_txns:>5}  {total_errors:>6}")


@cli.command()
def analyze() -> None:
    """Run analytics on the latest snapshot."""
    click.echo("analyze: not yet implemented")


@cli.command()
def report() -> None:
    """Generate and send the Telegram report."""
    click.echo("report: not yet implemented")


@cli.command()
def run() -> None:
    """Full pipeline: collect → analyze → report."""
    click.echo("run: not yet implemented")


@cli.command("import-kbank")
@click.argument("path", type=click.Path(exists=True))
def import_kbank(path: str) -> None:
    """Import a KBank PDF statement."""
    click.echo(f"import-kbank: not yet implemented (path={path})")
