"""CLI entry point for pfm."""

from __future__ import annotations

import asyncio
import json
import re
import sys
from decimal import Decimal
from typing import TYPE_CHECKING

import click

from pfm import __version__
from pfm.config import get_settings
from pfm.db.models import CollectorResult, init_db
from pfm.db.source_store import (
    DuplicateSourceError,
    InvalidCredentialsError,
    SourceNotFoundError,
    SourceStore,
)
from pfm.server.serializers import mask_secret as _mask
from pfm.source_types import SOURCE_TYPES

if TYPE_CHECKING:
    from collections.abc import Coroutine
    from datetime import date

    from pfm.db.models import Source


_COUNTRY_ACCESS_HINT_PATTERNS = (
    "service access appears restricted from your current network or region",
    "you don't have access from this country. use vpn or smth to handle this",
)


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

    name_w = max(len(s.name) for s in sources)
    name_w = max(name_w, 4)
    type_w = max(len(s.type) for s in sources)
    type_w = max(type_w, 4)

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

    default_name = source_type
    name = click.prompt("Instance name", default=default_name)

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

    if source_type == "bunq" and not (credentials.get("private_key_pem") and credentials.get("public_key_pem")):
        from pfm.collectors.bunq import generate_keypair_pem

        priv, pub = generate_keypair_pem()
        credentials["private_key_pem"] = priv
        credentials["public_key_pem"] = pub
        click.echo("Generated RSA-2048 keypair for bunq client identity.")

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


# ── Collect / Analyze ─────────────────────────────────────────────────


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
    from datetime import date as _date

    if source_type != "kbank":
        return

    statement_date = getattr(collector, "last_statement_date", None)
    if not isinstance(statement_date, _date):
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
