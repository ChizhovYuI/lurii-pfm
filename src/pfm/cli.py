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

from pfm.collectors import COLLECTOR_REGISTRY
from pfm.config import get_settings
from pfm.db.models import CollectorResult, init_db
from pfm.db.source_store import (
    DuplicateSourceError,
    InvalidCredentialsError,
    SourceNotFoundError,
    SourceStore,
)
from pfm.db.telegram_store import TelegramStore
from pfm.source_types import SOURCE_TYPES

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from pfm.ai import AnalyticsSummary
    from pfm.analytics import PnlResult
    from pfm.db.models import Source
    from pfm.db.repository import Repository

logger = logging.getLogger(__name__)
_COUNTRY_ACCESS_HINT_PATTERNS = (
    "service access appears restricted from your current network or region",
    "you don't have access from this country. use vpn or smth to handle this",
)


def _mask(value: str) -> str:
    """Mask a secret value, showing first 3 and last 3 chars."""
    if len(value) <= 8:  # noqa: PLR2004
        return "***"
    return f"{value[:3]}...{value[-3:]}"


def _get_store() -> SourceStore:
    """Get a SourceStore using the configured database path."""
    settings = get_settings()
    return SourceStore(settings.database_path)


def _get_telegram_store() -> TelegramStore:
    """Get a TelegramStore using the configured database path."""
    settings = get_settings()
    return TelegramStore(settings.database_path)


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

    pricing = PricingService(
        api_key=settings.coingecko_api_key,
        cache_db_path=settings.database_path,
    )
    results: list[CollectorResult] = []

    try:
        async with Repository(settings.database_path) as repo:
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
                try:
                    result = await collector.collect(repo)
                except BaseException as exc:
                    logger.exception("Unhandled collector exception from '%s'", src.name)
                    results.append(
                        CollectorResult(
                            source=src.name,
                            snapshots_count=0,
                            transactions_count=0,
                            errors=[f"Unhandled collector error: {exc}"],
                            duration_seconds=0.0,
                        )
                    )
                    continue

                results.append(result)
    finally:
        await pricing.close()

    return results


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
            _print_collect_error(err)

    click.echo("-" * 55)
    click.echo(f"{'TOTAL':<20}  {total_snaps:>5}  {total_txns:>5}  {total_errors:>6}")


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
    """Compute analytics for the latest snapshot date and cache results."""
    settings = get_settings()

    # Late imports to avoid circular dependencies and keep startup fast
    from pfm.analytics import (
        PnlPeriod,
        compute_allocation_by_asset,
        compute_allocation_by_category,
        compute_allocation_by_source,
        compute_currency_exposure,
        compute_net_worth,
        compute_pnl,
        compute_risk_metrics,
        compute_yield,
    )
    from pfm.db.repository import Repository

    async with Repository(settings.database_path) as repo:
        latest = await repo.get_latest_snapshots()
        if not latest:
            click.echo("No snapshots found. Run 'pfm collect' first.")
            return

        analysis_date = latest[0].date
        all_snapshots = await repo.get_snapshots_for_range(analysis_date, analysis_date)
        earliest_snapshots = await repo.get_snapshots_for_range(date.min, analysis_date)
        start_date = min((s.date for s in earliest_snapshots), default=analysis_date)

        net_worth = await compute_net_worth(repo, analysis_date)
        alloc_asset = await compute_allocation_by_asset(repo, analysis_date)
        alloc_source = await compute_allocation_by_source(repo, analysis_date)
        alloc_category = await compute_allocation_by_category(repo, analysis_date)
        currency_exposure = await compute_currency_exposure(repo, analysis_date)
        risk = await compute_risk_metrics(repo, analysis_date)

        pnl_daily = await compute_pnl(repo, analysis_date, PnlPeriod.DAILY)
        pnl_weekly = await compute_pnl(repo, analysis_date, PnlPeriod.WEEKLY)
        pnl_monthly = await compute_pnl(repo, analysis_date, PnlPeriod.MONTHLY)
        pnl_all_time = await compute_pnl(repo, analysis_date, PnlPeriod.ALL_TIME)

        snapshot_assets = {(s.source, s.asset.upper()) for s in all_snapshots}
        yield_inputs: list[tuple[str, str]] = []
        for source, asset in [("blend", "USDC"), ("okx", "USDC"), ("okx", "USDT")]:
            if (source, asset) in snapshot_assets:
                yield_inputs.append((source, asset))

        yield_results = [
            await compute_yield(repo, source, asset, start_date, analysis_date) for source, asset in yield_inputs
        ]

        # Cache computed metrics in analytics_cache table
        await repo.save_analytics_metric(analysis_date, "net_worth", json.dumps({"usd": str(net_worth)}))
        await repo.save_analytics_metric(
            analysis_date,
            "allocation_by_asset",
            json.dumps(
                [
                    {
                        "asset": row.asset,
                        "amount": str(row.amount),
                        "usd_value": str(row.usd_value),
                        "percentage": str(row.percentage),
                    }
                    for row in alloc_asset
                ]
            ),
        )
        await repo.save_analytics_metric(
            analysis_date,
            "allocation_by_source",
            json.dumps(
                [
                    {
                        "source": row.bucket,
                        "usd_value": str(row.usd_value),
                        "percentage": str(row.percentage),
                    }
                    for row in alloc_source
                ]
            ),
        )
        await repo.save_analytics_metric(
            analysis_date,
            "allocation_by_category",
            json.dumps(
                [
                    {
                        "category": row.bucket,
                        "usd_value": str(row.usd_value),
                        "percentage": str(row.percentage),
                    }
                    for row in alloc_category
                ]
            ),
        )
        await repo.save_analytics_metric(
            analysis_date,
            "currency_exposure",
            json.dumps(
                [
                    {
                        "currency": row.currency,
                        "usd_value": str(row.usd_value),
                        "percentage": str(row.percentage),
                    }
                    for row in currency_exposure
                ]
            ),
        )
        await repo.save_analytics_metric(
            analysis_date,
            "risk_metrics",
            json.dumps(
                {
                    "concentration_percentage": str(risk.concentration_percentage),
                    "hhi_index": str(risk.hhi_index),
                    "top_5_assets": [
                        {
                            "asset": row.asset,
                            "usd_value": str(row.usd_value),
                            "percentage": str(row.percentage),
                        }
                        for row in risk.top_5_assets
                    ],
                }
            ),
        )
        await repo.save_analytics_metric(
            analysis_date,
            "pnl",
            json.dumps(
                {
                    "daily": _pnl_result_to_dict(pnl_daily),
                    "weekly": _pnl_result_to_dict(pnl_weekly),
                    "monthly": _pnl_result_to_dict(pnl_monthly),
                    "all_time": _pnl_result_to_dict(pnl_all_time),
                }
            ),
        )
        await repo.save_analytics_metric(
            analysis_date,
            "yield",
            json.dumps(
                [
                    {
                        "source": row.source,
                        "asset": row.asset,
                        "principal_estimate": str(row.principal_estimate),
                        "current_value": str(row.current_value),
                        "yield_amount": str(row.yield_amount),
                        "yield_percentage": str(row.yield_percentage),
                        "annualized_rate": str(row.annualized_rate),
                    }
                    for row in yield_results
                ]
            ),
        )

    click.echo(f"Analytics date: {analysis_date.isoformat()}")
    click.echo(f"Net worth (USD): {_fmt_money(net_worth)}")
    click.echo("Top assets:")
    for asset_row in alloc_asset[:5]:
        click.echo(
            f"  {asset_row.asset}: ${_fmt_money(asset_row.usd_value)} "
            f"({asset_row.percentage.quantize(Decimal('0.01'))}%)"
        )
    click.echo("PnL:")
    for label, pnl in [
        ("daily", pnl_daily),
        ("weekly", pnl_weekly),
        ("monthly", pnl_monthly),
        ("all_time", pnl_all_time),
    ]:
        click.echo(
            f"  {label}: ${_fmt_money(pnl.absolute_change)} ({pnl.percentage_change.quantize(Decimal('0.01'))}%)"
        )
    if yield_results:
        click.echo("Yield:")
        for yield_row in yield_results:
            click.echo(
                f"  {yield_row.source}/{yield_row.asset}: ${_fmt_money(yield_row.yield_amount)} "
                f"({yield_row.yield_percentage.quantize(Decimal('0.01'))}%)"
            )
    click.echo("Cached analytics metrics: net_worth, allocations, currency_exposure, risk_metrics, pnl, yield")


def _pnl_result_to_dict(result: PnlResult) -> dict[str, object]:
    """Serialize PnL dataclass to a JSON-safe dict."""
    pnl = result
    return {
        "start_date": pnl.start_date.isoformat() if pnl.start_date else None,
        "end_date": pnl.end_date.isoformat() if pnl.end_date else None,
        "start_value": str(pnl.start_value),
        "end_value": str(pnl.end_value),
        "absolute_change": str(pnl.absolute_change),
        "percentage_change": str(pnl.percentage_change),
        "top_gainers": [
            {
                "asset": row.asset,
                "start_value": str(row.start_value),
                "end_value": str(row.end_value),
                "absolute_change": str(row.absolute_change),
                "percentage_change": str(row.percentage_change),
                "cost_basis_value": str(row.cost_basis_value) if row.cost_basis_value is not None else None,
            }
            for row in pnl.top_gainers
        ],
        "top_losers": [
            {
                "asset": row.asset,
                "start_value": str(row.start_value),
                "end_value": str(row.end_value),
                "absolute_change": str(row.absolute_change),
                "percentage_change": str(row.percentage_change),
                "cost_basis_value": str(row.cost_basis_value) if row.cost_basis_value is not None else None,
            }
            for row in pnl.top_losers
        ],
        "notes": list(pnl.notes),
    }


_REQUIRED_ANALYTICS_METRICS = (
    "net_worth",
    "allocation_by_asset",
    "allocation_by_source",
    "allocation_by_category",
    "currency_exposure",
    "risk_metrics",
    "pnl",
    "yield",
)


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
    from pfm.ai import generate_commentary
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
        commentary = await generate_commentary(analytics)
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
    """Load analytics cache for the latest snapshot date."""
    from pfm.ai import AnalyticsSummary

    latest = await repo.get_latest_snapshots()
    if not latest:
        click.echo("No snapshots found. Run 'pfm collect' and 'pfm analyze' first.")
        return None

    report_date = latest[0].date
    metrics = await repo.get_analytics_metrics_by_date(report_date)
    missing = [metric for metric in _REQUIRED_ANALYTICS_METRICS if metric not in metrics]
    if missing:
        click.echo(
            "Missing cached analytics metrics for latest snapshot date: "
            + ", ".join(missing)
            + ". Run 'pfm analyze' first.",
        )
        return None

    return AnalyticsSummary(
        as_of_date=report_date,
        net_worth_usd=_parse_net_worth_usd(metrics["net_worth"]),
        allocation_by_asset=metrics["allocation_by_asset"],
        allocation_by_source=metrics["allocation_by_source"],
        allocation_by_category=metrics["allocation_by_category"],
        currency_exposure=metrics["currency_exposure"],
        risk_metrics=metrics["risk_metrics"],
        pnl=metrics["pnl"],
        yield_metrics=metrics["yield"],
    )


def _parse_net_worth_usd(raw_json: str) -> Decimal:
    """Extract net worth USD value from cached metric JSON."""
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError:
        return Decimal(0)

    if not isinstance(parsed, dict):
        return Decimal(0)
    value = parsed.get("usd", "0")
    try:
        return Decimal(str(value))
    except ArithmeticError:
        return Decimal(0)
