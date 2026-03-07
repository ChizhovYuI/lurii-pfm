"""MCP server exposing Lurii Finance portfolio data to AI assistants."""

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from mcp.server.fastmcp import Context, FastMCP
from mcp.server.session import ServerSession

from pfm.db.repository import Repository

# ---------------------------------------------------------------------------
# Lifespan: open DB once, share across all tool calls
# ---------------------------------------------------------------------------

_MAX_TRANSACTIONS = 100
_MAX_SNAPSHOTS = 500


@dataclass
class AppContext:
    """Shared application state for all MCP tools."""

    repo: Repository
    db_path: Path


@asynccontextmanager
async def _lifespan(_server: FastMCP) -> AsyncIterator[AppContext]:
    from pfm.db.repository import Repository
    from pfm.server.daemon import get_db_path

    db_path = get_db_path()
    async with Repository(db_path) as repo:
        yield AppContext(repo=repo, db_path=db_path)


mcp = FastMCP(
    "Lurii Finance",
    instructions=(
        "Portfolio analytics server for Lurii Finance. "
        "Use tools to query balances, allocations, PnL, transactions, and yield data. "
        "All monetary values are in USD. Dates use ISO format (YYYY-MM-DD)."
    ),
    lifespan=_lifespan,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ctx_repo(ctx: Context[ServerSession, AppContext]) -> Repository:
    lc: AppContext = ctx.request_context.lifespan_context
    return lc.repo


def _ctx_db_path(ctx: Context[ServerSession, AppContext]) -> Path:
    lc: AppContext = ctx.request_context.lifespan_context
    return lc.db_path


def _today() -> date:
    return datetime.now(UTC).date()


def _parse_date(value: str | None) -> date:
    if value:
        return date.fromisoformat(value)
    return _today()


def _dec(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _dec2(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.01")))


def _pct(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01'))}%"


def _json(obj: object) -> str:
    return json.dumps(obj, indent=2, default=_json_default)


def _json_default(obj: object) -> str:
    if isinstance(obj, Decimal):
        return _dec(obj)
    if isinstance(obj, date):
        return obj.isoformat()
    msg = f"Object of type {type(obj).__name__} is not JSON serializable"
    raise TypeError(msg)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool()
async def get_portfolio_summary(
    ctx: Context[ServerSession, AppContext],
    date_str: str | None = None,
) -> str:
    """Get portfolio summary: net worth, top holdings, allocation by category, risk metrics, and data warnings.

    Use this for questions like "How is my portfolio doing?" or "What is my net worth?"
    """
    from pfm.analytics import (
        compute_allocation_by_asset,
        compute_allocation_by_category,
        compute_data_warnings,
        compute_net_worth,
        compute_risk_metrics,
    )
    from pfm.db.source_store import SourceStore

    repo = _ctx_repo(ctx)
    d = _parse_date(date_str)
    net_worth = await compute_net_worth(repo, d)
    alloc_asset = await compute_allocation_by_asset(repo, d)
    alloc_cat = await compute_allocation_by_category(repo, d)
    risk = await compute_risk_metrics(repo, d)

    snapshots = await repo.get_snapshots_resolved(d)
    store = SourceStore(_ctx_db_path(ctx))
    enabled_types = {s.type for s in await store.list_enabled()}
    warnings = compute_data_warnings(snapshots, enabled_types, d)

    top_holdings = [
        {
            "asset": r.asset,
            "asset_type": r.asset_type,
            "usd_value": _dec2(r.usd_value),
            "percentage": _pct(r.percentage),
        }
        for r in alloc_asset[:10]
    ]
    categories = [
        {
            "category": r.bucket,
            "usd_value": _dec2(r.usd_value),
            "percentage": _pct(r.percentage),
        }
        for r in alloc_cat
    ]
    result = {
        "as_of_date": d.isoformat(),
        "net_worth_usd": _dec2(net_worth),
        "top_holdings": top_holdings,
        "allocation_by_category": categories,
        "risk_metrics": {
            "concentration_percentage": _pct(risk.concentration_percentage),
            "hhi_index": _dec(risk.hhi_index),
        },
        "warnings": warnings,
    }
    return _json(result)


@mcp.tool()
async def get_allocation(
    ctx: Context[ServerSession, AppContext],
    by: str = "asset",
    date_str: str | None = None,
) -> str:
    """Get allocation breakdown by asset, source, or category.

    Args:
        by: One of "asset", "source", or "category".
        date_str: Date in YYYY-MM-DD format (defaults to today).

    Use this for questions like "Where is my money?" or "Show allocation by source."
    """
    from pfm.analytics import (
        compute_allocation_by_asset,
        compute_allocation_by_category,
        compute_allocation_by_source,
    )

    repo = _ctx_repo(ctx)
    d = _parse_date(date_str)

    data: list[dict[str, object]]
    if by == "source":
        rows = await compute_allocation_by_source(repo, d)
        data = [{"source": r.bucket, "usd_value": _dec2(r.usd_value), "percentage": _pct(r.percentage)} for r in rows]
    elif by == "category":
        rows = await compute_allocation_by_category(repo, d)
        data = [{"category": r.bucket, "usd_value": _dec2(r.usd_value), "percentage": _pct(r.percentage)} for r in rows]
    else:
        rows_asset = await compute_allocation_by_asset(repo, d)
        data = [
            {
                "asset": r.asset,
                "asset_type": r.asset_type,
                "sources": list(r.sources),
                "amount": _dec(r.amount),
                "usd_value": _dec2(r.usd_value),
                "price": _dec(r.price),
                "percentage": _pct(r.percentage),
            }
            for r in rows_asset
        ]
    return _json({"as_of_date": d.isoformat(), "by": by, "allocation": data})


@mcp.tool()
async def get_currency_exposure(
    ctx: Context[ServerSession, AppContext],
    date_str: str | None = None,
) -> str:
    """Get currency exposure breakdown (USD, GBP, THB, etc.).

    Use this for questions like "What is my currency risk?" or "How much do I have in GBP?"
    """
    from pfm.analytics import compute_currency_exposure

    repo = _ctx_repo(ctx)
    d = _parse_date(date_str)
    rows = await compute_currency_exposure(repo, d)
    data = [{"currency": r.currency, "usd_value": _dec2(r.usd_value), "percentage": _pct(r.percentage)} for r in rows]
    return _json({"as_of_date": d.isoformat(), "currency_exposure": data})


@mcp.tool()
async def get_risk_metrics(
    ctx: Context[ServerSession, AppContext],
    date_str: str | None = None,
) -> str:
    """Get risk metrics: concentration percentage, HHI index, top 5 concentrated assets.

    Use this for questions like "How concentrated is my portfolio?" or "What are my biggest risks?"
    """
    from pfm.analytics import compute_risk_metrics

    repo = _ctx_repo(ctx)
    d = _parse_date(date_str)
    risk = await compute_risk_metrics(repo, d)
    top5 = [
        {
            "asset": r.asset,
            "sources": list(r.sources),
            "usd_value": _dec2(r.usd_value),
            "percentage": _pct(r.percentage),
        }
        for r in risk.top_5_assets
    ]
    result = {
        "as_of_date": d.isoformat(),
        "concentration_percentage": _pct(risk.concentration_percentage),
        "hhi_index": _dec(risk.hhi_index),
        "top_5_assets": top5,
    }
    return _json(result)


@mcp.tool()
async def get_pnl(
    ctx: Context[ServerSession, AppContext],
    period: str = "weekly",
    date_str: str | None = None,
) -> str:
    """Get profit & loss for a period.

    Args:
        period: One of "daily", "weekly", "monthly", "all_time".
        date_str: Reference date in YYYY-MM-DD format (defaults to today).

    Use this for questions like "How did I perform this week?" or "What is my all-time PnL?"
    """
    from pfm.analytics.pnl import PnlPeriod, compute_pnl

    repo = _ctx_repo(ctx)
    d = _parse_date(date_str)
    pnl = await compute_pnl(repo, d, PnlPeriod(period))

    gainers = [
        {"asset": r.asset, "change_usd": _dec2(r.absolute_change), "change_pct": _pct(r.percentage_change)}
        for r in pnl.top_gainers[:5]
    ]
    losers = [
        {"asset": r.asset, "change_usd": _dec2(r.absolute_change), "change_pct": _pct(r.percentage_change)}
        for r in pnl.top_losers[:5]
    ]
    result = {
        "period": period,
        "start_date": pnl.start_date.isoformat() if pnl.start_date else None,
        "end_date": pnl.end_date.isoformat() if pnl.end_date else None,
        "start_value_usd": _dec2(pnl.start_value),
        "end_value_usd": _dec2(pnl.end_value),
        "absolute_change_usd": _dec2(pnl.absolute_change),
        "percentage_change": _pct(pnl.percentage_change),
        "top_gainers": gainers,
        "top_losers": losers,
        "notes": pnl.notes,
    }
    return _json(result)


@mcp.tool()
async def get_transactions(
    ctx: Context[ServerSession, AppContext],
    source: str | None = None,
    start: str | None = None,
    end: str | None = None,
    limit: int = 50,
) -> str:
    """Get transaction history with optional filters.

    Args:
        source: Filter by source name (e.g. "wise", "okx").
        start: Start date in YYYY-MM-DD format.
        end: End date in YYYY-MM-DD format.
        limit: Maximum number of transactions to return (default 50, max 100).

    Use this for questions like "Show my recent transactions" or "What did I trade on OKX?"
    """
    repo = _ctx_repo(ctx)
    start_date = date.fromisoformat(start) if start else None
    end_date = date.fromisoformat(end) if end else None
    capped_limit = min(limit, _MAX_TRANSACTIONS)

    txs = await repo.get_transactions(source=source, start=start_date, end=end_date)
    data = [
        {
            "date": t.date.isoformat(),
            "source": t.source,
            "type": t.tx_type.value,
            "asset": t.asset,
            "amount": _dec2(t.amount),
            "usd_value": _dec2(t.usd_value),
            "counterparty_asset": t.counterparty_asset or None,
            "counterparty_amount": _dec2(t.counterparty_amount) if t.counterparty_amount else None,
        }
        for t in txs[:capped_limit]
    ]
    return _json({"count": len(data), "transactions": data})


@mcp.tool()
async def get_snapshots(
    ctx: Context[ServerSession, AppContext],
    start: str,
    end: str,
    source: str | None = None,
) -> str:
    """Get historical balance snapshots for a date range.

    Args:
        start: Start date in YYYY-MM-DD format.
        end: End date in YYYY-MM-DD format.
        source: Filter by source name (optional).

    Use this for questions like "What did my portfolio look like last month?"
    """
    repo = _ctx_repo(ctx)
    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)

    if source:
        snaps = await repo.get_snapshots_by_source_name_and_date_range(source, start_date, end_date)
    else:
        snaps = await repo.get_snapshots_for_range(start_date, end_date)

    data = [
        {
            "date": s.date.isoformat(),
            "source": s.source_name or s.source,
            "asset": s.asset,
            "amount": _dec(s.amount),
            "usd_value": _dec2(s.usd_value),
            "price": _dec(s.price),
            "apy": _dec(s.apy) if s.apy else None,
        }
        for s in snaps[:_MAX_SNAPSHOTS]
    ]
    return _json({"count": len(data), "snapshots": data})


@mcp.tool()
async def get_yield_positions(
    ctx: Context[ServerSession, AppContext],
    date_str: str | None = None,
) -> str:
    """Get all active earn/DeFi positions with APY and portfolio percentage.

    Use this for questions like "What yield am I earning?" or "Show my DeFi positions."
    """
    from pfm.analytics import compute_net_worth

    repo = _ctx_repo(ctx)
    d = _parse_date(date_str)
    net_worth = await compute_net_worth(repo, d)
    snapshots = await repo.get_snapshots_resolved(d)

    earn_snaps = [s for s in snapshots if s.apy > 0]
    data = [
        {
            "asset": s.asset,
            "source": s.source_name or s.source,
            "usd_value": _dec2(s.usd_value),
            "apy": _pct(s.apy * 100),
            "portfolio_pct": _pct(s.usd_value / net_worth * 100) if net_worth else "0%",
        }
        for s in earn_snaps
    ]
    return _json({"as_of_date": d.isoformat(), "earn_positions": data})


@mcp.tool()
async def get_yield_history(
    ctx: Context[ServerSession, AppContext],
    source: str,
    asset: str,
    start: str,
    end: str,
) -> str:
    """Get yield tracking for a specific source+asset pair over a date range.

    Args:
        source: Source name (e.g. "blend", "okx").
        asset: Asset ticker (e.g. "USDC", "BTC").
        start: Start date in YYYY-MM-DD format.
        end: End date in YYYY-MM-DD format.

    Use this for questions like "How has my Blend USDC yield performed?"
    """
    from pfm.analytics import compute_yield

    repo = _ctx_repo(ctx)
    result = await compute_yield(repo, source, asset, date.fromisoformat(start), date.fromisoformat(end))
    data = {
        "source": result.source,
        "asset": result.asset,
        "start_date": result.start_date.isoformat() if result.start_date else None,
        "end_date": result.end_date.isoformat() if result.end_date else None,
        "principal_estimate_usd": _dec2(result.principal_estimate),
        "current_value_usd": _dec2(result.current_value),
        "yield_amount_usd": _dec2(result.yield_amount),
        "yield_percentage": _pct(result.yield_percentage),
        "annualized_rate": _pct(result.annualized_rate),
        "notes": result.notes,
    }
    return _json(data)


@mcp.tool()
async def get_sources(
    ctx: Context[ServerSession, AppContext],
) -> str:
    """List all configured data sources (names, types, enabled status). No credentials are exposed.

    Use this for questions like "Which accounts do I have?" or "What sources are connected?"
    """
    from pfm.db.source_store import SourceStore

    store = SourceStore(_ctx_db_path(ctx))
    sources = await store.list_all()
    data = [{"name": s.name, "type": s.type, "enabled": s.enabled} for s in sources]
    return _json({"sources": data})


# ---------------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------------


@mcp.resource("lurii://portfolio/summary")
async def resource_portfolio_summary() -> str:
    """Current portfolio summary: net worth, top holdings, categories, risk."""
    from pfm.analytics import (
        compute_allocation_by_asset,
        compute_allocation_by_category,
        compute_net_worth,
        compute_risk_metrics,
    )
    from pfm.db.repository import Repository
    from pfm.server.daemon import get_db_path

    d = _today()
    async with Repository(get_db_path()) as repo:
        net_worth = await compute_net_worth(repo, d)
        alloc_asset = await compute_allocation_by_asset(repo, d)
        alloc_cat = await compute_allocation_by_category(repo, d)
        risk = await compute_risk_metrics(repo, d)

    return _json(
        {
            "as_of_date": d.isoformat(),
            "net_worth_usd": _dec2(net_worth),
            "top_holdings": [
                {
                    "asset": r.asset,
                    "asset_type": r.asset_type,
                    "usd_value": _dec2(r.usd_value),
                    "percentage": _pct(r.percentage),
                }
                for r in alloc_asset[:10]
            ],
            "allocation_by_category": [
                {"category": r.bucket, "usd_value": _dec2(r.usd_value), "percentage": _pct(r.percentage)}
                for r in alloc_cat
            ],
            "risk": {
                "concentration_percentage": _pct(risk.concentration_percentage),
                "hhi_index": _dec(risk.hhi_index),
            },
        }
    )


@mcp.resource("lurii://portfolio/allocation")
async def resource_allocation() -> str:
    """Full allocation by asset with prices and amounts."""
    from pfm.analytics import compute_allocation_by_asset
    from pfm.db.repository import Repository
    from pfm.server.daemon import get_db_path

    d = _today()
    async with Repository(get_db_path()) as repo:
        rows = await compute_allocation_by_asset(repo, d)

    return _json(
        [
            {
                "asset": r.asset,
                "asset_type": r.asset_type,
                "sources": list(r.sources),
                "amount": _dec(r.amount),
                "usd_value": _dec2(r.usd_value),
                "price": _dec(r.price),
                "percentage": _pct(r.percentage),
            }
            for r in rows
        ]
    )


@mcp.resource("lurii://portfolio/risk")
async def resource_risk() -> str:
    """Risk metrics with top concentrated assets."""
    from pfm.analytics import compute_risk_metrics
    from pfm.db.repository import Repository
    from pfm.server.daemon import get_db_path

    d = _today()
    async with Repository(get_db_path()) as repo:
        risk = await compute_risk_metrics(repo, d)

    return _json(
        {
            "concentration_percentage": _pct(risk.concentration_percentage),
            "hhi_index": _dec(risk.hhi_index),
            "top_5_assets": [
                {"asset": r.asset, "usd_value": _dec2(r.usd_value), "percentage": _pct(r.percentage)}
                for r in risk.top_5_assets
            ],
        }
    )


@mcp.resource("lurii://portfolio/earn")
async def resource_earn() -> str:
    """Active yield/earn positions with APY."""
    from pfm.analytics import compute_net_worth
    from pfm.db.repository import Repository
    from pfm.server.daemon import get_db_path

    d = _today()
    async with Repository(get_db_path()) as repo:
        net_worth = await compute_net_worth(repo, d)
        snapshots = await repo.get_snapshots_resolved(d)

    earn_snaps = [s for s in snapshots if s.apy > 0]
    return _json(
        [
            {
                "asset": s.asset,
                "source": s.source_name or s.source,
                "usd_value": _dec2(s.usd_value),
                "apy": _pct(s.apy * 100),
                "portfolio_pct": _pct(s.usd_value / net_worth * 100) if net_worth else "0%",
            }
            for s in earn_snaps
        ]
    )


@mcp.resource("lurii://portfolio/transactions/recent")
async def resource_recent_transactions() -> str:
    """Last 7 days of deposits, withdrawals, and transfers."""
    from pfm.db.repository import Repository
    from pfm.server.daemon import get_db_path

    d = _today()
    async with Repository(get_db_path()) as repo:
        txs = await repo.get_transactions(start=d - timedelta(days=7), end=d)

    move_types = {"deposit", "withdrawal", "transfer"}
    data = [
        {
            "date": t.date.isoformat(),
            "source": t.source,
            "type": t.tx_type.value,
            "asset": t.asset,
            "amount": _dec2(t.amount),
            "usd_value": _dec2(t.usd_value),
        }
        for t in txs
        if t.tx_type.value in move_types
    ]
    return _json(data[:50])


@mcp.resource("lurii://sources")
async def resource_sources() -> str:
    """Configured data sources (no credentials)."""
    from pfm.db.source_store import SourceStore
    from pfm.server.daemon import get_db_path

    store = SourceStore(get_db_path())
    sources = await store.list_all()
    return _json([{"name": s.name, "type": s.type, "enabled": s.enabled} for s in sources])


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------


@mcp.prompt()
async def investment_review(focus: str = "") -> str:
    """Start a portfolio investment review discussion.

    Args:
        focus: Optional focus area (e.g. "risk", "rebalancing", "yield", "performance").
    """
    from pfm.db.repository import Repository
    from pfm.server.analytics_helper import build_analytics_summary
    from pfm.server.daemon import get_db_path

    d = _today()
    db_path = get_db_path()
    async with Repository(db_path) as repo:
        summary = await build_analytics_summary(repo, d, db_path=db_path)

    parts = [
        f"Review my investment portfolio as of {d.isoformat()}.",
        f"Net worth: ${summary.net_worth_usd:,.2f}",
        "",
        f"Holdings: {summary.allocation_by_asset}",
        f"Categories: {summary.allocation_by_category}",
        f"Risk: {summary.risk_metrics}",
    ]
    if summary.earn_positions:
        parts.append(f"Earn positions: {summary.earn_positions}")
    if summary.weekly_pnl:
        parts.append(f"Weekly PnL: {summary.weekly_pnl}")
    if summary.recent_transactions:
        parts.append(f"Recent transactions: {summary.recent_transactions}")
    if summary.warnings:
        parts.append(f"Data warnings: {', '.join(summary.warnings)}")
    if focus:
        parts.append(f"\nPlease focus on: {focus}")
    else:
        parts.append(
            "\nProvide a concise review covering: portfolio health, risk assessment, "
            "rebalancing opportunities, and actionable recommendations."
        )
    return "\n".join(parts)


@mcp.prompt()
async def weekly_check_in() -> str:
    """Weekly portfolio check-in with PnL and recent activity."""
    from pfm.analytics import compute_net_worth
    from pfm.analytics.pnl import PnlPeriod, compute_pnl
    from pfm.db.repository import Repository
    from pfm.server.daemon import get_db_path

    d = _today()
    async with Repository(get_db_path()) as repo:
        net_worth = await compute_net_worth(repo, d)
        pnl = await compute_pnl(repo, d, PnlPeriod.WEEKLY)
        txs = await repo.get_transactions(start=d - timedelta(days=7), end=d)

    move_types = {"deposit", "withdrawal", "transfer"}
    recent = [t for t in txs if t.tx_type.value in move_types]

    parts = [
        f"Weekly check-in for my portfolio as of {d.isoformat()}.",
        f"Current net worth: ${net_worth:,.2f}",
        "",
        "Weekly PnL:",
        f"  Start: ${pnl.start_value:,.2f} | End: ${pnl.end_value:,.2f}",
        f"  Change: ${pnl.absolute_change:,.2f} ({pnl.percentage_change:.2f}%)",
    ]
    if pnl.top_gainers:
        gainers = ", ".join(f"{g.asset} ({g.percentage_change:+.1f}%)" for g in pnl.top_gainers[:3])
        parts.append(f"  Top gainers: {gainers}")
    if pnl.top_losers:
        losers = ", ".join(f"{r.asset} ({r.percentage_change:+.1f}%)" for r in pnl.top_losers[:3])
        parts.append(f"  Top losers: {losers}")
    if recent:
        parts.append("")
        parts.append("Recent fund movements:")
        parts.extend(f"  {t.date} | {t.source} | {t.tx_type.value} | {t.asset} {t.amount}" for t in recent[:20])
    parts.append(
        "\nSummarize this week's performance, highlight any notable movements, "
        "and suggest what I should focus on next week."
    )
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the MCP server over stdio."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
