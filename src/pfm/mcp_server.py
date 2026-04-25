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

from pfm.db.ai_report_memory_store import AI_REPORT_MEMORY_MAX_CHARS, AIReportMemoryStore, normalize_ai_report_memory
from pfm.db.metadata_store import MetadataStore
from pfm.db.models import CategoryRule, TransactionMetadata, TypeRule
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
    metadata_store: MetadataStore


@asynccontextmanager
async def _lifespan(_server: FastMCP) -> AsyncIterator[AppContext]:
    from pfm.db.repository import Repository
    from pfm.server.daemon import get_db_path

    db_path = get_db_path()
    async with Repository(db_path) as repo:
        store = MetadataStore(repo.connection)
        yield AppContext(repo=repo, db_path=db_path, metadata_store=store)


mcp = FastMCP(
    "Lurii Finance",
    instructions=(
        "Portfolio analytics server for Lurii Finance. "
        "Use tools/resources to query balances, allocations, PnL, transactions, and yield data, "
        "fetch weekly report prompt packs, and manage weekly AI report memory. "
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


def _ctx_store(ctx: Context[ServerSession, AppContext]) -> MetadataStore:
    lc: AppContext = ctx.request_context.lifespan_context
    return lc.metadata_store


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
    enabled_sources = await store.list_enabled()
    warnings = compute_data_warnings(snapshots, enabled_sources, d)

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
async def get_transactions(  # noqa: PLR0913
    ctx: Context[ServerSession, AppContext],
    source: str | None = None,
    source_name: str | None = None,
    start: str | None = None,
    end: str | None = None,
    limit: int = 50,
) -> str:
    """Get transaction history with optional filters.

    Args:
        source: Filter by source type (e.g. "wise", "okx").
        source_name: Filter by configured source instance name (e.g. "wise-main").
        start: Start date in YYYY-MM-DD format.
        end: End date in YYYY-MM-DD format.
        limit: Maximum number of transactions to return (default 50, max 100).

    Use this for questions like "Show my recent transactions" or "What did I trade on OKX?"
    """
    repo = _ctx_repo(ctx)
    start_date = date.fromisoformat(start) if start else None
    end_date = date.fromisoformat(end) if end else None
    capped_limit = min(limit, _MAX_TRANSACTIONS)

    txs = await repo.get_transactions(source=source, source_name=source_name, start=start_date, end=end_date)
    data = [
        {
            "date": t.date.isoformat(),
            "source": t.source,
            "source_name": t.source_name or t.source,
            "type": t.tx_type.value,
            "asset": t.asset,
            "amount": _dec2(t.amount),
            "usd_value": _dec2(t.usd_value),
            "counterparty_asset": t.counterparty_asset or None,
            "counterparty_amount": _dec2(t.counterparty_amount) if t.counterparty_amount else None,
            "trade_side": t.trade_side or None,
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


@mcp.tool()
async def get_ai_report_memory(
    ctx: Context[ServerSession, AppContext],
) -> str:
    """Read the current weekly AI report memory."""
    store = AIReportMemoryStore(_ctx_db_path(ctx))
    memory = await store.get()
    return _json(_memory_payload(memory))


@mcp.tool()
async def set_ai_report_memory(
    ctx: Context[ServerSession, AppContext],
    content: str,
) -> str:
    """Replace the weekly AI report memory with new content."""
    normalized = normalize_ai_report_memory(content)
    store = AIReportMemoryStore(_ctx_db_path(ctx))
    await store.set(normalized)
    return _json({"updated": True, **_memory_payload(normalized)})


@mcp.tool()
async def clear_ai_report_memory(
    ctx: Context[ServerSession, AppContext],
) -> str:
    """Clear the weekly AI report memory."""
    store = AIReportMemoryStore(_ctx_db_path(ctx))
    await store.set("")
    return _json({"updated": True, "cleared": True, **_memory_payload("")})


# ---------------------------------------------------------------------------
# Categorization tools (ADR-028)
# ---------------------------------------------------------------------------


def _category_rule_dict(rule: CategoryRule) -> dict[str, object]:
    return {
        "id": rule.id,
        "type_match": rule.type_match,
        "type_operator": rule.type_operator,
        "result_category": rule.result_category,
        "source": rule.source,
        "field_name": rule.field_name,
        "field_operator": rule.field_operator,
        "field_value": rule.field_value,
        "priority": rule.priority,
        "builtin": rule.builtin,
        "deleted": rule.deleted,
    }


def _type_rule_dict(rule: TypeRule) -> dict[str, object]:
    return {
        "id": rule.id,
        "result_type": rule.result_type,
        "source": rule.source,
        "field_name": rule.field_name,
        "field_operator": rule.field_operator,
        "field_value": rule.field_value,
        "priority": rule.priority,
        "builtin": rule.builtin,
        "deleted": rule.deleted,
    }


def _metadata_dict(meta: TransactionMetadata | None) -> dict[str, object] | None:
    if meta is None:
        return None
    return {
        "transaction_id": meta.transaction_id,
        "category": meta.category,
        "category_source": meta.category_source,
        "category_confidence": meta.category_confidence,
        "type_override": meta.type_override,
        "is_internal_transfer": meta.is_internal_transfer,
        "transfer_pair_id": meta.transfer_pair_id,
        "transfer_detected_by": meta.transfer_detected_by,
        "reviewed": meta.reviewed,
        "notes": meta.notes,
    }


def _parse_raw_dict(raw_json: str) -> dict[str, object]:
    if not raw_json:
        return {}
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _uncategorized_item_dict(tx: object, meta: TransactionMetadata | None) -> dict[str, object]:
    raw = _parse_raw_dict(tx.raw_json)  # type: ignore[attr-defined]
    return {
        "id": tx.id,  # type: ignore[attr-defined]
        "tx_id": tx.tx_id,  # type: ignore[attr-defined]
        "source_name": tx.source_name or tx.source,  # type: ignore[attr-defined]
        "date": tx.date.isoformat(),  # type: ignore[attr-defined]
        "tx_type": tx.tx_type.value,  # type: ignore[attr-defined]
        "asset": tx.asset,  # type: ignore[attr-defined]
        "amount": _dec2(tx.amount),  # type: ignore[attr-defined]
        "usd_value": _dec2(tx.usd_value),  # type: ignore[attr-defined]
        "category": meta.category if meta else None,
        "type_override": meta.type_override if meta else None,
        "raw_keys": list(raw.keys()),
        "raw_sample": {k: str(v)[:200] for k, v in raw.items()},
    }


@mcp.tool()
async def list_category_rules(
    ctx: Context[ServerSession, AppContext],
    source: str | None = None,
    *,
    include_deleted: bool = False,
) -> str:
    """List category rules ordered by priority."""
    store = _ctx_store(ctx)
    rules = await store.get_category_rules(source=source, include_deleted=include_deleted)
    return _json({"count": len(rules), "rules": [_category_rule_dict(r) for r in rules]})


@mcp.tool()
async def list_type_rules(
    ctx: Context[ServerSession, AppContext],
    source: str | None = None,
    *,
    include_deleted: bool = False,
) -> str:
    """List type rules ordered by priority."""
    store = _ctx_store(ctx)
    rules = await store.get_type_rules(source=source, include_deleted=include_deleted)
    return _json({"count": len(rules), "rules": [_type_rule_dict(r) for r in rules]})


@mcp.tool()
async def list_categories(
    ctx: Context[ServerSession, AppContext],
    tx_type: str | None = None,
) -> str:
    """List valid categories, optionally filtered by tx_type."""
    store = _ctx_store(ctx)
    cats = await store.get_categories(tx_type=tx_type)
    data = [
        {"tx_type": c.tx_type, "category": c.category, "display_name": c.display_name, "sort_order": c.sort_order}
        for c in cats
    ]
    return _json({"count": len(data), "categories": data})


@mcp.tool()
async def categorization_summary(
    ctx: Context[ServerSession, AppContext],
    source: str | None = None,
) -> str:
    """Per-source categorization counts: total, unknown_type, no_category, internal_transfer."""
    store = _ctx_store(ctx)
    rows = await store.get_categorization_summary(source_name=source)
    return _json({"sources": rows})


@mcp.tool()
async def get_rule_suggestions(
    ctx: Context[ServerSession, AppContext],
    min_evidence: int = 2,
) -> str:
    """Suggest new category rules learned from manual category choices."""
    store = _ctx_store(ctx)
    suggestions = await store.get_category_suggestions(min_evidence=min_evidence)
    return _json({"count": len(suggestions), "suggestions": suggestions})


@mcp.tool()
async def list_uncategorized_transactions(  # noqa: PLR0913
    ctx: Context[ServerSession, AppContext],
    source: str | None = None,
    *,
    missing_type: bool = False,
    missing_category: bool = False,
    limit: int = 100,
    offset: int = 0,
) -> str:
    """List uncategorized transactions with raw_json preview for rule authoring."""
    store = _ctx_store(ctx)
    items, total = await store.get_uncategorized_transactions(
        source_name=source,
        missing_type=missing_type,
        missing_category=missing_category,
        limit=limit,
        offset=offset,
    )
    data = [_uncategorized_item_dict(tx, meta) for tx, meta in items]
    return _json({"count": len(data), "total": total, "items": data})


@mcp.tool()
async def get_transaction_detail(
    ctx: Context[ServerSession, AppContext],
    transaction_id: int,
) -> str:
    """Full transaction + metadata + parsed raw_json + currently-winning rule_id."""
    from pfm.analytics.categorizer import categorize_transaction

    store = _ctx_store(ctx)
    pair = await store.get_transaction_by_id(transaction_id)
    if pair is None:
        return _json({"error": "not found", "transaction_id": transaction_id})
    tx, meta = pair
    rules = await store.get_category_rules()
    winner = categorize_transaction(tx, rules, meta)
    winning_rule_id = winner.rule_id if winner else None
    return _json(
        {
            "transaction": {
                "id": tx.id,
                "tx_id": tx.tx_id,
                "source": tx.source,
                "source_name": tx.source_name or tx.source,
                "date": tx.date.isoformat(),
                "tx_type": tx.tx_type.value,
                "asset": tx.asset,
                "amount": _dec2(tx.amount),
                "usd_value": _dec2(tx.usd_value),
                "counterparty_asset": tx.counterparty_asset or None,
                "counterparty_amount": _dec2(tx.counterparty_amount) if tx.counterparty_amount else None,
                "trade_side": tx.trade_side or None,
            },
            "metadata": _metadata_dict(meta),
            "raw_json": _parse_raw_dict(tx.raw_json),
            "winning_rule_id": winning_rule_id,
        }
    )


@mcp.tool()
async def create_category_rule(  # noqa: PLR0913
    ctx: Context[ServerSession, AppContext],
    type_match: str,
    result_category: str,
    type_operator: str = "eq",
    field_name: str = "",
    field_operator: str = "",
    field_value: str = "",
    source: str = "*",
    priority: int | None = None,
) -> str:
    """Create a category rule. Validates regex; auto-computes priority if omitted."""
    store = _ctx_store(ctx)
    rule = await store.create_category_rule(
        type_match,
        result_category,
        type_operator=type_operator,
        field_name=field_name,
        field_operator=field_operator,
        field_value=field_value,
        source=source,
        priority=priority,
    )
    return _json({"rule": _category_rule_dict(rule)})


@mcp.tool()
async def delete_category_rule(
    ctx: Context[ServerSession, AppContext],
    rule_id: int,
) -> str:
    """Delete a category rule. Builtin rules are soft-deleted."""
    store = _ctx_store(ctx)
    deleted = await store.delete_category_rule(rule_id)
    return _json({"deleted": deleted, "rule_id": rule_id})


@mcp.tool()
async def create_type_rule(  # noqa: PLR0913
    ctx: Context[ServerSession, AppContext],
    result_type: str,
    source: str = "*",
    field_name: str = "",
    field_operator: str = "eq",
    field_value: str = "",
    priority: int | None = None,
) -> str:
    """Create a type rule. Validates regex; auto-computes priority if omitted."""
    store = _ctx_store(ctx)
    rule = await store.create_type_rule(
        result_type,
        source=source,
        field_name=field_name,
        field_operator=field_operator,
        field_value=field_value,
        priority=priority,
    )
    return _json({"rule": _type_rule_dict(rule)})


@mcp.tool()
async def delete_type_rule(
    ctx: Context[ServerSession, AppContext],
    rule_id: int,
) -> str:
    """Delete a type rule. Builtin rules are soft-deleted."""
    store = _ctx_store(ctx)
    deleted = await store.delete_type_rule(rule_id)
    return _json({"deleted": deleted, "rule_id": rule_id})


@mcp.tool()
async def set_transaction_category(
    ctx: Context[ServerSession, AppContext],
    transaction_id: int,
    category: str,
) -> str:
    """Manually set the category for a transaction; recorded for rule learning."""
    from pfm.db.models import effective_type

    store = _ctx_store(ctx)
    pair = await store.get_transaction_by_id(transaction_id)
    if pair is None:
        return _json({"error": "not found", "transaction_id": transaction_id})
    tx, prev_meta = pair
    previous_category = prev_meta.category if prev_meta else ""
    meta = await store.upsert_metadata(transaction_id, category=category, category_source="manual")
    await store.record_category_choice(
        transaction_id,
        tx.source,
        effective_type(tx, prev_meta),
        category,
        field_snapshot=tx.raw_json,
        previous_category=previous_category or "",
    )
    return _json({"metadata": _metadata_dict(meta)})


@mcp.tool()
async def link_transfer(
    ctx: Context[ServerSession, AppContext],
    tx_id_a: int,
    tx_id_b: int,
) -> str:
    """Link two transactions as an internal transfer pair."""
    store = _ctx_store(ctx)
    await store.link_transfer(tx_id_a, tx_id_b)
    return _json({"ok": True, "tx_id_a": tx_id_a, "tx_id_b": tx_id_b})


@mcp.tool()
async def unlink_transfer(
    ctx: Context[ServerSession, AppContext],
    transaction_id: int,
) -> str:
    """Unlink a transaction from its transfer pair (clears both sides)."""
    store = _ctx_store(ctx)
    await store.unlink_transfer(transaction_id)
    return _json({"ok": True, "transaction_id": transaction_id})


@mcp.tool()
async def dry_run_category_rule(  # noqa: PLR0913
    ctx: Context[ServerSession, AppContext],
    type_match: str,
    result_category: str,
    type_operator: str = "eq",
    field_name: str = "",
    field_operator: str = "",
    field_value: str = "",
    source: str = "*",
    priority: int | None = None,
    scope_source: str | None = None,
    limit: int = 200,
) -> str:
    """Simulate applying a category rule without saving.

    Returns matched/unchanged/changed/shadowed_by_higher buckets plus
    overlapping_rules and raw_field_samples. `changed` reflects the post-priority
    real effect; `shadowed_by_higher` lists tx the candidate matches but loses to
    a higher-precedence existing rule (lower priority value, or same priority
    with lower id).
    """
    from pfm.analytics.rule_dryrun import dry_run_category_rule as _impl

    repo = _ctx_repo(ctx)
    store = _ctx_store(ctx)
    result = await _impl(
        repo,
        store,
        type_match=type_match,
        result_category=result_category,
        type_operator=type_operator,
        field_name=field_name,
        field_operator=field_operator,
        field_value=field_value,
        source=source,
        priority=priority,
        scope_source=scope_source,
        limit=limit,
    )
    return _json(result)


@mcp.tool()
async def dry_run_type_rule(  # noqa: PLR0913
    ctx: Context[ServerSession, AppContext],
    result_type: str,
    source: str = "*",
    field_name: str = "",
    field_operator: str = "eq",
    field_value: str = "",
    priority: int | None = None,
    scope_source: str | None = None,
    limit: int = 200,
) -> str:
    """Simulate applying a type rule without saving.

    Returns matched/unchanged/changed/shadowed_by_higher buckets plus
    overlapping_rules and raw_field_samples. `changed` reflects the post-priority
    real effect; `shadowed_by_higher` lists tx the candidate matches but loses to
    a higher-precedence existing rule (lower priority value, or same priority
    with lower id).
    """
    from pfm.analytics.rule_dryrun import dry_run_type_rule as _impl

    repo = _ctx_repo(ctx)
    store = _ctx_store(ctx)
    result = await _impl(
        repo,
        store,
        result_type=result_type,
        source=source,
        field_name=field_name,
        field_operator=field_operator,
        field_value=field_value,
        priority=priority,
        scope_source=scope_source,
        limit=limit,
    )
    return _json(result)


@mcp.tool()
async def apply_categorization(
    ctx: Context[ServerSession, AppContext],
    *,
    force: bool = False,
) -> str:
    """Run the categorization pipeline (types -> transfers -> categories). Returns counts."""
    from pfm.analytics.categorization_runner import run_categorization

    repo = _ctx_repo(ctx)
    store = _ctx_store(ctx)
    result = await run_categorization(repo, store, force=force)
    return _json(result)


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


@mcp.resource("lurii://ai/report-memory")
async def resource_ai_report_memory() -> str:
    """Current weekly AI report memory."""
    from pfm.server.daemon import get_db_path

    memory = await AIReportMemoryStore(get_db_path()).get()
    return _json(_memory_payload(memory))


@mcp.resource("lurii://ai/weekly-report/prompt")
async def resource_weekly_report_prompt() -> str:
    """Production weekly report prompt pack for external AI assistants."""
    from pfm.ai import build_weekly_report_prompt_pack
    from pfm.db.repository import Repository
    from pfm.server.daemon import get_db_path

    db_path = get_db_path()
    d = _today()
    async with Repository(db_path) as repo:
        pack = await build_weekly_report_prompt_pack(repo, db_path, d)
    return _json(pack)


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------


@mcp.prompt()
async def investment_review(focus: str = "") -> str:
    """Start a portfolio investment review discussion.

    Args:
        focus: Optional focus area (e.g. "risk", "rebalancing", "yield", "performance").
    """
    from pfm.ai import build_weekly_report_prompt_pack
    from pfm.db.repository import Repository
    from pfm.server.daemon import get_db_path

    d = _today()
    db_path = get_db_path()
    async with Repository(db_path) as repo:
        pack = await build_weekly_report_prompt_pack(repo, db_path, d)

    if pack.get("error"):
        return f"Unable to build weekly review prompt: {pack['error']}"

    sections = pack["sections"]
    titles = ", ".join(section["title"] for section in sections)

    parts = [
        f"Review my investment portfolio as of {d.isoformat()}.",
        "Use the weekly report prompt pack below as the authoritative contract.",
        "",
        f"Section order: {titles}",
        "",
        "System prompt:",
        pack["system_prompt"],
        "",
        "Analytics context:",
        pack["analytics_context"],
    ]
    if pack.get("investor_memory"):
        parts.extend(["", "Investor memory:", pack["investor_memory"]])
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
    from pfm.analytics.flow_bridge import build_capital_flows_summary, build_internal_conversions_summary
    from pfm.analytics.pnl import PnlPeriod, compute_pnl
    from pfm.db.repository import Repository
    from pfm.server.daemon import get_db_path

    d = _today()
    async with Repository(get_db_path()) as repo:
        net_worth = await compute_net_worth(repo, d)
        pnl = await compute_pnl(repo, d, PnlPeriod.WEEKLY)
        txs = await repo.get_transactions(start=d - timedelta(days=7), end=d)

    recent_flows = build_capital_flows_summary(txs)
    conversions = build_internal_conversions_summary(txs)

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
    if recent_flows:
        parts.append("")
        parts.append("Recent capital and income flows:")
        parts.extend(
            f"  {row['date']} | {row['source']} | {row['kind']} | {row['asset']} {row['amount']}"
            for row in recent_flows[:20]
        )
    if conversions:
        parts.append("")
        parts.append("Recent internal conversions / redeployments:")
        parts.extend(
            f"  {row['date']} | {row['source']} | {row['from_asset']} {row['from_amount']} -> "
            f"{row['to_asset']} {row['to_amount']}"
            for row in conversions[:20]
        )
    parts.append(
        "\nSummarize this week's performance, highlight any notable movements, "
        "and suggest what I should focus on next week."
    )
    return "\n".join(parts)


def _memory_payload(memory: str) -> dict[str, object]:
    return {
        "memory": memory,
        "length": len(memory),
        "normalized": True,
        "max_chars": AI_REPORT_MEMORY_MAX_CHARS,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the MCP server over stdio."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
