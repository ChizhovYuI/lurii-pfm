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
from pfm.pricing.coingecko import PricingService

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
    pricing: PricingService


@asynccontextmanager
async def _lifespan(_server: FastMCP) -> AsyncIterator[AppContext]:
    from pfm.config import get_settings
    from pfm.db.repository import Repository
    from pfm.server.daemon import get_db_path

    db_path = get_db_path()
    settings = get_settings()
    async with Repository(db_path) as repo:
        store = MetadataStore(repo.connection)
        pricing = PricingService(api_key=settings.coingecko_api_key, cache_db_path=db_path)
        try:
            yield AppContext(repo=repo, db_path=db_path, metadata_store=store, pricing=pricing)
        finally:
            await pricing.close()


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


def _ctx_pricing(ctx: Context[ServerSession, AppContext]) -> PricingService:
    lc: AppContext = ctx.request_context.lifespan_context
    return lc.pricing


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
async def list_sources(
    ctx: Context[ServerSession, AppContext],
) -> str:
    """List all configured sources with id, name, type, enabled, tx_count, snap_count.

    Counts come from the ``source_id`` FK on ``transactions`` / ``snapshots``
    (ADR-030). Use this for the categorization-curator survey pass to map
    source ids to names and gauge per-source data volume before drilling
    into uncategorized rows.
    """
    repo = _ctx_repo(ctx)
    sources = await repo.list_sources_with_counts()
    return _json({"count": len(sources), "sources": sources})


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
        "source_type": rule.source_type,
        "source_id": rule.source_id,
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
        "source_type": rule.source_type,
        "source_id": rule.source_id,
        "field_name": rule.field_name,
        "field_operator": rule.field_operator,
        "field_value": rule.field_value,
        "priority": rule.priority,
        "builtin": rule.builtin,
        "deleted": rule.deleted,
    }


def _winning_category_rule_dict(rule: CategoryRule | None) -> dict[str, object] | None:
    if rule is None:
        return None
    return {
        "id": rule.id,
        "priority": rule.priority,
        "field_name": rule.field_name,
        "field_value": rule.field_value,
        "result_category": rule.result_category,
    }


def _winning_type_rule_dict(rule: TypeRule | None) -> dict[str, object] | None:
    if rule is None:
        return None
    return {
        "id": rule.id,
        "priority": rule.priority,
        "field_name": rule.field_name,
        "field_value": rule.field_value,
        "result_type": rule.result_type,
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


async def _resolve_legacy_source(
    repo: Repository,
    legacy: str | None,
) -> tuple[str | None, int | None]:
    """Map a legacy ``source`` arg into ``(source_type, source_id)``.

    Deprecation alias for ADR-030 Stage 3 — old callers passing
    ``source="kbank-main"`` (a sources.name) get rewritten to
    ``source_id=9``; ``source="kbank"`` (a type) becomes
    ``source_type="kbank"``; ``source="*"`` / None / "" → both NULL.
    """
    if legacy is None or legacy in {"", "*"}:
        return None, None
    sources = await repo.list_sources_with_counts()
    by_name: dict[str, int] = {}
    for s in sources:
        name = s.get("name")
        sid = s.get("id")
        if isinstance(name, str) and isinstance(sid, int):
            by_name[name] = sid
    if legacy in by_name:
        return None, by_name[legacy]
    return legacy, None


def _uncategorized_item_dict(
    tx: object,
    meta: TransactionMetadata | None,
    *,
    include_raw_sample: bool = False,
) -> dict[str, object]:
    raw = _parse_raw_dict(tx.raw_json)  # type: ignore[attr-defined]
    item: dict[str, object] = {
        "id": tx.id,  # type: ignore[attr-defined]
        "tx_id": tx.tx_id,  # type: ignore[attr-defined]
        "source_name": tx.source_name or tx.source,  # type: ignore[attr-defined]
        "source_id": tx.source_id,  # type: ignore[attr-defined]
        "date": tx.date.isoformat(),  # type: ignore[attr-defined]
        "tx_type": tx.tx_type.value,  # type: ignore[attr-defined]
        "asset": tx.asset,  # type: ignore[attr-defined]
        "amount": _dec2(tx.amount),  # type: ignore[attr-defined]
        "usd_value": _dec2(tx.usd_value),  # type: ignore[attr-defined]
        "category": meta.category if meta else None,
        "type_override": meta.type_override if meta else None,
        "raw_keys": list(raw.keys()),
    }
    if include_raw_sample:
        item["raw_sample"] = {k: str(v)[:200] for k, v in raw.items()}
    return item


@mcp.tool()
async def list_category_rules(
    ctx: Context[ServerSession, AppContext],
    source_type: str | None = None,
    source_id: int | None = None,
    *,
    source: str | None = None,
    include_deleted: bool = False,
) -> str:
    """List category rules ordered by priority.

    ``source_type`` filters to rules pinned to that source type plus catch-alls.
    ``source_id`` adds rules pinned to that specific source instance.
    ``source`` is a deprecation alias for legacy callers (ADR-030 Stage 3) —
    a sources.name resolves to ``source_id``, anything else to ``source_type``.
    """
    repo = _ctx_repo(ctx)
    store = _ctx_store(ctx)
    if source is not None and source_type is None and source_id is None:
        source_type, source_id = await _resolve_legacy_source(repo, source)
    rules = await store.get_category_rules(
        source_type=source_type, source_id=source_id, include_deleted=include_deleted
    )
    return _json({"count": len(rules), "rules": [_category_rule_dict(r) for r in rules]})


@mcp.tool()
async def list_type_rules(
    ctx: Context[ServerSession, AppContext],
    source_type: str | None = None,
    source_id: int | None = None,
    *,
    source: str | None = None,
    include_deleted: bool = False,
) -> str:
    """List type rules ordered by priority. Same semantics as :func:`list_category_rules`."""
    repo = _ctx_repo(ctx)
    store = _ctx_store(ctx)
    if source is not None and source_type is None and source_id is None:
        source_type, source_id = await _resolve_legacy_source(repo, source)
    rules = await store.get_type_rules(source_type=source_type, source_id=source_id, include_deleted=include_deleted)
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
    *,
    include_non_discriminating: bool = False,
) -> str:
    """Suggest new category rules learned from manual category choices.

    Suggestions where the same (source, field, value) maps to more than one
    chosen category are non-discriminating and dropped by default. Pass
    ``include_non_discriminating=True`` to surface them flagged with
    ``"non_discriminating": true`` and ``"conflicting_categories": [...]``.
    """
    store = _ctx_store(ctx)
    suggestions = await store.get_category_suggestions(
        min_evidence=min_evidence,
        include_non_discriminating=include_non_discriminating,
    )
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
    include_raw_sample: bool = False,
) -> str:
    """List uncategorized transactions with raw_json preview for rule authoring.

    By default returns only ``raw_keys`` (cheap). Pass
    ``include_raw_sample=True`` to also include ``raw_sample`` (each value
    truncated to 200 chars) — needed for pattern discovery but considerably
    larger payload. Prefer keys-only on the survey pass; opt in for the
    discovery pass on a narrowed limit.
    """
    store = _ctx_store(ctx)
    items, total = await store.get_uncategorized_transactions(
        source_name=source,
        missing_type=missing_type,
        missing_category=missing_category,
        limit=limit,
        offset=offset,
    )
    data = [_uncategorized_item_dict(tx, meta, include_raw_sample=include_raw_sample) for tx, meta in items]
    return _json({"count": len(data), "total": total, "items": data})


@mcp.tool()
async def get_transaction_detail(
    ctx: Context[ServerSession, AppContext],
    transaction_id: int,
) -> str:
    """Full transaction + metadata + parsed raw_json + winning category & type rules.

    ``winning_category_rule`` is the rule producing the current category
    (or null when categorized via fallback / not categorized at all).
    ``winning_type_rule`` is the type rule that maps the raw tx to its
    effective type — null when ``tx_type`` came in directly from the
    source (no rule needed) or no rule matched. Each value is a small
    rule snapshot (``id``, ``priority``, ``field_name``, ``field_value``,
    ``result_*``) — enough to answer "why is this tx X" without a second
    listing call. ``winning_rule_id`` is kept as a back-compat alias for
    ``winning_category_rule.id``.
    """
    from pfm.analytics.categorizer import categorize_transaction
    from pfm.analytics.type_resolver import resolve_type_winner

    store = _ctx_store(ctx)
    pair = await store.get_transaction_by_id(transaction_id)
    if pair is None:
        return _json({"error": "not found", "transaction_id": transaction_id})
    tx, meta = pair
    cat_rules = await store.get_category_rules()
    type_rules = await store.get_type_rules()
    cat_winner = categorize_transaction(tx, cat_rules, meta)
    type_winner = resolve_type_winner(tx, type_rules)
    cat_rule_obj = next((r for r in cat_rules if r.id == cat_winner.rule_id), None) if cat_winner is not None else None
    return _json(
        {
            "transaction": {
                "id": tx.id,
                "tx_id": tx.tx_id,
                "source": tx.source,
                "source_name": tx.source_name or tx.source,
                "source_id": tx.source_id,
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
            "winning_category_rule": _winning_category_rule_dict(cat_rule_obj),
            "winning_type_rule": _winning_type_rule_dict(type_winner),
            "winning_rule_id": cat_rule_obj.id if cat_rule_obj is not None else None,
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
    source_type: str | None = None,
    source_id: int | None = None,
    priority: int | None = None,
    *,
    source: str | None = None,
) -> str:
    """Create a category rule. Validates regex; auto-computes priority if omitted.

    Source filter is XOR: pass ``source_type`` for a type-wide rule, ``source_id``
    for an instance-pinned rule, or neither for a catch-all. ``source`` is a
    deprecation alias (ADR-030 Stage 3) — auto-resolved against ``sources``.

    On validation failure (e.g. malformed regex, both source filters set)
    returns ``{"error": "validation", "message": ...}`` instead of raising.
    """
    repo = _ctx_repo(ctx)
    store = _ctx_store(ctx)
    if source is not None and source_type is None and source_id is None:
        source_type, source_id = await _resolve_legacy_source(repo, source)
    try:
        rule = await store.create_category_rule(
            type_match,
            result_category,
            type_operator=type_operator,
            field_name=field_name,
            field_operator=field_operator,
            field_value=field_value,
            source_type=source_type,
            source_id=source_id,
            priority=priority,
        )
    except ValueError as exc:
        return _json({"error": "validation", "message": str(exc)})
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
async def bulk_delete_category_rules(
    ctx: Context[ServerSession, AppContext],
    rule_ids: list[int],
) -> str:
    """Delete several category rules in one call. Builtin rules are soft-deleted.

    Returns ``{"deleted": [...], "not_found": [...]}`` — same set semantics
    as iterating ``delete_category_rule`` but in one round-trip. Order of
    ``rule_ids`` is preserved within each bucket.
    """
    store = _ctx_store(ctx)
    deleted: list[int] = []
    not_found: list[int] = []
    for rid in rule_ids:
        if await store.delete_category_rule(rid):
            deleted.append(rid)
        else:
            not_found.append(rid)
    return _json({"deleted": deleted, "not_found": not_found})


@mcp.tool()
async def create_type_rule(  # noqa: PLR0913
    ctx: Context[ServerSession, AppContext],
    result_type: str,
    source_type: str | None = None,
    source_id: int | None = None,
    field_name: str = "",
    field_operator: str = "eq",
    field_value: str = "",
    priority: int | None = None,
    *,
    source: str | None = None,
) -> str:
    """Create a type rule. Same source-filter semantics as :func:`create_category_rule`."""
    repo = _ctx_repo(ctx)
    store = _ctx_store(ctx)
    if source is not None and source_type is None and source_id is None:
        source_type, source_id = await _resolve_legacy_source(repo, source)
    try:
        rule = await store.create_type_rule(
            result_type,
            source_type=source_type,
            source_id=source_id,
            field_name=field_name,
            field_operator=field_operator,
            field_value=field_value,
            priority=priority,
        )
    except ValueError as exc:
        return _json({"error": "validation", "message": str(exc)})
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
async def bulk_delete_type_rules(
    ctx: Context[ServerSession, AppContext],
    rule_ids: list[int],
) -> str:
    """Delete several type rules in one call. Builtin rules are soft-deleted.

    Returns ``{"deleted": [...], "not_found": [...]}`` — same set semantics
    as iterating ``delete_type_rule`` but in one round-trip. Order of
    ``rule_ids`` is preserved within each bucket.
    """
    store = _ctx_store(ctx)
    deleted: list[int] = []
    not_found: list[int] = []
    for rid in rule_ids:
        if await store.delete_type_rule(rid):
            deleted.append(rid)
        else:
            not_found.append(rid)
    return _json({"deleted": deleted, "not_found": not_found})


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
async def audit_category_rules(
    ctx: Context[ServerSession, AppContext],
    source_type: str | None = None,
    source_id: int | None = None,
    scope_source: str | None = None,
    *,
    source: str | None = None,
) -> str:
    """Audit category rules — count matches and post-priority wins per rule.

    Returns ``{rules, dead, shadowed_dead}`` sorted by ``matched_count`` ascending.

    ``source_type`` / ``source_id`` filter the rules under audit;
    ``scope_source`` filters the transactions used to evaluate them
    (matches against ``sources.name``). ``source`` is a deprecation alias
    for the rule filter (ADR-030 Stage 3).
    """
    from pfm.analytics.rule_audit import audit_category_rules as _impl

    repo = _ctx_repo(ctx)
    store = _ctx_store(ctx)
    if source is not None and source_type is None and source_id is None:
        source_type, source_id = await _resolve_legacy_source(repo, source)
    result = await _impl(
        repo,
        store,
        source_type=source_type,
        source_id=source_id,
        scope_source=scope_source,
    )
    return _json(result)


@mcp.tool()
async def audit_type_rules(
    ctx: Context[ServerSession, AppContext],
    source_type: str | None = None,
    source_id: int | None = None,
    scope_source: str | None = None,
    *,
    source: str | None = None,
) -> str:
    """Audit type rules — same semantics as :func:`audit_category_rules`."""
    from pfm.analytics.rule_audit import audit_type_rules as _impl

    repo = _ctx_repo(ctx)
    store = _ctx_store(ctx)
    if source is not None and source_type is None and source_id is None:
        source_type, source_id = await _resolve_legacy_source(repo, source)
    result = await _impl(
        repo,
        store,
        source_type=source_type,
        source_id=source_id,
        scope_source=scope_source,
    )
    return _json(result)


@mcp.tool()
async def validate_rule_args(
    ctx: Context[ServerSession, AppContext],  # noqa: ARG001
    field_operator: str = "",
    field_value: str = "",
) -> str:
    """Pre-check rule arguments before dry-run / create.

    Cheap validation pass — no DB scan. Currently checks regex compile
    when ``field_operator='regex'`` and ``field_value`` is set. Returns
    ``{"valid": true}`` or ``{"valid": false, "error": "validation",
    "message": ...}``. Skill should call this before dry-run when authoring
    a regex rule to short-circuit on malformed patterns.
    """
    from pfm.db.metadata_store import _validate_regex_value

    if field_operator == "regex" and field_value:
        try:
            _validate_regex_value(field_value)
        except ValueError as exc:
            return _json({"valid": False, "error": "validation", "message": str(exc)})
    return _json({"valid": True})


_DRY_RUN_SUMMARY_TOP_N = 5


def _summarize_dry_run(result: dict[str, object]) -> dict[str, object]:
    """Trim dry_run output to counts + top-N samples per bucket.

    Used when ``summary_only=True``. Keeps ``matched`` (already a count),
    ``overlapping_rules`` (typically small), and ``raw_field_samples``
    (already capped at 5). Replaces ``unchanged``, ``changed``, and
    ``shadowed_by_higher`` lists with ``{count, sample}`` objects.
    """
    summary = dict(result)
    for bucket in ("unchanged", "changed", "shadowed_by_higher"):
        items = summary.get(bucket)
        if isinstance(items, list):
            summary[bucket] = {
                "count": len(items),
                "sample": items[:_DRY_RUN_SUMMARY_TOP_N],
            }
    return summary


@mcp.tool()
async def dry_run_category_rule(  # noqa: PLR0913
    ctx: Context[ServerSession, AppContext],
    type_match: str,
    result_category: str,
    type_operator: str = "eq",
    field_name: str = "",
    field_operator: str = "",
    field_value: str = "",
    source_type: str | None = None,
    source_id: int | None = None,
    priority: int | None = None,
    scope_source: str | None = None,
    limit: int = 200,
    *,
    source: str | None = None,
    summary_only: bool = False,
) -> str:
    """Simulate applying a category rule without saving.

    Source filter is XOR: ``source_type`` for type-wide, ``source_id`` for an
    instance, or neither for catch-all. ``source`` is a deprecation alias.

    Returns matched/unchanged/changed/shadowed_by_higher buckets plus
    overlapping_rules and raw_field_samples; ``summary_only=True`` collapses
    the lists to counts + first 5.
    """
    from pfm.analytics.rule_dryrun import dry_run_category_rule as _impl

    repo = _ctx_repo(ctx)
    store = _ctx_store(ctx)
    if source is not None and source_type is None and source_id is None:
        source_type, source_id = await _resolve_legacy_source(repo, source)
    try:
        result = await _impl(
            repo,
            store,
            type_match=type_match,
            result_category=result_category,
            type_operator=type_operator,
            field_name=field_name,
            field_operator=field_operator,
            field_value=field_value,
            source_type=source_type,
            source_id=source_id,
            priority=priority,
            scope_source=scope_source,
            limit=limit,
        )
    except ValueError as exc:
        return _json({"error": "validation", "message": str(exc)})
    if summary_only:
        result = _summarize_dry_run(result)
    return _json(result)


@mcp.tool()
async def dry_run_type_rule(  # noqa: PLR0913
    ctx: Context[ServerSession, AppContext],
    result_type: str,
    source_type: str | None = None,
    source_id: int | None = None,
    field_name: str = "",
    field_operator: str = "eq",
    field_value: str = "",
    priority: int | None = None,
    scope_source: str | None = None,
    limit: int = 200,
    *,
    source: str | None = None,
    summary_only: bool = False,
) -> str:
    """Simulate applying a type rule without saving.

    Same source-filter and summary semantics as :func:`dry_run_category_rule`.
    """
    from pfm.analytics.rule_dryrun import dry_run_type_rule as _impl

    repo = _ctx_repo(ctx)
    store = _ctx_store(ctx)
    if source is not None and source_type is None and source_id is None:
        source_type, source_id = await _resolve_legacy_source(repo, source)
    try:
        result = await _impl(
            repo,
            store,
            result_type=result_type,
            source_type=source_type,
            source_id=source_id,
            field_name=field_name,
            field_operator=field_operator,
            field_value=field_value,
            priority=priority,
            scope_source=scope_source,
            limit=limit,
        )
    except ValueError as exc:
        return _json({"error": "validation", "message": str(exc)})
    if summary_only:
        result = _summarize_dry_run(result)
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
# Manual data entry & source configuration
# ---------------------------------------------------------------------------


async def _best_effort_broadcast(event_type: str) -> None:
    """Notify the running daemon to push a WS event to subscribed clients.

    No-op if the daemon is not reachable or the call fails. The DB write has
    already committed; broadcasting is purely a UI hint.
    """
    from pfm.server.client import get_base_url, is_daemon_reachable

    if not is_daemon_reachable():
        return

    import httpx

    try:
        async with httpx.AsyncClient(base_url=get_base_url(), timeout=2.0) as client:
            await client.post("/api/v1/internal/broadcast", json={"type": event_type})
    except httpx.HTTPError:
        pass


async def _best_effort_collect(source_name: str) -> dict[str, object]:
    """Trigger a single-source collect on the running daemon (best-effort).

    Returns a status dict for the caller to surface. No-op-like result when the
    daemon is unreachable — the source row is already committed; collection can
    happen later via ``pfm refresh`` or app open.
    """
    from pfm.server.client import get_base_url, is_daemon_reachable

    if not is_daemon_reachable():
        return {"collect": "skipped", "reason": "daemon unreachable"}

    import httpx

    try:
        async with httpx.AsyncClient(base_url=get_base_url(), timeout=10.0) as client:
            resp = await client.post("/api/v1/collect", json={"source": source_name})
        if resp.status_code == 409:  # noqa: PLR2004
            return {"collect": "skipped", "reason": "another collection is running"}
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        return {"collect": "skipped", "reason": f"daemon error: {exc}"}
    return {"collect": "started", "source": source_name}


@mcp.tool()
async def list_supported_fiat_currencies(
    ctx: Context[ServerSession, AppContext],
) -> str:
    """Return the list of fiat currency codes accepted by the manual cash source."""
    del ctx
    from pfm.cash_manual import SUPPORTED_FIAT_CURRENCIES

    return _json({"supported_currencies": list(SUPPORTED_FIAT_CURRENCIES)})


@mcp.tool()
async def get_cash_balance(
    ctx: Context[ServerSession, AppContext],
) -> str:
    """Return the current manual cash balances and selected currencies.

    Use this for "How much cash do I have?" or before calling ``set_cash_balance``.
    """
    from pfm.cash_manual import (
        CashSourceAmbiguousError,
        CashSourceNotFoundError,
        get_cash_balance_view,
    )

    repo = _ctx_repo(ctx)
    db_path = _ctx_db_path(ctx)
    try:
        view = await get_cash_balance_view(repo=repo, db_path=db_path, target_date=_today())
    except CashSourceNotFoundError as exc:
        return _json({"error": str(exc)})
    except CashSourceAmbiguousError as exc:
        return _json({"error": str(exc), "matches": exc.names})
    return _json(view.to_dict())


@mcp.tool()
async def set_cash_balance(
    ctx: Context[ServerSession, AppContext],
    balances: dict[str, str],
    selected_currencies: list[str] | None = None,
) -> str:
    """Upsert today's manual cash balances for selected fiat currencies.

    ``balances`` maps currency code -> non-negative decimal string (e.g. ``{"USD": "100", "EUR": "50"}``).
    ``selected_currencies`` defaults to the keys of ``balances``. Currencies previously selected
    but absent from this call get zeroed out (matches the REST PUT semantics).
    """
    from pfm.cash_manual import (
        SUPPORTED_FIAT_CURRENCIES,
        CashSourceAmbiguousError,
        CashSourceNotFoundError,
        CashValidationError,
        parse_selected_amounts,
        parse_selected_currencies,
        resolve_cash_source,
        snapshots_to_balance_dict,
        upsert_manual_cash,
    )
    from pfm.db.source_store import InvalidCredentialsError

    repo = _ctx_repo(ctx)
    db_path = _ctx_db_path(ctx)
    pricing = _ctx_pricing(ctx)

    try:
        source = await resolve_cash_source(db_path)
    except CashSourceNotFoundError as exc:
        return _json({"error": str(exc)})
    except CashSourceAmbiguousError as exc:
        return _json({"error": str(exc), "matches": exc.names})

    selected_input: object = selected_currencies if selected_currencies is not None else list(balances.keys())
    try:
        selected = parse_selected_currencies(selected_input)
        amounts = parse_selected_amounts(balances, selected)
    except CashValidationError as exc:
        return _json({"error": str(exc)})

    today = _today()
    try:
        snapshots = await upsert_manual_cash(
            repo=repo,
            pricing=pricing,
            db_path=db_path,
            source_name=source.name,
            selected_currencies=selected,
            amounts=amounts,
            today=today,
        )
    except InvalidCredentialsError as exc:
        return _json({"error": str(exc)})

    await _best_effort_broadcast("snapshot_updated")

    return _json(
        {
            "updated": True,
            "date": today.isoformat(),
            "source_name": source.name,
            "selected_currencies": selected,
            "supported_currencies": list(SUPPORTED_FIAT_CURRENCIES),
            "latest_snapshot_date": today.isoformat(),
            "balances": snapshots_to_balance_dict(snapshots),
        }
    )


class _ManualSnapshotInputError(ValueError):
    """Validation error for ``add_manual_snapshot`` input."""


def _parse_decimal_field(value: str, field: str, *, allow_negative: bool = False) -> Decimal:
    try:
        parsed = Decimal(str(value).strip())
    except (ArithmeticError, ValueError) as exc:
        msg = f"Invalid {field}: {value!r}"
        raise _ManualSnapshotInputError(msg) from exc
    if not allow_negative and parsed < 0:
        msg = f"{field} must be non-negative"
        raise _ManualSnapshotInputError(msg)
    return parsed


def _normalize_apy(apy_percent: str | None) -> Decimal:
    if apy_percent is None:
        return Decimal(0)
    apy_pct = _parse_decimal_field(apy_percent, "apy_percent", allow_negative=True)
    return apy_pct / Decimal(100)


@mcp.tool()
async def add_manual_snapshot(  # noqa: PLR0913
    ctx: Context[ServerSession, AppContext],
    source_name: str,
    asset: str,
    amount: str,
    *,
    usd_value: str | None = None,
    apy_percent: str | None = None,
    snapshot_date: str | None = None,
    raw_metadata: dict[str, object] | None = None,
) -> str:
    """Save a manual snapshot row for an existing source (assets the API can't return).

    Mirrors the ``/api/v1/ext/snapshot`` ingest path: prices ``asset`` against USD when
    ``usd_value`` is omitted. Use for browser-extension style manual ingestion of
    DeFi positions, CEX rows, etc.

    ``apy_percent`` is always interpreted as a percent string and divided by 100
    (e.g. ``"4.25"`` -> ``0.0425`` stored). Pass ``"0.8"`` for 0.8% APY.
    Omit (or pass ``None``) for non-yield-bearing positions; downstream queries
    treat ``apy == 0`` as "no yield" (same convention as auto collectors).
    """
    from pfm.db.models import Snapshot
    from pfm.db.source_store import SourceNotFoundError, SourceStore

    repo = _ctx_repo(ctx)
    pricing = _ctx_pricing(ctx)

    try:
        source = await SourceStore(_ctx_db_path(ctx)).get(source_name)
    except SourceNotFoundError:
        return _json({"error": f"Source {source_name!r} not found"})

    asset_code = asset.strip().upper()
    if not asset_code:
        return _json({"error": "asset must be non-empty"})

    try:
        amount_dec = _parse_decimal_field(amount, "amount")
        if usd_value is None:
            try:
                price = await pricing.get_price_usd(asset_code)
            except Exception as exc:  # noqa: BLE001
                return _json({"error": f"Failed to price {asset_code}: {exc}"})
            usd_dec = amount_dec * price
        else:
            usd_dec = _parse_decimal_field(usd_value, "usd_value")
            price = usd_dec / amount_dec if amount_dec != 0 else Decimal(0)
        apy = _normalize_apy(apy_percent)
    except _ManualSnapshotInputError as exc:
        return _json({"error": str(exc)})

    target_date = _parse_date(snapshot_date)

    raw: dict[str, object] = {"manual": True, "via": "mcp"}
    if raw_metadata:
        raw.update(raw_metadata)

    snapshot = Snapshot(
        date=target_date,
        source=source.type,
        source_name=source.name,
        asset=asset_code,
        amount=amount_dec,
        usd_value=usd_dec,
        price=price,
        apy=apy,
        raw_json=json.dumps(raw, default=_json_default),
    )
    await repo.save_snapshots([snapshot])
    await _best_effort_broadcast("snapshot_updated")
    return _json(
        {
            "saved": 1,
            "date": target_date.isoformat(),
            "source_name": source.name,
            "asset": asset_code,
            "amount": _dec(amount_dec),
            "usd_value": _dec(usd_dec),
            "price": _dec(price),
            "apy": _dec(apy),
        }
    )


@mcp.tool()
async def get_collect_status(
    ctx: Context[ServerSession, AppContext],
) -> str:
    """Return whether a collection cycle is in progress on the running daemon.

    Reads ``/api/v1/collect/status`` on the local daemon (default port 19274).
    Returns ``{"daemon": "unreachable"}`` if the daemon is not running.
    """
    del ctx
    from pfm.server.client import get_base_url, is_daemon_reachable

    if not is_daemon_reachable():
        return _json({"daemon": "unreachable"})

    import httpx

    try:
        async with httpx.AsyncClient(base_url=get_base_url(), timeout=5.0) as client:
            resp = await client.get("/api/v1/collect/status")
            resp.raise_for_status()
            payload: dict[str, object] = resp.json()
    except httpx.HTTPError as exc:
        return _json({"error": f"Daemon request failed: {exc}"})
    return _json(payload)


@mcp.tool()
async def trigger_collect(
    ctx: Context[ServerSession, AppContext],
    source: str | None = None,
) -> str:
    """Start a background collection cycle on the running daemon.

    ``source`` (optional) restricts collection to a single source name; otherwise all
    enabled sources are collected. Requires the daemon to be running because collection
    runs in its background pipeline. Returns immediately with status; check
    ``get_collect_status`` for completion.
    """
    del ctx
    from pfm.server.client import get_base_url, is_daemon_reachable

    if not is_daemon_reachable():
        return _json({"error": "Daemon is not reachable on 127.0.0.1:19274. Start it first."})

    import httpx

    body: dict[str, str] = {}
    if source:
        body["source"] = source

    try:
        async with httpx.AsyncClient(base_url=get_base_url(), timeout=10.0) as client:
            resp = await client.post("/api/v1/collect", json=body)

        if resp.status_code == 409:  # noqa: PLR2004
            return _json({"error": "Collection already in progress", "status": "running"})
        resp.raise_for_status()
        payload: dict[str, object] = resp.json()
    except httpx.HTTPError as exc:
        return _json({"error": f"Daemon request failed: {exc}"})
    return _json(payload)


@mcp.tool()
async def list_apy_rules(
    ctx: Context[ServerSession, AppContext],
    source_name: str,
) -> str:
    """List APY rules for a source. Only valid for sources in ``APY_RULES_TYPES``."""
    from pfm.db.apy_rules_store import ApyRulesStore, rule_to_dict
    from pfm.db.source_store import SourceNotFoundError, SourceStore
    from pfm.source_types import APY_RULES_TYPES

    db_path = _ctx_db_path(ctx)
    try:
        source = await SourceStore(db_path).get(source_name)
    except SourceNotFoundError:
        return _json({"error": f"Source {source_name!r} not found"})
    if source.type not in APY_RULES_TYPES:
        return _json({"error": f"APY rules are not supported for {source.type!r} sources"})

    rules = await ApyRulesStore(db_path).load_rules(source_name)
    return _json({"source_name": source_name, "rules": [rule_to_dict(r) for r in rules]})


@mcp.tool()
async def create_apy_rule(
    ctx: Context[ServerSession, AppContext],
    source_name: str,
    rule: dict[str, object],
) -> str:
    """Create an APY rule for a source.

    ``rule`` shape: ``{"protocol": "aave", "coin": "usdc", "type": "base"|"bonus",
    "limits": [{"from_amount": "0", "to_amount": "10000", "apy": "0.05"}, ...],
    "started_at": "YYYY-MM-DD", "finished_at": "YYYY-MM-DD"}``. APY values are decimal
    fractions (0.05 = 5%).
    """
    from pfm.db.apy_rules_store import ApyRulesStore, ApyRuleValidationError, rule_to_dict
    from pfm.db.source_store import SourceNotFoundError, SourceStore
    from pfm.source_types import APY_RULES_TYPES

    db_path = _ctx_db_path(ctx)
    try:
        source = await SourceStore(db_path).get(source_name)
    except SourceNotFoundError:
        return _json({"error": f"Source {source_name!r} not found"})
    if source.type not in APY_RULES_TYPES:
        return _json({"error": f"APY rules are not supported for {source.type!r} sources"})

    try:
        rules = await ApyRulesStore(db_path).add_rule(source_name, dict(rule))
    except ApyRuleValidationError as exc:
        return _json({"error": str(exc)})
    return _json({"source_name": source_name, "rules": [rule_to_dict(r) for r in rules]})


@mcp.tool()
async def update_apy_rule(
    ctx: Context[ServerSession, AppContext],
    source_name: str,
    rule_id: str,
    rule: dict[str, object],
) -> str:
    """Replace an APY rule by id. Same ``rule`` shape as ``create_apy_rule``."""
    from pfm.db.apy_rules_store import (
        ApyRuleNotFoundError,
        ApyRulesStore,
        ApyRuleValidationError,
        rule_to_dict,
    )
    from pfm.db.source_store import SourceNotFoundError, SourceStore
    from pfm.source_types import APY_RULES_TYPES

    db_path = _ctx_db_path(ctx)
    try:
        source = await SourceStore(db_path).get(source_name)
    except SourceNotFoundError:
        return _json({"error": f"Source {source_name!r} not found"})
    if source.type not in APY_RULES_TYPES:
        return _json({"error": f"APY rules are not supported for {source.type!r} sources"})

    try:
        rules = await ApyRulesStore(db_path).update_rule(source_name, rule_id, dict(rule))
    except ApyRuleNotFoundError:
        return _json({"error": f"Rule {rule_id!r} not found"})
    except ApyRuleValidationError as exc:
        return _json({"error": str(exc)})
    return _json({"source_name": source_name, "rules": [rule_to_dict(r) for r in rules]})


@mcp.tool()
async def delete_apy_rule(
    ctx: Context[ServerSession, AppContext],
    source_name: str,
    rule_id: str,
) -> str:
    """Delete an APY rule by id."""
    from pfm.db.apy_rules_store import ApyRuleNotFoundError, ApyRulesStore, rule_to_dict
    from pfm.db.source_store import SourceNotFoundError, SourceStore
    from pfm.source_types import APY_RULES_TYPES

    db_path = _ctx_db_path(ctx)
    try:
        source = await SourceStore(db_path).get(source_name)
    except SourceNotFoundError:
        return _json({"error": f"Source {source_name!r} not found"})
    if source.type not in APY_RULES_TYPES:
        return _json({"error": f"APY rules are not supported for {source.type!r} sources"})

    try:
        rules = await ApyRulesStore(db_path).delete_rule(source_name, rule_id)
    except ApyRuleNotFoundError:
        return _json({"error": f"Rule {rule_id!r} not found"})
    return _json({"source_name": source_name, "rules": [rule_to_dict(r) for r in rules]})


@mcp.tool()
async def list_earn_overrides(
    ctx: Context[ServerSession, AppContext],
    source_name: str,
) -> str:
    """List earn overrides (manual APR / settlement) for a source."""
    from pfm.db.earn_override_store import EarnOverrideStore

    overrides = await EarnOverrideStore(_ctx_db_path(ctx)).load(source_name)
    return _json({"source_name": source_name, "overrides": overrides})


@mcp.tool()
async def set_earn_overrides(
    ctx: Context[ServerSession, AppContext],
    source_name: str,
    overrides: list[dict[str, str]],
) -> str:
    """Replace all earn overrides for a source.

    Each override needs ``category`` and ``coin``. Optional fields:
    ``apr`` (decimal fraction string, e.g. ``"0.05"`` for 5%, non-negative)
    and ``settlement_at`` (ISO date ``YYYY-MM-DD``).
    """
    from datetime import date as _date

    from pfm.db.earn_override_store import EarnOverrideStore

    for index, override in enumerate(overrides):
        if not override.get("category") or not override.get("coin"):
            return _json({"error": f"override[{index}]: must have category and coin"})
        apr = override.get("apr")
        if apr not in (None, ""):
            try:
                apr_dec = Decimal(str(apr).strip())
            except ArithmeticError:
                return _json({"error": f"override[{index}]: invalid apr {apr!r}"})
            if not apr_dec.is_finite() or apr_dec < 0:
                return _json({"error": f"override[{index}]: apr must be finite and non-negative"})
        settlement_at = override.get("settlement_at")
        if settlement_at not in (None, ""):
            try:
                _date.fromisoformat(str(settlement_at))
            except ValueError:
                return _json({"error": f"override[{index}]: settlement_at must be ISO date (YYYY-MM-DD)"})

    try:
        await EarnOverrideStore(_ctx_db_path(ctx)).save(source_name, overrides)
    except Exception as exc:  # noqa: BLE001
        return _json({"error": f"Failed to save earn overrides: {exc}"})
    await _best_effort_broadcast("snapshot_updated")
    return _json({"source_name": source_name, "overrides": overrides})


@mcp.tool()
async def delete_earn_overrides(
    ctx: Context[ServerSession, AppContext],
    source_name: str,
) -> str:
    """Delete all earn overrides for a source."""
    from pfm.db.earn_override_store import EarnOverrideStore

    await EarnOverrideStore(_ctx_db_path(ctx)).save(source_name, [])
    await _best_effort_broadcast("snapshot_updated")
    return _json({"source_name": source_name, "overrides": []})


# ---------------------------------------------------------------------------
# Source CRUD
# ---------------------------------------------------------------------------


def _field_to_dict(field: object) -> dict[str, object]:
    """Serialize a CredentialField for MCP output."""
    return {
        "name": getattr(field, "name", ""),
        "prompt": getattr(field, "prompt", ""),
        "required": bool(getattr(field, "required", False)),
        "default": getattr(field, "default", ""),
        "secret": bool(getattr(field, "secret", True)),
        "tip": getattr(field, "tip", ""),
    }


@mcp.tool()
async def get_source_schema(
    ctx: Context[ServerSession, AppContext],
    source_type: str | None = None,
) -> str:
    """Return credential field schema(s) for source types.

    Without ``source_type``: dict mapping every known type → {fields, supported_apy_rules}.
    With ``source_type``: same payload for that one type, or an error if unknown.
    Use this before ``add_source`` / ``update_source`` to know what credential
    fields to collect.
    """
    del ctx
    from pfm.source_types import APY_RULES_TYPES, SOURCE_TYPES

    def _type_payload(type_name: str) -> dict[str, object]:
        return {
            "fields": [_field_to_dict(f) for f in SOURCE_TYPES[type_name]],
            "supported_apy_rules": [
                {"protocol": p.protocol, "coins": list(p.coins)} for p in APY_RULES_TYPES.get(type_name, ())
            ],
        }

    if source_type is None:
        return _json({name: _type_payload(name) for name in SOURCE_TYPES})

    if source_type not in SOURCE_TYPES:
        return _json(
            {
                "error": f"Unknown source type: {source_type!r}",
                "valid_types": sorted(SOURCE_TYPES),
            }
        )
    return _json({source_type: _type_payload(source_type)})


def _source_to_public_dict(source: object) -> dict[str, object]:
    """Public source row (no credential values)."""
    return {
        "id": getattr(source, "id", None),
        "name": getattr(source, "name", ""),
        "type": getattr(source, "type", ""),
        "enabled": bool(getattr(source, "enabled", False)),
        "created_at": getattr(source, "created_at", None),
    }


@mcp.tool()
async def add_source(
    ctx: Context[ServerSession, AppContext],
    name: str,
    source_type: str,
    credentials: dict[str, str],
) -> str:
    """Add a new data source. Triggers a best-effort single-source collect on success.

    ``credentials`` shape depends on ``source_type`` — call ``get_source_schema``
    first to learn the required fields. Errors:

    - unknown source type → ``{"error": "...", "valid_types": [...]}``
    - missing required fields → ``{"error": "Missing required field: ..."}``
    - duplicate name (or duplicate cash source) → ``{"error": "...already exists"}``
    """
    from pfm.db.source_store import (
        DuplicateSourceError,
        InvalidCredentialsError,
        InvalidSourceTypeError,
        SourceStore,
    )
    from pfm.source_types import SOURCE_TYPES

    if not name or not name.strip():
        return _json({"error": "name must not be empty"})
    if source_type not in SOURCE_TYPES:
        return _json(
            {
                "error": f"Unknown source type: {source_type!r}",
                "valid_types": sorted(SOURCE_TYPES),
            }
        )

    store = SourceStore(_ctx_db_path(ctx))
    try:
        source = await store.add(name.strip(), source_type, credentials)
    except InvalidSourceTypeError as exc:
        return _json({"error": str(exc), "valid_types": sorted(SOURCE_TYPES)})
    except InvalidCredentialsError as exc:
        return _json({"error": str(exc)})
    except DuplicateSourceError as exc:
        return _json({"error": str(exc)})

    if source.enabled:
        collect_status: dict[str, object] = await _best_effort_collect(source.name)
    else:
        collect_status = {"collect": "skipped", "reason": "source disabled"}
    await _best_effort_broadcast("sources_changed")

    return _json({"added": True, "source": _source_to_public_dict(source), "auto_refresh": collect_status})


@mcp.tool()
async def update_source(
    ctx: Context[ServerSession, AppContext],
    name: str,
    *,
    new_name: str | None = None,
    credentials: dict[str, str] | None = None,
    enabled: bool | None = None,
) -> str:
    """Update a source. Any of ``new_name`` / ``credentials`` / ``enabled`` may be set.

    ``credentials`` is partial-merged into the existing dict — pass only fields
    that change. The source ``type`` is immutable; create a new source if the
    type needs to change. Renames preserve historical transactions/snapshots
    (FK is on ``source_id``, not name).
    """
    from pfm.db.source_store import (
        DuplicateSourceError,
        InvalidCredentialsError,
        SourceNotFoundError,
        SourceStore,
    )

    if new_name is None and credentials is None and enabled is None:
        return _json({"error": "at least one of new_name, credentials, enabled must be provided"})

    store = SourceStore(_ctx_db_path(ctx))
    try:
        source = await store.update(
            name,
            new_name=new_name,
            credentials=credentials,
            enabled=enabled,
        )
    except SourceNotFoundError:
        return _json({"error": f"Source {name!r} not found"})
    except InvalidCredentialsError as exc:
        return _json({"error": str(exc)})
    except DuplicateSourceError as exc:
        return _json({"error": str(exc)})

    await _best_effort_broadcast("sources_changed")
    return _json({"updated": True, "source": _source_to_public_dict(source)})


@mcp.tool()
async def delete_source(
    ctx: Context[ServerSession, AppContext],
    name: str,
    *,
    cascade: bool = False,
) -> str:
    """Delete a source.

    Refuses by default if the source has any transactions or snapshots. Pass
    ``cascade=true`` to also delete all dependent rows (transactions, snapshots,
    analytics_cache for affected dates, APY rules, earn overrides) in one
    transaction. Cascade is destructive and irreversible.
    """
    from pfm.db.source_store import SourceNotFoundError, SourceStore

    repo = _ctx_repo(ctx)
    db_path = _ctx_db_path(ctx)

    try:
        await SourceStore(db_path).get(name)
    except SourceNotFoundError:
        return _json({"error": f"Source {name!r} not found"})

    if not cascade:
        sources = await repo.list_sources_with_counts()
        target = next((s for s in sources if s.get("name") == name), None)
        tx_raw = target.get("tx_count") if target else None
        snap_raw = target.get("snap_count") if target else None
        tx_count = int(tx_raw) if isinstance(tx_raw, int) else 0
        snap_count = int(snap_raw) if isinstance(snap_raw, int) else 0
        if tx_count > 0 or snap_count > 0:
            return _json(
                {
                    "error": (
                        f"Source {name!r} has {tx_count} transaction(s) and {snap_count} snapshot(s). "
                        "Pass cascade=true to delete the source and all its data."
                    ),
                    "tx_count": tx_count,
                    "snap_count": snap_count,
                }
            )

    try:
        result = await repo.delete_source_cascade(name)
    except SourceNotFoundError:
        return _json({"error": f"Source {name!r} not found"})

    await _best_effort_broadcast("sources_changed")
    await _best_effort_broadcast("snapshot_updated")
    return _json(
        {
            "deleted": True,
            "name": result.name,
            "removed": {
                "snapshots": result.snapshots,
                "transactions": result.transactions,
                "analytics_metrics": result.analytics_metrics,
                "apy_rules": result.apy_rules,
            },
        }
    )


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
