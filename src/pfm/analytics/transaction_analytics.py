"""Transaction analytics: spending by category, income by source, monthly trends."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from pfm.db.models import TransactionType
from pfm.server.price_resolver import build_price_map, resolve_usd
from pfm.server.serializers import _str_decimal

if TYPE_CHECKING:
    from pfm.db.metadata_store import MetadataStore
    from pfm.db.repository import Repository


_SPENDING_TYPES = frozenset({TransactionType.SPEND, TransactionType.FEE})
_INCOME_TYPES = frozenset({TransactionType.YIELD})
_INCOME_CATEGORY = "income"


async def compute_analytics_summary(
    repo: Repository,
    store: MetadataStore,
    start: date,
    end: date,
) -> dict[str, object]:
    """Compute spending/income by category for a date range."""
    txs = await repo.get_transactions(start=start, end=end)
    tx_ids = [tx.id for tx in txs if tx.id is not None]
    metadata_map = await store.get_metadata_batch(tx_ids)

    dates = list({tx.date for tx in txs})
    prices = await build_price_map(repo, dates)

    spending_by_category: dict[str, Decimal] = defaultdict(Decimal)
    income_by_category: dict[str, Decimal] = defaultdict(Decimal)
    total_spending = Decimal(0)
    total_income = Decimal(0)

    for tx in txs:
        if tx.id is None:
            continue
        meta = metadata_map.get(tx.id)

        # Skip internal transfers.
        if meta and meta.is_internal_transfer:
            continue

        category = meta.category if meta else None
        if not category:
            category = f"uncategorized_{tx.tx_type.value}"

        usd = resolve_usd(tx, prices)
        if tx.tx_type in _SPENDING_TYPES:
            spending_by_category[category] += usd
            total_spending += usd
        elif tx.tx_type in _INCOME_TYPES or (tx.tx_type == TransactionType.DEPOSIT and category == _INCOME_CATEGORY):
            income_by_category[category] += usd
            total_income += usd

    # Get category display names.
    categories = await store.get_categories()
    display_map = {cat.category: cat.display_name for cat in categories}

    spending_items = sorted(spending_by_category.items(), key=lambda item: item[1], reverse=True)
    income_items = sorted(income_by_category.items(), key=lambda item: item[1], reverse=True)

    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "total_spending": _str_decimal(total_spending),
        "total_income": _str_decimal(total_income),
        "spending_by_category": [
            {
                "category": cat,
                "display_name": display_map.get(cat, cat),
                "usd_value": _str_decimal(value),
                "percentage": _str_decimal(value / total_spending * 100 if total_spending else Decimal(0)),
            }
            for cat, value in spending_items
        ],
        "income_by_category": [
            {
                "category": cat,
                "display_name": display_map.get(cat, cat),
                "usd_value": _str_decimal(value),
                "percentage": _str_decimal(value / total_income * 100 if total_income else Decimal(0)),
            }
            for cat, value in income_items
        ],
    }


async def compute_monthly_trends(
    repo: Repository,
    store: MetadataStore,
    months: int = 6,
) -> dict[str, object]:
    """Compute monthly category breakdown for the last N months."""
    end = datetime.now(tz=UTC).date()
    start = end - timedelta(days=months * 31)

    txs = await repo.get_transactions(start=start, end=end)
    tx_ids = [tx.id for tx in txs if tx.id is not None]
    metadata_map = await store.get_metadata_batch(tx_ids)

    dates = list({tx.date for tx in txs})
    prices = await build_price_map(repo, dates)

    # Group by month.
    monthly: dict[str, dict[str, Decimal]] = defaultdict(lambda: defaultdict(Decimal))

    for tx in txs:
        if tx.id is None:
            continue
        meta = metadata_map.get(tx.id)
        if meta and meta.is_internal_transfer:
            continue

        month_key = tx.date.strftime("%Y-%m")
        category: str = meta.category if meta and meta.category else f"uncategorized_{tx.tx_type.value}"

        if tx.tx_type in _SPENDING_TYPES:
            monthly[month_key][category] += resolve_usd(tx, prices)

    # Get category display names.
    categories = await store.get_categories()
    display_map = {cat.category: cat.display_name for cat in categories}

    months_list = sorted(monthly.keys())
    return {
        "months": [
            {
                "month": month,
                "categories": [
                    {
                        "category": cat,
                        "display_name": display_map.get(cat, cat),
                        "usd_value": _str_decimal(value),
                    }
                    for cat, value in sorted(cats.items(), key=lambda item: item[1], reverse=True)
                ],
                "total": _str_decimal(sum(cats.values(), Decimal(0))),
            }
            for month, cats in ((m, monthly[m]) for m in months_list)
        ],
    }
