"""Simulate applying a rule over candidate transactions without persisting.

Returns matched/unchanged/changed buckets, overlapping existing rules, and
field-value samples so a Claude Code skill can author rules safely.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pfm.analytics.categorizer import (
    _match_category_rule,
    _resolve_field,
    categorize_transaction,
)
from pfm.analytics.type_resolver import _resolve_raw_field, match_type_rule, resolve_type_winner
from pfm.db.metadata_store import _validate_regex_value
from pfm.db.models import CategoryRule, TypeRule, effective_type

if TYPE_CHECKING:
    from pfm.db.metadata_store import MetadataStore
    from pfm.db.models import Transaction, TransactionMetadata
    from pfm.db.repository import Repository


_SAMPLE_LIMIT = 5
_SAMPLE_TRUNCATE = 200
_TIE_LAST = float("inf")


def _rule_sort_key(rule: CategoryRule | TypeRule) -> tuple[int, float]:
    """Sort rules like the engine: priority ASC, id ASC. Candidate (id is None) ties last."""
    return (rule.priority, float(rule.id) if rule.id is not None else _TIE_LAST)


async def dry_run_category_rule(  # noqa: PLR0913, C901
    repo: Repository,
    store: MetadataStore,
    *,
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
) -> dict[str, object]:
    """Simulate applying a category rule. No DB writes."""
    if field_operator == "regex" and field_value:
        _validate_regex_value(field_value)

    candidate = CategoryRule(
        type_match=type_match,
        result_category=result_category,
        type_operator=type_operator,
        field_name=field_name,
        field_operator=field_operator,
        field_value=field_value,
        source=source,
        priority=priority if priority is not None else 300,
    )

    txs = (await repo.get_transactions(source_name=scope_source))[:limit]
    existing = await store.get_category_rules()
    existing = [r for r in existing if r.id != candidate.id]
    meta_map = await store.get_metadata_batch([tx.id for tx in txs if tx.id is not None])

    matched: list[tuple[Transaction, TransactionMetadata | None]] = []
    for tx in txs:
        meta = meta_map.get(tx.id) if tx.id is not None else None
        etype = effective_type(tx, meta)
        if _match_category_rule(etype, tx, candidate):
            matched.append((tx, meta))

    combined = sorted([*existing, candidate], key=_rule_sort_key)

    unchanged: list[dict[str, object]] = []
    changed: list[dict[str, object]] = []
    shadowed: list[dict[str, object]] = []
    overlapping_ids: dict[int, CategoryRule] = {}
    for tx, meta in matched:
        current = meta.category if meta else None
        winner = categorize_transaction(tx, combined, meta)
        candidate_won = winner is not None and winner.rule_id is None
        if candidate_won:
            entry: dict[str, object] = {"tx_id": tx.tx_id, "current_category": current}
            if current == result_category:
                unchanged.append(entry)
            else:
                entry["proposed_category"] = result_category
                changed.append(entry)
        else:
            wr = next(
                (r for r in existing if winner is not None and r.id == winner.rule_id),
                None,
            )
            shadowed.append(
                {
                    "tx_id": tx.tx_id,
                    "current_category": current,
                    "winning_rule_id": wr.id if wr is not None else None,
                    "winning_priority": wr.priority if wr is not None else None,
                    "winning_category": wr.result_category if wr is not None else None,
                },
            )
            if wr is not None and wr.id is not None:
                overlapping_ids.setdefault(wr.id, wr)

    for tx, meta in matched:
        winner = categorize_transaction(tx, existing, meta)
        if winner is None or winner.rule_id is None:
            continue
        rule = next((r for r in existing if r.id == winner.rule_id), None)
        if rule is not None and rule.id is not None:
            overlapping_ids.setdefault(rule.id, rule)

    overlapping_rules = [
        {
            "id": r.id,
            "field_name": r.field_name,
            "field_value": r.field_value,
            "result_category": r.result_category,
            "priority": r.priority,
        }
        for r in sorted(overlapping_ids.values(), key=lambda r: (r.priority, r.id or 0))
    ]

    raw_field_samples = _collect_samples(matched, field_name, _resolve_field)

    return {
        "matched": len(matched),
        "unchanged": unchanged,
        "changed": changed,
        "shadowed_by_higher": shadowed,
        "overlapping_rules": overlapping_rules,
        "raw_field_samples": raw_field_samples,
    }


async def dry_run_type_rule(  # noqa: PLR0913, C901, PLR0912
    repo: Repository,
    store: MetadataStore,
    *,
    result_type: str,
    source: str = "*",
    field_name: str = "",
    field_operator: str = "eq",
    field_value: str = "",
    priority: int | None = None,
    scope_source: str | None = None,
    limit: int = 200,
) -> dict[str, object]:
    """Simulate applying a type rule. No DB writes."""
    if field_operator == "regex" and field_value:
        _validate_regex_value(field_value)

    candidate = TypeRule(
        source=source,
        field_name=field_name,
        field_operator=field_operator,
        field_value=field_value,
        result_type=result_type,
        priority=priority if priority is not None else 100,
    )

    txs = (await repo.get_transactions(source_name=scope_source))[:limit]
    existing = await store.get_type_rules()
    existing = [r for r in existing if r.id != candidate.id]
    meta_map = await store.get_metadata_batch([tx.id for tx in txs if tx.id is not None])

    matched: list[tuple[Transaction, TransactionMetadata | None]] = []
    for tx in txs:
        meta = meta_map.get(tx.id) if tx.id is not None else None
        if match_type_rule(tx, candidate):
            matched.append((tx, meta))

    combined = sorted([*existing, candidate], key=_rule_sort_key)

    unchanged: list[dict[str, object]] = []
    changed: list[dict[str, object]] = []
    shadowed: list[dict[str, object]] = []
    overlapping_ids: dict[int, TypeRule] = {}
    for tx, meta in matched:
        current_override = meta.type_override if meta else None
        current_type = effective_type(tx, meta)
        winner = resolve_type_winner(tx, combined)
        candidate_won = winner is not None and winner.id is None
        if candidate_won:
            entry: dict[str, object] = {
                "tx_id": tx.tx_id,
                "current_type": current_type,
                "current_type_override": current_override,
            }
            if current_type == result_type:
                unchanged.append(entry)
            else:
                entry["proposed_type"] = result_type
                changed.append(entry)
        else:
            shadowed.append(
                {
                    "tx_id": tx.tx_id,
                    "current_type": current_type,
                    "current_type_override": current_override,
                    "winning_rule_id": winner.id if winner is not None else None,
                    "winning_priority": winner.priority if winner is not None else None,
                    "winning_type": winner.result_type if winner is not None else None,
                },
            )
            if winner is not None and winner.id is not None:
                overlapping_ids.setdefault(winner.id, winner)

    for tx, _ in matched:
        for rule in existing:
            if match_type_rule(tx, rule):
                if rule.id is not None:
                    overlapping_ids.setdefault(rule.id, rule)
                break

    overlapping_rules = [
        {
            "id": r.id,
            "field_name": r.field_name,
            "field_value": r.field_value,
            "result_type": r.result_type,
            "priority": r.priority,
        }
        for r in sorted(overlapping_ids.values(), key=lambda r: (r.priority, r.id or 0))
    ]

    raw_field_samples = _collect_samples(matched, field_name, _resolve_raw_field)

    return {
        "matched": len(matched),
        "unchanged": unchanged,
        "changed": changed,
        "shadowed_by_higher": shadowed,
        "overlapping_rules": overlapping_rules,
        "raw_field_samples": raw_field_samples,
    }


def _collect_samples(
    matched: list[tuple[Transaction, TransactionMetadata | None]],
    field_name: str,
    resolver: object,
) -> list[str]:
    """Collect up to N distinct truncated field values from matched transactions."""
    if not field_name:
        return []
    samples: list[str] = []
    seen: set[str] = set()
    for tx, _ in matched:
        val = resolver(tx, field_name)  # type: ignore[operator]
        if val is None:
            continue
        truncated = str(val)[:_SAMPLE_TRUNCATE]
        if truncated in seen:
            continue
        seen.add(truncated)
        samples.append(truncated)
        if len(samples) >= _SAMPLE_LIMIT:
            break
    return samples
