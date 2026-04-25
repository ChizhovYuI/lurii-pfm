"""Audit existing rules — count matches and wins per rule against current data.

Used by skill auditing flow to surface dead rules (matched_count == 0)
and shadowed-dead rules (matched > 0 but winning == 0). Pure read; no DB
writes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pfm.analytics.categorizer import _match_category_rule, categorize_transaction
from pfm.analytics.type_resolver import match_type_rule, resolve_type_winner
from pfm.db.models import effective_type

if TYPE_CHECKING:
    from pfm.db.metadata_store import MetadataStore
    from pfm.db.repository import Repository


async def audit_category_rules(
    repo: Repository,
    store: MetadataStore,
    *,
    source_type: str | None = None,
    source_id: int | None = None,
    scope_source: str | None = None,
) -> dict[str, object]:
    """Count isolation-matches and post-priority wins per category rule.

    - ``source_type`` / ``source_id``: filter the rules under audit.
    - ``scope_source``: filter the transactions evaluated by ``sources.name``.

    Returns ``{rules, dead, shadowed_dead}`` sorted by ``matched_count`` asc.
    """
    rules = await store.get_category_rules(source_type=source_type, source_id=source_id)
    rules = [r for r in rules if not r.deleted and r.id is not None]
    txs = await repo.get_transactions(source_name=scope_source)
    meta_map = await store.get_metadata_batch([tx.id for tx in txs if tx.id is not None])

    matched: dict[int, int] = {r.id: 0 for r in rules if r.id is not None}
    winning: dict[int, int] = {r.id: 0 for r in rules if r.id is not None}

    for tx in txs:
        meta = meta_map.get(tx.id) if tx.id is not None else None
        etype = effective_type(tx, meta)
        for rule in rules:
            if rule.id is None:
                continue
            if _match_category_rule(etype, tx, rule):
                matched[rule.id] += 1
        winner = categorize_transaction(tx, rules, meta)
        if winner is not None and winner.rule_id is not None and winner.rule_id in winning:
            winning[winner.rule_id] += 1

    sorted_rules = sorted(
        (r for r in rules if r.id is not None),
        key=lambda r: (matched.get(r.id or 0, 0), r.id or 0),
    )
    rule_rows: list[dict[str, object]] = [
        {
            "id": rule.id,
            "priority": rule.priority,
            "source_type": rule.source_type,
            "source_id": rule.source_id,
            "type_match": rule.type_match,
            "field_name": rule.field_name,
            "field_value": rule.field_value,
            "result_category": rule.result_category,
            "matched_count": matched[rule.id] if rule.id is not None else 0,
            "winning_count": winning[rule.id] if rule.id is not None else 0,
            "builtin": rule.builtin,
        }
        for rule in sorted_rules
    ]

    dead = [rid for rid, count in matched.items() if count == 0]
    shadowed_dead = [rid for rid, count in matched.items() if count > 0 and winning.get(rid, 0) == 0]
    return {
        "rules": rule_rows,
        "dead": dead,
        "shadowed_dead": shadowed_dead,
    }


async def audit_type_rules(
    repo: Repository,
    store: MetadataStore,
    *,
    source_type: str | None = None,
    source_id: int | None = None,
    scope_source: str | None = None,
) -> dict[str, object]:
    """Count isolation-matches and post-priority wins per type rule.

    Same shape as :func:`audit_category_rules`; ``rules`` rows carry
    ``result_type`` instead of ``result_category``.
    """
    rules = await store.get_type_rules(source_type=source_type, source_id=source_id)
    rules = [r for r in rules if not r.deleted and r.id is not None]
    txs = await repo.get_transactions(source_name=scope_source)

    matched: dict[int, int] = {r.id: 0 for r in rules if r.id is not None}
    winning: dict[int, int] = {r.id: 0 for r in rules if r.id is not None}

    for tx in txs:
        for rule in rules:
            if rule.id is None:
                continue
            if match_type_rule(tx, rule):
                matched[rule.id] += 1
        winner = resolve_type_winner(tx, rules)
        if winner is not None and winner.id is not None and winner.id in winning:
            winning[winner.id] += 1

    sorted_rules = sorted(
        (r for r in rules if r.id is not None),
        key=lambda r: (matched.get(r.id or 0, 0), r.id or 0),
    )
    rule_rows: list[dict[str, object]] = [
        {
            "id": rule.id,
            "priority": rule.priority,
            "source_type": rule.source_type,
            "source_id": rule.source_id,
            "field_name": rule.field_name,
            "field_value": rule.field_value,
            "result_type": rule.result_type,
            "matched_count": matched[rule.id] if rule.id is not None else 0,
            "winning_count": winning[rule.id] if rule.id is not None else 0,
            "builtin": rule.builtin,
        }
        for rule in sorted_rules
    ]

    dead = [rid for rid, count in matched.items() if count == 0]
    shadowed_dead = [rid for rid, count in matched.items() if count > 0 and winning.get(rid, 0) == 0]
    return {
        "rules": rule_rows,
        "dead": dead,
        "shadowed_dead": shadowed_dead,
    }
