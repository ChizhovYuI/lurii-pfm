"""Type resolution from DB rules for transactions with unknown type."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from pfm.analytics.categorizer import _match_values
from pfm.db.models import TransactionType

if TYPE_CHECKING:
    from pfm.db.models import Transaction, TypeRule


def resolve_type(tx: Transaction, rules: list[TypeRule]) -> TransactionType | None:
    """Resolve the type for a single transaction using ordered rules.

    Rules are evaluated in priority order (ascending). First match wins.
    """
    for rule in rules:
        if rule.deleted:
            continue
        if match_type_rule(tx, rule):
            try:
                return TransactionType(rule.result_type)
            except ValueError:
                continue
    return None


def resolve_type_winner(tx: Transaction, rules: list[TypeRule]) -> TypeRule | None:
    """Mirror :func:`resolve_type` but return the winning rule (not its enum).

    Useful when callers need to surface the rule that produced a tx's
    type — e.g. dry-run shadow detection or transaction inspection.
    """
    for rule in rules:
        if rule.deleted:
            continue
        if match_type_rule(tx, rule):
            return rule
    return None


def resolve_type_batch(
    txs: list[Transaction],
    rules: list[TypeRule],
) -> list[tuple[int, TransactionType]]:
    """Resolve types for a batch of transactions.

    Returns (tx.id, resolved_type) pairs.
    """
    updates: list[tuple[int, TransactionType]] = []
    for tx in txs:
        if tx.id is None:
            continue
        result = resolve_type(tx, rules)
        if result is not None:
            updates.append((tx.id, result))
    return updates


def match_type_rule(tx: Transaction, rule: TypeRule) -> bool:
    """Check whether a type rule matches a transaction."""
    # Source filter — XOR semantics: see :func:`_match_category_rule`.
    if rule.source_id is not None and rule.source_id != tx.source_id:
        return False
    if rule.source_type is not None and rule.source_type != tx.source:
        return False

    # Field match (optional — rules with no field_name are source-only fallbacks).
    if rule.field_name:
        field_val = _resolve_raw_field(tx, rule.field_name)
        if field_val is None:
            return False
        if not _match_values(field_val, rule.field_value, rule.field_operator):
            return False

    return True


def _resolve_raw_field(tx: Transaction, field_name: str) -> str | None:
    """Extract a field value from raw_json."""
    if not tx.raw_json:
        return None
    try:
        parsed = json.loads(tx.raw_json)
        if isinstance(parsed, dict):
            val = parsed.get(field_name)
            return str(val) if val is not None else None
    except (json.JSONDecodeError, TypeError):
        pass
    return None
