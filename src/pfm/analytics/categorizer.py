"""Rule-based transaction categorization pipeline."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING

from pfm.db.models import effective_type

if TYPE_CHECKING:
    from pfm.db.models import CategoryRule, Transaction, TransactionMetadata


# ── Value parsing ──────────────────────────────────────────────────────


def _parse_values(rule_val: str) -> list[str]:
    """Parse a rule value into a list. JSON arrays are expanded; plain strings become [val]."""
    if rule_val.startswith("["):
        try:
            parsed = json.loads(rule_val)
            if isinstance(parsed, list):
                return [str(v) for v in parsed]
        except (json.JSONDecodeError, TypeError):
            pass
    return [rule_val]


@lru_cache(maxsize=512)
def _compile_regex(pattern: str) -> re.Pattern[str] | None:
    """Compile and cache a regex pattern. Returns None for invalid patterns."""
    try:
        return re.compile(pattern)
    except re.error:
        return None


def _match_values(field_val: str, rule_val: str, operator: str) -> bool:
    """Compare a field value against a rule value using the given operator."""
    values = _parse_values(rule_val)
    if operator == "eq":
        return field_val in values
    if operator == "contains":
        return any(v.lower() in field_val.lower() for v in values)
    if operator == "regex":
        for v in values:
            compiled = _compile_regex(v)
            if compiled is not None and compiled.search(field_val):
                return True
        return False
    return False


# ── Result ─────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class CategoryResult:
    """Result of categorizing a single transaction."""

    category: str
    source: str  # 'rule'
    confidence: float
    rule_id: int | None = None


# ── Field resolution ───────────────────────────────────────────────────


def _extract_description(tx: Transaction) -> str:
    """Extract description from raw_json if present."""
    if not tx.raw_json:
        return ""
    try:
        parsed = json.loads(tx.raw_json)
        if isinstance(parsed, dict):
            return str(parsed.get("description", ""))
    except (json.JSONDecodeError, TypeError):
        pass
    return ""


def _resolve_field(tx: Transaction, field_name: str) -> str | None:
    """Extract a field value for rule matching."""
    if field_name == "asset":
        return tx.asset.upper()
    if field_name == "description":
        return _extract_description(tx) or None
    # Any other field: look up in raw_json.
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


# ── Compound rule matching ─────────────────────────────────────────────


def _match_category_rule(  # noqa: PLR0911
    etype: str,
    tx: Transaction,
    rule: CategoryRule,
) -> bool:
    """Check whether a compound category rule matches."""
    if rule.deleted:
        return False

    # Condition 1: type match (required).
    if not _match_values(etype, rule.type_match, rule.type_operator):
        return False

    # Source filter — XOR semantics:
    # source_id set → match only that specific source instance;
    # source_type set → match every transaction of that type;
    # both None → catch-all.
    if rule.source_id is not None and rule.source_id != tx.source_id:
        return False
    if rule.source_type is not None and rule.source_type != tx.source:
        return False

    # Condition 2: field match (optional).
    if rule.field_name:
        field_val = _resolve_field(tx, rule.field_name)
        if field_val is None:
            return False
        if not _match_values(field_val, rule.field_value, rule.field_operator):
            return False

    return True


# ── Main pipeline ──────────────────────────────────────────────────────


def categorize_transaction(
    tx: Transaction,
    rules: list[CategoryRule],
    meta: TransactionMetadata | None = None,
) -> CategoryResult | None:
    """Categorize a single transaction via DB rules.

    Rules are evaluated in priority order (ascending). First match wins.
    Returns None when no rule matches.
    """
    etype = effective_type(tx, meta)

    for rule in rules:
        if _match_category_rule(etype, tx, rule):
            confidence = 0.90 if rule.builtin else 0.95
            return CategoryResult(
                category=rule.result_category,
                source="rule",
                confidence=confidence,
                rule_id=rule.id,
            )

    return None


def categorize_batch(
    txs: list[Transaction],
    rules: list[CategoryRule],
    meta_map: dict[int, TransactionMetadata] | None = None,
) -> list[tuple[Transaction, CategoryResult | None]]:
    """Categorize a batch of transactions."""
    meta_map = meta_map or {}
    return [
        (tx, categorize_transaction(tx, rules, meta_map.get(tx.id)))  # type: ignore[arg-type]
        for tx in txs
    ]
