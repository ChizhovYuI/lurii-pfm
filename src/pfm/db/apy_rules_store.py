"""APY rules storage and computation for yield-bearing positions."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

import aiosqlite

if TYPE_CHECKING:
    from pathlib import Path

KNOWN_PROTOCOLS: frozenset[str] = frozenset({"aave"})
KNOWN_COINS: frozenset[str] = frozenset({"usdc", "usdt"})
KNOWN_TYPES: frozenset[str] = frozenset({"base", "bonus"})


class ApyRuleError(Exception):
    """Base error for APY rule operations."""


class ApyRuleNotFoundError(ApyRuleError):
    """Raised when a rule ID does not exist."""


class ApyRuleValidationError(ApyRuleError):
    """Raised when rule data is invalid."""


@dataclass(frozen=True, slots=True)
class RuleLimit:
    """A single APY bracket within a rule.

    from_amount: exclusive lower bound.
    to_amount: inclusive upper bound (None = infinity).
    apy: decimal fraction (0.10 = 10%).
    """

    from_amount: Decimal
    to_amount: Decimal | None
    apy: Decimal


@dataclass(frozen=True, slots=True)
class ApyRule:
    """A user-configurable APY override/supplement rule."""

    id: str
    protocol: str
    coin: str
    type: str  # "base" | "bonus"
    limits: tuple[RuleLimit, ...]
    started_at: date
    finished_at: date


def compute_effective_apy(  # noqa: PLR0913
    protocol_apy: Decimal,
    rules: list[ApyRule],
    protocol: str,
    coin: str,
    amount: Decimal,
    snapshot_date: date,
) -> Decimal:
    """Compute effective APY by applying matching rules to the protocol APY.

    For "base" rules: replaces the protocol APY with the bracket APY.
    For "bonus" rules: adds the bracket APY on top.
    """
    matching = [
        r for r in rules if r.protocol == protocol and r.coin == coin and r.started_at <= snapshot_date <= r.finished_at
    ]

    base = protocol_apy
    for rule in matching:
        if rule.type != "base":
            continue
        bracket = _find_bracket(rule.limits, amount)
        if bracket is not None:
            base = bracket.apy

    bonus = Decimal(0)
    for rule in matching:
        if rule.type != "bonus":
            continue
        bracket = _find_bracket(rule.limits, amount)
        if bracket is not None:
            bonus += bracket.apy

    return base + bonus


def _find_bracket(limits: tuple[RuleLimit, ...], amount: Decimal) -> RuleLimit | None:
    """Find the limit bracket where from_amount < amount <= to_amount."""
    for limit in limits:
        if amount <= limit.from_amount:
            continue
        if limit.to_amount is not None and amount > limit.to_amount:
            continue
        return limit
    return None


class ApyRulesStore:
    """CRUD for APY rules stored in app_settings table."""

    _KEY_PREFIX = "apy_rules:"

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)

    async def load_rules(self, source_name: str) -> list[ApyRule]:
        """Load all APY rules for a source."""
        key = self._key(source_name)
        async with aiosqlite.connect(self._db_path) as db:
            row = await (await db.execute("SELECT value FROM app_settings WHERE key = ?", (key,))).fetchone()
        if row is None:
            return []
        return _deserialize_rules(str(row[0]))

    async def save_rules(self, source_name: str, rules: list[ApyRule]) -> None:
        """Save all APY rules for a source."""
        key = self._key(source_name)
        value = _serialize_rules(rules)
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute(
                "INSERT INTO app_settings (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, "
                "updated_at = datetime('now')",
                (key, value),
            )
            await db.commit()

    async def add_rule(self, source_name: str, data: dict[str, Any]) -> list[ApyRule]:
        """Add a new APY rule. Returns the updated rules list."""
        rule = _validate_and_build(data, rule_id=str(uuid.uuid4()))
        rules = await self.load_rules(source_name)
        rules.append(rule)
        await self.save_rules(source_name, rules)
        return rules

    async def update_rule(self, source_name: str, rule_id: str, data: dict[str, Any]) -> list[ApyRule]:
        """Update an existing APY rule. Returns the updated rules list."""
        rules = await self.load_rules(source_name)
        idx = _find_rule_index(rules, rule_id)
        updated = _validate_and_build(data, rule_id=rule_id)
        rules[idx] = updated
        await self.save_rules(source_name, rules)
        return rules

    async def delete_rule(self, source_name: str, rule_id: str) -> list[ApyRule]:
        """Delete an APY rule. Returns the updated rules list."""
        rules = await self.load_rules(source_name)
        idx = _find_rule_index(rules, rule_id)
        rules.pop(idx)
        await self.save_rules(source_name, rules)
        return rules

    def _key(self, source_name: str) -> str:
        return f"{self._KEY_PREFIX}{source_name}"


def _find_rule_index(rules: list[ApyRule], rule_id: str) -> int:
    for i, rule in enumerate(rules):
        if rule.id == rule_id:
            return i
    msg = f"APY rule {rule_id!r} not found"
    raise ApyRuleNotFoundError(msg)


def _validate_and_build(data: dict[str, Any], *, rule_id: str) -> ApyRule:
    """Validate input data and build an ApyRule."""
    protocol = str(data.get("protocol", "")).lower()
    if protocol not in KNOWN_PROTOCOLS:
        msg = f"Unknown protocol: {protocol!r}. Valid: {', '.join(sorted(KNOWN_PROTOCOLS))}"
        raise ApyRuleValidationError(msg)

    coin = str(data.get("coin", "")).lower()
    if coin not in KNOWN_COINS:
        msg = f"Unknown coin: {coin!r}. Valid: {', '.join(sorted(KNOWN_COINS))}"
        raise ApyRuleValidationError(msg)

    rule_type = str(data.get("type", "")).lower()
    if rule_type not in KNOWN_TYPES:
        msg = f"Unknown type: {rule_type!r}. Valid: {', '.join(sorted(KNOWN_TYPES))}"
        raise ApyRuleValidationError(msg)

    raw_limits = data.get("limits")
    if not isinstance(raw_limits, list) or not raw_limits:
        msg = "limits must be a non-empty list"
        raise ApyRuleValidationError(msg)

    limits: list[RuleLimit] = []
    for lim in raw_limits:
        if not isinstance(lim, dict):
            msg = "Each limit must be an object"
            raise ApyRuleValidationError(msg)
        try:
            from_amount = Decimal(str(lim["from_amount"]))
            to_amount = Decimal(str(lim["to_amount"])) if lim.get("to_amount") is not None else None
            apy = Decimal(str(lim["apy"]))
        except (KeyError, InvalidOperation) as exc:
            msg = f"Invalid limit fields: {exc}"
            raise ApyRuleValidationError(msg) from exc
        limits.append(RuleLimit(from_amount=from_amount, to_amount=to_amount, apy=apy))

    try:
        started_at = date.fromisoformat(str(data["started_at"]))
        finished_at = date.fromisoformat(str(data["finished_at"]))
    except (KeyError, ValueError) as exc:
        msg = f"Invalid date fields: {exc}"
        raise ApyRuleValidationError(msg) from exc

    if started_at > finished_at:
        msg = "started_at must be <= finished_at"
        raise ApyRuleValidationError(msg)

    return ApyRule(
        id=rule_id,
        protocol=protocol,
        coin=coin,
        type=rule_type,
        limits=tuple(limits),
        started_at=started_at,
        finished_at=finished_at,
    )


def _serialize_rules(rules: list[ApyRule]) -> str:
    """Serialize rules to JSON string."""
    return json.dumps([_rule_to_dict(r) for r in rules])


def _deserialize_rules(raw: str) -> list[ApyRule]:
    """Deserialize JSON string to list of ApyRule."""
    data = json.loads(raw)
    if not isinstance(data, list):
        return []
    rules: list[ApyRule] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        limits = tuple(
            RuleLimit(
                from_amount=Decimal(str(lim["from_amount"])),
                to_amount=Decimal(str(lim["to_amount"])) if lim.get("to_amount") is not None else None,
                apy=Decimal(str(lim["apy"])),
            )
            for lim in item.get("limits", [])
        )
        rules.append(
            ApyRule(
                id=item["id"],
                protocol=item["protocol"],
                coin=item["coin"],
                type=item["type"],
                limits=limits,
                started_at=date.fromisoformat(item["started_at"]),
                finished_at=date.fromisoformat(item["finished_at"]),
            )
        )
    return rules


def rule_to_dict(rule: ApyRule) -> dict[str, Any]:
    """Convert an ApyRule to a JSON-serializable dict (public API)."""
    return _rule_to_dict(rule)


def _rule_to_dict(rule: ApyRule) -> dict[str, Any]:
    return {
        "id": rule.id,
        "protocol": rule.protocol,
        "coin": rule.coin,
        "type": rule.type,
        "limits": [
            {
                "from_amount": str(limit.from_amount),
                "to_amount": str(limit.to_amount) if limit.to_amount is not None else None,
                "apy": str(limit.apy),
            }
            for limit in rule.limits
        ],
        "started_at": rule.started_at.isoformat(),
        "finished_at": rule.finished_at.isoformat(),
    }
