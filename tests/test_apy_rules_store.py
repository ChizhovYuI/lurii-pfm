"""Tests for APY rules store and computation logic."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from pfm.db.apy_rules_store import (
    ApyRule,
    ApyRuleNotFoundError,
    ApyRulesStore,
    ApyRuleValidationError,
    RuleLimit,
    compute_effective_apy,
)
from pfm.db.models import init_db

# ── compute_effective_apy tests ──────────────────────────────────────


def _base_rule(
    *,
    started_at: date = date(2024, 1, 1),
    finished_at: date = date(2025, 12, 31),
) -> ApyRule:
    """Stablecoin Earn Plus tiered base rule: 10% for 0-5000, 2.97% above."""
    return ApyRule(
        id="base-1",
        protocol="aave",
        coin="usdc",
        type="base",
        limits=(
            RuleLimit(from_amount=Decimal(0), to_amount=Decimal(5000), apy=Decimal("0.10")),
            RuleLimit(from_amount=Decimal(5000), to_amount=None, apy=Decimal("0.0297")),
        ),
        started_at=started_at,
        finished_at=finished_at,
    )


def _bonus_rule(
    *,
    started_at: date = date(2024, 6, 1),
    finished_at: date = date(2024, 6, 7),
) -> ApyRule:
    """7-day bonus boost: +18.8% for any amount."""
    return ApyRule(
        id="bonus-1",
        protocol="aave",
        coin="usdc",
        type="bonus",
        limits=(RuleLimit(from_amount=Decimal(0), to_amount=None, apy=Decimal("0.188")),),
        started_at=started_at,
        finished_at=finished_at,
    )


def test_no_rules_returns_protocol_apy():
    result = compute_effective_apy(Decimal("0.031"), [], "aave", "usdc", Decimal(1000), date(2024, 3, 1))
    assert result == Decimal("0.031")


def test_base_rule_lower_bracket():
    rules = [_base_rule()]
    result = compute_effective_apy(Decimal("0.031"), rules, "aave", "usdc", Decimal(1000), date(2024, 3, 1))
    assert result == Decimal("0.10")


def test_base_rule_upper_bracket():
    rules = [_base_rule()]
    result = compute_effective_apy(Decimal("0.031"), rules, "aave", "usdc", Decimal("5000.01"), date(2024, 3, 1))
    assert result == Decimal("0.0297")


def test_base_rule_exact_boundary_5000():
    """Amount == 5000 falls in the lower bracket (from < amount <= to)."""
    rules = [_base_rule()]
    result = compute_effective_apy(Decimal("0.031"), rules, "aave", "usdc", Decimal(5000), date(2024, 3, 1))
    assert result == Decimal("0.10")


def test_base_rule_zero_amount_no_bracket():
    """Amount == 0 does not match any bracket (from=0 is exclusive lower bound)."""
    rules = [_base_rule()]
    result = compute_effective_apy(Decimal("0.031"), rules, "aave", "usdc", Decimal(0), date(2024, 3, 1))
    assert result == Decimal("0.031")


def test_bonus_stacking():
    rules = [_base_rule(), _bonus_rule()]
    result = compute_effective_apy(Decimal("0.031"), rules, "aave", "usdc", Decimal(1000), date(2024, 6, 3))
    assert result == Decimal("0.10") + Decimal("0.188")


def test_bonus_outside_date_range():
    rules = [_base_rule(), _bonus_rule()]
    result = compute_effective_apy(Decimal("0.031"), rules, "aave", "usdc", Decimal(1000), date(2024, 7, 1))
    # Only base rule applies
    assert result == Decimal("0.10")


def test_wrong_protocol_ignores_rule():
    rules = [_base_rule()]
    result = compute_effective_apy(Decimal("0.031"), rules, "compound", "usdc", Decimal(1000), date(2024, 3, 1))
    assert result == Decimal("0.031")


def test_wrong_coin_ignores_rule():
    rules = [_base_rule()]
    result = compute_effective_apy(Decimal("0.031"), rules, "aave", "usdt", Decimal(1000), date(2024, 3, 1))
    assert result == Decimal("0.031")


def test_date_boundary_started_at():
    rules = [_base_rule(started_at=date(2024, 3, 1))]
    result = compute_effective_apy(Decimal("0.031"), rules, "aave", "usdc", Decimal(1000), date(2024, 3, 1))
    assert result == Decimal("0.10")


def test_date_boundary_finished_at():
    rules = [_base_rule(finished_at=date(2024, 3, 1))]
    result = compute_effective_apy(Decimal("0.031"), rules, "aave", "usdc", Decimal(1000), date(2024, 3, 1))
    assert result == Decimal("0.10")


def test_date_before_started_at():
    rules = [_base_rule(started_at=date(2024, 3, 2))]
    result = compute_effective_apy(Decimal("0.031"), rules, "aave", "usdc", Decimal(1000), date(2024, 3, 1))
    assert result == Decimal("0.031")


def test_multiple_bonus_rules_stack():
    bonus1 = ApyRule(
        id="b1",
        protocol="aave",
        coin="usdc",
        type="bonus",
        limits=(RuleLimit(from_amount=Decimal(0), to_amount=None, apy=Decimal("0.05")),),
        started_at=date(2024, 1, 1),
        finished_at=date(2024, 12, 31),
    )
    bonus2 = ApyRule(
        id="b2",
        protocol="aave",
        coin="usdc",
        type="bonus",
        limits=(RuleLimit(from_amount=Decimal(0), to_amount=None, apy=Decimal("0.03")),),
        started_at=date(2024, 1, 1),
        finished_at=date(2024, 12, 31),
    )
    result = compute_effective_apy(Decimal("0.031"), [bonus1, bonus2], "aave", "usdc", Decimal(100), date(2024, 6, 1))
    assert result == Decimal("0.031") + Decimal("0.05") + Decimal("0.03")


# ── ApyRulesStore CRUD tests ────────────────────────────────────────


@pytest.fixture
async def db_path(tmp_path):
    path = tmp_path / "test.db"
    await init_db(path)
    return path


@pytest.fixture
def store(db_path):
    return ApyRulesStore(db_path)


_VALID_RULE_DATA = {
    "protocol": "aave",
    "coin": "usdc",
    "type": "base",
    "limits": [
        {"from_amount": "0", "to_amount": "5000", "apy": "0.10"},
        {"from_amount": "5000", "to_amount": None, "apy": "0.0297"},
    ],
    "started_at": "2024-01-01",
    "finished_at": "2025-12-31",
}


async def test_add_and_load(store):
    rules = await store.add_rule("test-src", _VALID_RULE_DATA)
    assert len(rules) == 1
    assert rules[0].protocol == "aave"
    assert rules[0].coin == "usdc"
    assert len(rules[0].limits) == 2

    loaded = await store.load_rules("test-src")
    assert len(loaded) == 1
    assert loaded[0].id == rules[0].id


async def test_load_empty(store):
    rules = await store.load_rules("nonexistent")
    assert rules == []


async def test_update_rule(store):
    rules = await store.add_rule("test-src", _VALID_RULE_DATA)
    rule_id = rules[0].id

    updated_data = {**_VALID_RULE_DATA, "type": "bonus"}
    rules = await store.update_rule("test-src", rule_id, updated_data)
    assert len(rules) == 1
    assert rules[0].type == "bonus"
    assert rules[0].id == rule_id


async def test_update_rule_not_found(store):
    await store.add_rule("test-src", _VALID_RULE_DATA)
    with pytest.raises(ApyRuleNotFoundError):
        await store.update_rule("test-src", "nonexistent-id", _VALID_RULE_DATA)


async def test_delete_rule(store):
    rules = await store.add_rule("test-src", _VALID_RULE_DATA)
    rule_id = rules[0].id

    rules = await store.delete_rule("test-src", rule_id)
    assert rules == []

    loaded = await store.load_rules("test-src")
    assert loaded == []


async def test_delete_rule_not_found(store):
    with pytest.raises(ApyRuleNotFoundError):
        await store.delete_rule("test-src", "nonexistent-id")


async def test_multiple_rules(store):
    await store.add_rule("test-src", _VALID_RULE_DATA)
    bonus_data = {
        "protocol": "aave",
        "coin": "usdc",
        "type": "bonus",
        "limits": [{"from_amount": "0", "to_amount": None, "apy": "0.188"}],
        "started_at": "2024-06-01",
        "finished_at": "2024-06-07",
    }
    rules = await store.add_rule("test-src", bonus_data)
    assert len(rules) == 2


# ── Validation tests ────────────────────────────────────────────────


async def test_validate_unknown_protocol(store):
    data = {**_VALID_RULE_DATA, "protocol": "compound"}
    with pytest.raises(ApyRuleValidationError, match="Unknown protocol"):
        await store.add_rule("test-src", data)


async def test_validate_unknown_coin(store):
    data = {**_VALID_RULE_DATA, "coin": "dai"}
    with pytest.raises(ApyRuleValidationError, match="Unknown coin"):
        await store.add_rule("test-src", data)


async def test_validate_unknown_type(store):
    data = {**_VALID_RULE_DATA, "type": "reward"}
    with pytest.raises(ApyRuleValidationError, match="Unknown type"):
        await store.add_rule("test-src", data)


async def test_validate_empty_limits(store):
    data = {**_VALID_RULE_DATA, "limits": []}
    with pytest.raises(ApyRuleValidationError, match="non-empty list"):
        await store.add_rule("test-src", data)


async def test_validate_started_after_finished(store):
    data = {**_VALID_RULE_DATA, "started_at": "2025-01-01", "finished_at": "2024-01-01"}
    with pytest.raises(ApyRuleValidationError, match="started_at must be <= finished_at"):
        await store.add_rule("test-src", data)


async def test_validate_invalid_date(store):
    data = {**_VALID_RULE_DATA, "started_at": "not-a-date"}
    with pytest.raises(ApyRuleValidationError, match="Invalid date"):
        await store.add_rule("test-src", data)


async def test_validate_missing_limit_fields(store):
    data = {**_VALID_RULE_DATA, "limits": [{"from_amount": "0"}]}
    with pytest.raises(ApyRuleValidationError, match="Invalid limit"):
        await store.add_rule("test-src", data)
