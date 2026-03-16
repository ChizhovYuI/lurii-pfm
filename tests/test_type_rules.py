"""Tests for category rule matching and the _parse_values utility."""

from __future__ import annotations

from pfm.analytics.categorizer import _match_values, _parse_values


class TestParseValues:
    def test_json_array(self) -> None:
        assert _parse_values('["a", "b"]') == ["a", "b"]

    def test_plain_string(self) -> None:
        assert _parse_values("hello") == ["hello"]

    def test_invalid_json_treated_as_string(self) -> None:
        assert _parse_values("[invalid") == ["[invalid"]


class TestMatchValues:
    def test_eq_plain(self) -> None:
        assert _match_values("Payment", "Payment", "eq")
        assert not _match_values("Other", "Payment", "eq")

    def test_eq_array(self) -> None:
        assert _match_values("Payment", '["Payment", "Refund"]', "eq")
        assert _match_values("Refund", '["Payment", "Refund"]', "eq")
        assert not _match_values("Other", '["Payment", "Refund"]', "eq")

    def test_contains_plain(self) -> None:
        assert _match_values("swap_token", "swap", "contains")
        assert not _match_values("deposit", "swap", "contains")

    def test_contains_array(self) -> None:
        assert _match_values("swap_token", '["swap", "trade"]', "contains")
        assert _match_values("my_trade_x", '["swap", "trade"]', "contains")
        assert not _match_values("deposit", '["swap", "trade"]', "contains")

    def test_unknown_operator(self) -> None:
        assert not _match_values("x", "x", "unknown")
