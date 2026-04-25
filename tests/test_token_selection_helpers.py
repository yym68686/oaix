import pytest

from oaix_gateway.token_store import (
    DEFAULT_TOKEN_SELECTION_STRATEGY,
    TOKEN_SELECTION_STRATEGY_FILL_FIRST,
    TOKEN_SELECTION_STRATEGY_LEAST_RECENTLY_USED,
    _token_selection_order_clauses,
    normalize_token_selection_order,
    normalize_token_selection_strategy,
    parse_token_selection_strategy,
)


def test_parse_token_selection_strategy_accepts_fill_first_variants() -> None:
    assert parse_token_selection_strategy("fill-first") == TOKEN_SELECTION_STRATEGY_FILL_FIRST
    assert parse_token_selection_strategy("fill_first") == TOKEN_SELECTION_STRATEGY_FILL_FIRST


def test_parse_token_selection_strategy_accepts_oldest_unused_alias() -> None:
    assert parse_token_selection_strategy("oldest_unused") == TOKEN_SELECTION_STRATEGY_LEAST_RECENTLY_USED
    assert parse_token_selection_strategy("least_recently_used") == TOKEN_SELECTION_STRATEGY_LEAST_RECENTLY_USED


def test_parse_token_selection_strategy_rejects_unknown_values() -> None:
    with pytest.raises(ValueError):
        parse_token_selection_strategy("round-robin")


def test_normalize_token_selection_strategy_falls_back_to_default() -> None:
    assert normalize_token_selection_strategy("unknown") == DEFAULT_TOKEN_SELECTION_STRATEGY


def test_normalize_token_selection_order_keeps_unique_positive_ids() -> None:
    assert normalize_token_selection_order([7, "3", 7, 0, -2, "bad", None, 9]) == (7, 3, 9)


def test_fill_first_order_clauses_rank_custom_order_before_id() -> None:
    clauses = _token_selection_order_clauses(TOKEN_SELECTION_STRATEGY_FILL_FIRST, token_order=[9, 3])

    assert len(clauses) == 2
    assert "CASE" in str(clauses[0].compile(compile_kwargs={"literal_binds": True}))
