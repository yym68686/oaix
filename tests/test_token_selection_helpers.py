import pytest

from oaix_gateway.token_store import (
    DEFAULT_TOKEN_SELECTION_STRATEGY,
    TOKEN_SELECTION_STRATEGY_FILL_FIRST,
    TOKEN_SELECTION_STRATEGY_LEAST_RECENTLY_USED,
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
