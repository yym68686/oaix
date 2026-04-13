from oaix_gateway.token_identity import collect_refresh_token_aliases, merge_refresh_token_aliases, normalize_refresh_token


def test_normalize_refresh_token_trims_empty_values():
    assert normalize_refresh_token("  rt_123  ") == "rt_123"
    assert normalize_refresh_token("   ") is None
    assert normalize_refresh_token(None) is None


def test_merge_refresh_token_aliases_preserves_order_and_uniqueness():
    aliases = merge_refresh_token_aliases(["rt_1", "rt_2", "rt_1"], "rt_3", "rt_2", "", None)
    assert aliases == ["rt_1", "rt_2", "rt_3"]


def test_collect_refresh_token_aliases_includes_current_and_seed_payload_tokens():
    aliases = collect_refresh_token_aliases(
        ["rt_2"],
        "rt_3",
        {
            "refresh_token": "rt_1",
        },
    )
    assert aliases == ["rt_2", "rt_3", "rt_1"]

