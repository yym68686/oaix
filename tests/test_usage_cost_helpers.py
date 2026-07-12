from oaix_gateway.usage_cost import extract_usage_metrics, resolve_model_pricing


def test_extract_usage_metrics_estimates_cost_with_cached_input() -> None:
    usage = extract_usage_metrics(
        {
            "model": "gpt-5.4-mini",
            "usage": {
                "input_tokens": 1000,
                "output_tokens": 500,
                "total_tokens": 1500,
                "input_tokens_details": {
                    "cached_tokens": 200,
                },
            },
        },
        model_name="gpt-5.4-mini",
    )

    assert usage is not None
    assert usage.input_tokens == 1000
    assert usage.output_tokens == 500
    assert usage.total_tokens == 1500
    assert usage.cached_input_tokens == 200
    assert usage.pricing_model == "gpt-5.4-mini"
    assert usage.estimated_cost_usd is not None
    assert usage.estimated_cost_usd > 0


def test_extract_usage_metrics_supports_nested_response_usage() -> None:
    usage = extract_usage_metrics(
        {
            "type": "response.completed",
            "response": {
                "model": "gpt-5.4",
                "usage": {
                    "input_tokens": 12,
                    "output_tokens": 8,
                    "total_tokens": 20,
                },
            },
        },
        model_name="gpt-5.4",
    )

    assert usage is not None
    assert usage.total_tokens == 20
    assert usage.output_tokens == 8


def test_resolve_model_pricing_aliases_compact_to_mini() -> None:
    pricing_model, pricing = resolve_model_pricing("gpt-5.4-compact")

    assert pricing_model == "gpt-5.4-mini"
    assert pricing is not None
    assert pricing.output_per_million_usd > 0


def test_resolve_model_pricing_supports_gpt_5_5() -> None:
    pricing_model, pricing = resolve_model_pricing("gpt-5.5")

    assert pricing_model == "gpt-5.5"
    assert pricing is not None
    assert pricing.input_per_million_usd == 5.0
    assert pricing.cached_input_per_million_usd == 0.5
    assert pricing.output_per_million_usd == 30.0


def test_resolve_model_pricing_falls_back_to_gpt_5_5_family() -> None:
    pricing_model, pricing = resolve_model_pricing("gpt-5.5-2026-05-01")

    assert pricing_model == "gpt-5.5"
    assert pricing is not None
    assert pricing.output_per_million_usd == 30.0


def test_extract_usage_metrics_supports_gpt_5_6_cache_write_billing() -> None:
    usage = extract_usage_metrics(
        {
            "usage": {
                "input_tokens": 100,
                "input_tokens_details": {
                    "cache_write_tokens": 20,
                    "cached_tokens": 30,
                },
                "output_tokens": 10,
                "total_tokens": 110,
            },
        },
        model_name="gpt-5.6-luna",
    )

    assert usage is not None
    assert usage.cache_write_input_tokens == 20
    assert usage.cached_input_tokens == 30
    assert usage.pricing_model == "gpt-5.6-luna"
    assert usage.estimated_cost_usd == 0.000138


def test_resolve_model_pricing_supports_gpt_5_6_families() -> None:
    expected = {
        "gpt-5.6-sol": (5.0, 6.25, 0.5, 30.0),
        "gpt-5.6-terra-preview": (2.5, 3.125, 0.25, 15.0),
        "gpt-5.6-luna": (1.0, 1.25, 0.1, 6.0),
    }

    for model, rates in expected.items():
        _, pricing = resolve_model_pricing(model)
        assert pricing is not None
        assert (
            pricing.input_per_million_usd,
            pricing.cache_write_input_per_million_usd,
            pricing.cached_input_per_million_usd,
            pricing.output_per_million_usd,
        ) == rates


def test_gpt_5_6_unknown_variant_is_not_guessed() -> None:
    pricing_model, pricing = resolve_model_pricing("gpt-5.6-unknown")

    assert pricing_model is None
    assert pricing is None


def test_cache_write_explicit_zero_has_priority_over_fallback_fields() -> None:
    usage = extract_usage_metrics(
        {
            "usage": {
                "input_tokens": 100,
                "input_tokens_details": {
                    "cache_write_tokens": 0,
                    "cached_tokens": 80,
                },
                "cache_creation_input_tokens": 100,
                "output_tokens": 1,
            },
        },
        model_name="gpt-5.6-sol",
    )

    assert usage is not None
    assert usage.cache_write_input_tokens == 0
