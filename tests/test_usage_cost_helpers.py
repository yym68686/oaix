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
