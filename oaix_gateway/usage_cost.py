import json
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any


@dataclass(frozen=True)
class ModelPricing:
    input_per_million_usd: float
    output_per_million_usd: float
    cached_input_per_million_usd: float | None = None


@dataclass(frozen=True)
class UsageMetrics:
    input_tokens: int
    output_tokens: int
    total_tokens: int
    cached_input_tokens: int = 0
    estimated_cost_usd: float | None = None
    pricing_model: str | None = None


DEFAULT_MODEL_PRICING: dict[str, ModelPricing] = {
    "gpt-5": ModelPricing(input_per_million_usd=1.25, cached_input_per_million_usd=0.125, output_per_million_usd=10.0),
    "gpt-5-mini": ModelPricing(input_per_million_usd=0.25, cached_input_per_million_usd=0.025, output_per_million_usd=2.0),
    "gpt-5-nano": ModelPricing(input_per_million_usd=0.05, cached_input_per_million_usd=0.005, output_per_million_usd=0.4),
    "gpt-5.4": ModelPricing(input_per_million_usd=2.5, cached_input_per_million_usd=0.25, output_per_million_usd=15.0),
    "gpt-5.4-mini": ModelPricing(input_per_million_usd=0.75, cached_input_per_million_usd=0.075, output_per_million_usd=4.5),
    "gpt-5.4-nano": ModelPricing(input_per_million_usd=0.2, cached_input_per_million_usd=0.02, output_per_million_usd=1.25),
}

DEFAULT_MODEL_ALIASES = {
    "gpt-5.4-compact": "gpt-5.4-mini",
}


def _normalize_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return max(0, int(value))
    except (TypeError, ValueError):
        return None


def _mapping_get(mapping: Any, *keys: str) -> Any:
    current = mapping
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _normalize_model_name(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    return text or None


@lru_cache(maxsize=1)
def _env_model_pricing() -> dict[str, ModelPricing]:
    raw = str(os.getenv("MODEL_PRICING_JSON", "") or "").strip()
    if not raw:
        return {}

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}

    pricing: dict[str, ModelPricing] = {}
    for key, value in payload.items():
        if not isinstance(value, dict):
            continue
        model_name = _normalize_model_name(key)
        if not model_name:
            continue
        try:
            input_price = float(value["input_per_million_usd"])
            output_price = float(value["output_per_million_usd"])
        except (KeyError, TypeError, ValueError):
            continue

        cached_raw = value.get("cached_input_per_million_usd")
        try:
            cached_price = float(cached_raw) if cached_raw is not None else None
        except (TypeError, ValueError):
            cached_price = None

        pricing[model_name] = ModelPricing(
            input_per_million_usd=max(0.0, input_price),
            output_per_million_usd=max(0.0, output_price),
            cached_input_per_million_usd=max(0.0, cached_price) if cached_price is not None else None,
        )
    return pricing


def resolve_model_pricing(model_name: str | None) -> tuple[str | None, ModelPricing | None]:
    normalized = _normalize_model_name(model_name)
    if not normalized:
        return None, None

    env_pricing = _env_model_pricing()
    if normalized in env_pricing:
        return normalized, env_pricing[normalized]
    if normalized in DEFAULT_MODEL_PRICING:
        return normalized, DEFAULT_MODEL_PRICING[normalized]

    alias = DEFAULT_MODEL_ALIASES.get(normalized)
    if alias:
        pricing = env_pricing.get(alias) or DEFAULT_MODEL_PRICING.get(alias)
        if pricing is not None:
            return alias, pricing

    for suffix in ("-mini", "-nano"):
        if suffix in normalized:
            family_name = f"gpt-5.4{suffix}" if normalized.startswith("gpt-5.4") else f"gpt-5{suffix}"
            pricing = env_pricing.get(family_name) or DEFAULT_MODEL_PRICING.get(family_name)
            if pricing is not None:
                return family_name, pricing

    for family_name in ("gpt-5.4", "gpt-5"):
        if normalized.startswith(family_name):
            pricing = env_pricing.get(family_name) or DEFAULT_MODEL_PRICING.get(family_name)
            if pricing is not None:
                return family_name, pricing

    return None, None


def estimate_usage_cost(
    *,
    model_name: str | None,
    input_tokens: int,
    output_tokens: int,
    cached_input_tokens: int = 0,
) -> tuple[float | None, str | None]:
    pricing_model, pricing = resolve_model_pricing(model_name)
    if pricing is None:
        return None, None

    non_cached_input_tokens = max(0, input_tokens - cached_input_tokens)
    estimated_cost_usd = (
        (non_cached_input_tokens / 1_000_000.0) * pricing.input_per_million_usd
        + (output_tokens / 1_000_000.0) * pricing.output_per_million_usd
    )
    if pricing.cached_input_per_million_usd is not None and cached_input_tokens > 0:
        estimated_cost_usd += (cached_input_tokens / 1_000_000.0) * pricing.cached_input_per_million_usd

    return round(estimated_cost_usd, 8), pricing_model


def extract_usage_metrics(payload: Any, *, model_name: str | None = None) -> UsageMetrics | None:
    usage_obj = None
    if isinstance(payload, dict):
        if isinstance(payload.get("usage"), dict):
            usage_obj = payload["usage"]
        elif isinstance(_mapping_get(payload, "response", "usage"), dict):
            usage_obj = _mapping_get(payload, "response", "usage")

    if not isinstance(usage_obj, dict):
        return None

    input_tokens = _normalize_int(usage_obj.get("input_tokens"))
    if input_tokens is None:
        input_tokens = _normalize_int(usage_obj.get("prompt_tokens"))

    output_tokens = _normalize_int(usage_obj.get("output_tokens"))
    if output_tokens is None:
        output_tokens = _normalize_int(usage_obj.get("completion_tokens"))

    total_tokens = _normalize_int(usage_obj.get("total_tokens"))
    if total_tokens is None and (input_tokens is not None or output_tokens is not None):
        total_tokens = max(0, (input_tokens or 0) + (output_tokens or 0))

    cached_input_tokens = _normalize_int(
        usage_obj.get("input_cached_tokens")
        or _mapping_get(usage_obj, "input_tokens_details", "cached_tokens")
        or _mapping_get(usage_obj, "prompt_tokens_details", "cached_tokens")
    ) or 0

    if input_tokens is None and output_tokens is None and total_tokens is None:
        return None

    effective_input_tokens = input_tokens or 0
    effective_output_tokens = output_tokens or 0
    effective_total_tokens = total_tokens if total_tokens is not None else effective_input_tokens + effective_output_tokens
    estimated_cost_usd, pricing_model = estimate_usage_cost(
        model_name=model_name,
        input_tokens=effective_input_tokens,
        output_tokens=effective_output_tokens,
        cached_input_tokens=cached_input_tokens,
    )
    return UsageMetrics(
        input_tokens=effective_input_tokens,
        output_tokens=effective_output_tokens,
        total_tokens=effective_total_tokens,
        cached_input_tokens=cached_input_tokens,
        estimated_cost_usd=estimated_cost_usd,
        pricing_model=pricing_model,
    )
