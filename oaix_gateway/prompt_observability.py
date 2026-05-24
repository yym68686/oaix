from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .prompt_cache import (
    _content_digestible,
    _first_user_message,
    _reasoning_effort,
    _responses_input_messages,
    _responses_system_messages,
    _system_messages,
    normalize_seed_json,
    short_hash,
)


def _hash_text(value: str) -> str:
    return short_hash(value, length=64)


def _normalized_text(value: Any) -> str:
    return normalize_seed_json(_content_digestible(value))


def _value_size_bytes(value: Any) -> int:
    return len(_normalized_text(value).encode("utf-8"))


def _value_count(value: Any) -> int:
    if isinstance(value, Mapping):
        return len(value)
    if isinstance(value, (list, tuple, set)):
        return len(value)
    return 1 if value is not None else 0


def _fingerprint(name: str, value: Any) -> dict[str, Any]:
    normalized = _normalized_text(value)
    return {
        "name": name,
        "hash": _hash_text(normalized),
        "bytes": len(normalized.encode("utf-8")),
        "count": _value_count(value),
        "present": value is not None,
    }


def _stage_hash(name: str, components: list[dict[str, Any]]) -> dict[str, Any]:
    normalized = normalize_seed_json({"name": name, "components": components})
    return {
        "name": name,
        "hash": _hash_text(normalized),
        "bytes": len(normalized.encode("utf-8")),
        "component_count": len(components),
    }


def _prompt_kind(payload: Mapping[str, Any] | None) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    if isinstance(payload.get("messages"), list):
        return "chat_completions"
    if "input" in payload:
        return "responses"
    return None


def _prompt_components(payload: Mapping[str, Any] | None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not isinstance(payload, Mapping):
        return [], []

    kind = _prompt_kind(payload)
    if kind == "chat_completions":
        messages = payload.get("messages")
        message_list = messages if isinstance(messages, list) else []
        static_components = [
            _fingerprint("model", payload.get("model")),
            _fingerprint("reasoning_effort", _reasoning_effort(payload)),
            _fingerprint("system_messages", _system_messages(message_list)),
            _fingerprint("tools", payload.get("tools")),
            _fingerprint("functions", payload.get("functions")),
            _fingerprint("tool_choice", payload.get("tool_choice")),
            _fingerprint("function_call", payload.get("function_call")),
            _fingerprint("response_format", payload.get("response_format")),
        ]
        dynamic_components = [
            _fingerprint("messages", message_list),
            _fingerprint("first_user", _first_user_message(message_list)),
        ]
        return static_components, dynamic_components

    if kind == "responses":
        input_value = payload.get("input")
        static_components = [
            _fingerprint("model", payload.get("model")),
            _fingerprint("reasoning_effort", _reasoning_effort(payload)),
            _fingerprint("instructions", payload.get("instructions")),
            _fingerprint("system_messages", _responses_system_messages(input_value)),
            _fingerprint("tools", payload.get("tools")),
            _fingerprint("tool_choice", payload.get("tool_choice")),
            _fingerprint("text", payload.get("text")),
        ]
        dynamic_components = [
            _fingerprint("input", _responses_input_messages(input_value)),
            _fingerprint("first_user", _first_user_message(_responses_input_messages(input_value))),
        ]
        return static_components, dynamic_components

    normalized = _fingerprint("payload", payload)
    return [normalized], []


def _prompt_structure(payload: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, Mapping):
        return None

    kind = _prompt_kind(payload) or "unknown"
    static_components, dynamic_components = _prompt_components(payload)
    prompt_payload = {
        "kind": kind,
        "template_hash": _stage_hash("template", static_components)["hash"],
        "dynamic_hash": _stage_hash("dynamic", dynamic_components)["hash"] if dynamic_components else None,
        "static_components": static_components,
        "dynamic_components": dynamic_components,
        "prefix_hashes": [
            _stage_hash("static_prefix", static_components),
            _stage_hash(
                "static_plus_dynamic",
                static_components + dynamic_components[:1],
            )
            if dynamic_components
            else _stage_hash("static_plus_dynamic", static_components),
            _stage_hash("full_prompt", static_components + dynamic_components),
        ],
    }
    if prompt_payload["dynamic_hash"] is None:
        prompt_payload.pop("dynamic_hash")
    return prompt_payload


def build_prompt_cache_trace(
    *,
    endpoint: str,
    model: str | None,
    compact: bool = False,
    raw_payload: Mapping[str, Any] | None = None,
    upstream_payload: Mapping[str, Any] | None = None,
    prompt_cache_context: Any | None = None,
    session_id: str | None = None,
    session_id_source: str | None = None,
    upstream_response_id: str | None = None,
    cache_affinity_result: str | None = None,
    cache_affinity_lane_index: int | None = None,
    cache_affinity_lane_count: int | None = None,
    input_tokens: int | None = None,
    cached_input_tokens: int | None = None,
    output_tokens: int | None = None,
    total_tokens: int | None = None,
    status_code: int | None = None,
) -> dict[str, Any]:
    request_payload = raw_payload if isinstance(raw_payload, Mapping) else None
    active_payload = upstream_payload if isinstance(upstream_payload, Mapping) else request_payload

    def _payload_hash_and_size(value: Mapping[str, Any] | None) -> tuple[str | None, int | None]:
        if not isinstance(value, Mapping):
            return None, None
        normalized = normalize_seed_json(value)
        return _hash_text(normalized), len(normalized.encode("utf-8"))

    request_payload_hash, request_payload_bytes = _payload_hash_and_size(request_payload)
    upstream_payload_hash, upstream_payload_bytes = _payload_hash_and_size(active_payload)

    previous_response_id = str(getattr(prompt_cache_context, "previous_response_id", "") or "").strip() or None
    prompt_cache_key_hash = str(getattr(prompt_cache_context, "prompt_cache_key_hash", "") or "").strip() or None
    prompt_cache_source = str(getattr(prompt_cache_context, "source", "") or "").strip() or None
    prompt_cache_retention_requested = None
    prompt_cache_retention_sent = None
    if isinstance(request_payload, Mapping):
        prompt_cache_retention_requested = request_payload.get("prompt_cache_retention")
    if isinstance(active_payload, Mapping):
        prompt_cache_retention_sent = active_payload.get("prompt_cache_retention")

    trace: dict[str, Any] = {
        "trace_version": 1,
        "endpoint": endpoint,
        "model": model,
        "compact": bool(compact),
        "request_kind": _prompt_kind(request_payload),
        "request_payload_hash": request_payload_hash,
        "request_payload_bytes": request_payload_bytes,
        "upstream_kind": _prompt_kind(active_payload),
        "upstream_payload_hash": upstream_payload_hash,
        "upstream_payload_bytes": upstream_payload_bytes,
        "prompt_cache_key_hash": prompt_cache_key_hash,
        "prompt_cache_key_source": prompt_cache_source,
        "prompt_cache_retention_requested": prompt_cache_retention_requested,
        "prompt_cache_retention_sent": prompt_cache_retention_sent,
        "previous_response_id_hash": _hash_text(previous_response_id) if previous_response_id else None,
        "session_id_hash": _hash_text(session_id) if session_id else None,
        "session_id_source": session_id_source,
        "route": {
            "cache_affinity_result": cache_affinity_result,
            "cache_affinity_lane_index": cache_affinity_lane_index,
            "cache_affinity_lane_count": cache_affinity_lane_count,
        },
        "usage": {
            "input_tokens": input_tokens,
            "cached_input_tokens": cached_input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total_tokens,
            "cache_hit_ratio": (
                round((cached_input_tokens or 0) / input_tokens, 4)
                if input_tokens and cached_input_tokens is not None
                else None
            ),
        },
        "response": {
            "response_id": upstream_response_id,
            "status_code": status_code,
        },
        "prompt": _prompt_structure(active_payload),
    }
    if request_payload is not None and active_payload is not None and request_payload is not active_payload:
        trace["request_prompt"] = _prompt_structure(request_payload)
    return trace
