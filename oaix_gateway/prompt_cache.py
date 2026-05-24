import hashlib
import json
import uuid
from typing import Any, Mapping


def normalize_seed_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def short_hash(value: str, *, length: int = 32) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return digest[: max(8, min(length, len(digest)))]


def deterministic_uuid(seed: str) -> str:
    raw = bytearray(hashlib.sha256(seed.encode("utf-8")).digest()[:16])
    raw[6] = (raw[6] & 0x0F) | 0x40
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))


def prompt_cache_key_hash(prompt_cache_key: str) -> str:
    return short_hash(prompt_cache_key, length=32)


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if value is None:
        return ""
    return str(value).strip()


def _mapping_get(mapping: Any, key: str) -> Any:
    if isinstance(mapping, Mapping):
        return mapping.get(key)
    return None


def extract_previous_response_id(payload: Mapping[str, Any] | None) -> str | None:
    value = _mapping_get(payload, "previous_response_id")
    text = _text(value)
    return text or None


def extract_prompt_cache_key(payload: Mapping[str, Any] | None) -> str | None:
    value = _mapping_get(payload, "prompt_cache_key")
    text = _text(value)
    return text or None


def _message_role(message: Any) -> str:
    return _text(_mapping_get(message, "role")).lower()


def _content_digestible(value: Any) -> Any:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        normalized: list[Any] = []
        for item in value:
            if isinstance(item, Mapping):
                item_type = _text(item.get("type"))
                if item_type in {"input_text", "output_text", "text"}:
                    normalized.append({"type": item_type, "text": item.get("text")})
                    continue
                if item_type in {"image_url", "input_image"}:
                    normalized.append(
                        {
                            "type": item_type,
                            "image_url": item.get("image_url"),
                            "detail": item.get("detail"),
                        }
                    )
                    continue
            normalized.append(item)
        return normalized
    return value


def _first_user_message(messages: list[Any]) -> Any:
    for message in messages:
        if _message_role(message) == "user":
            return _content_digestible(_mapping_get(message, "content"))
    return None


def _system_messages(messages: list[Any]) -> list[Any]:
    result: list[Any] = []
    for message in messages:
        if _message_role(message) in {"system", "developer"}:
            result.append(_content_digestible(_mapping_get(message, "content")))
    return result


def _responses_input_messages(input_value: Any) -> list[Any]:
    if isinstance(input_value, list):
        return list(input_value)
    if isinstance(input_value, str):
        return [{"role": "user", "content": input_value}]
    return []


def _responses_first_user(input_value: Any) -> Any:
    if isinstance(input_value, str):
        return input_value
    return _first_user_message(_responses_input_messages(input_value))


def _responses_system_messages(input_value: Any) -> list[Any]:
    return _system_messages(_responses_input_messages(input_value))


def _reasoning_effort(payload: Mapping[str, Any]) -> Any:
    reasoning = payload.get("reasoning")
    if isinstance(reasoning, Mapping):
        return reasoning.get("effort")
    return payload.get("reasoning_effort")


def derive_responses_prompt_cache_key(payload: Mapping[str, Any]) -> str | None:
    model = _text(payload.get("model"))
    if not model:
        return None
    seed = {
        "kind": "responses",
        "model": model,
        "reasoning_effort": _reasoning_effort(payload),
        "instructions": payload.get("instructions"),
        "system": _responses_system_messages(payload.get("input")),
        "first_user": _responses_first_user(payload.get("input")),
        "tools": payload.get("tools"),
        "tool_choice": payload.get("tool_choice"),
        "text": payload.get("text"),
    }
    normalized = normalize_seed_json(seed)
    return f"oaix:resp:{model}:{short_hash(normalized, length=32)}"


def derive_chat_prompt_cache_key(payload: Mapping[str, Any]) -> str | None:
    model = _text(payload.get("model"))
    if not model:
        return None
    messages = payload.get("messages")
    message_list = messages if isinstance(messages, list) else []
    seed = {
        "kind": "chat_completions",
        "model": model,
        "reasoning_effort": _reasoning_effort(payload),
        "system": _system_messages(message_list),
        "first_user": _first_user_message(message_list),
        "tools": payload.get("tools"),
        "functions": payload.get("functions"),
        "tool_choice": payload.get("tool_choice"),
        "function_call": payload.get("function_call"),
        "response_format": payload.get("response_format"),
    }
    normalized = normalize_seed_json(seed)
    return f"oaix:chat:{model}:{short_hash(normalized, length=32)}"


def client_scope_hash(headers: Mapping[str, Any] | None) -> str:
    if headers is None:
        return "anonymous"
    authorization = _text(headers.get("authorization") or headers.get("Authorization"))
    if authorization:
        return short_hash(authorization, length=16)
    return "anonymous"
