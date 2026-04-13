from typing import Any


def normalize_refresh_token(value: Any) -> str | None:
    token = str(value or "").strip()
    return token or None


def merge_refresh_token_aliases(existing: Any, *refresh_tokens: Any) -> list[str]:
    merged: list[str] = []

    if isinstance(existing, list):
        candidates = [*existing, *refresh_tokens]
    else:
        candidates = list(refresh_tokens)

    for candidate in candidates:
        token = normalize_refresh_token(candidate)
        if token and token not in merged:
            merged.append(token)
    return merged


def collect_refresh_token_aliases(
    refresh_token_aliases: Any,
    current_refresh_token: Any,
    raw_payload: Any | None = None,
) -> list[str]:
    payload_refresh_token = None
    if isinstance(raw_payload, dict):
        payload_refresh_token = raw_payload.get("refresh_token")
    return merge_refresh_token_aliases(refresh_token_aliases, current_refresh_token, payload_refresh_token)

