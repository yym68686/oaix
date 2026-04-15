from collections import defaultdict
from collections.abc import Callable, Iterable
from typing import Any, TypeVar


T = TypeVar("T")


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


def group_rows_by_refresh_token_history(
    rows: Iterable[T],
    *,
    alias_getter: Callable[[T], Iterable[str] | None],
) -> list[list[T]]:
    row_list = list(rows)
    if not row_list:
        return []

    parents = list(range(len(row_list)))

    def find(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root == right_root:
            return
        parents[right_root] = left_root

    first_index_by_alias: dict[str, int] = {}
    for index, row in enumerate(row_list):
        aliases = []
        for alias in alias_getter(row) or ():
            token = normalize_refresh_token(alias)
            if token:
                aliases.append(token)
        for alias in aliases:
            previous = first_index_by_alias.get(alias)
            if previous is None:
                first_index_by_alias[alias] = index
            else:
                union(previous, index)

    grouped: dict[int, list[T]] = defaultdict(list)
    for index, row in enumerate(row_list):
        grouped[find(index)].append(row)
    return list(grouped.values())
