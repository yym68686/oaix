import base64
import binascii
import copy
import hashlib
import os
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from urllib.parse import unquote_to_bytes

from sqlalchemy import select, update

from .database import ChatImageCheckpoint, ChatImageCheckpointImage, get_read_session, get_session, utcnow


DEFAULT_CHAT_IMAGE_CHECKPOINT_TTL_HOURS = 24 * 30


@dataclass(frozen=True)
class ResolvedChatImageState:
    checkpoint_id: int
    upstream_response_id: str | None
    image_call_id: str
    output_item: dict[str, Any]


def _normalize_optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _int_env(name: str, default: int, *, minimum: int = 1) -> int:
    raw = str(os.getenv(name, "") or "").strip()
    try:
        value = int(raw) if raw else int(default)
    except ValueError:
        value = int(default)
    return max(minimum, value)


def _checkpoint_ttl() -> timedelta:
    return timedelta(hours=_int_env("CHAT_IMAGE_CHECKPOINT_TTL_HOURS", DEFAULT_CHAT_IMAGE_CHECKPOINT_TTL_HOURS))


def _sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _decode_base64_payload(raw: str) -> bytes | None:
    normalized = str(raw or "").strip()
    if not normalized:
        return None
    padding = (-len(normalized)) % 4
    if padding:
        normalized += "=" * padding
    try:
        return base64.b64decode(normalized, validate=False)
    except (ValueError, binascii.Error):
        return None


def decode_data_url_bytes(image_url: str) -> tuple[str | None, bytes] | None:
    normalized = _normalize_optional_text(image_url)
    if normalized is None or not normalized.lower().startswith("data:"):
        return None

    header, separator, data = normalized.partition(",")
    if separator != ",":
        return None

    media_type = header[5:]
    is_base64 = False
    if ";" in media_type:
        parts = media_type.split(";")
        media_type = parts[0]
        is_base64 = any(part.lower() == "base64" for part in parts[1:])
    media_type = _normalize_optional_text(media_type)

    if is_base64:
        payload = _decode_base64_payload(data)
    else:
        try:
            payload = unquote_to_bytes(data)
        except Exception:
            payload = None
    if payload is None:
        return None
    return media_type, payload


def image_sha256_from_data_url(image_url: str) -> str | None:
    decoded = decode_data_url_bytes(image_url)
    if decoded is None:
        return None
    _, payload = decoded
    return _sha256_hex(payload)


def _extract_response_object(payload: dict[str, Any]) -> dict[str, Any] | None:
    response_obj = payload.get("response")
    if isinstance(response_obj, dict):
        return response_obj
    if isinstance(payload.get("output"), list) or payload.get("id") is not None:
        return payload
    return None


async def create_chat_image_checkpoint(
    *,
    scope_hash: str,
    client_model: str,
    responses_payload: dict[str, Any],
    assistant_content: str | None,
    request_log_id: int | None = None,
) -> int | None:
    response_obj = _extract_response_object(responses_payload)
    if response_obj is None:
        return None

    output_items = response_obj.get("output")
    if not isinstance(output_items, list):
        return None

    prepared_rows: list[dict[str, Any]] = []
    for output_index, item in enumerate(output_items):
        if not isinstance(item, dict):
            continue
        if _normalize_optional_text(item.get("type")) != "image_generation_call":
            continue

        image_call_id = _normalize_optional_text(item.get("id"))
        result_b64 = _normalize_optional_text(item.get("result"))
        if image_call_id is None or result_b64 is None:
            continue

        image_bytes = _decode_base64_payload(result_b64)
        if image_bytes is None:
            continue

        prepared_rows.append(
            {
                "image_index": output_index,
                "image_call_id": image_call_id,
                "image_sha256": _sha256_hex(image_bytes),
                "mime_type": _normalize_optional_text(item.get("output_format")),
                "output_item": copy.deepcopy(item),
            }
        )

    if not prepared_rows:
        return None

    now = utcnow()
    assistant_content_normalized = _normalize_optional_text(assistant_content)
    assistant_content_sha256 = (
        _sha256_hex(assistant_content_normalized.encode("utf-8")) if assistant_content_normalized is not None else None
    )

    async with get_session() as session:
        async with session.begin():
            checkpoint = ChatImageCheckpoint(
                scope_hash=scope_hash,
                client_model=client_model,
                upstream_response_id=_normalize_optional_text(response_obj.get("id")),
                request_log_id=request_log_id,
                assistant_content_sha256=assistant_content_sha256,
                image_count=len(prepared_rows),
                last_accessed_at=now,
                expires_at=now + _checkpoint_ttl(),
            )
            session.add(checkpoint)
            await session.flush()

            for row in prepared_rows:
                session.add(
                    ChatImageCheckpointImage(
                        checkpoint_id=checkpoint.id,
                        image_index=row["image_index"],
                        image_call_id=row["image_call_id"],
                        image_sha256=row["image_sha256"],
                        mime_type=row["mime_type"],
                        output_item=row["output_item"],
                    )
                )

            return checkpoint.id


async def resolve_chat_image_output_items(
    *,
    scope_hash: str,
    client_model: str,
    image_urls: list[str],
) -> list[ResolvedChatImageState | None]:
    image_hashes = [image_sha256_from_data_url(image_url) for image_url in image_urls]
    query_hashes = sorted({image_hash for image_hash in image_hashes if image_hash is not None})
    if not query_hashes:
        return [None] * len(image_urls)

    now = utcnow()
    async with get_read_session() as session:
        stmt = (
            select(ChatImageCheckpointImage, ChatImageCheckpoint)
            .join(ChatImageCheckpoint, ChatImageCheckpoint.id == ChatImageCheckpointImage.checkpoint_id)
            .where(ChatImageCheckpoint.scope_hash == scope_hash)
            .where(ChatImageCheckpoint.client_model == client_model)
            .where(ChatImageCheckpointImage.image_sha256.in_(query_hashes))
            .where(ChatImageCheckpoint.expires_at.is_(None) | (ChatImageCheckpoint.expires_at > now))
            .order_by(
                ChatImageCheckpoint.created_at.desc(),
                ChatImageCheckpointImage.created_at.desc(),
                ChatImageCheckpointImage.id.desc(),
            )
        )
        rows = (await session.execute(stmt)).all()

    latest_by_hash: dict[str, ResolvedChatImageState] = {}
    touched_checkpoint_ids: set[int] = set()
    for image_row, checkpoint_row in rows:
        if image_row.image_sha256 in latest_by_hash:
            continue
        latest_by_hash[image_row.image_sha256] = ResolvedChatImageState(
            checkpoint_id=checkpoint_row.id,
            upstream_response_id=checkpoint_row.upstream_response_id,
            image_call_id=image_row.image_call_id,
            output_item=copy.deepcopy(image_row.output_item or {}),
        )
        touched_checkpoint_ids.add(checkpoint_row.id)

    if touched_checkpoint_ids:
        async with get_session() as session:
            async with session.begin():
                await session.execute(
                    update(ChatImageCheckpoint)
                    .where(ChatImageCheckpoint.id.in_(touched_checkpoint_ids))
                    .values(last_accessed_at=now)
                )

    return [latest_by_hash.get(image_hash) if image_hash is not None else None for image_hash in image_hashes]
