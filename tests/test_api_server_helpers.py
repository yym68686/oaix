import asyncio
import json
from collections.abc import AsyncIterator

import pytest
from fastapi import HTTPException
from oaix_gateway.api_server import (
    _extract_usage_limit_cooldown_seconds,
    _get_compact_codex_server_error_cooling_time,
    _is_permanent_account_disable_error,
    _normalize_responses_compact_upstream_url,
    _prime_responses_upstream_stream,
    _responses_failure_http_exception,
    _sanitize_codex_payload,
    _should_retry_upstream_server_error,
)


def test_extract_usage_limit_cooldown_from_resets_in_seconds() -> None:
    payload = json.dumps(
        {
            "error": {
                "type": "usage_limit_reached",
                "resets_in_seconds": 123,
            }
        }
    )
    assert _extract_usage_limit_cooldown_seconds(429, payload) == 123


def test_extract_usage_limit_cooldown_returns_none_for_other_errors() -> None:
    payload = json.dumps({"error": {"type": "server_error", "message": "boom"}})
    assert _extract_usage_limit_cooldown_seconds(500, payload) is None
    assert _extract_usage_limit_cooldown_seconds(429, payload) is None


def test_detects_permanent_disable_errors() -> None:
    payload_402 = json.dumps({"detail": {"code": "deactivated_workspace"}})
    payload_401 = json.dumps({"error": {"code": "account_deactivated"}})
    assert _is_permanent_account_disable_error(402, payload_402) is True
    assert _is_permanent_account_disable_error(401, payload_401) is True


def test_sanitize_codex_payload_removes_unsupported_fields() -> None:
    payload = {
        "model": "gpt-4.1",
        "input": "hi",
        "max_output_tokens": 100,
        "response_format": {"type": "json_schema"},
        "previous_response_id": "abc",
        "prompt_cache_retention": "ephemeral",
        "safety_identifier": "sid",
    }
    sanitized = _sanitize_codex_payload(payload)
    assert sanitized == {
        "model": "gpt-4.1",
        "input": "hi",
        "instructions": "",
    }


def test_sanitize_codex_payload_compact_strips_store() -> None:
    payload = {
        "model": "gpt-4.1",
        "input": "hi",
        "store": False,
        "response_format": {"type": "json_schema"},
    }
    sanitized = _sanitize_codex_payload(payload, compact=True)
    assert sanitized == {
        "model": "gpt-4.1",
        "input": "hi",
        "instructions": "",
    }


def test_normalize_responses_compact_upstream_url() -> None:
    assert (
        _normalize_responses_compact_upstream_url("https://example.com/v1/responses")
        == "https://example.com/v1/responses/compact"
    )
    assert (
        _normalize_responses_compact_upstream_url("https://example.com/backend-api/codex/responses")
        == "https://example.com/backend-api/codex/responses/compact"
    )
    assert (
        _normalize_responses_compact_upstream_url("https://example.com/v1/responses/compact")
        == "https://example.com/v1/responses/compact"
    )


def test_compact_codex_server_error_cooling_time_defaults_to_60(monkeypatch) -> None:
    monkeypatch.delenv("COMPACT_SERVER_ERROR_COOLDOWN_SECONDS", raising=False)

    assert (
        _get_compact_codex_server_error_cooling_time(
            compact=True,
            status_code=500,
            error_text="internal server error",
        )
        == 60
    )
    assert (
        _get_compact_codex_server_error_cooling_time(
            compact=False,
            status_code=500,
            error_text="internal server error",
        )
        == 0
    )
    assert (
        _get_compact_codex_server_error_cooling_time(
            compact=True,
            status_code=429,
            error_text='{"error":{"type":"usage_limit_reached","resets_in_seconds":12}}',
        )
        == 0
    )


def test_responses_failure_http_exception_detects_response_failed_payload() -> None:
    exc = _responses_failure_http_exception(
        {
            "type": "response.failed",
            "response": {
                "status": "failed",
                "error": {
                    "code": "rate_limit_exceeded",
                    "message": "Too many requests",
                },
            },
        }
    )

    assert exc is not None
    assert exc.status_code == 429
    assert json.loads(exc.detail) == {
        "error": {
            "code": "rate_limit_exceeded",
            "message": "Too many requests",
        }
    }


def test_prime_responses_upstream_stream_commits_after_first_non_prefight_event() -> None:
    async def upstream() -> AsyncIterator[bytes]:
        yield b'event: response.created\ndata: {"type":"response.created"}\n\n'
        yield b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"hi"}\n\n'

    buffered_chunks, stream_committed = asyncio.run(_prime_responses_upstream_stream(upstream()))

    assert stream_committed is True
    assert buffered_chunks == [
        b'event: response.created\ndata: {"type":"response.created"}\n\n',
        b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"hi"}\n\n',
    ]


def test_prime_responses_upstream_stream_raises_on_semantic_failure_event() -> None:
    async def upstream() -> AsyncIterator[bytes]:
        yield b'event: response.created\ndata: {"type":"response.created"}\n\n'
        yield b'event: error\ndata: {"type":"error","error":{"type":"rate_limit_error","message":"Too many requests"}}\n\n'

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(_prime_responses_upstream_stream(upstream()))

    assert exc_info.value.status_code == 429


def test_prime_responses_upstream_stream_raises_on_incomplete_sse_event() -> None:
    async def upstream() -> AsyncIterator[bytes]:
        yield b'event: response.created\ndata: {"type":"response.created"}'

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(_prime_responses_upstream_stream(upstream()))

    assert exc_info.value.status_code == 502


def test_should_retry_upstream_server_error_only_for_5xx() -> None:
    assert _should_retry_upstream_server_error(500) is True
    assert _should_retry_upstream_server_error(503) is True
    assert _should_retry_upstream_server_error(429) is False
    assert _should_retry_upstream_server_error(400) is False
