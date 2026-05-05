import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx
import pytest
from fastapi import HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.requests import Request
from oaix_gateway.api_server import (
    ChatCompletionsRequest,
    ImageProxyRequest,
    _HTTPPhaseTraceRecorder,
    _ProxyStreamCapture,
    _RequestLogHandle,
    _RequestTimingRecorder,
    _AsyncioSocketSendWarningFilter,
    _SSEProxyStreamingResponse,
    _TOKEN_ACTIVE_REQUESTS,
    _chat_completions_request_to_responses_request,
    _claim_next_active_token_for_request,
    _claim_next_token_for_model_request,
    _collect_images_api_response_from_sse,
    _execute_proxy_request_with_failover,
    ResponseTrafficController,
    _parse_admin_token_ids,
    _parse_images_edits_request,
    _parse_images_generations_request,
    _pop_sse_event_from_buffer,
    _proxy_chat_completions_with_token,
    _proxy_image_request_with_token,
    _collect_responses_json_from_sse,
    _extract_image_rate_limit_cooldown_seconds,
    _extract_usage_limit_cooldown_seconds,
    _get_compact_codex_server_error_cooling_time,
    _is_permanent_account_disable_error,
    _normalize_responses_compact_upstream_url,
    _proxy_request_with_token,
    _prime_responses_upstream_stream,
    _probe_token_with_latest_access_token,
    _resolve_token_observed_cost_usd,
    _resolved_chat_image_to_responses_input_item,
    _request_requires_non_free_codex_token,
    _responses_failure_http_exception,
    _iterate_with_sse_keepalive,
    _mark_token_success_with_timing,
    _stream_keepalive_interval_seconds,
    _upstream_http_max_connections,
    _upstream_http_max_keepalive_connections,
    _wrap_sse_stream_with_initial_keepalive,
    _build_stream_error_event,
    _build_admin_token_items,
    _build_admin_token_quota_items,
    _build_upstream_headers,
    _stream_responses_to_chat_completions,
    _stream_upstream_image_response,
    _stream_upstream_response,
    ResponsesRequest,
    _serialize_admin_token_item,
    _sanitize_codex_payload,
    _should_retry_upstream_server_error,
    _translate_responses_image_compat_payload,
    _upstream_error_http_exception,
    _wrap_streaming_body_iterator,
    _flush_request_log_write_queue,
    create_app,
)
from oaix_gateway.database import ChatImageCheckpoint, ChatImageCheckpointImage, CodexToken, GatewayRequestLog
from oaix_gateway.quota import CodexPlanInfo, CodexQuotaSnapshot
from oaix_gateway.token_import_jobs import (
    IMPORT_JOB_STATUS_COMPLETED,
    IMPORT_JOB_STATUS_QUEUED,
    IMPORT_JOB_STATUS_RUNNING,
    TokenImportBackgroundWorker,
    TokenImportJobLease,
    TokenImportJobState,
    get_token_import_job,
    process_token_import_job,
    IMPORT_ITEM_STATUS_QUEUED,
    IMPORT_ITEM_STATUS_VALIDATED,
    IMPORT_ITEM_STATUS_FAILED,
    IMPORT_ITEM_STATUS_PUBLISH_PENDING,
    IMPORT_ITEM_STATUS_PUBLISHED,
)
from oaix_gateway.token_store import (
    TOKEN_SELECTION_STRATEGY_FILL_FIRST,
    TokenPublishBatchResult,
    TokenSelectionSettings,
    TokenUpsertResult,
)


@pytest.mark.parametrize("separator", ["\n\n", "\r\n\n", "\n\r\n", "\r\n\r\n"])
def test_pop_sse_event_from_buffer_accepts_supported_separators(separator: str) -> None:
    raw_event, remaining, scan_start = _pop_sse_event_from_buffer(
        f"event: response.created{separator}data: next\n\n"
    )

    assert raw_event == "event: response.created"
    assert remaining == "data: next\n\n"
    assert scan_start == 0


def test_pop_sse_event_from_buffer_scans_incrementally_across_chunks() -> None:
    text_buffer = "data: " + ("x" * 10000)

    raw_event, text_buffer, scan_start = _pop_sse_event_from_buffer(text_buffer)

    assert raw_event is None
    assert scan_start == len(text_buffer) - 3

    text_buffer += "\r"
    raw_event, text_buffer, scan_start = _pop_sse_event_from_buffer(text_buffer, scan_start)

    assert raw_event is None
    assert scan_start == len(text_buffer) - 3

    text_buffer += "\n\r\nrest"
    raw_event, remaining, scan_start = _pop_sse_event_from_buffer(text_buffer, scan_start)

    assert raw_event == "data: " + ("x" * 10000)
    assert remaining == "rest"
    assert scan_start == 0


def test_mark_token_success_with_timing_skips_obviously_healthy_token(monkeypatch) -> None:
    mark_success_calls: list[int] = []
    token = CodexToken(
        id=501,
        refresh_token="refresh-501",
        token_type="codex",
        is_active=True,
        cooldown_until=None,
        last_error=None,
        merged_into_token_id=None,
    )

    async def fake_mark_token_success(token_id: int) -> None:
        mark_success_calls.append(token_id)

    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)

    asyncio.run(_mark_token_success_with_timing(None, token.id, token_row=token))

    assert mark_success_calls == []


def _make_json_request(path: str, payload: dict) -> Request:
    body = json.dumps(payload).encode("utf-8")
    delivered = False

    async def receive() -> dict[str, object]:
        nonlocal delivered
        if delivered:
            return {"type": "http.request", "body": b"", "more_body": False}
        delivered = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": path,
            "headers": [(b"content-type", b"application/json")],
        },
        receive,
    )


def test_asyncio_socket_send_warning_filter_suppresses_disconnect_noise() -> None:
    log_filter = _AsyncioSocketSendWarningFilter()
    noisy_record = logging.LogRecord(
        "asyncio",
        logging.WARNING,
        __file__,
        1,
        "socket.send() raised exception.",
        (),
        None,
    )
    other_record = logging.LogRecord("asyncio", logging.WARNING, __file__, 1, "other warning", (), None)

    assert log_filter.filter(noisy_record) is False
    assert log_filter.filter(other_record) is True


def test_sse_proxy_streaming_response_stops_after_send_failure() -> None:
    closed = False

    async def source() -> AsyncIterator[bytes]:
        nonlocal closed
        try:
            yield b"data: one\n\n"
            yield b"data: two\n\n"
        finally:
            closed = True

    sent: list[dict[str, object]] = []

    async def send(message: dict[str, object]) -> None:
        sent.append(message)
        if message["type"] == "http.response.body" and message.get("body"):
            raise RuntimeError("client disconnected")

    async def receive() -> dict[str, object]:
        return {"type": "http.request", "body": b"", "more_body": False}

    response = _SSEProxyStreamingResponse(source(), media_type="text/event-stream")
    asyncio.run(response({"type": "http", "method": "GET", "path": "/"}, receive, send))

    assert closed is True
    assert [message["type"] for message in sent] == ["http.response.start", "http.response.body"]


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


def test_extract_image_rate_limit_cooldown_from_retry_after_milliseconds(monkeypatch) -> None:
    monkeypatch.delenv("IMAGE_RATE_LIMIT_MIN_COOLDOWN_SECONDS", raising=False)
    payload = json.dumps(
        {
            "error": {
                "type": "input-images",
                "code": "rate_limit_exceeded",
                "message": (
                    "Rate limit reached for gpt-image-2 on input-images per min: "
                    "Limit 250, Used 250, Requested 1. Please try again in 240ms."
                ),
            }
        }
    )

    assert (
        _extract_image_rate_limit_cooldown_seconds(
            429,
            payload,
            request_model="gpt-image-2",
        )
        == 1
    )
    assert (
        _extract_image_rate_limit_cooldown_seconds(
            429,
            json.dumps({"detail": payload}),
            request_model="gpt-image-2",
        )
        == 1
    )
    assert _extract_image_rate_limit_cooldown_seconds(429, payload, request_model="gpt-5.4-mini") is None


def test_detects_permanent_disable_errors() -> None:
    payload_402 = json.dumps({"detail": {"code": "deactivated_workspace"}})
    payload_401 = json.dumps({"error": {"code": "account_deactivated"}})
    assert _is_permanent_account_disable_error(402, payload_402) is True
    assert _is_permanent_account_disable_error(401, payload_401) is True


def test_request_requires_non_free_codex_token_only_for_image_tool_model() -> None:
    assert _request_requires_non_free_codex_token("gpt-image-2") is True
    assert _request_requires_non_free_codex_token("gpt-5.5") is False
    assert _request_requires_non_free_codex_token("gpt-5.4-mini") is False


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
        "store": False,
        "instructions": "",
    }


def test_sanitize_codex_payload_compact_strips_store() -> None:
    payload = {
        "model": "gpt-4.1",
        "input": "hi",
        "store": True,
        "response_format": {"type": "json_schema"},
    }
    sanitized = _sanitize_codex_payload(payload, compact=True)
    assert sanitized == {
        "model": "gpt-4.1",
        "input": "hi",
        "instructions": "",
    }


def test_sanitize_codex_payload_forces_store_false_for_non_compact() -> None:
    payload = {
        "model": "gpt-4.1",
        "input": "hi",
        "store": True,
    }
    sanitized = _sanitize_codex_payload(payload)
    assert sanitized == {
        "model": "gpt-4.1",
        "input": "hi",
        "store": False,
        "instructions": "",
    }


def test_sanitize_codex_payload_can_preserve_previous_response_id() -> None:
    payload = {
        "model": "gpt-image-2",
        "input": "hi",
        "previous_response_id": "resp_prev_123",
    }
    sanitized = _sanitize_codex_payload(payload, preserve_previous_response_id=True)
    assert sanitized == {
        "model": "gpt-image-2",
        "input": "hi",
        "previous_response_id": "resp_prev_123",
        "store": False,
        "instructions": "",
    }


def test_build_upstream_headers_uses_current_codex_user_agent_default() -> None:
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/responses",
            "headers": [],
        }
    )

    headers = _build_upstream_headers(request, access_token="access-token", account_id="account-1", stream=False)

    assert headers["Authorization"] == "Bearer access-token"
    assert headers["Accept"] == "application/json"
    assert headers["Version"] == "0.125.0"
    assert headers["User-Agent"] == "codex_cli_rs/0.125.0"


def test_build_upstream_headers_overrides_stale_client_codex_version() -> None:
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/responses",
            "headers": [(b"version", b"0.21.0"), (b"user-agent", b"yaak")],
        }
    )

    headers = _build_upstream_headers(request, access_token="access-token", account_id=None, stream=True)

    assert headers["Version"] == "0.125.0"
    assert headers["User-Agent"] == "yaak"


def test_translate_responses_image_compat_payload_injects_image_tool() -> None:
    translated, response_model_alias = _translate_responses_image_compat_payload(
        {
            "model": "gpt-image-2",
            "input": [{"role": "user", "content": "Draw a mug"}],
            "previous_response_id": "resp_prev_123",
            "include": ["custom.include"],
            "size": "1024x1024",
            "output_format": "png",
        },
        compact=False,
    )

    assert response_model_alias == "gpt-image-2"
    assert translated["model"] == "gpt-5.5"
    assert translated["previous_response_id"] == "resp_prev_123"
    assert translated["store"] is False
    assert translated["parallel_tool_calls"] is True
    assert translated["reasoning"] == {"effort": "medium", "summary": "auto"}
    assert translated["include"] == ["custom.include", "reasoning.encrypted_content"]
    assert "tool_choice" not in translated
    assert translated["tools"] == [
        {
            "type": "image_generation",
            "model": "gpt-image-2",
            "action": "auto",
            "size": "1024x1024",
            "output_format": "png",
        }
    ]
    assert "size" not in translated
    assert "output_format" not in translated


def test_translate_responses_image_compat_payload_rejects_compact() -> None:
    with pytest.raises(HTTPException, match="gpt-image-2 is only supported on /v1/responses"):
        _translate_responses_image_compat_payload(
            {
                "model": "gpt-image-2",
                "input": "Draw a mug",
            },
            compact=True,
        )


def test_translate_responses_image_compat_payload_rejects_too_many_input_images(monkeypatch) -> None:
    monkeypatch.setenv("IMAGE_INPUT_MAX_PER_REQUEST", "2")

    with pytest.raises(HTTPException) as exc_info:
        _translate_responses_image_compat_payload(
            {
                "model": "gpt-image-2",
                "input": [
                    {
                        "type": "message",
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": "edit these"},
                            {"type": "input_image", "image_url": "data:image/png;base64,one"},
                            {"type": "input_image", "image_url": "data:image/png;base64,two"},
                            {"type": "input_image", "image_url": "data:image/png;base64,three"},
                        ],
                    }
                ],
            },
            compact=False,
        )

    assert exc_info.value.status_code == 400
    assert "Too many input images: 3" in str(exc_info.value.detail)


def test_chat_completions_request_to_responses_request_parses_markdown_image_history(monkeypatch) -> None:
    async def fake_resolve_chat_image_output_items(*, scope_hash: str, client_model: str, image_urls: list[str]):
        assert client_model == "gpt-image-2"
        assert image_urls == ["data:image/png;base64,prev-image"]
        return [
            SimpleNamespace(
                image_call_id="ig_prev",
                output_item={
                    "type": "image_generation_call",
                    "id": "ig_prev",
                    "status": "completed",
                    "action": "generate",
                    "background": "opaque",
                    "quality": "medium",
                    "size": "1024x1024",
                    "result": "prev-image",
                    "output_format": "png",
                }
            )
        ]

    monkeypatch.setattr(
        "oaix_gateway.api_server.resolve_chat_image_output_items",
        fake_resolve_chat_image_output_items,
    )

    request_data = ChatCompletionsRequest(
        model="gpt-image-2",
        messages=[
            {
                "role": "assistant",
                "content": "Here is the previous render.\n\n![image](data:image/png;base64,prev-image)",
            },
            {
                "role": "user",
                "content": "Make the mug blue",
            },
        ],
        stream=False,
        size="1024x1024",
        output_format="png",
    )

    responses_request = asyncio.run(_chat_completions_request_to_responses_request(request_data))
    payload = responses_request.model_dump(exclude_unset=True)

    assert payload["model"] == "gpt-image-2"
    assert payload["size"] == "1024x1024"
    assert payload["output_format"] == "png"
    assert payload["input"] == [
        {
            "type": "message",
            "role": "assistant",
            "content": [
                {"type": "output_text", "text": "Here is the previous render.\n\n"},
            ],
        },
        {
            "type": "image_generation_call",
            "id": "ig_prev",
            "result": "prev-image",
            "status": "completed",
        },
        {
            "type": "message",
            "role": "user",
            "content": [
                {"type": "input_text", "text": "Make the mug blue"},
            ],
        },
    ]


def test_resolved_chat_image_to_responses_input_item_uses_schema_legal_subset() -> None:
    translated = _resolved_chat_image_to_responses_input_item(
        SimpleNamespace(
            image_call_id="ig_prev_subset",
            output_item={
                "type": "image_generation_call",
                "id": "ig_prev_subset",
                "action": "generate",
                "background": "opaque",
                "quality": "medium",
                "result": "prev-image",
                "output_format": "png",
            },
        )
    )

    assert translated == {
        "type": "image_generation_call",
        "id": "ig_prev_subset",
        "result": "prev-image",
        "status": "completed",
    }


def test_chat_completions_request_to_responses_request_rejects_unresolved_assistant_image_history(
    monkeypatch,
) -> None:
    async def fake_resolve_chat_image_output_items(*, scope_hash: str, client_model: str, image_urls: list[str]):
        return [None]

    monkeypatch.setattr(
        "oaix_gateway.api_server.resolve_chat_image_output_items",
        fake_resolve_chat_image_output_items,
    )

    request_data = ChatCompletionsRequest(
        model="gpt-image-2",
        messages=[
            {
                "role": "assistant",
                "content": "![image](data:image/png;base64,prev-image)",
            },
            {
                "role": "user",
                "content": "Make the mug blue",
            },
        ],
        stream=False,
    )

    with pytest.raises(HTTPException, match="Assistant image history could not be resolved"):
        asyncio.run(_chat_completions_request_to_responses_request(request_data))


def test_chat_completions_request_to_responses_request_resolves_assistant_image_url_history(
    monkeypatch,
) -> None:
    async def fake_resolve_chat_image_output_items(*, scope_hash: str, client_model: str, image_urls: list[str]):
        assert image_urls == ["data:image/png;base64,prev-image"]
        return [
            SimpleNamespace(
                image_call_id="ig_prev_url",
                output_item={
                    "type": "image_generation_call",
                    "id": "ig_prev_url",
                    "status": "completed",
                    "action": "generate",
                    "background": "opaque",
                    "quality": "medium",
                    "result": "prev-image",
                    "output_format": "png",
                }
            )
        ]

    monkeypatch.setattr(
        "oaix_gateway.api_server.resolve_chat_image_output_items",
        fake_resolve_chat_image_output_items,
    )

    request_data = ChatCompletionsRequest(
        model="gpt-image-2",
        messages=[
            {
                "role": "assistant",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": "data:image/png;base64,prev-image"},
                    }
                ],
            },
            {
                "role": "user",
                "content": "杯子变成红色",
            },
        ],
        stream=False,
    )

    responses_request = asyncio.run(_chat_completions_request_to_responses_request(request_data))
    payload = responses_request.model_dump(exclude_unset=True)

    assert payload["input"] == [
        {
            "type": "image_generation_call",
            "id": "ig_prev_url",
            "result": "prev-image",
            "status": "completed",
        },
        {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "杯子变成红色"}],
        },
    ]


def test_parse_images_generations_request_builds_image_tool_payload() -> None:
    request = _make_json_request(
        "/v1/images/generations",
        {
            "prompt": "draw a cat astronaut",
            "size": "1024x1024",
            "output_format": "webp",
            "response_format": "url",
            "stream": True,
        },
    )

    image_request = asyncio.run(_parse_images_generations_request(request))

    assert image_request.model_name == "gpt-image-2"
    assert image_request.response_format == "url"
    assert image_request.stream is True
    assert image_request.stream_prefix == "image_generation"
    assert image_request.responses_payload["model"] == "gpt-5.5"
    assert image_request.responses_payload["stream"] is True
    assert "tool_choice" not in image_request.responses_payload
    assert image_request.responses_payload["tools"][0] == {
        "type": "image_generation",
        "action": "generate",
        "model": "gpt-image-2",
        "size": "1024x1024",
        "output_format": "webp",
    }
    assert image_request.responses_payload["input"][0]["content"] == [
        {"type": "input_text", "text": "draw a cat astronaut"}
    ]


def test_parse_images_edits_request_builds_image_inputs_and_mask() -> None:
    request = _make_json_request(
        "/v1/images/edits",
        {
            "prompt": "replace the sky",
            "model": "gpt-image-2",
            "images": [
                {"image_url": "data:image/png;base64,abc"},
                {"image_url": {"url": "https://example.com/second.png"}},
            ],
            "mask": {"image_url": "https://example.com/mask.png"},
            "output_format": "png",
        },
    )

    image_request = asyncio.run(_parse_images_edits_request(request))

    assert image_request.model_name == "gpt-image-2"
    assert image_request.stream is False
    assert image_request.stream_prefix == "image_edit"
    assert image_request.responses_payload["tools"][0] == {
        "type": "image_generation",
        "action": "edit",
        "model": "gpt-image-2",
        "output_format": "png",
        "input_image_mask": {"image_url": "https://example.com/mask.png"},
    }
    assert image_request.responses_payload["input"][0]["content"] == [
        {"type": "input_text", "text": "replace the sky"},
        {"type": "input_image", "image_url": "data:image/png;base64,abc"},
        {"type": "input_image", "image_url": "https://example.com/second.png"},
    ]


def test_parse_images_edits_request_rejects_too_many_input_images(monkeypatch) -> None:
    monkeypatch.setenv("IMAGE_INPUT_MAX_PER_REQUEST", "2")
    request = _make_json_request(
        "/v1/images/edits",
        {
            "prompt": "replace the sky",
            "images": [
                {"image_url": "data:image/png;base64,abc"},
                {"image_url": "https://example.com/second.png"},
            ],
            "mask": {"image_url": "https://example.com/mask.png"},
        },
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(_parse_images_edits_request(request))

    assert exc_info.value.status_code == 400
    assert "Too many input images: 3" in str(exc_info.value.detail)


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


def test_responses_failure_http_exception_marks_moderation_blocked_nonretryable() -> None:
    exc = _responses_failure_http_exception(
        {
            "type": "response.failed",
            "response": {
                "status": "failed",
                "error": {
                    "type": "image_generation_user_error",
                    "code": "moderation_blocked",
                    "message": "Your request was rejected by the safety system.",
                },
            },
        }
    )

    assert exc is not None
    assert exc.status_code == 400
    assert getattr(exc, "retryable", None) is False
    assert getattr(exc, "record_token_error", None) is False
    assert json.loads(exc.detail)["error"]["code"] == "moderation_blocked"


def test_upstream_error_http_exception_marks_plain_safety_rejection_nonretryable() -> None:
    detail = "Your request was rejected by the safety system. Include request ID 701166b1."

    exc = _upstream_error_http_exception(500, detail)

    assert exc.status_code == 400
    assert exc.detail == detail
    assert getattr(exc, "retryable", None) is False
    assert getattr(exc, "record_token_error", None) is False


def test_upstream_error_http_exception_maps_wrapped_moderation_detail_to_bad_request() -> None:
    detail = json.dumps(
        {
            "detail": json.dumps(
                {
                    "error": {
                        "type": "image_generation_user_error",
                        "code": "moderation_blocked",
                        "message": "Your request was rejected by the safety system. safety_violations=[sexual].",
                        "param": None,
                    }
                }
            )
        }
    )

    exc = _upstream_error_http_exception(500, detail)

    assert exc.status_code == 400
    assert exc.detail == detail
    assert getattr(exc, "retryable", None) is False
    assert getattr(exc, "record_token_error", None) is False


def test_prime_responses_upstream_stream_commits_after_first_non_prefight_event() -> None:
    async def upstream() -> AsyncIterator[bytes]:
        yield b'event: response.created\ndata: {"type":"response.created","response":{"model":"gpt-5.4"}}\n\n'
        yield b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"hi"}\n\n'

    primed_stream = asyncio.run(_prime_responses_upstream_stream(upstream()))

    assert primed_stream.stream_committed is True
    assert primed_stream.first_token_observed is True
    assert primed_stream.emit_initial_keepalive is False
    assert primed_stream.model_name == "gpt-5.4"
    assert primed_stream.buffered_chunks == [
        b'event: response.created\ndata: {"type":"response.created","response":{"model":"gpt-5.4"}}\n\n',
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


def test_prime_responses_upstream_stream_times_out_into_initial_keepalive() -> None:
    async def upstream() -> AsyncIterator[bytes]:
        yield b'event: response.created\ndata: {"type":"response.created","response":{"model":"gpt-5.4-mini"}}\n\n'
        await asyncio.sleep(0.02)
        yield b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"hello"}\n\n'

    primed_stream = asyncio.run(
        _prime_responses_upstream_stream(
            upstream(),
            heartbeat_interval_seconds=0.001,
        )
    )

    assert primed_stream.stream_committed is True
    assert primed_stream.first_token_observed is False
    assert primed_stream.emit_initial_keepalive is True
    assert primed_stream.model_name == "gpt-5.4-mini"
    assert primed_stream.buffered_chunks == [
        b'event: response.created\ndata: {"type":"response.created","response":{"model":"gpt-5.4-mini"}}\n\n'
    ]
    assert primed_stream.pending_chunk_task is not None


def test_stream_keepalive_interval_defaults_to_30_seconds(monkeypatch) -> None:
    monkeypatch.delenv("STREAM_KEEPALIVE_INTERVAL_SECONDS", raising=False)

    assert _stream_keepalive_interval_seconds() == 30.0


def test_iterate_with_sse_keepalive_emits_comment_before_delayed_chunk() -> None:
    async def upstream() -> AsyncIterator[bytes]:
        await asyncio.sleep(0.01)
        yield b"data: payload\n\n"

    async def collect() -> list[bytes]:
        chunks: list[bytes] = []
        async for chunk in _iterate_with_sse_keepalive(
            upstream(),
            heartbeat_interval_seconds=0.001,
        ):
            chunks.append(chunk)
        return chunks

    chunks = asyncio.run(collect())

    assert chunks[0].startswith(b": keepalive")
    assert chunks[-1] == b"data: payload\n\n"


def test_wrap_sse_stream_with_initial_keepalive_emits_comment_before_first_item() -> None:
    async def upstream() -> AsyncIterator[bytes]:
        await asyncio.sleep(0.01)
        yield b"data: payload\n\n"

    async def collect() -> list[bytes]:
        chunks: list[bytes] = []
        async for chunk in _wrap_sse_stream_with_initial_keepalive(
            upstream(),
            heartbeat_interval_seconds=0.001,
        ):
            chunks.append(chunk)
            if chunk == b"data: payload\n\n":
                break
        return chunks

    chunks = asyncio.run(collect())

    assert chunks[0].startswith(b": keepalive")
    assert chunks[-1] == b"data: payload\n\n"


def test_wrap_sse_stream_with_initial_keepalive_emits_error_event_on_http_exception() -> None:
    async def upstream() -> AsyncIterator[bytes]:
        raise HTTPException(status_code=429, detail="Too many requests")
        yield b""

    async def collect() -> bytes:
        chunks: list[bytes] = []
        async for chunk in _wrap_sse_stream_with_initial_keepalive(
            upstream(),
            heartbeat_interval_seconds=0.001,
        ):
            chunks.append(chunk)
        return b"".join(chunks)

    body = asyncio.run(collect()).decode("utf-8")

    assert '"status_code":429' in body.replace(" ", "")
    assert "Too many requests" in body
    assert "data: [DONE]" in body


def test_stream_responses_to_chat_completions_forwards_keepalive_comment() -> None:
    async def upstream() -> AsyncIterator[bytes]:
        yield b": keepalive\n\n"
        yield b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"hello"}\n\n'
        yield b"data: [DONE]\n\n"

    async def collect() -> str:
        chunks: list[bytes] = []
        async for chunk in _stream_responses_to_chat_completions(
            upstream(),
            request_model="gpt-image-2",
        ):
            chunks.append(chunk)
        return b"".join(chunks).decode("utf-8")

    body = asyncio.run(collect())

    assert ": keepalive" in body
    assert '"content": "hello"' in body
    assert "data: [DONE]" in body


def test_stream_responses_to_chat_completions_rewrites_error_event_as_data_frame() -> None:
    async def upstream() -> AsyncIterator[bytes]:
        yield _build_stream_error_event(502, "Upstream image stream aborted before completion")
        yield b"data: [DONE]\n\n"

    async def collect() -> str:
        chunks: list[bytes] = []
        async for chunk in _stream_responses_to_chat_completions(
            upstream(),
            request_model="gpt-image-2",
        ):
            chunks.append(chunk)
        return b"".join(chunks).decode("utf-8")

    body = asyncio.run(collect())

    assert "event: error" not in body
    assert '"message": "Upstream image stream aborted before completion"' in body
    assert "data: [DONE]" in body


def test_stream_responses_to_chat_completions_synthesizes_image_from_output_item_done() -> None:
    async def upstream() -> AsyncIterator[bytes]:
        yield (
            b'event: response.created\n'
            b'data: {"type":"response.created","response":{"id":"resp_img_done","status":"in_progress","model":"gpt-image-2","created_at":1710000000}}\n\n'
        )
        yield (
            b'event: response.output_item.done\n'
            b'data: {"type":"response.output_item.done","output_index":0,"item":{"type":"image_generation_call","id":"ig_done","status":"completed","result":"done-image","output_format":"png"}}\n\n'
        )
        yield b"data: [DONE]\n\n"

    async def collect() -> str:
        chunks: list[bytes] = []
        async for chunk in _stream_responses_to_chat_completions(
            upstream(),
            request_model="gpt-image-2",
        ):
            chunks.append(chunk)
        return b"".join(chunks).decode("utf-8")

    body = asyncio.run(collect())

    assert "data:image/png;base64,done-image" in body
    assert '"finish_reason":"stop"' in body.replace(" ", "")
    assert body.rstrip().endswith("data: [DONE]")


def test_stream_responses_to_chat_completions_emits_usage_on_terminal_chunk() -> None:
    async def upstream() -> AsyncIterator[bytes]:
        yield (
            b'event: response.created\n'
            b'data: {"type":"response.created","response":{"id":"resp_usage_chunk","status":"in_progress","model":"gpt-5.4-mini","created_at":1710000000}}\n\n'
        )
        yield (
            b'event: response.completed\n'
            b'data: {"type":"response.completed","response":{"id":"resp_usage_chunk","status":"completed","model":"gpt-5.4-mini","created_at":1710000000,"output":[{"type":"message","content":[{"type":"output_text","text":"hello"}]}],"usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}}}\n\n'
        )
        yield b"data: [DONE]\n\n"

    async def collect() -> list[dict[str, object]]:
        frames: list[dict[str, object]] = []
        async for chunk in _stream_responses_to_chat_completions(
            upstream(),
            request_model="gpt-5.4-mini",
        ):
            text = chunk.decode("utf-8").strip()
            if not text.startswith("data: ") or text == "data: [DONE]":
                continue
            frames.append(json.loads(text[6:]))
        return frames

    frames = asyncio.run(collect())

    assert len(frames) == 2
    assert frames[0]["choices"][0]["delta"]["content"] == "hello"
    assert frames[1]["choices"][0]["finish_reason"] == "stop"
    assert frames[1]["usage"] == {
        "prompt_tokens": 2,
        "completion_tokens": 3,
        "total_tokens": 5,
    }


def test_stream_upstream_response_image_compat_emits_keepalive_comment(monkeypatch) -> None:
    class DummyStreamContext:
        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    async def upstream() -> AsyncIterator[bytes]:
        await asyncio.sleep(0.01)
        yield (
            b'event: response.completed\n'
            b'data: {"type":"response.completed","response":{"id":"resp_keepalive","status":"completed","model":"gpt-5.4-mini","output":[]}}\n\n'
        )
        yield b"data: [DONE]\n\n"

    async def collect() -> str:
        chunks: list[bytes] = []
        async for chunk in _stream_upstream_response(
            DummyStreamContext(),
            upstream(),
            [],
            stream_committed=True,
            response_model_alias="gpt-image-2",
        ):
            chunks.append(chunk)
        return b"".join(chunks).decode("utf-8")

    monkeypatch.setattr("oaix_gateway.api_server._stream_keepalive_interval_seconds", lambda: 0.001)
    body = asyncio.run(collect())

    assert ": keepalive" in body
    assert '"model": "gpt-image-2"' in body
    assert "data: [DONE]" in body


def test_stream_upstream_response_image_compat_emits_initial_keepalive_after_prime_timeout() -> None:
    class DummyStreamContext:
        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    async def upstream() -> AsyncIterator[bytes]:
        yield b'event: response.created\ndata: {"type":"response.created","response":{"model":"gpt-5.4-mini"}}\n\n'
        await asyncio.sleep(0.02)
        yield (
            b'event: response.completed\n'
            b'data: {"type":"response.completed","response":{"id":"resp_prime_keepalive","status":"completed","model":"gpt-5.4-mini","output":[]}}\n\n'
        )
        yield b"data: [DONE]\n\n"

    async def collect() -> str:
        upstream_iter = upstream()
        primed_stream = await _prime_responses_upstream_stream(
            upstream_iter,
            heartbeat_interval_seconds=0.001,
        )
        chunks: list[bytes] = []
        async for chunk in _stream_upstream_response(
            DummyStreamContext(),
            upstream_iter=upstream_iter,
            buffered_chunks=primed_stream.buffered_chunks,
            stream_committed=primed_stream.stream_committed,
            response_model_alias="gpt-image-2",
            emit_initial_keepalive=primed_stream.emit_initial_keepalive,
            pending_chunk_task=primed_stream.pending_chunk_task,
        ):
            chunks.append(chunk)
        return b"".join(chunks).decode("utf-8")

    body = asyncio.run(collect())

    assert body.startswith(": keepalive")
    assert '"model": "gpt-image-2"' in body
    assert "data: [DONE]" in body


def test_stream_upstream_response_image_compat_emits_error_event_on_network_abort() -> None:
    class DummyStreamContext:
        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    async def upstream() -> AsyncIterator[bytes]:
        raise httpx.RemoteProtocolError("incomplete chunked read")
        yield b""

    capture = _ProxyStreamCapture(initial_model_name="gpt-image-2")

    async def collect() -> str:
        chunks: list[bytes] = []
        async for chunk in _stream_upstream_response(
            DummyStreamContext(),
            upstream(),
            [],
            stream_committed=True,
            stream_capture=capture,
            response_model_alias="gpt-image-2",
        ):
            chunks.append(chunk)
        return b"".join(chunks).decode("utf-8")

    body = asyncio.run(collect())

    assert "event: error" in body
    assert "Upstream image stream aborted before completion" in body
    assert "data: [DONE]" in body
    assert capture.completed is False
    assert capture.error_status_code == 502
    assert capture.error_message is not None
    assert "RemoteProtocolError" in capture.error_message


def test_stream_upstream_response_emits_error_event_on_network_abort() -> None:
    class DummyStreamContext:
        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    async def upstream() -> AsyncIterator[bytes]:
        raise httpx.RemoteProtocolError("incomplete chunked read")
        yield b""

    capture = _ProxyStreamCapture(initial_model_name="gpt-5.4")

    async def collect() -> str:
        chunks: list[bytes] = []
        async for chunk in _stream_upstream_response(
            DummyStreamContext(),
            upstream(),
            [],
            stream_committed=True,
            stream_capture=capture,
        ):
            chunks.append(chunk)
        return b"".join(chunks).decode("utf-8")

    body = asyncio.run(collect())

    assert "event: error" in body
    assert "Upstream stream aborted before completion" in body
    assert "data: [DONE]" in body
    assert capture.completed is False
    assert capture.error_status_code == 502
    assert capture.error_message is not None
    assert "RemoteProtocolError" in capture.error_message


def test_collect_responses_json_from_sse_merges_response_and_output_text() -> None:
    async def upstream() -> AsyncIterator[bytes]:
        yield (
            b'event: response.created\n'
            b'data: {"type":"response.created","response":{"id":"resp_123","status":"in_progress","model":"gpt-5.4"}}\n\n'
        )
        yield b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"hello"}\n\n'
        yield (
            b'event: response.completed\n'
            b'data: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}\n\n'
        )

    response, first_token_at = asyncio.run(
        _collect_responses_json_from_sse(upstream(), model="gpt-5.4")
    )

    assert first_token_at is not None
    assert response["id"] == "resp_123"
    assert response["status"] == "completed"
    assert response["model"] == "gpt-5.4"
    assert response["usage"]["total_tokens"] == 2
    assert response["output"][0]["content"][0]["text"] == "hello"


def test_collect_responses_json_from_sse_falls_back_on_done_without_response_completed() -> None:
    async def upstream() -> AsyncIterator[bytes]:
        yield b'event: response.created\ndata: {"type":"response.created"}\n\n'
        yield b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"hello"}\n\n'
        yield b"data: [DONE]\n\n"

    response, first_token_at = asyncio.run(
        _collect_responses_json_from_sse(upstream(), model="gpt-5.4")
    )

    assert first_token_at is not None
    assert response["status"] == "completed"
    assert response["model"] == "gpt-5.4"
    assert response["output"][0]["content"][0]["text"] == "hello"


def test_collect_responses_json_from_sse_patches_output_item_done() -> None:
    async def upstream() -> AsyncIterator[bytes]:
        yield (
            b'event: response.created\n'
            b'data: {"type":"response.created","response":{"id":"resp_img_compat","status":"in_progress","model":"gpt-image-2"}}\n\n'
        )
        yield (
            b'event: response.output_item.done\n'
            b'data: {"type":"response.output_item.done","output_index":0,"item":{"type":"image_generation_call","result":"patched-b64","output_format":"png"}}\n\n'
        )
        yield (
            b'event: response.completed\n'
            b'data: {"type":"response.completed","response":{"id":"resp_img_compat","status":"completed","output":[],"usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}}}\n\n'
        )

    response, first_token_at = asyncio.run(
        _collect_responses_json_from_sse(upstream(), model="gpt-image-2")
    )

    assert first_token_at is not None
    assert response["id"] == "resp_img_compat"
    assert response["model"] == "gpt-image-2"
    assert response["output"][0]["type"] == "image_generation_call"
    assert response["output"][0]["result"] == "patched-b64"


def test_collect_images_api_response_from_sse_returns_openai_images_shape() -> None:
    async def upstream() -> AsyncIterator[bytes]:
        yield (
            b'event: response.created\n'
            b'data: {"type":"response.created","response":{"id":"resp_img_123","status":"in_progress"}}\n\n'
        )
        yield (
            b'event: response.completed\n'
            b'data: {"type":"response.completed","response":{"created_at":1710000000,"status":"completed","output":[{"type":"image_generation_call","result":"abc123","output_format":"png","revised_prompt":"a refined prompt","size":"1024x1024","background":"transparent","quality":"high"}],"tool_usage":{"image_gen":{"images":1}}}}\n\n'
        )

    response, first_token_at = asyncio.run(
        _collect_images_api_response_from_sse(upstream(), response_format="b64_json")
    )

    assert first_token_at is not None
    assert response == {
        "created": 1710000000,
        "data": [{"b64_json": "abc123", "revised_prompt": "a refined prompt"}],
        "background": "transparent",
        "output_format": "png",
        "quality": "high",
        "size": "1024x1024",
        "usage": {"images": 1},
    }


def test_collect_images_api_response_from_sse_patches_output_item_done() -> None:
    async def upstream() -> AsyncIterator[bytes]:
        yield (
            b'event: response.output_item.done\n'
            b'data: {"type":"response.output_item.done","output_index":0,"item":{"type":"image_generation_call","result":"patched-img","output_format":"png","revised_prompt":"patched prompt","size":"1024x1024"}}\n\n'
        )
        yield (
            b'event: response.completed\n'
            b'data: {"type":"response.completed","response":{"created_at":1710000005,"status":"completed","output":[],"tool_usage":{"image_gen":{"images":1}}}}\n\n'
        )

    response, first_token_at = asyncio.run(
        _collect_images_api_response_from_sse(upstream(), response_format="b64_json")
    )

    assert first_token_at is not None
    assert response == {
        "created": 1710000005,
        "data": [{"b64_json": "patched-img", "revised_prompt": "patched prompt"}],
        "output_format": "png",
        "size": "1024x1024",
        "usage": {"images": 1},
    }


def test_collect_images_api_response_from_sse_marks_empty_output_retryable() -> None:
    async def upstream() -> AsyncIterator[bytes]:
        yield (
            b'event: response.completed\n'
            b'data: {"type":"response.completed","response":{"created_at":1710000007,"status":"completed","output":[]}}\n\n'
        )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(_collect_images_api_response_from_sse(upstream(), response_format="b64_json"))

    exc = exc_info.value
    assert exc.status_code == 502
    assert getattr(exc, "retryable", False) is True
    assert getattr(exc, "record_token_error", False) is True
    assert "Upstream did not return image output" in str(exc.detail)


def test_stream_upstream_image_response_emits_keepalive_comment() -> None:
    class DummyStreamContext:
        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    async def upstream() -> AsyncIterator[bytes]:
        await asyncio.sleep(0.01)
        yield (
            b'event: response.completed\n'
            b'data: {"type":"response.completed","response":{"id":"resp_img_keepalive","status":"completed","output":[{"type":"image_generation_call","result":"img-b64","output_format":"png"}]}}\n\n'
        )
        yield b"data: [DONE]\n\n"

    async def collect() -> str:
        chunks: list[bytes] = []
        async for chunk in _stream_upstream_image_response(
            DummyStreamContext(),
            upstream(),
            [],
            stream_committed=True,
            response_format="b64_json",
            stream_prefix="image_generation",
            heartbeat_interval_seconds=0.001,
        ):
            chunks.append(chunk)
        return b"".join(chunks).decode("utf-8")

    body = asyncio.run(collect())

    assert ": keepalive" in body
    assert "event: image_generation.completed" in body
    assert '"b64_json": "img-b64"' in body


def test_stream_upstream_image_response_emits_initial_keepalive_after_prime_timeout() -> None:
    class DummyStreamContext:
        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    async def upstream() -> AsyncIterator[bytes]:
        yield b'event: response.created\ndata: {"type":"response.created","response":{"model":"gpt-5.4-mini"}}\n\n'
        await asyncio.sleep(0.02)
        yield (
            b'event: response.completed\n'
            b'data: {"type":"response.completed","response":{"id":"resp_img_prime_keepalive","status":"completed","output":[{"type":"image_generation_call","result":"img-b64","output_format":"png"}]}}\n\n'
        )
        yield b"data: [DONE]\n\n"

    async def collect() -> str:
        upstream_iter = upstream()
        primed_stream = await _prime_responses_upstream_stream(
            upstream_iter,
            heartbeat_interval_seconds=0.001,
        )
        chunks: list[bytes] = []
        async for chunk in _stream_upstream_image_response(
            DummyStreamContext(),
            upstream_iter=upstream_iter,
            buffered_chunks=primed_stream.buffered_chunks,
            stream_committed=primed_stream.stream_committed,
            response_format="b64_json",
            stream_prefix="image_generation",
            heartbeat_interval_seconds=0.001,
            emit_initial_keepalive=primed_stream.emit_initial_keepalive,
            pending_chunk_task=primed_stream.pending_chunk_task,
        ):
            chunks.append(chunk)
        return b"".join(chunks).decode("utf-8")

    body = asyncio.run(collect())

    assert body.startswith(": keepalive")
    assert "event: image_generation.completed" in body
    assert '"b64_json": "img-b64"' in body


def test_stream_upstream_image_response_emits_error_event_on_network_abort() -> None:
    class DummyStreamContext:
        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    async def upstream() -> AsyncIterator[bytes]:
        raise httpx.RemoteProtocolError("incomplete chunked read")
        yield b""

    capture = _ProxyStreamCapture(initial_model_name="gpt-image-2")

    async def collect() -> str:
        chunks: list[bytes] = []
        async for chunk in _stream_upstream_image_response(
            DummyStreamContext(),
            upstream(),
            [],
            stream_committed=True,
            response_format="b64_json",
            stream_prefix="image_generation",
            stream_capture=capture,
        ):
            chunks.append(chunk)
        return b"".join(chunks).decode("utf-8")

    body = asyncio.run(collect())

    assert "event: error" in body
    assert "Upstream image stream aborted before completion" in body
    assert "data: [DONE]" in body
    assert capture.completed is False
    assert capture.error_status_code == 502
    assert capture.error_message is not None
    assert "RemoteProtocolError" in capture.error_message


def test_collect_images_api_response_from_sse_enforces_read_timeout() -> None:
    async def upstream() -> AsyncIterator[bytes]:
        await asyncio.sleep(0.05)
        if False:
            yield b""

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            _collect_images_api_response_from_sse(
                upstream(),
                response_format="b64_json",
                total_timeout_seconds=1.0,
                read_timeout_seconds=0.01,
                disconnect_poll_seconds=0.005,
            )
        )

    exc = exc_info.value
    assert exc.status_code == 504
    assert getattr(exc, "retryable", False) is True
    assert getattr(exc, "record_token_error", False) is True
    assert "read timeout" in str(exc.detail)


def test_collect_images_api_response_from_sse_aborts_on_client_disconnect() -> None:
    class DisconnectingRequest:
        def __init__(self) -> None:
            self.calls = 0

        async def is_disconnected(self) -> bool:
            self.calls += 1
            return self.calls >= 1

    async def upstream() -> AsyncIterator[bytes]:
        await asyncio.sleep(0.05)
        if False:
            yield b""

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            _collect_images_api_response_from_sse(
                upstream(),
                response_format="b64_json",
                http_request=DisconnectingRequest(),
                total_timeout_seconds=1.0,
                read_timeout_seconds=1.0,
                disconnect_poll_seconds=0.005,
            )
        )

    exc = exc_info.value
    assert exc.status_code == 499
    assert getattr(exc, "retryable", True) is False
    assert getattr(exc, "record_token_error", True) is False
    assert "Client disconnected" in str(exc.detail)


def test_serialize_admin_token_item_includes_created_at(monkeypatch) -> None:
    created_at = datetime(2026, 4, 15, 9, 30, tzinfo=timezone.utc)
    token = CodexToken(
        id=7,
        email="user@example.com",
        account_id="acct_123",
        refresh_token="refresh_token",
        is_active=True,
    )
    token.created_at = created_at
    _TOKEN_ACTIVE_REQUESTS.clear()
    _TOKEN_ACTIVE_REQUESTS[token.id] = 3
    monkeypatch.setenv("FILL_FIRST_TOKEN_ACTIVE_STREAM_CAP", "10")

    item = _serialize_admin_token_item(
        token,
        plan_info=CodexPlanInfo(
            chatgpt_account_id=None,
            plan_type="plus",
            subscription_active_start=None,
            subscription_active_until=None,
        ),
        quota_snapshot=None,
        observed_cost_usd=None,
    )

    assert item["created_at"] == created_at
    assert item["active_streams"] == 3
    assert item["active_stream_cap"] == 10
    _TOKEN_ACTIVE_REQUESTS.clear()


def test_claim_next_active_token_for_request_skips_saturated_fill_first_token(monkeypatch) -> None:
    tokens = {
        11: SimpleNamespace(id=11),
        12: SimpleNamespace(id=12),
    }
    calls: list[set[int]] = []

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None, token_order=None):
        del selection_strategy
        excluded = {int(token_id) for token_id in (exclude_token_ids or ())}
        calls.append(excluded)
        for token_id in token_order or ():
            if int(token_id) not in excluded:
                return tokens[int(token_id)]
        return None

    monkeypatch.setenv("FILL_FIRST_TOKEN_ACTIVE_STREAM_CAP", "2")
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    _TOKEN_ACTIVE_REQUESTS.clear()
    _TOKEN_ACTIVE_REQUESTS[11] = 2
    timing = _RequestTimingRecorder()

    result = asyncio.run(
        _claim_next_active_token_for_request(
            TokenSelectionSettings(
                strategy=TOKEN_SELECTION_STRATEGY_FILL_FIRST,
                token_order=(11, 12),
                active_stream_cap=2,
            ),
            exclude_token_ids=set(),
            timing=timing,
        )
    )

    assert result is tokens[12]
    assert 11 in calls[0]
    assert _TOKEN_ACTIVE_REQUESTS[11] == 2
    assert _TOKEN_ACTIVE_REQUESTS[12] == 1
    assert timing.snapshot()["selected_token_active_streams"] == 0
    _TOKEN_ACTIVE_REQUESTS.clear()


def test_claim_next_active_token_for_request_does_not_db_fallback_when_snapshot_exists(monkeypatch) -> None:
    async def fake_claim_next_active_token(**kwargs):
        del kwargs
        raise AssertionError("request hot path should not query DB when a token pool snapshot exists")

    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    timing = _RequestTimingRecorder()
    snapshot = SimpleNamespace(tokens=(), scoped_cooldowns={}, loaded_at=0.0)

    result = asyncio.run(
        _claim_next_active_token_for_request(
            TokenSelectionSettings(
                strategy=TOKEN_SELECTION_STRATEGY_FILL_FIRST,
                token_order=(11, 12),
                active_stream_cap=2,
            ),
            exclude_token_ids=set(),
            timing=timing,
            token_pool_snapshot=snapshot,
        )
    )

    assert result is None
    spans = timing.snapshot()
    assert spans["claim_token_source"] == "memory"
    assert spans["claim_token_memory_miss_count"] == 1


def test_claim_next_token_for_model_request_prefilters_free_snapshot_tokens() -> None:
    free_token = SimpleNamespace(
        id=21,
        plan_type="free",
        refresh_token="rt_free",
        token_type="codex",
        is_active=True,
        cooldown_until=None,
        merged_into_token_id=None,
    )
    plus_token = SimpleNamespace(
        id=22,
        plan_type="plus",
        refresh_token="rt_plus",
        token_type="codex",
        is_active=True,
        cooldown_until=None,
        merged_into_token_id=None,
    )
    snapshot = SimpleNamespace(
        tokens=(free_token, plus_token),
        scoped_cooldowns={},
        loaded_at=0.0,
    )

    _TOKEN_ACTIVE_REQUESTS.clear()
    result = asyncio.run(
        _claim_next_token_for_model_request(
            TokenSelectionSettings(
                strategy=TOKEN_SELECTION_STRATEGY_FILL_FIRST,
                token_order=(21, 22),
                plan_order_enabled=True,
                plan_order=("free", "plus", "team", "pro"),
                active_stream_cap=5,
            ),
            exclude_token_ids=set(),
            scoped_cooldown_scope=None,
            timing=_RequestTimingRecorder(),
            token_pool_snapshot=snapshot,
            require_non_free_token=True,
        )
    )

    assert result is plus_token
    assert 21 not in _TOKEN_ACTIVE_REQUESTS
    assert _TOKEN_ACTIVE_REQUESTS[22] == 1
    _TOKEN_ACTIVE_REQUESTS.clear()


def test_claim_next_token_for_model_request_prefers_known_non_free_before_unknown_snapshot_token() -> None:
    unknown_token = SimpleNamespace(
        id=31,
        plan_type=None,
        raw_payload={},
        id_token=None,
        account_id="acct_unknown",
        refresh_token="rt_unknown",
        token_type="codex",
        is_active=True,
        cooldown_until=None,
        merged_into_token_id=None,
    )
    plus_token = SimpleNamespace(
        id=32,
        plan_type="plus",
        refresh_token="rt_plus",
        token_type="codex",
        is_active=True,
        cooldown_until=None,
        merged_into_token_id=None,
    )
    snapshot = SimpleNamespace(
        tokens=(unknown_token, plus_token),
        scoped_cooldowns={},
        loaded_at=0.0,
    )

    _TOKEN_ACTIVE_REQUESTS.clear()
    result = asyncio.run(
        _claim_next_token_for_model_request(
            TokenSelectionSettings(
                strategy=TOKEN_SELECTION_STRATEGY_FILL_FIRST,
                token_order=(31, 32),
                plan_order_enabled=False,
                active_stream_cap=5,
            ),
            exclude_token_ids=set(),
            scoped_cooldown_scope=None,
            timing=_RequestTimingRecorder(),
            token_pool_snapshot=snapshot,
            require_non_free_token=True,
        )
    )

    assert result is plus_token
    assert 31 not in _TOKEN_ACTIVE_REQUESTS
    assert _TOKEN_ACTIVE_REQUESTS[32] == 1
    _TOKEN_ACTIVE_REQUESTS.clear()


def test_claim_next_token_for_model_request_returns_none_when_snapshot_has_only_free_tokens() -> None:
    free_token = SimpleNamespace(
        id=41,
        plan_type="free",
        refresh_token="rt_free",
        token_type="codex",
        is_active=True,
        cooldown_until=None,
        merged_into_token_id=None,
    )
    snapshot = SimpleNamespace(
        tokens=(free_token,),
        scoped_cooldowns={},
        loaded_at=0.0,
    )

    _TOKEN_ACTIVE_REQUESTS.clear()
    result = asyncio.run(
        _claim_next_token_for_model_request(
            TokenSelectionSettings(strategy=TOKEN_SELECTION_STRATEGY_FILL_FIRST, token_order=(41,)),
            exclude_token_ids=set(),
            scoped_cooldown_scope=None,
            timing=_RequestTimingRecorder(),
            token_pool_snapshot=snapshot,
            require_non_free_token=True,
        )
    )

    assert result is None
    assert _TOKEN_ACTIVE_REQUESTS == {}


def test_upstream_http_pool_size_is_configurable(monkeypatch) -> None:
    monkeypatch.setenv("UPSTREAM_HTTP_MAX_CONNECTIONS", "512")
    monkeypatch.setenv("UPSTREAM_HTTP_MAX_KEEPALIVE_CONNECTIONS", "128")

    assert _upstream_http_max_connections() == 512
    assert _upstream_http_max_keepalive_connections() == 128


def test_resolve_token_observed_cost_usd_prefers_direct_token_cost() -> None:
    token = CodexToken(
        id=7,
        account_id="acct_123",
        refresh_token="refresh_token",
        is_active=True,
    )

    observed_cost = _resolve_token_observed_cost_usd(
        {7: 12.5},
        {"acct_123": 99.0},
        {"acct_123": 1},
        token_row=token,
    )

    assert observed_cost == 12.5


def test_resolve_token_observed_cost_usd_falls_back_only_for_unique_account() -> None:
    token = CodexToken(
        id=8,
        account_id="acct_456",
        refresh_token="refresh_token",
        is_active=True,
    )

    assert (
        _resolve_token_observed_cost_usd(
            {},
            {"acct_456": 4.25},
            {"acct_456": 1},
            token_row=token,
        )
        == 4.25
    )

    assert (
        _resolve_token_observed_cost_usd(
            {},
            {"acct_456": 4.25},
            {"acct_456": 2},
            token_row=token,
        )
        is None
    )


def test_proxy_stream_capture_extracts_usage_and_model() -> None:
    capture = _ProxyStreamCapture()
    capture.feed(
        b'event: response.created\ndata: {"type":"response.created","response":{"model":"gpt-5.4-mini"}}\n\n'
    )
    capture.feed(
        b'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15}}}\n\n'
    )

    assert capture.model_name == "gpt-5.4-mini"
    assert capture.usage_metrics is not None
    assert capture.usage_metrics.total_tokens == 15
    assert capture.usage_metrics.output_tokens == 5


def test_proxy_request_with_token_forces_upstream_stream_for_non_stream_request() -> None:
    class DummyStreamingResponse:
        def __init__(self, chunks: list[bytes]) -> None:
            self.status_code = 200
            self._chunks = chunks

        def aiter_raw(self) -> AsyncIterator[bytes]:
            async def iterator() -> AsyncIterator[bytes]:
                for chunk in self._chunks:
                    yield chunk

            return iterator()

        async def aread(self) -> bytes:
            return b"".join(self._chunks)

    class DummyStreamContext:
        def __init__(self, response: DummyStreamingResponse) -> None:
            self._response = response

        async def __aenter__(self) -> DummyStreamingResponse:
            return self._response

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class DummyClient:
        def __init__(self) -> None:
            self.stream_calls: list[dict[str, object]] = []

        def stream(self, method, url, headers, content, timeout):
            self.stream_calls.append(
                {
                    "method": method,
                    "url": url,
                    "headers": headers,
                    "content": content,
                    "timeout": timeout,
                }
            )
            response = DummyStreamingResponse(
                [
                    b'event: response.created\ndata: {"type":"response.created","response":{"id":"resp_456","status":"in_progress","model":"gpt-5.4-mini"}}\n\n',
                    b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"hello"}\n\n',
                    b'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}\n\n',
                ]
            )
            return DummyStreamContext(response)

        async def post(self, *args, **kwargs):
            raise AssertionError("non-stream downstream requests should not call client.post")

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/responses",
            "headers": [(b"user-agent", b"yaak")],
        }
    )
    request_data = ResponsesRequest(
        model="gpt-5.4",
        input=[{"role": "user", "content": "hello"}],
        stream=False,
    )

    client = DummyClient()
    result = asyncio.run(
        _proxy_request_with_token(
            client,
            request,
            request_data,
            access_token="access-token",
            account_id="account-1",
        )
    )

    assert len(client.stream_calls) == 1
    stream_call = client.stream_calls[0]
    assert stream_call["headers"]["Accept"] == "text/event-stream"
    upstream_payload = json.loads(stream_call["content"])
    assert upstream_payload["stream"] is True
    assert upstream_payload["store"] is False
    assert result.first_token_at is not None
    assert result.status_code == 200
    assert result.model_name == "gpt-5.4-mini"
    assert result.usage_metrics is not None
    assert result.usage_metrics.total_tokens == 2
    body = json.loads(result.response.body)
    assert body["id"] == "resp_456"
    assert body["model"] == "gpt-5.4-mini"
    assert body["status"] == "completed"
    assert body["output"][0]["content"][0]["text"] == "hello"


def test_proxy_request_with_token_auto_translates_gpt_image_responses_request() -> None:
    class DummyStreamingResponse:
        def __init__(self, chunks: list[bytes]) -> None:
            self.status_code = 200
            self._chunks = chunks

        def aiter_raw(self) -> AsyncIterator[bytes]:
            async def iterator() -> AsyncIterator[bytes]:
                for chunk in self._chunks:
                    yield chunk

            return iterator()

        async def aread(self) -> bytes:
            return b"".join(self._chunks)

    class DummyStreamContext:
        def __init__(self, response: DummyStreamingResponse) -> None:
            self._response = response

        async def __aenter__(self) -> DummyStreamingResponse:
            return self._response

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class DummyClient:
        def __init__(self) -> None:
            self.stream_calls: list[dict[str, object]] = []

        def stream(self, method, url, headers, content, timeout):
            self.stream_calls.append(
                {
                    "method": method,
                        "url": url,
                        "headers": headers,
                        "content": content,
                        "timeout": timeout,
                    }
                )
            response = DummyStreamingResponse(
                [
                    b'event: response.created\ndata: {"type":"response.created","response":{"id":"resp_img_123","status":"in_progress","model":"gpt-5.4-mini"}}\n\n',
                    b'event: response.completed\ndata: {"type":"response.completed","response":{"id":"resp_img_123","status":"completed","model":"gpt-5.4-mini","output":[{"type":"image_generation_call","result":"img-b64","output_format":"png","revised_prompt":"drawn mug"}],"usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}}}\n\n',
                ]
            )
            return DummyStreamContext(response)

        async def post(self, *args, **kwargs):
            raise AssertionError("non-stream downstream requests should not call client.post")

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/responses",
            "headers": [(b"user-agent", b"yaak")],
        }
    )
    request_data = ResponsesRequest(
        model="gpt-image-2",
        input=[{"role": "user", "content": [{"type": "input_text", "text": "Draw a mug"}]}],
        stream=False,
        previous_response_id="resp_prev_123",
        size="1024x1024",
        output_format="png",
    )

    client = DummyClient()
    result = asyncio.run(
        _proxy_request_with_token(
            client,
            request,
            request_data,
            access_token="access-token",
            account_id="account-1",
        )
    )

    assert len(client.stream_calls) == 1
    stream_call = client.stream_calls[0]
    upstream_payload = json.loads(stream_call["content"])
    assert upstream_payload["model"] == "gpt-5.5"
    assert upstream_payload["stream"] is True
    assert upstream_payload["store"] is False
    assert upstream_payload["parallel_tool_calls"] is True
    assert upstream_payload["reasoning"] == {"effort": "medium", "summary": "auto"}
    assert upstream_payload["include"] == ["reasoning.encrypted_content"]
    assert upstream_payload["previous_response_id"] == "resp_prev_123"
    assert "tool_choice" not in upstream_payload
    assert upstream_payload["tools"] == [
        {
            "type": "image_generation",
            "model": "gpt-image-2",
            "action": "auto",
            "size": "1024x1024",
            "output_format": "png",
        }
    ]
    assert result.status_code == 200
    assert result.model_name == "gpt-image-2"
    assert result.first_token_at is not None
    assert result.usage_metrics is not None
    assert result.usage_metrics.total_tokens == 5
    body = json.loads(result.response.body)
    assert body["id"] == "resp_img_123"
    assert body["model"] == "gpt-image-2"
    assert body["output"][0]["type"] == "image_generation_call"
    assert body["output"][0]["result"] == "img-b64"


def test_proxy_request_with_token_for_compact_non_stream_request_does_not_force_upstream_stream() -> None:
    class DummyStreamingResponse:
        def __init__(self, chunks: list[bytes]) -> None:
            self.status_code = 200
            self._chunks = chunks

        def aiter_raw(self) -> AsyncIterator[bytes]:
            async def iterator() -> AsyncIterator[bytes]:
                for chunk in self._chunks:
                    yield chunk

            return iterator()

        async def aread(self) -> bytes:
            return b"".join(self._chunks)

    class DummyStreamContext:
        def __init__(self, response: DummyStreamingResponse) -> None:
            self._response = response

        async def __aenter__(self) -> DummyStreamingResponse:
            return self._response

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class DummyClient:
        def __init__(self) -> None:
            self.stream_calls: list[dict[str, object]] = []

        def stream(self, method, url, headers, content, timeout):
            self.stream_calls.append(
                {
                    "method": method,
                    "url": url,
                    "headers": headers,
                    "content": content,
                    "timeout": timeout,
                }
            )
            response = DummyStreamingResponse(
                [
                    json.dumps(
                        {
                            "id": "resp_compact_123",
                            "model": "gpt-5.4-compact",
                            "status": "completed",
                            "output": [
                                {
                                    "content": [
                                        {
                                            "type": "output_text",
                                            "text": "hello compact",
                                        }
                                    ]
                                }
                            ],
                        }
                    ).encode("utf-8")
                ]
            )
            return DummyStreamContext(response)

        async def post(self, *args, **kwargs):
            raise AssertionError("compact non-stream requests should not call client.post")

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/responses/compact",
            "headers": [(b"user-agent", b"yaak")],
        }
    )
    request_data = ResponsesRequest(
        model="gpt-5.4",
        input=[{"role": "user", "content": "hello compact"}],
        stream=False,
        store=True,
    )

    client = DummyClient()
    result = asyncio.run(
        _proxy_request_with_token(
            client,
            request,
            request_data,
            access_token="access-token",
            account_id="account-1",
            compact=True,
        )
    )

    assert len(client.stream_calls) == 1
    stream_call = client.stream_calls[0]
    assert stream_call["headers"]["Accept"] == "application/json"
    upstream_payload = json.loads(stream_call["content"])
    assert "stream" not in upstream_payload
    assert "store" not in upstream_payload
    assert stream_call["timeout"].read == 45.0
    assert result.first_token_at is not None
    assert result.status_code == 200
    assert result.model_name == "gpt-5.4-compact"
    assert result.usage_metrics is None
    body = json.loads(result.response.body)
    assert body["id"] == "resp_compact_123"
    assert body["model"] == "gpt-5.4-compact"
    assert body["status"] == "completed"
    assert body["output"][0]["content"][0]["text"] == "hello compact"


def test_proxy_request_with_token_compact_timeout_fallback_sets_store_false() -> None:
    class DummyStreamingResponse:
        def __init__(self, chunks: list[bytes]) -> None:
            self.status_code = 200
            self._chunks = chunks

        def aiter_raw(self) -> AsyncIterator[bytes]:
            async def iterator() -> AsyncIterator[bytes]:
                for chunk in self._chunks:
                    yield chunk

            return iterator()

        async def aread(self) -> bytes:
            return b"".join(self._chunks)

    class DummyStreamContext:
        def __init__(self, response: DummyStreamingResponse | None = None, *, raise_timeout: bool = False) -> None:
            self._response = response
            self._raise_timeout = raise_timeout

        async def __aenter__(self) -> DummyStreamingResponse:
            if self._raise_timeout:
                raise httpx.ReadTimeout("compact timed out")
            assert self._response is not None
            return self._response

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class DummyClient:
        def __init__(self) -> None:
            self.stream_calls: list[dict[str, object]] = []

        def stream(self, method, url, headers, content, timeout):
            self.stream_calls.append(
                {
                    "method": method,
                    "url": url,
                    "headers": headers,
                    "content": content,
                    "timeout": timeout,
                }
            )
            if len(self.stream_calls) == 1:
                return DummyStreamContext(raise_timeout=True)
            return DummyStreamContext(
                DummyStreamingResponse(
                    [
                        b'event: response.created\ndata: {"type":"response.created","response":{"id":"resp_fallback","status":"in_progress","model":"gpt-5.4"}}\n\n',
                        b'event: response.completed\ndata: {"type":"response.completed","response":{"id":"resp_fallback","status":"completed","model":"gpt-5.4","output":[{"content":[{"type":"output_text","text":"fallback ok"}]}],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}\n\n',
                        b"data: [DONE]\n\n",
                    ]
                )
            )

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/responses/compact",
            "headers": [(b"user-agent", b"yaak")],
        }
    )
    request_data = ResponsesRequest(
        model="gpt-5.4",
        input=[{"role": "user", "content": "hello compact"}],
        stream=False,
        store=True,
    )

    client = DummyClient()
    result = asyncio.run(
        _proxy_request_with_token(
            client,
            request,
            request_data,
            access_token="access-token",
            account_id="account-1",
            compact=True,
        )
    )

    assert len(client.stream_calls) == 2
    compact_payload = json.loads(client.stream_calls[0]["content"])
    fallback_payload = json.loads(client.stream_calls[1]["content"])
    assert "store" not in compact_payload
    assert "stream" not in compact_payload
    assert fallback_payload["store"] is False
    assert fallback_payload["stream"] is True
    assert result.status_code == 200
    assert result.model_name == "gpt-5.4"
    assert result.usage_metrics is not None
    assert result.usage_metrics.total_tokens == 3


def test_proxy_request_with_token_stream_rewrites_gpt_image_model_alias() -> None:
    class DummyStreamingResponse:
        def __init__(self, chunks: list[bytes]) -> None:
            self.status_code = 200
            self._chunks = chunks

        def aiter_raw(self) -> AsyncIterator[bytes]:
            async def iterator() -> AsyncIterator[bytes]:
                for chunk in self._chunks:
                    yield chunk

            return iterator()

        async def aread(self) -> bytes:
            return b"".join(self._chunks)

    class DummyStreamContext:
        def __init__(self, response: DummyStreamingResponse) -> None:
            self._response = response

        async def __aenter__(self) -> DummyStreamingResponse:
            return self._response

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class DummyClient:
        def __init__(self) -> None:
            self.stream_calls: list[dict[str, object]] = []

        def stream(self, method, url, headers, content, timeout):
            self.stream_calls.append(
                {
                    "method": method,
                    "url": url,
                    "headers": headers,
                    "content": content,
                }
            )
            response = DummyStreamingResponse(
                [
                    b'event: response.created\ndata: {"type":"response.created","response":{"id":"resp_stream_img","status":"in_progress","model":"gpt-5.4-mini"}}\n\n',
                    b'event: response.completed\ndata: {"type":"response.completed","response":{"id":"resp_stream_img","status":"completed","model":"gpt-5.4-mini","output":[{"type":"image_generation_call","result":"img-stream","output_format":"png"}],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}\n\n',
                    b'data: [DONE]\n\n',
                ]
            )
            return DummyStreamContext(response)

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/responses",
            "headers": [(b"user-agent", b"yaak")],
        }
    )
    request_data = ResponsesRequest(
        model="gpt-image-2",
        input=[{"role": "user", "content": [{"type": "input_text", "text": "Draw a mug"}]}],
        stream=True,
    )

    async def run_stream() -> tuple[object, str, list[dict[str, object]]]:
        client = DummyClient()
        result = await _proxy_request_with_token(
            client,
            request,
            request_data,
            access_token="access-token",
            account_id="account-1",
        )
        chunks: list[bytes] = []
        async for chunk in result.response.body_iterator:
            chunks.append(chunk)
        return result, b"".join(chunks).decode("utf-8"), client.stream_calls

    result, body, stream_calls = asyncio.run(run_stream())

    upstream_payload = json.loads(stream_calls[0]["content"])
    assert upstream_payload["model"] == "gpt-5.5"
    assert upstream_payload["store"] is False
    assert upstream_payload["parallel_tool_calls"] is True
    assert upstream_payload["reasoning"] == {"effort": "medium", "summary": "auto"}
    assert upstream_payload["include"] == ["reasoning.encrypted_content"]
    assert "tool_choice" not in upstream_payload
    assert upstream_payload["tools"][0]["model"] == "gpt-image-2"
    assert result.status_code == 200
    assert result.model_name == "gpt-image-2"
    assert result.stream_capture is not None
    assert result.stream_capture.model_name == "gpt-image-2"
    assert result.stream_capture.usage_metrics is not None
    assert result.stream_capture.usage_metrics.total_tokens == 3
    assert '"model": "gpt-image-2"' in body
    assert "gpt-5.4-mini" not in body


def test_proxy_request_with_token_stream_rewrites_gpt_image_model_alias_after_prime_keepalive(monkeypatch) -> None:
    class DummyStreamingResponse:
        def __init__(self, chunks: list[bytes]) -> None:
            self.status_code = 200
            self._chunks = chunks

        def aiter_raw(self) -> AsyncIterator[bytes]:
            async def iterator() -> AsyncIterator[bytes]:
                for chunk in self._chunks:
                    yield chunk

            return iterator()

        async def aread(self) -> bytes:
            return b"".join(self._chunks)

    class DummyStreamContext:
        def __init__(self, response: DummyStreamingResponse) -> None:
            self._response = response

        async def __aenter__(self) -> DummyStreamingResponse:
            return self._response

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class DummyClient:
        def stream(self, method, url, headers, content, timeout):
            return DummyStreamContext(
                DummyStreamingResponse(
                    [
                        b'event: response.created\ndata: {"type":"response.created","response":{"id":"resp_prime","status":"in_progress","model":"gpt-5.4-mini"}}\n\n',
                    ]
                )
            )

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/responses",
            "headers": [(b"user-agent", b"yaak")],
        }
    )
    request_data = ResponsesRequest(
        model="gpt-image-2",
        input=[{"role": "user", "content": [{"type": "input_text", "text": "Draw a mug"}]}],
        stream=True,
    )

    monkeypatch.setattr("oaix_gateway.api_server._stream_keepalive_interval_seconds", lambda: 0.001)

    async def run_stream() -> str:
        original_aiter_raw = DummyStreamingResponse.aiter_raw

        async def delayed_aiter_raw(self) -> AsyncIterator[bytes]:
            async for chunk in original_aiter_raw(self):
                yield chunk
                if b"response.created" in chunk:
                    await asyncio.sleep(0.02)
                    yield (
                        b'event: response.completed\ndata: {"type":"response.completed","response":{"id":"resp_prime","status":"completed","model":"gpt-5.4-mini","output":[{"type":"image_generation_call","result":"prime-image","output_format":"png"}]}}\n\n'
                    )
                    yield b"data: [DONE]\n\n"
                    return

        DummyStreamingResponse.aiter_raw = delayed_aiter_raw  # type: ignore[method-assign]
        result = await _proxy_request_with_token(
            DummyClient(),
            request,
            request_data,
            access_token="access-token",
            account_id="account-1",
        )
        chunks: list[bytes] = []
        async for chunk in result.response.body_iterator:
            chunks.append(chunk)
        return b"".join(chunks).decode("utf-8")

    body = asyncio.run(run_stream())

    assert body.startswith(": keepalive")
    assert '"model": "gpt-image-2"' in body


def test_stream_upstream_image_response_synthesizes_completed_event_from_output_item_done() -> None:
    class DummyStreamContext:
        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    async def upstream() -> AsyncIterator[bytes]:
        yield (
            b'event: response.created\n'
            b'data: {"type":"response.created","response":{"id":"resp_img_done","status":"in_progress","model":"gpt-5.4-mini","created_at":1710000000}}\n\n'
        )
        yield (
            b'event: response.output_item.done\n'
            b'data: {"type":"response.output_item.done","output_index":0,"item":{"type":"image_generation_call","id":"ig_done","status":"completed","result":"final-done","output_format":"png","revised_prompt":"patched done"}}\n\n'
        )
        yield b"data: [DONE]\n\n"

    async def collect() -> str:
        chunks: list[bytes] = []
        async for chunk in _stream_upstream_image_response(
            DummyStreamContext(),
            upstream(),
            [],
            stream_committed=True,
            response_format="b64_json",
            stream_prefix="image_generation",
        ):
            chunks.append(chunk)
        return b"".join(chunks).decode("utf-8")

    body = asyncio.run(collect())

    assert "event: image_generation.completed" in body
    assert '"b64_json": "final-done"' in body
    assert '"revised_prompt": "patched done"' in body


def test_proxy_chat_completions_with_token_non_stream_returns_markdown_image_response(monkeypatch) -> None:
    async def fake_resolve_chat_image_output_items(*, scope_hash: str, client_model: str, image_urls: list[str]):
        assert client_model == "gpt-image-2"
        assert image_urls == ["data:image/png;base64,prev-image"]
        return [
            SimpleNamespace(
                image_call_id="ig_prev",
                output_item={
                    "type": "image_generation_call",
                    "id": "ig_prev",
                    "status": "completed",
                    "action": "generate",
                    "background": "opaque",
                    "quality": "medium",
                    "size": "1024x1024",
                    "result": "prev-image",
                    "output_format": "png",
                }
            )
        ]

    persisted: dict[str, object] = {}

    async def fake_create_chat_image_checkpoint(
        *,
        scope_hash: str,
        client_model: str,
        responses_payload: dict[str, object],
        assistant_content: str | None,
        request_log_id: int | None = None,
    ) -> int | None:
        persisted["scope_hash"] = scope_hash
        persisted["client_model"] = client_model
        persisted["responses_payload"] = responses_payload
        persisted["assistant_content"] = assistant_content
        persisted["request_log_id"] = request_log_id
        return 7

    monkeypatch.setattr(
        "oaix_gateway.api_server.resolve_chat_image_output_items",
        fake_resolve_chat_image_output_items,
    )
    monkeypatch.setattr(
        "oaix_gateway.api_server.create_chat_image_checkpoint",
        fake_create_chat_image_checkpoint,
    )

    class DummyStreamingResponse:
        def __init__(self, chunks: list[bytes]) -> None:
            self.status_code = 200
            self._chunks = chunks

        def aiter_raw(self) -> AsyncIterator[bytes]:
            async def iterator() -> AsyncIterator[bytes]:
                for chunk in self._chunks:
                    yield chunk

            return iterator()

        async def aread(self) -> bytes:
            return b"".join(self._chunks)

    class DummyStreamContext:
        def __init__(self, response: DummyStreamingResponse) -> None:
            self._response = response

        async def __aenter__(self) -> DummyStreamingResponse:
            return self._response

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class DummyClient:
        def __init__(self) -> None:
            self.stream_calls: list[dict[str, object]] = []

        def stream(self, method, url, headers, content, timeout):
            self.stream_calls.append(
                {
                    "method": method,
                    "url": url,
                    "headers": headers,
                    "content": content,
                }
            )
            response = DummyStreamingResponse(
                [
                    b'event: response.created\ndata: {"type":"response.created","response":{"id":"resp_chat_img","status":"in_progress","model":"gpt-5.4-mini"}}\n\n',
                    b'event: response.output_item.done\ndata: {"type":"response.output_item.done","output_index":0,"item":{"type":"image_generation_call","id":"ig_chat","result":"chat-image","output_format":"png"}}\n\n',
                    b'event: response.completed\ndata: {"type":"response.completed","response":{"id":"resp_chat_img","status":"completed","model":"gpt-5.4-mini","output":[],"usage":{"input_tokens":4,"output_tokens":6,"total_tokens":10}}}\n\n',
                ]
            )
            return DummyStreamContext(response)

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": [(b"user-agent", b"yaak")],
        }
    )
    request_data = ChatCompletionsRequest(
        model="gpt-image-2",
        messages=[
            {
                "role": "assistant",
                "content": "![image](data:image/png;base64,prev-image)",
            },
            {
                "role": "user",
                "content": "Make it blue",
            },
        ],
        stream=False,
        size="1024x1024",
        output_format="png",
    )

    client = DummyClient()
    result = asyncio.run(
        _proxy_chat_completions_with_token(
            client,
            request,
            request_data,
            access_token="access-token",
            account_id="account-1",
        )
    )

    upstream_payload = json.loads(client.stream_calls[0]["content"])
    assert upstream_payload["model"] == "gpt-5.5"
    assert upstream_payload["store"] is False
    assert upstream_payload["parallel_tool_calls"] is True
    assert upstream_payload["reasoning"] == {"effort": "medium", "summary": "auto"}
    assert upstream_payload["include"] == ["reasoning.encrypted_content"]
    assert "tool_choice" not in upstream_payload
    assert upstream_payload["tools"] == [
        {
            "type": "image_generation",
            "model": "gpt-image-2",
            "action": "auto",
            "size": "1024x1024",
            "output_format": "png",
        }
    ]
    assert upstream_payload["input"][0] == {
        "type": "image_generation_call",
        "id": "ig_prev",
        "result": "prev-image",
        "status": "completed",
    }
    assert upstream_payload["input"][1]["content"] == [
        {"type": "input_text", "text": "Make it blue"}
    ]
    assert result.status_code == 200
    assert result.model_name == "gpt-image-2"
    body = json.loads(result.response.body)
    assert body["object"] == "chat.completion"
    assert body["model"] == "gpt-image-2"
    assert body["choices"][0]["message"]["role"] == "assistant"
    assert body["choices"][0]["message"]["content"] == "![image](data:image/png;base64,chat-image)"
    assert body["choices"][0]["finish_reason"] == "stop"
    assert body["usage"]["total_tokens"] == 10
    assert result.on_success is not None
    asyncio.run(result.on_success(101))
    assert persisted["client_model"] == "gpt-image-2"
    assert persisted["assistant_content"] == "![image](data:image/png;base64,chat-image)"
    assert persisted["request_log_id"] == 101
    assert persisted["responses_payload"]["id"] == "resp_chat_img"


def test_proxy_chat_completions_with_token_stream_returns_markdown_image_chunks(monkeypatch) -> None:
    persisted: dict[str, object] = {}

    async def fake_create_chat_image_checkpoint(
        *,
        scope_hash: str,
        client_model: str,
        responses_payload: dict[str, object],
        assistant_content: str | None,
        request_log_id: int | None = None,
    ) -> int | None:
        persisted["scope_hash"] = scope_hash
        persisted["client_model"] = client_model
        persisted["responses_payload"] = responses_payload
        persisted["assistant_content"] = assistant_content
        persisted["request_log_id"] = request_log_id
        return 9

    monkeypatch.setattr(
        "oaix_gateway.api_server.create_chat_image_checkpoint",
        fake_create_chat_image_checkpoint,
    )

    class DummyStreamingResponse:
        def __init__(self, chunks: list[bytes]) -> None:
            self.status_code = 200
            self._chunks = chunks

        def aiter_raw(self) -> AsyncIterator[bytes]:
            async def iterator() -> AsyncIterator[bytes]:
                for chunk in self._chunks:
                    yield chunk

            return iterator()

        async def aread(self) -> bytes:
            return b"".join(self._chunks)

    class DummyStreamContext:
        def __init__(self, response: DummyStreamingResponse) -> None:
            self._response = response

        async def __aenter__(self) -> DummyStreamingResponse:
            return self._response

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class DummyClient:
        def __init__(self) -> None:
            self.stream_calls: list[dict[str, object]] = []

        def stream(self, method, url, headers, content, timeout):
            self.stream_calls.append(
                {
                    "method": method,
                    "url": url,
                    "headers": headers,
                    "content": content,
                }
            )
            response = DummyStreamingResponse(
                [
                    b'event: response.created\ndata: {"type":"response.created","response":{"id":"resp_chat_stream","status":"in_progress","model":"gpt-5.4-mini"}}\n\n',
                    b'event: response.output_item.done\ndata: {"type":"response.output_item.done","output_index":0,"item":{"type":"image_generation_call","id":"ig_stream","result":"stream-image","output_format":"png"}}\n\n',
                    b'event: response.completed\ndata: {"type":"response.completed","response":{"id":"resp_chat_stream","status":"completed","model":"gpt-5.4-mini","output":[],"usage":{"input_tokens":3,"output_tokens":4,"total_tokens":7}}}\n\n',
                    b'data: [DONE]\n\n',
                ]
            )
            return DummyStreamContext(response)

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": [(b"user-agent", b"yaak")],
        }
    )
    request_data = ChatCompletionsRequest(
        model="gpt-image-2",
        messages=[{"role": "user", "content": "Draw a mug"}],
        stream=True,
    )

    client = DummyClient()

    async def run_stream() -> tuple[object, str]:
        result = await _proxy_chat_completions_with_token(
            client,
            request,
            request_data,
            access_token="access-token",
            account_id="account-1",
        )
        chunks: list[bytes] = []
        async for chunk in result.response.body_iterator:
            chunks.append(chunk)
        return result, b"".join(chunks).decode("utf-8")

    result, body = asyncio.run(run_stream())

    upstream_payload = json.loads(client.stream_calls[0]["content"])
    assert upstream_payload["model"] == "gpt-5.5"
    assert upstream_payload["store"] is False
    assert upstream_payload["parallel_tool_calls"] is True
    assert upstream_payload["reasoning"] == {"effort": "medium", "summary": "auto"}
    assert upstream_payload["include"] == ["reasoning.encrypted_content"]
    assert "tool_choice" not in upstream_payload
    assert upstream_payload["tools"] == [
        {
            "type": "image_generation",
            "model": "gpt-image-2",
            "action": "auto",
        }
    ]
    assert result.status_code == 200
    assert result.model_name == "gpt-image-2"
    assert result.stream_capture is not None
    assert result.stream_capture.model_name == "gpt-image-2"
    assert "chat.completion.chunk" in body
    assert '"model": "gpt-image-2"' in body
    assert '"content": "![image](data:image/png;base64,stream-image)"' in body
    assert '"finish_reason": "stop"' in body
    assert '"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}' in body.replace(" ", "")
    assert "data: [DONE]" in body
    assert result.on_success is not None
    asyncio.run(result.on_success(202))
    assert persisted["client_model"] == "gpt-image-2"
    assert persisted["assistant_content"] == "![image](data:image/png;base64,stream-image)"
    assert persisted["request_log_id"] == 202
    assert persisted["responses_payload"]["output"][0]["id"] == "ig_stream"


def test_proxy_image_request_with_token_non_stream_collects_openai_images_response() -> None:
    class DummyStreamingResponse:
        def __init__(self, chunks: list[bytes]) -> None:
            self.status_code = 200
            self._chunks = chunks

        def aiter_raw(self) -> AsyncIterator[bytes]:
            async def iterator() -> AsyncIterator[bytes]:
                for chunk in self._chunks:
                    yield chunk

            return iterator()

        async def aread(self) -> bytes:
            return b"".join(self._chunks)

    class DummyStreamContext:
        def __init__(self, response: DummyStreamingResponse) -> None:
            self._response = response

        async def __aenter__(self) -> DummyStreamingResponse:
            return self._response

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class DummyClient:
        def __init__(self) -> None:
            self.stream_calls: list[dict[str, object]] = []

        def stream(self, method, url, headers, content, timeout):
            self.stream_calls.append(
                {
                    "method": method,
                    "url": url,
                    "headers": headers,
                    "content": content,
                }
            )
            response = DummyStreamingResponse(
                [
                    b'event: response.created\ndata: {"type":"response.created","response":{"status":"in_progress"}}\n\n',
                    b'event: response.completed\ndata: {"type":"response.completed","response":{"created_at":1710000001,"status":"completed","output":[{"type":"image_generation_call","result":"img-final","output_format":"png","revised_prompt":"final prompt"}],"tool_usage":{"image_gen":{"images":1}}}}\n\n',
                ]
            )
            return DummyStreamContext(response)

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/images/generations",
            "headers": [(b"user-agent", b"yaak")],
        }
    )
    image_request = ImageProxyRequest(
        model_name="gpt-image-2",
        response_format="b64_json",
        stream=False,
        stream_prefix="image_generation",
        responses_payload={
            "model": "gpt-5.4-mini",
            "stream": True,
            "tools": [{"type": "image_generation", "action": "generate", "model": "gpt-image-2"}],
            "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "draw"}]}],
        },
    )

    client = DummyClient()
    result = asyncio.run(
        _proxy_image_request_with_token(
            client,
            request,
            image_request,
            access_token="access-token",
            account_id="account-1",
        )
    )

    assert len(client.stream_calls) == 1
    stream_call = client.stream_calls[0]
    assert stream_call["headers"]["Accept"] == "text/event-stream"
    assert json.loads(stream_call["content"])["tools"][0]["model"] == "gpt-image-2"
    assert result.status_code == 200
    assert result.model_name == "gpt-image-2"
    assert result.first_token_at is not None
    body = json.loads(result.response.body)
    assert body == {
        "created": 1710000001,
        "data": [{"b64_json": "img-final", "revised_prompt": "final prompt"}],
        "output_format": "png",
        "usage": {"images": 1},
    }


def test_proxy_image_request_with_token_stream_translates_image_events() -> None:
    class DummyStreamingResponse:
        def __init__(self, chunks: list[bytes]) -> None:
            self.status_code = 200
            self._chunks = chunks

        def aiter_raw(self) -> AsyncIterator[bytes]:
            async def iterator() -> AsyncIterator[bytes]:
                for chunk in self._chunks:
                    yield chunk

            return iterator()

        async def aread(self) -> bytes:
            return b"".join(self._chunks)

    class DummyStreamContext:
        def __init__(self, response: DummyStreamingResponse) -> None:
            self._response = response

        async def __aenter__(self) -> DummyStreamingResponse:
            return self._response

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class DummyClient:
        def stream(self, method, url, headers, content, timeout):
            response = DummyStreamingResponse(
                [
                    b'event: response.created\ndata: {"type":"response.created","response":{"status":"in_progress"}}\n\n',
                    b'event: response.image_generation_call.partial_image\ndata: {"type":"response.image_generation_call.partial_image","partial_image_b64":"partial-1","partial_image_index":0,"output_format":"png"}\n\n',
                    b'event: response.completed\ndata: {"type":"response.completed","response":{"created_at":1710000002,"status":"completed","output":[{"type":"image_generation_call","result":"final-1","output_format":"png","revised_prompt":"done"}],"tool_usage":{"image_gen":{"images":1}}}}\n\n',
                ]
            )
            return DummyStreamContext(response)

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/images/generations",
            "headers": [(b"user-agent", b"yaak")],
        }
    )
    image_request = ImageProxyRequest(
        model_name="gpt-image-2",
        response_format="b64_json",
        stream=True,
        stream_prefix="image_generation",
        responses_payload={
            "model": "gpt-5.4-mini",
            "stream": True,
            "tools": [{"type": "image_generation", "action": "generate", "model": "gpt-image-2"}],
            "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "draw"}]}],
        },
    )

    async def run_stream() -> tuple[object, str]:
        result = await _proxy_image_request_with_token(
            DummyClient(),
            request,
            image_request,
            access_token="access-token",
            account_id="account-1",
        )
        chunks: list[bytes] = []
        async for chunk in result.response.body_iterator:
            chunks.append(chunk)
        return result, b"".join(chunks).decode("utf-8")

    result, body = asyncio.run(run_stream())

    assert result.status_code == 200
    assert result.model_name == "gpt-image-2"
    assert result.first_token_at is None
    assert result.stream_capture is not None
    assert result.stream_capture.first_token_at is not None
    assert "event: image_generation.partial_image" in body
    assert '"b64_json": "partial-1"' in body
    assert "event: image_generation.completed" in body
    assert '"b64_json": "final-1"' in body
    assert '"usage": {"images": 1}' in body


def test_proxy_image_request_with_token_stream_patches_output_item_done() -> None:
    class DummyStreamingResponse:
        def __init__(self, chunks: list[bytes]) -> None:
            self.status_code = 200
            self._chunks = chunks

        def aiter_raw(self) -> AsyncIterator[bytes]:
            async def iterator() -> AsyncIterator[bytes]:
                for chunk in self._chunks:
                    yield chunk

            return iterator()

        async def aread(self) -> bytes:
            return b"".join(self._chunks)

    class DummyStreamContext:
        def __init__(self, response: DummyStreamingResponse) -> None:
            self._response = response

        async def __aenter__(self) -> DummyStreamingResponse:
            return self._response

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class DummyClient:
        def stream(self, method, url, headers, content, timeout):
            response = DummyStreamingResponse(
                [
                    b'event: response.created\ndata: {"type":"response.created","response":{"status":"in_progress"}}\n\n',
                    b'event: response.output_item.done\ndata: {"type":"response.output_item.done","output_index":0,"item":{"type":"image_generation_call","result":"patched-final","output_format":"png","revised_prompt":"patched done"}}\n\n',
                    b'event: response.completed\ndata: {"type":"response.completed","response":{"created_at":1710000003,"status":"completed","output":[],"tool_usage":{"image_gen":{"images":1}}}}\n\n',
                ]
            )
            return DummyStreamContext(response)

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/images/generations",
            "headers": [(b"user-agent", b"yaak")],
        }
    )
    image_request = ImageProxyRequest(
        model_name="gpt-image-2",
        response_format="b64_json",
        stream=True,
        stream_prefix="image_generation",
        responses_payload={
            "model": "gpt-5.4-mini",
            "stream": True,
            "tools": [{"type": "image_generation", "action": "generate", "model": "gpt-image-2"}],
            "input": [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "draw"}]}],
        },
    )

    async def run_stream() -> tuple[object, str]:
        result = await _proxy_image_request_with_token(
            DummyClient(),
            request,
            image_request,
            access_token="access-token",
            account_id="account-1",
        )
        chunks: list[bytes] = []
        async for chunk in result.response.body_iterator:
            chunks.append(chunk)
        return result, b"".join(chunks).decode("utf-8")

    result, body = asyncio.run(run_stream())

    assert result.status_code == 200
    assert result.model_name == "gpt-image-2"
    assert "event: image_generation.completed" in body
    assert '"b64_json": "patched-final"' in body
    assert '"revised_prompt": "patched done"' in body
    assert '"usage": {"images": 1}' in body


def test_should_retry_upstream_server_error_only_for_5xx() -> None:
    assert _should_retry_upstream_server_error(500) is True
    assert _should_retry_upstream_server_error(503) is True
    assert _should_retry_upstream_server_error(429) is False
    assert _should_retry_upstream_server_error(400) is False


def test_request_log_handle_finalizes_via_async_outbox_without_waiting(monkeypatch) -> None:
    written_batches: list[list[dict[str, object]]] = []
    release_write = asyncio.Event()

    async def fake_upsert_request_logs(payloads, *, timing_recorder=None):
        del timing_recorder
        written_batches.append(payloads)
        await release_write.wait()
        return [
            {
                "request_id": payload["request_id"],
                "id": 777,
                "timing_spans": payload.get("timing_spans"),
            }
            for payload in payloads
        ]

    monkeypatch.setattr("oaix_gateway.api_server.upsert_request_logs", fake_upsert_request_logs)

    async def run() -> None:
        timing = _RequestTimingRecorder()
        handle = _RequestLogHandle.start(
            endpoint="/v1/responses",
            model="gpt-5.5",
            is_stream=True,
            started_at=datetime.now(timezone.utc),
            client_ip="127.0.0.1",
            user_agent="pytest",
            timing=timing,
        )
        await asyncio.sleep(0)
        request_log_id, spans = await asyncio.wait_for(
            handle.finalize(
                timing=timing,
                status_code=200,
                success=True,
                attempt_count=1,
                finished_at=datetime.now(timezone.utc),
                first_token_at=None,
                token_id=6,
                account_id="acct_6",
                model_name="gpt-5.5",
            ),
            timeout=0.05,
        )
        assert request_log_id is None
        assert spans is None
        release_write.set()
        await _flush_request_log_write_queue()

    asyncio.run(run())

    flattened = [payload for batch in written_batches for payload in batch]
    finalized = [payload for payload in flattened if payload.get("status_code") == 200]
    assert finalized
    assert finalized[0]["token_id"] == 6
    assert "request_log_queue_wait_ms" in finalized[0]["timing_spans"]


def test_execute_proxy_request_with_failover_finalizes_cancelled_request(monkeypatch) -> None:
    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=object(),
        )
    )
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/images/generations",
            "headers": [(b"user-agent", b"test-client")],
            "client": ("127.0.0.1", 9000),
            "app": app,
        },
        receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
    )
    finalized: list[dict[str, object]] = []
    claim_started = asyncio.Event()

    async def fake_create_request_log(**kwargs):
        return SimpleNamespace(id=515)

    async def fake_finalize_request_log(request_log_id: int, **kwargs) -> None:
        finalized.append({"request_log_id": request_log_id, **kwargs})

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None):
        claim_started.set()
        await asyncio.Event().wait()

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        raise AssertionError("proxy call should not run after cancellation")

    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)

    async def run() -> None:
        task = asyncio.create_task(
            _execute_proxy_request_with_failover(
                request,
                endpoint="/v1/images/generations",
                request_model="gpt-image-2",
                is_stream=False,
                proxy_call=fake_proxy_call,
            )
        )
        await claim_started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())

    assert finalized[0]["request_log_id"] == 515
    assert finalized[0]["status_code"] == 499
    assert finalized[0]["success"] is False
    assert finalized[0]["attempt_count"] == 0
    assert finalized[0]["token_id"] is None
    assert finalized[0]["account_id"] is None
    assert finalized[0]["error_message"] == "Client disconnected"
    assert app.state.response_traffic.active_responses == 0


def test_execute_proxy_request_with_failover_excludes_failed_token_on_retry(monkeypatch) -> None:
    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=None,
        )
    )

    class DummyOAuthManager:
        async def get_access_token(
            self,
            token_row,
            client,
            *,
            force_refresh: bool = False,
            reactivate_on_refresh: bool = True,
        ) -> tuple[str, bool]:
            return f"access-token-{token_row.id}", False

        def invalidate(self, token_id: int) -> None:
            raise AssertionError("invalidate should not run for retryable server errors")

    app.state.oauth_manager = DummyOAuthManager()
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/responses",
            "headers": [(b"user-agent", b"test-client")],
            "client": ("127.0.0.1", 9000),
            "app": app,
        },
        receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
    )
    first_token = CodexToken(
        id=31,
        account_id="acct_31",
        refresh_token="refresh-31",
        token_type="codex",
        is_active=True,
    )
    second_token = CodexToken(
        id=32,
        account_id="acct_32",
        refresh_token="refresh-32",
        token_type="codex",
        is_active=True,
        last_error="previous error",
    )
    claim_calls: list[tuple[int, ...]] = []
    mark_error_calls: list[tuple[int, str]] = []
    mark_success_calls: list[int] = []
    finalized: list[dict[str, object]] = []

    async def fake_get_token_counts():
        return SimpleNamespace(available=2)

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None):
        excluded = tuple(sorted(int(token_id) for token_id in (exclude_token_ids or ())))
        claim_calls.append(excluded)
        if first_token.id not in excluded:
            return first_token
        if second_token.id not in excluded:
            return second_token
        return None

    async def fake_create_request_log(**kwargs):
        return SimpleNamespace(id=131)

    async def fake_finalize_request_log(request_log_id: int, **kwargs) -> None:
        finalized.append({"request_log_id": request_log_id, **kwargs})

    async def fake_mark_token_error(token_id: int, message: str, **kwargs) -> None:
        mark_error_calls.append((token_id, message))

    async def fake_mark_token_success(token_id: int) -> None:
        mark_success_calls.append(token_id)

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        if account_id == "acct_31":
            raise HTTPException(status_code=504, detail="upstream gateway timeout")
        return SimpleNamespace(
            response=JSONResponse({"ok": True}),
            status_code=200,
            model_name="gpt-5",
            first_token_at=None,
            usage_metrics=None,
            on_success=None,
            stream_capture=None,
        )

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_error", fake_mark_token_error)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)

    response = asyncio.run(
        _execute_proxy_request_with_failover(
            request,
            endpoint="/v1/responses",
            request_model="gpt-5",
            is_stream=False,
            proxy_call=fake_proxy_call,
        )
    )

    assert response.status_code == 200
    assert claim_calls == [(), (31,)]
    assert mark_error_calls == [(31, "upstream gateway timeout")]
    assert mark_success_calls == [32]
    assert finalized[0]["attempt_count"] == 2
    assert finalized[0]["token_id"] == 32


def test_execute_proxy_request_with_failover_scopes_gpt_image_input_rate_limit(monkeypatch) -> None:
    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=None,
        )
    )

    class DummyOAuthManager:
        async def get_access_token(
            self,
            token_row,
            client,
            *,
            force_refresh: bool = False,
            reactivate_on_refresh: bool = True,
        ) -> tuple[str, bool]:
            return f"access-token-{token_row.id}", False

        def invalidate(self, token_id: int) -> None:
            raise AssertionError("invalidate should not run for image input rate limits")

    app.state.oauth_manager = DummyOAuthManager()
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/images/generations",
            "headers": [(b"user-agent", b"test-client")],
            "client": ("127.0.0.1", 9000),
            "app": app,
        },
        receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
    )
    first_token = CodexToken(
        id=131,
        account_id="acct_131",
        refresh_token="refresh-131",
        token_type="codex",
        is_active=True,
        plan_type="team",
    )
    second_token = CodexToken(
        id=132,
        account_id="acct_132",
        refresh_token="refresh-132",
        token_type="codex",
        is_active=True,
        plan_type="team",
        last_error="previous error",
    )
    scoped_cooling_token_ids: set[int] = set()
    claim_scopes: list[str | None] = []
    scoped_cooldown_calls: list[dict[str, object]] = []
    mark_error_calls: list[tuple[int, str]] = []
    mark_success_calls: list[int] = []
    finalized: list[dict[str, object]] = []

    async def fake_claim_next_active_token(
        *,
        selection_strategy: str,
        exclude_token_ids=None,
        scoped_cooldown_scope: str | None = None,
    ):
        claim_scopes.append(scoped_cooldown_scope)
        excluded = {int(token_id) for token_id in (exclude_token_ids or ())}
        for token in (first_token, second_token):
            if token.id in excluded:
                continue
            if scoped_cooldown_scope == "gpt-image-2:input-images" and token.id in scoped_cooling_token_ids:
                continue
            return token
        return None

    async def fake_create_request_log(**kwargs):
        return SimpleNamespace(id=231)

    async def fake_finalize_request_log(request_log_id: int, **kwargs) -> None:
        finalized.append({"request_log_id": request_log_id, **kwargs})

    async def fake_mark_token_scoped_cooldown(
        token_id: int,
        scope: str,
        detail: str,
        *,
        cooldown_seconds: int,
    ) -> None:
        scoped_cooling_token_ids.add(token_id)
        scoped_cooldown_calls.append(
            {
                "token_id": token_id,
                "scope": scope,
                "cooldown_seconds": cooldown_seconds,
                "detail": detail,
            }
        )

    async def fake_mark_token_error(token_id: int, message: str, **kwargs) -> None:
        mark_error_calls.append((token_id, message))

    async def fake_mark_token_success(token_id: int) -> None:
        mark_success_calls.append(token_id)

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        if account_id == "acct_131":
            raise HTTPException(
                status_code=429,
                detail=json.dumps(
                    {
                        "error": {
                            "type": "input-images",
                            "code": "rate_limit_exceeded",
                            "message": (
                                "Rate limit reached for gpt-image-2 on input-images per min: "
                                "Limit 250, Used 250, Requested 1. Please try again in 240ms."
                            ),
                        }
                    }
                ),
            )
        return SimpleNamespace(
            response=JSONResponse({"ok": True}),
            status_code=200,
            model_name="gpt-image-2",
            first_token_at=None,
            usage_metrics=None,
            on_success=None,
            stream_capture=None,
        )

    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_scoped_cooldown", fake_mark_token_scoped_cooldown)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_error", fake_mark_token_error)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)

    response = asyncio.run(
        _execute_proxy_request_with_failover(
            request,
            endpoint="/v1/images/generations",
            request_model="gpt-image-2",
            is_stream=False,
            proxy_call=fake_proxy_call,
        )
    )

    assert response.status_code == 200
    assert claim_scopes == ["gpt-image-2:input-images", "gpt-image-2:input-images"]
    assert scoped_cooldown_calls[0]["token_id"] == 131
    assert scoped_cooldown_calls[0]["scope"] == "gpt-image-2:input-images"
    assert scoped_cooldown_calls[0]["cooldown_seconds"] == 1
    assert mark_error_calls == []
    assert mark_success_calls == [132]
    assert finalized[0]["attempt_count"] == 2
    assert finalized[0]["token_id"] == 132


def test_execute_proxy_request_with_failover_returns_stream_before_mark_success(monkeypatch) -> None:
    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=None,
        )
    )

    class DummyOAuthManager:
        async def get_access_token(self, token_row, client) -> tuple[str, bool]:
            return "access-token", False

        def invalidate(self, token_id: int) -> None:
            raise AssertionError("invalidate should not run for successful stream")

    app.state.oauth_manager = DummyOAuthManager()
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/responses",
            "headers": [(b"user-agent", b"test-client")],
            "client": ("127.0.0.1", 9000),
            "app": app,
        },
        receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
    )
    token_row = CodexToken(
        id=41,
        account_id="acct_41",
        refresh_token="refresh-41",
        token_type="codex",
        is_active=True,
        plan_type="team",
        last_error="previous error",
    )
    mark_started = asyncio.Event()
    mark_continue = asyncio.Event()
    create_started = asyncio.Event()
    create_continue = asyncio.Event()
    finalized: list[dict[str, object]] = []

    async def fake_get_token_counts():
        raise AssertionError("get_token_counts should not run in the proxy hot path")

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None):
        return token_row

    async def fake_create_request_log(**kwargs):
        create_started.set()
        await create_continue.wait()
        return SimpleNamespace(id=141)

    async def fake_finalize_request_log(request_log_id: int, **kwargs) -> None:
        finalized.append({"request_log_id": request_log_id, **kwargs})

    async def fake_mark_token_success(token_id: int) -> None:
        mark_started.set()
        await mark_continue.wait()

    async def delayed_stream() -> AsyncIterator[bytes]:
        yield b"data: first\n\n"
        yield b"data: [DONE]\n\n"

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        return SimpleNamespace(
            response=StreamingResponse(delayed_stream(), media_type="text/event-stream"),
            status_code=200,
            model_name="gpt-5.5",
            first_token_at=None,
            usage_metrics=None,
            on_success=None,
            stream_capture=None,
        )

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)

    async def run() -> tuple[StreamingResponse, list[bytes]]:
        execute_task = asyncio.create_task(
            _execute_proxy_request_with_failover(
                request,
                endpoint="/v1/responses",
                request_model="gpt-5.5",
                is_stream=True,
                proxy_call=fake_proxy_call,
            )
        )
        await asyncio.wait_for(create_started.wait(), timeout=0.05)
        response = await asyncio.wait_for(execute_task, timeout=0.05)
        assert isinstance(response, StreamingResponse)
        assert not mark_started.is_set()

        async def collect() -> list[bytes]:
            return [chunk async for chunk in response.body_iterator]

        collect_task = asyncio.create_task(collect())
        create_continue.set()
        await asyncio.wait_for(mark_started.wait(), timeout=0.05)
        mark_continue.set()
        chunks = await asyncio.wait_for(collect_task, timeout=0.05)
        return response, chunks

    response, chunks = asyncio.run(run())

    assert response.status_code == 200
    assert chunks == [b"data: first\n\n", b"data: [DONE]\n\n"]
    assert finalized[0]["request_log_id"] == 141
    assert finalized[0]["success"] is True
    assert app.state.response_traffic.active_responses == 0


def test_execute_proxy_request_with_failover_does_not_count_tokens_on_hot_path(monkeypatch) -> None:
    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=None,
        )
    )

    class DummyOAuthManager:
        async def get_access_token(self, token_row, client) -> tuple[str, bool]:
            return "access-token", False

        def invalidate(self, token_id: int) -> None:
            raise AssertionError("invalidate should not run for successful request")

    app.state.oauth_manager = DummyOAuthManager()
    token_row = CodexToken(
        id=42,
        account_id="acct_42",
        refresh_token="refresh-42",
        token_type="codex",
        is_active=True,
        plan_type="team",
    )
    log_id = 200

    async def fake_get_token_counts():
        raise AssertionError("get_token_counts should not run in the proxy hot path")

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None):
        return token_row

    async def fake_create_request_log(**kwargs):
        nonlocal log_id
        log_id += 1
        return SimpleNamespace(id=log_id)

    async def fake_finalize_request_log(request_log_id: int, **kwargs) -> None:
        return None

    async def fake_mark_token_success(token_id: int) -> None:
        return None

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        return SimpleNamespace(
            response=JSONResponse({"ok": True}),
            status_code=200,
            model_name="gpt-5.5",
            first_token_at=None,
            usage_metrics=None,
            on_success=None,
            stream_capture=None,
        )

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)

    async def run_once() -> None:
        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/v1/responses",
                "headers": [(b"user-agent", b"test-client")],
                "client": ("127.0.0.1", 9000),
                "app": app,
            },
            receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
        )
        response = await _execute_proxy_request_with_failover(
            request,
            endpoint="/v1/responses",
            request_model="gpt-5.5",
            is_stream=False,
            proxy_call=fake_proxy_call,
        )
        assert response.status_code == 200

    async def run() -> None:
        await run_once()
        await run_once()

    asyncio.run(run())

    assert app.state.response_traffic.active_responses == 0


def test_execute_proxy_request_with_failover_retries_httpx_close_attribute_error(monkeypatch) -> None:
    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=None,
        )
    )

    class DummyOAuthManager:
        async def get_access_token(
            self,
            token_row,
            client,
            *,
            force_refresh: bool = False,
            reactivate_on_refresh: bool = True,
        ) -> tuple[str, bool]:
            return f"access-token-{token_row.id}", False

        def invalidate(self, token_id: int) -> None:
            raise AssertionError("invalidate should not run for transient transport errors")

    app.state.oauth_manager = DummyOAuthManager()
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/responses",
            "headers": [(b"user-agent", b"test-client")],
            "client": ("127.0.0.1", 9000),
            "app": app,
        },
        receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
    )
    first_token = CodexToken(
        id=41,
        account_id="acct_41",
        refresh_token="refresh-41",
        token_type="codex",
        is_active=True,
    )
    second_token = CodexToken(
        id=42,
        account_id="acct_42",
        refresh_token="refresh-42",
        token_type="codex",
        is_active=True,
        last_error="previous error",
    )
    claim_calls: list[tuple[int, ...]] = []
    mark_error_calls: list[tuple[int, str]] = []
    mark_success_calls: list[int] = []
    finalized: list[dict[str, object]] = []

    async def fake_get_token_counts():
        return SimpleNamespace(available=2)

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None):
        excluded = tuple(sorted(int(token_id) for token_id in (exclude_token_ids or ())))
        claim_calls.append(excluded)
        if first_token.id not in excluded:
            return first_token
        if second_token.id not in excluded:
            return second_token
        return None

    async def fake_create_request_log(**kwargs):
        return SimpleNamespace(id=141)

    async def fake_finalize_request_log(request_log_id: int, **kwargs) -> None:
        finalized.append({"request_log_id": request_log_id, **kwargs})

    async def fake_mark_token_error(token_id: int, message: str, **kwargs) -> None:
        mark_error_calls.append((token_id, message))

    async def fake_mark_token_success(token_id: int) -> None:
        mark_success_calls.append(token_id)

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        if account_id == "acct_41":
            raise AttributeError("'NoneType' object has no attribute 'call_soon'")
        return SimpleNamespace(
            response=JSONResponse({"ok": True}),
            status_code=200,
            model_name="gpt-5",
            first_token_at=None,
            usage_metrics=None,
            on_success=None,
            stream_capture=None,
        )

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_error", fake_mark_token_error)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)

    response = asyncio.run(
        _execute_proxy_request_with_failover(
            request,
            endpoint="/v1/responses",
            request_model="gpt-5",
            is_stream=False,
            proxy_call=fake_proxy_call,
        )
    )

    assert response.status_code == 200
    assert claim_calls == [(), (41,)]
    assert mark_error_calls[0][0] == 41
    assert "AttributeError" in mark_error_calls[0][1]
    assert mark_success_calls == [42]
    assert finalized[0]["attempt_count"] == 2
    assert finalized[0]["token_id"] == 42


def test_execute_proxy_request_with_failover_does_not_retry_nonretryable_http_exception(
    monkeypatch,
) -> None:
    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=None,
        )
    )

    class DummyOAuthManager:
        async def get_access_token(
            self,
            token_row,
            client,
            *,
            force_refresh: bool = False,
            reactivate_on_refresh: bool = True,
        ) -> tuple[str, bool]:
            return "access-token", False

        def invalidate(self, token_id: int) -> None:
            raise AssertionError("invalidate should not run for non-retryable gateway errors")

    app.state.oauth_manager = DummyOAuthManager()
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/images/generations",
            "headers": [(b"user-agent", b"yaak")],
            "client": ("127.0.0.1", 9000),
            "app": app,
        },
        receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
    )
    token_row = CodexToken(
        id=11,
        account_id="acct_11",
        refresh_token="refresh-token",
        is_active=True,
        plan_type="team",
    )

    claim_calls: list[int] = []
    mark_token_error_calls: list[dict[str, object]] = []
    finalized: list[dict[str, object]] = []

    async def fake_get_token_counts():
        return SimpleNamespace(available=5)

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None):
        claim_calls.append(1)
        return token_row if len(claim_calls) == 1 else None

    async def fake_create_request_log(**kwargs):
        return SimpleNamespace(id=77)

    async def fake_finalize_request_log(request_log_id: int, **kwargs) -> None:
        finalized.append({"request_log_id": request_log_id, **kwargs})

    async def fake_mark_token_error(
        token_id: int,
        message: str,
        *,
        deactivate: bool = False,
        cooldown_seconds: int | None = None,
        clear_access_token: bool = False,
    ) -> None:
        mark_token_error_calls.append(
            {
                "token_id": token_id,
                "message": message,
                "deactivate": deactivate,
                "cooldown_seconds": cooldown_seconds,
                "clear_access_token": clear_access_token,
            }
        )

    async def fake_mark_token_success(token_id: int) -> None:
        raise AssertionError("mark_token_success should not run when proxy_call fails")

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        exc = HTTPException(status_code=502, detail="Upstream did not return image output")
        exc.retryable = False
        exc.record_token_error = True
        raise exc

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_error", fake_mark_token_error)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            _execute_proxy_request_with_failover(
                request,
                endpoint="/v1/images/generations",
                request_model="gpt-image-2",
                is_stream=False,
                proxy_call=fake_proxy_call,
            )
        )

    assert exc_info.value.status_code == 502
    assert str(exc_info.value.detail) == "Upstream did not return image output"
    assert claim_calls == [1]
    assert mark_token_error_calls == [
        {
            "token_id": 11,
            "message": "Upstream did not return image output",
            "deactivate": False,
            "cooldown_seconds": None,
            "clear_access_token": False,
        }
    ]
    assert len(finalized) == 1
    assert finalized[0]["request_log_id"] == 77
    assert finalized[0]["status_code"] == 502
    assert finalized[0]["success"] is False
    assert finalized[0]["attempt_count"] == 1
    assert finalized[0]["finished_at"] is not None
    assert finalized[0]["token_id"] == 11
    assert finalized[0]["account_id"] == "acct_11"
    assert finalized[0]["error_message"] == "Upstream did not return image output"


def test_execute_proxy_request_with_failover_does_not_retry_image_moderation_error(
    monkeypatch,
) -> None:
    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=None,
        )
    )

    class DummyOAuthManager:
        async def get_access_token(
            self,
            token_row,
            client,
            *,
            force_refresh: bool = False,
            reactivate_on_refresh: bool = True,
        ) -> tuple[str, bool]:
            return "access-token", False

        def invalidate(self, token_id: int) -> None:
            raise AssertionError("invalidate should not run for moderation errors")

    app.state.oauth_manager = DummyOAuthManager()
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/images/generations",
            "headers": [(b"user-agent", b"yaak")],
            "client": ("127.0.0.1", 9000),
            "app": app,
        },
        receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
    )
    tokens = [
        CodexToken(id=11, account_id="acct_11", refresh_token="refresh-11", is_active=True, plan_type="plus"),
        CodexToken(id=12, account_id="acct_12", refresh_token="refresh-12", is_active=True, plan_type="plus"),
    ]

    claim_calls: list[int] = []
    finalized: list[dict[str, object]] = []
    detail = json.dumps(
        {
            "error": {
                "type": "image_generation_user_error",
                "code": "moderation_blocked",
                "message": "Your request was rejected by the safety system.",
                "param": None,
            }
        }
    )

    async def fake_get_token_counts():
        return SimpleNamespace(available=2)

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None):
        claim_calls.append(1)
        return tokens[len(claim_calls) - 1] if len(claim_calls) <= len(tokens) else None

    async def fake_create_request_log(**kwargs):
        return SimpleNamespace(id=78)

    async def fake_finalize_request_log(request_log_id: int, **kwargs) -> None:
        finalized.append({"request_log_id": request_log_id, **kwargs})

    async def fake_mark_token_error(*args, **kwargs) -> None:
        raise AssertionError("moderation errors should not mark the token unhealthy")

    async def fake_mark_token_success(token_id: int) -> None:
        raise AssertionError("mark_token_success should not run when proxy_call fails")

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        raise _upstream_error_http_exception(500, detail)

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_error", fake_mark_token_error)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            _execute_proxy_request_with_failover(
                request,
                endpoint="/v1/images/generations",
                request_model="gpt-image-2",
                is_stream=False,
                proxy_call=fake_proxy_call,
            )
        )

    assert exc_info.value.status_code == 400
    assert str(exc_info.value.detail) == detail
    assert claim_calls == [1]
    assert len(finalized) == 1
    assert finalized[0]["request_log_id"] == 78
    assert finalized[0]["status_code"] == 400
    assert finalized[0]["success"] is False
    assert finalized[0]["attempt_count"] == 1
    assert finalized[0]["token_id"] == 11
    assert finalized[0]["account_id"] == "acct_11"
    assert finalized[0]["error_message"] == detail


def test_execute_proxy_request_with_failover_skips_free_tokens_for_gpt_image_2(monkeypatch) -> None:
    class DummyQuotaService:
        def get_cached_snapshot(self, token_id: int):
            plan_type = "free" if token_id == 11 else "plus"
            return CodexQuotaSnapshot(
                fetched_at=datetime.now(timezone.utc),
                error=None,
                plan_type=plan_type,
                windows=[],
            )

        async def get_snapshot(self, *args, **kwargs):
            raise AssertionError("gpt-image-2 token selection should not fetch quota snapshots on the hot path")

    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=None,
            quota_service=DummyQuotaService(),
        )
    )

    class DummyOAuthManager:
        async def get_access_token(
            self,
            token_row,
            client,
            *,
            force_refresh: bool = False,
            reactivate_on_refresh: bool = True,
        ) -> tuple[str, bool]:
            return f"access-token-{token_row.id}", False

        def invalidate(self, token_id: int) -> None:
            raise AssertionError("invalidate should not run in successful failover test")

    app.state.oauth_manager = DummyOAuthManager()
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": [(b"user-agent", b"yaak")],
            "client": ("127.0.0.1", 9000),
            "app": app,
        },
        receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
    )
    free_token = CodexToken(
        id=11,
        account_id="acct_free",
        refresh_token="refresh-free",
        is_active=True,
        raw_payload={},
    )
    paid_token = CodexToken(
        id=12,
        account_id="acct_paid",
        refresh_token="refresh-paid",
        is_active=True,
        raw_payload={},
        last_error="previous error",
    )

    claim_calls: list[tuple[int, ...]] = []
    mark_success_calls: list[int] = []
    proxy_calls: list[tuple[str, str | None]] = []
    finalized: list[dict[str, object]] = []

    async def fake_get_token_counts():
        return SimpleNamespace(available=2)

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None):
        excluded = tuple(sorted(int(token_id) for token_id in (exclude_token_ids or ())))
        claim_calls.append(excluded)
        if 11 not in excluded:
            return free_token
        return paid_token

    async def fake_create_request_log(**kwargs):
        return SimpleNamespace(id=88)

    async def fake_finalize_request_log(request_log_id: int, **kwargs) -> None:
        finalized.append({"request_log_id": request_log_id, **kwargs})

    async def fake_mark_token_success(token_id: int) -> None:
        mark_success_calls.append(token_id)

    async def fake_mark_token_error(*args, **kwargs) -> None:
        raise AssertionError("mark_token_error should not run on successful request")

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        proxy_calls.append((access_token, account_id))
        return SimpleNamespace(
            response=JSONResponse({"ok": True}),
            status_code=200,
            model_name="gpt-image-2",
            first_token_at=None,
            usage_metrics=None,
            on_success=None,
            stream_capture=None,
        )

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_error", fake_mark_token_error)

    response = asyncio.run(
        _execute_proxy_request_with_failover(
            request,
            endpoint="/v1/chat/completions",
            request_model="gpt-image-2",
            is_stream=False,
            proxy_call=fake_proxy_call,
        )
    )

    assert response.status_code == 200
    assert claim_calls == [(), (11,)]
    assert proxy_calls == [("access-token-12", "acct_paid")]
    assert mark_success_calls == [12]
    assert len(finalized) == 1
    assert finalized[0]["request_log_id"] == 88
    assert finalized[0]["status_code"] == 200
    assert finalized[0]["success"] is True
    assert finalized[0]["attempt_count"] == 1
    assert finalized[0]["finished_at"] is not None
    assert finalized[0]["first_token_at"] is None
    assert finalized[0]["token_id"] == 12
    assert finalized[0]["account_id"] == "acct_paid"
    assert finalized[0]["model_name"] == "gpt-image-2"


def test_execute_proxy_request_with_failover_wraps_gpt_image_2_stream_with_outer_keepalive(
    monkeypatch,
) -> None:
    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=None,
        )
    )

    class DummyOAuthManager:
        async def get_access_token(
            self,
            token_row,
            client,
            *,
            force_refresh: bool = False,
            reactivate_on_refresh: bool = True,
        ) -> tuple[str, bool]:
            return "access-token", False

        def invalidate(self, token_id: int) -> None:
            raise AssertionError("invalidate should not run in successful stream test")

    app.state.oauth_manager = DummyOAuthManager()
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": [(b"user-agent", b"yaak")],
            "client": ("127.0.0.1", 9000),
            "app": app,
        },
        receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
    )
    token_row = CodexToken(
        id=13,
        account_id="acct_paid",
        refresh_token="refresh-token",
        is_active=True,
        raw_payload={},
        last_error="previous error",
    )

    claim_calls = 0
    mark_success_calls: list[int] = []
    finalized: list[dict[str, object]] = []

    async def fake_get_token_counts():
        return SimpleNamespace(available=1)

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None):
        nonlocal claim_calls
        claim_calls += 1
        return token_row if claim_calls == 1 else None

    async def fake_create_request_log(**kwargs):
        return SimpleNamespace(id=92)

    async def fake_finalize_request_log(request_log_id: int, **kwargs) -> None:
        finalized.append({"request_log_id": request_log_id, **kwargs})

    async def fake_mark_token_success(token_id: int) -> None:
        mark_success_calls.append(token_id)

    async def fake_mark_token_error(*args, **kwargs) -> None:
        raise AssertionError("mark_token_error should not run on successful stream")

    async def delayed_stream() -> AsyncIterator[bytes]:
        await asyncio.sleep(0.05)
        yield b"data: delayed\n\n"
        yield b"data: [DONE]\n\n"

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        return SimpleNamespace(
            response=StreamingResponse(delayed_stream(), media_type="text/event-stream"),
            status_code=200,
            model_name="gpt-image-2",
            first_token_at=None,
            usage_metrics=None,
            on_success=None,
            stream_capture=None,
        )

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_error", fake_mark_token_error)
    monkeypatch.setattr("oaix_gateway.api_server._stream_keepalive_interval_seconds", lambda: 0.01)

    response = asyncio.run(
        _execute_proxy_request_with_failover(
            request,
            endpoint="/v1/chat/completions",
            request_model="gpt-image-2",
            is_stream=True,
            proxy_call=fake_proxy_call,
        )
    )

    async def collect_chunks() -> list[bytes]:
        chunks: list[bytes] = []
        async for chunk in response.body_iterator:
            chunks.append(chunk)
        return chunks

    chunks = asyncio.run(collect_chunks())

    assert isinstance(response, StreamingResponse)
    assert response.status_code == 200
    assert chunks[0].startswith(b": keepalive")
    assert b"data: delayed\n\n" in chunks
    assert chunks[-1] == b"data: [DONE]\n\n"
    assert mark_success_calls == [13]
    assert len(finalized) == 1
    assert finalized[0]["request_log_id"] == 92
    assert finalized[0]["status_code"] == 200
    assert finalized[0]["success"] is True
    assert finalized[0]["attempt_count"] == 1
    assert finalized[0]["token_id"] == 13
    assert finalized[0]["account_id"] == "acct_paid"
    assert finalized[0]["model_name"] == "gpt-image-2"


def test_execute_proxy_request_with_failover_marks_gpt_image_stream_abort_as_failed(
    monkeypatch,
) -> None:
    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=None,
        )
    )

    class DummyOAuthManager:
        async def get_access_token(
            self,
            token_row,
            client,
            *,
            force_refresh: bool = False,
            reactivate_on_refresh: bool = True,
        ) -> tuple[str, bool]:
            return "access-token", False

        def invalidate(self, token_id: int) -> None:
            raise AssertionError("invalidate should not run in stream abort test")

    app.state.oauth_manager = DummyOAuthManager()
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": [(b"user-agent", b"yaak")],
            "client": ("127.0.0.1", 9000),
            "app": app,
        },
        receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
    )
    token_row = CodexToken(
        id=33,
        account_id="acct_paid",
        refresh_token="refresh-token",
        is_active=True,
        raw_payload={},
    )

    claim_calls = 0
    mark_success_calls: list[int] = []
    mark_error_calls: list[tuple[int, str]] = []
    finalized: list[dict[str, object]] = []
    success_hook_calls: list[int] = []

    async def fake_get_token_counts():
        return SimpleNamespace(available=1)

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None):
        nonlocal claim_calls
        claim_calls += 1
        return token_row if claim_calls == 1 else None

    async def fake_create_request_log(**kwargs):
        return SimpleNamespace(id=99)

    async def fake_finalize_request_log(request_log_id: int, **kwargs) -> None:
        finalized.append({"request_log_id": request_log_id, **kwargs})

    async def fake_mark_token_success(token_id: int) -> None:
        mark_success_calls.append(token_id)

    async def fake_mark_token_error(
        token_id: int,
        message: str,
        *,
        deactivate: bool = False,
        cooldown_seconds: int | None = None,
        clear_access_token: bool = False,
    ) -> None:
        mark_error_calls.append((token_id, message))

    async def fake_on_success(request_log_id: int) -> None:
        success_hook_calls.append(request_log_id)

    async def failed_stream(capture: _ProxyStreamCapture) -> AsyncIterator[bytes]:
        error_event = _build_stream_error_event(
            502,
            "Upstream image stream aborted before completion: RemoteProtocolError: incomplete chunked read",
        )
        capture.feed(error_event)
        yield error_event
        yield b"data: [DONE]\n\n"

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        capture = _ProxyStreamCapture(initial_model_name="gpt-image-2")
        return SimpleNamespace(
            response=StreamingResponse(failed_stream(capture), media_type="text/event-stream"),
            status_code=200,
            model_name="gpt-image-2",
            first_token_at=None,
            usage_metrics=None,
            on_success=fake_on_success,
            stream_capture=capture,
        )

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_error", fake_mark_token_error)

    response = asyncio.run(
        _execute_proxy_request_with_failover(
            request,
            endpoint="/v1/chat/completions",
            request_model="gpt-image-2",
            is_stream=True,
            proxy_call=fake_proxy_call,
        )
    )

    async def collect_chunks() -> list[bytes]:
        chunks: list[bytes] = []
        async for chunk in response.body_iterator:
            chunks.append(chunk)
        return chunks

    chunks = asyncio.run(collect_chunks())
    body = b"".join(chunks).decode("utf-8")

    assert isinstance(response, StreamingResponse)
    assert response.status_code == 200
    assert chunks[0].startswith(b": keepalive")
    assert "event: error" in body
    assert "data: [DONE]" in body
    assert mark_success_calls == []
    assert len(mark_error_calls) == 1
    assert mark_error_calls[0][0] == 33
    assert "RemoteProtocolError" in mark_error_calls[0][1]
    assert success_hook_calls == []
    assert len(finalized) == 1
    assert finalized[0]["request_log_id"] == 99
    assert finalized[0]["status_code"] == 502
    assert finalized[0]["success"] is False
    assert finalized[0]["attempt_count"] == 1
    assert finalized[0]["token_id"] == 33
    assert finalized[0]["account_id"] == "acct_paid"
    assert "RemoteProtocolError" in str(finalized[0]["error_message"])


def test_execute_proxy_request_with_failover_emits_terminal_error_after_first_output_timeout(
    monkeypatch,
) -> None:
    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=None,
        )
    )

    class DummyOAuthManager:
        async def get_access_token(
            self,
            token_row,
            client,
            *,
            force_refresh: bool = False,
            reactivate_on_refresh: bool = True,
        ) -> tuple[str, bool]:
            return f"access-token-{token_row.id}", False

        def invalidate(self, token_id: int) -> None:
            raise AssertionError("invalidate should not run in first-output-timeout retry test")

    app.state.oauth_manager = DummyOAuthManager()
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": [(b"user-agent", b"yaak")],
            "client": ("127.0.0.1", 9000),
            "app": app,
        },
        receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
    )
    token_row = CodexToken(
        id=61,
        account_id="acct-first",
        refresh_token="refresh-first",
        is_active=True,
        raw_payload={},
    )

    claim_calls = 0
    mark_success_calls: list[int] = []
    mark_error_calls: list[tuple[int, str]] = []
    finalized: list[dict[str, object]] = []

    async def fake_get_token_counts():
        return SimpleNamespace(available=1)

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None):
        nonlocal claim_calls
        claim_calls += 1
        return token_row if claim_calls == 1 else None

    async def fake_create_request_log(**kwargs):
        return SimpleNamespace(id=111)

    async def fake_finalize_request_log(request_log_id: int, **kwargs) -> None:
        finalized.append({"request_log_id": request_log_id, **kwargs})

    async def fake_mark_token_success(token_id: int) -> None:
        mark_success_calls.append(token_id)

    async def fake_mark_token_error(
        token_id: int,
        message: str,
        *,
        deactivate: bool = False,
        cooldown_seconds: int | None = None,
        clear_access_token: bool = False,
    ) -> None:
        mark_error_calls.append((token_id, message))

    async def stalled_stream() -> AsyncIterator[bytes]:
        await asyncio.sleep(0.02)
        yield b": upstream-keepalive\n\n"

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        return SimpleNamespace(
            response=StreamingResponse(stalled_stream(), media_type="text/event-stream"),
            status_code=200,
            model_name="gpt-image-2",
            first_token_at=None,
            usage_metrics=None,
            on_success=None,
            stream_capture=_ProxyStreamCapture(initial_model_name="gpt-image-2"),
        )

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_error", fake_mark_token_error)
    monkeypatch.setattr("oaix_gateway.api_server._stream_keepalive_interval_seconds", lambda: 0.005)
    monkeypatch.setattr("oaix_gateway.api_server._image_stream_first_output_timeout_seconds", lambda: 0.01)

    response = asyncio.run(
        _execute_proxy_request_with_failover(
            request,
            endpoint="/v1/chat/completions",
            request_model="gpt-image-2",
            is_stream=True,
            proxy_call=fake_proxy_call,
        )
    )

    async def collect_chunks() -> list[bytes]:
        chunks: list[bytes] = []
        async for chunk in response.body_iterator:
            chunks.append(chunk)
        return chunks

    chunks = asyncio.run(collect_chunks())
    body = b"".join(chunks).decode("utf-8")

    assert isinstance(response, StreamingResponse)
    assert response.status_code == 200
    assert chunks[0].startswith(b": keepalive")
    assert '"status_code":504' in body.replace(" ", "")
    assert "timed out before producing a response" in body
    assert "data: [DONE]" in body
    assert mark_success_calls == []
    assert mark_error_calls == [
        (
            61,
            "Upstream image stream timed out before producing a response (0.01s without image output)",
        )
    ]
    assert len(finalized) == 1
    assert finalized[0]["request_log_id"] == 111
    assert finalized[0]["status_code"] == 504
    assert finalized[0]["success"] is False
    assert finalized[0]["attempt_count"] == 1
    assert finalized[0]["token_id"] == 61
    assert finalized[0]["account_id"] == "acct-first"
    assert "timed out before producing a response" in str(finalized[0]["error_message"])


def test_execute_proxy_request_with_failover_retries_gpt_image_stream_after_premature_done(
    monkeypatch,
) -> None:
    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=None,
        )
    )

    class DummyOAuthManager:
        async def get_access_token(
            self,
            token_row,
            client,
            *,
            force_refresh: bool = False,
            reactivate_on_refresh: bool = True,
        ) -> tuple[str, bool]:
            return f"access-token-{token_row.id}", False

        def invalidate(self, token_id: int) -> None:
            raise AssertionError("invalidate should not run in premature-done retry test")

    app.state.oauth_manager = DummyOAuthManager()
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": [(b"user-agent", b"yaak")],
            "client": ("127.0.0.1", 9000),
            "app": app,
        },
        receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
    )
    first_token = CodexToken(
        id=71,
        account_id="acct-first",
        refresh_token="refresh-first",
        is_active=True,
        raw_payload={},
        plan_type="team",
    )
    second_token = CodexToken(
        id=72,
        account_id="acct-second",
        refresh_token="refresh-second",
        is_active=True,
        raw_payload={},
        plan_type="team",
        last_error="previous error",
    )

    claim_calls = 0
    seen_excludes: list[set[int]] = []
    mark_success_calls: list[int] = []
    mark_error_calls: list[tuple[int, str]] = []
    finalized: list[dict[str, object]] = []

    async def fake_get_token_counts():
        return SimpleNamespace(available=2)

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None):
        nonlocal claim_calls
        claim_calls += 1
        excludes = set(exclude_token_ids or [])
        seen_excludes.append(excludes)
        if first_token.id not in excludes:
            return first_token
        if second_token.id not in excludes:
            return second_token
        return None

    async def fake_create_request_log(**kwargs):
        return SimpleNamespace(id=211)

    async def fake_finalize_request_log(request_log_id: int, **kwargs) -> None:
        finalized.append({"request_log_id": request_log_id, **kwargs})

    async def fake_mark_token_success(token_id: int) -> None:
        mark_success_calls.append(token_id)

    async def fake_mark_token_error(token_id: int, message: str, **kwargs) -> None:
        mark_error_calls.append((token_id, message))

    async def premature_done_stream() -> AsyncIterator[bytes]:
        yield b": upstream-keepalive\n\n"
        yield b"data: [DONE]\n\n"

    async def success_stream() -> AsyncIterator[bytes]:
        yield b'data: {"id":"chatcmpl_success","choices":[{"index":0,"delta":{"content":"![image](data:image/png;base64,done-image)"},"finish_reason":null}]}\n\n'
        yield b'data: {"id":"chatcmpl_success","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n'
        yield b"data: [DONE]\n\n"

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        if account_id == "acct-first":
            return SimpleNamespace(
                response=StreamingResponse(premature_done_stream(), media_type="text/event-stream"),
                status_code=200,
                model_name="gpt-image-2",
                first_token_at=None,
                usage_metrics=None,
                on_success=None,
                stream_capture=_ProxyStreamCapture(initial_model_name="gpt-image-2"),
            )
        return SimpleNamespace(
            response=StreamingResponse(success_stream(), media_type="text/event-stream"),
            status_code=200,
            model_name="gpt-image-2",
            first_token_at=None,
            usage_metrics=None,
            on_success=None,
            stream_capture=_ProxyStreamCapture(initial_model_name="gpt-image-2"),
        )

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_error", fake_mark_token_error)
    monkeypatch.setattr("oaix_gateway.api_server._stream_keepalive_interval_seconds", lambda: 0.005)

    response = asyncio.run(
        _execute_proxy_request_with_failover(
            request,
            endpoint="/v1/chat/completions",
            request_model="gpt-image-2",
            is_stream=True,
            proxy_call=fake_proxy_call,
        )
    )

    async def collect_chunks() -> list[bytes]:
        chunks: list[bytes] = []
        async for chunk in response.body_iterator:
            chunks.append(chunk)
        return chunks

    chunks = asyncio.run(collect_chunks())
    body = b"".join(chunks).decode("utf-8")

    assert isinstance(response, StreamingResponse)
    assert response.status_code == 200
    assert body.count(": keepalive") >= 1
    assert "done-image" in body
    assert "Upstream image stream ended before producing a response" not in body
    assert mark_error_calls == [(71, "Upstream image stream ended before producing a response")]
    assert mark_success_calls == [72]
    assert seen_excludes[0] == set()
    assert first_token.id in seen_excludes[1]
    assert len(finalized) == 1
    assert finalized[0]["request_log_id"] == 211
    assert finalized[0]["status_code"] == 200
    assert finalized[0]["success"] is True
    assert finalized[0]["attempt_count"] == 2
    assert finalized[0]["token_id"] == 72
    assert finalized[0]["account_id"] == "acct-second"


def test_execute_proxy_request_with_failover_returns_immediately_for_gpt_image_2_stream_while_auth_is_pending(
    monkeypatch,
) -> None:
    auth_gate = asyncio.Event()
    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=None,
        )
    )

    class DummyOAuthManager:
        async def get_access_token(
            self,
            token_row,
            client,
            *,
            force_refresh: bool = False,
            reactivate_on_refresh: bool = True,
        ) -> tuple[str, bool]:
            await auth_gate.wait()
            return "access-token", False

        def invalidate(self, token_id: int) -> None:
            raise AssertionError("invalidate should not run in successful stream test")

    app.state.oauth_manager = DummyOAuthManager()

    async def receive() -> dict[str, object]:
        return {"type": "http.request", "body": b"", "more_body": False}

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": [(b"user-agent", b"yaak")],
            "client": ("127.0.0.1", 9000),
            "app": app,
        },
        receive=receive,
    )
    token_row = CodexToken(
        id=31,
        account_id="acct_paid",
        refresh_token="refresh-token",
        is_active=True,
        raw_payload={},
        plan_type="team",
        last_error="previous error",
    )

    claim_calls = 0
    mark_success_calls: list[int] = []
    finalized: list[dict[str, object]] = []

    async def fake_get_token_counts():
        return SimpleNamespace(available=1)

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None):
        nonlocal claim_calls
        claim_calls += 1
        return token_row if claim_calls == 1 else None

    async def fake_create_request_log(**kwargs):
        return SimpleNamespace(id=101)

    async def fake_finalize_request_log(request_log_id: int, **kwargs) -> None:
        finalized.append({"request_log_id": request_log_id, **kwargs})

    async def fake_mark_token_success(token_id: int) -> None:
        mark_success_calls.append(token_id)

    async def fake_mark_token_error(*args, **kwargs) -> None:
        raise AssertionError("mark_token_error should not run on successful stream")

    async def delayed_stream() -> AsyncIterator[bytes]:
        yield b"data: delayed\n\n"
        yield b"data: [DONE]\n\n"

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        return SimpleNamespace(
            response=StreamingResponse(delayed_stream(), media_type="text/event-stream"),
            status_code=200,
            model_name="gpt-image-2",
            first_token_at=None,
            usage_metrics=None,
            on_success=None,
            stream_capture=None,
        )

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_error", fake_mark_token_error)
    monkeypatch.setattr("oaix_gateway.api_server._stream_keepalive_interval_seconds", lambda: 0.01)

    async def run() -> tuple[StreamingResponse, list[bytes]]:
        response = await asyncio.wait_for(
            _execute_proxy_request_with_failover(
                request,
                endpoint="/v1/chat/completions",
                request_model="gpt-image-2",
                is_stream=True,
                proxy_call=fake_proxy_call,
            ),
            timeout=0.02,
        )
        iterator = response.body_iterator.__aiter__()
        first_chunk = await asyncio.wait_for(iterator.__anext__(), timeout=0.03)
        auth_gate.set()
        chunks = [first_chunk]
        async for chunk in iterator:
            chunks.append(chunk)
        return response, chunks

    response, chunks = asyncio.run(run())

    assert isinstance(response, StreamingResponse)
    assert response.status_code == 200
    assert chunks[0].startswith(b": keepalive")
    assert b"data: delayed\n\n" in chunks
    assert chunks[-1] == b"data: [DONE]\n\n"
    assert mark_success_calls == [31]
    assert len(finalized) == 1
    assert finalized[0]["request_log_id"] == 101
    assert finalized[0]["status_code"] == 200
    assert finalized[0]["success"] is True
    assert finalized[0]["attempt_count"] == 1
    assert finalized[0]["token_id"] == 31
    assert finalized[0]["account_id"] == "acct_paid"


def test_execute_proxy_request_with_failover_keeps_retrying_gpt_image_2_stream_after_keepalive(
    monkeypatch,
) -> None:
    first_attempt_gate = asyncio.Event()
    second_attempt_gate = asyncio.Event()
    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=None,
        )
    )

    class DummyOAuthManager:
        async def get_access_token(
            self,
            token_row,
            client,
            *,
            force_refresh: bool = False,
            reactivate_on_refresh: bool = True,
        ) -> tuple[str, bool]:
            return f"access-token-{token_row.id}", False

        def invalidate(self, token_id: int) -> None:
            raise AssertionError("invalidate should not run in retry stream test")

    app.state.oauth_manager = DummyOAuthManager()

    async def receive() -> dict[str, object]:
        return {"type": "http.request", "body": b"", "more_body": False}

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": [(b"user-agent", b"yaak")],
            "client": ("127.0.0.1", 9000),
            "app": app,
        },
        receive=receive,
    )
    first_token = CodexToken(
        id=41,
        account_id="acct-first",
        refresh_token="refresh-first",
        is_active=True,
        raw_payload={},
        plan_type="team",
    )
    second_token = CodexToken(
        id=42,
        account_id="acct-second",
        refresh_token="refresh-second",
        is_active=True,
        raw_payload={},
        plan_type="team",
        last_error="previous error",
    )

    claim_calls = 0
    mark_success_calls: list[int] = []
    mark_error_calls: list[tuple[int, str]] = []
    finalized: list[dict[str, object]] = []

    async def fake_get_token_counts():
        return SimpleNamespace(available=2)

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None):
        nonlocal claim_calls
        claim_calls += 1
        if claim_calls == 1:
            return first_token
        if claim_calls == 2:
            return second_token
        return None

    async def fake_create_request_log(**kwargs):
        return SimpleNamespace(id=202)

    async def fake_finalize_request_log(request_log_id: int, **kwargs) -> None:
        finalized.append({"request_log_id": request_log_id, **kwargs})

    async def fake_mark_token_success(token_id: int) -> None:
        mark_success_calls.append(token_id)

    async def fake_mark_token_error(token_id: int, detail: str, **kwargs) -> None:
        mark_error_calls.append((token_id, detail))

    async def success_stream() -> AsyncIterator[bytes]:
        yield b"data: success\n\n"
        yield b"data: [DONE]\n\n"

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        if account_id == "acct-first":
            await first_attempt_gate.wait()
            raise HTTPException(status_code=502, detail="first attempt failed")
        await second_attempt_gate.wait()
        return SimpleNamespace(
            response=StreamingResponse(success_stream(), media_type="text/event-stream"),
            status_code=200,
            model_name="gpt-image-2",
            first_token_at=None,
            usage_metrics=None,
            on_success=None,
            stream_capture=None,
        )

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_error", fake_mark_token_error)
    monkeypatch.setattr("oaix_gateway.api_server._stream_keepalive_interval_seconds", lambda: 0.01)

    async def run() -> list[bytes]:
        response = await asyncio.wait_for(
            _execute_proxy_request_with_failover(
                request,
                endpoint="/v1/chat/completions",
                request_model="gpt-image-2",
                is_stream=True,
                proxy_call=fake_proxy_call,
            ),
            timeout=0.02,
        )
        iterator = response.body_iterator.__aiter__()
        first_chunk = await asyncio.wait_for(iterator.__anext__(), timeout=0.03)
        first_attempt_gate.set()
        second_chunk = await asyncio.wait_for(iterator.__anext__(), timeout=0.03)
        second_attempt_gate.set()
        chunks = [first_chunk, second_chunk]
        async for chunk in iterator:
            chunks.append(chunk)
        return chunks

    chunks = asyncio.run(run())
    body = b"".join(chunks)

    assert chunks[0].startswith(b": keepalive")
    assert chunks[1].startswith(b": keepalive")
    assert b"data: success\n\n" in body
    assert b"first attempt failed" not in body
    assert mark_error_calls == [(41, "first attempt failed")]
    assert mark_success_calls == [42]
    assert len(finalized) == 1
    assert finalized[0]["request_log_id"] == 202
    assert finalized[0]["status_code"] == 200
    assert finalized[0]["success"] is True
    assert finalized[0]["attempt_count"] == 2
    assert finalized[0]["token_id"] == 42
    assert finalized[0]["account_id"] == "acct-second"


def test_execute_proxy_request_with_failover_allows_free_tokens_for_gpt_5_5(monkeypatch) -> None:
    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=None,
        )
    )

    class DummyOAuthManager:
        async def get_access_token(
            self,
            token_row,
            client,
            *,
            force_refresh: bool = False,
            reactivate_on_refresh: bool = True,
        ) -> tuple[str, bool]:
            return f"access-token-{token_row.id}", False

        def invalidate(self, token_id: int) -> None:
            raise AssertionError("invalidate should not run in successful request test")

    app.state.oauth_manager = DummyOAuthManager()
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/responses",
            "headers": [(b"user-agent", b"yaak")],
            "client": ("127.0.0.1", 9000),
            "app": app,
        },
        receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
    )
    free_token = CodexToken(
        id=21,
        account_id="acct_free",
        refresh_token="refresh-free",
        is_active=True,
        raw_payload={"plan_type": "free"},
        last_error="previous error",
    )

    claim_calls: list[tuple[int, ...]] = []
    mark_success_calls: list[int] = []
    proxy_calls: list[tuple[str, str | None]] = []

    async def fake_get_token_counts():
        return SimpleNamespace(available=1)

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None):
        excluded = tuple(sorted(int(token_id) for token_id in (exclude_token_ids or ())))
        claim_calls.append(excluded)
        return free_token if len(claim_calls) == 1 else None

    async def fake_create_request_log(**kwargs):
        return SimpleNamespace(id=89)

    async def fake_finalize_request_log(*args, **kwargs) -> None:
        return None

    async def fake_mark_token_success(token_id: int) -> None:
        mark_success_calls.append(token_id)

    async def fake_mark_token_error(*args, **kwargs) -> None:
        raise AssertionError("mark_token_error should not run on successful request")

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        proxy_calls.append((access_token, account_id))
        return SimpleNamespace(
            response=JSONResponse({"ok": True}),
            status_code=200,
            model_name="gpt-5.5",
            first_token_at=None,
            usage_metrics=None,
            on_success=None,
            stream_capture=None,
        )

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_error", fake_mark_token_error)

    response = asyncio.run(
        _execute_proxy_request_with_failover(
            request,
            endpoint="/v1/responses",
            request_model="gpt-5.5",
            is_stream=False,
            proxy_call=fake_proxy_call,
        )
    )

    assert response.status_code == 200
    assert claim_calls == [()]
    assert proxy_calls == [("access-token-21", "acct_free")]
    assert mark_success_calls == [21]


def test_execute_proxy_request_with_failover_returns_clear_503_when_only_free_tokens_exist_for_gpt_image_2(
    monkeypatch,
) -> None:
    class DummyQuotaService:
        def get_cached_snapshot(self, token_id: int):
            return CodexQuotaSnapshot(
                fetched_at=datetime.now(timezone.utc),
                error=None,
                plan_type="free",
                windows=[],
            )

        async def get_snapshot(self, *args, **kwargs):
            raise AssertionError("gpt-image-2 token selection should not fetch quota snapshots on the hot path")

    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=None,
            quota_service=DummyQuotaService(),
        )
    )

    class DummyOAuthManager:
        async def get_access_token(
            self,
            token_row,
            client,
            *,
            force_refresh: bool = False,
            reactivate_on_refresh: bool = True,
        ) -> tuple[str, bool]:
            raise AssertionError("free tokens should be skipped before fetching access tokens")

        def invalidate(self, token_id: int) -> None:
            raise AssertionError("invalidate should never run when no request is attempted")

    app.state.oauth_manager = DummyOAuthManager()
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/images/generations",
            "headers": [(b"user-agent", b"yaak")],
            "client": ("127.0.0.1", 9000),
            "app": app,
        },
        receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
    )
    free_token = CodexToken(
        id=31,
        account_id="acct_free",
        refresh_token="refresh-free",
        is_active=True,
        raw_payload={},
    )

    claim_calls: list[tuple[int, ...]] = []
    finalized: list[dict[str, object]] = []

    async def fake_get_token_counts():
        return SimpleNamespace(available=1)

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None):
        excluded = tuple(sorted(int(token_id) for token_id in (exclude_token_ids or ())))
        claim_calls.append(excluded)
        if 31 in excluded:
            return None
        return free_token

    async def fake_create_request_log(**kwargs):
        return SimpleNamespace(id=90)

    async def fake_finalize_request_log(request_log_id: int, **kwargs) -> None:
        finalized.append({"request_log_id": request_log_id, **kwargs})

    async def fake_mark_token_success(token_id: int) -> None:
        raise AssertionError("mark_token_success should not run when no request is attempted")

    async def fake_mark_token_error(*args, **kwargs) -> None:
        raise AssertionError("mark_token_error should not run when free tokens are skipped")

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        raise AssertionError("proxy_call should not run when only free tokens are available")

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_error", fake_mark_token_error)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            _execute_proxy_request_with_failover(
                request,
                endpoint="/v1/images/generations",
                request_model="gpt-image-2",
                is_stream=False,
                proxy_call=fake_proxy_call,
            )
        )

    assert exc_info.value.status_code == 503
    assert str(exc_info.value.detail) == "No non-free Codex token available for gpt-image-2 requests"
    assert claim_calls == [(), (31,)]
    assert len(finalized) == 1
    assert finalized[0]["request_log_id"] == 90
    assert finalized[0]["status_code"] == 503
    assert finalized[0]["success"] is False
    assert finalized[0]["attempt_count"] == 0
    assert finalized[0]["token_id"] is None
    assert finalized[0]["account_id"] is None
    assert finalized[0]["error_message"] == "No non-free Codex token available for gpt-image-2 requests"


def test_execute_proxy_request_with_failover_falls_back_to_declared_plan_type_when_quota_missing(
    monkeypatch,
) -> None:
    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=None,
            quota_service=None,
        )
    )

    class DummyOAuthManager:
        async def get_access_token(
            self,
            token_row,
            client,
            *,
            force_refresh: bool = False,
            reactivate_on_refresh: bool = True,
        ) -> tuple[str, bool]:
            raise AssertionError("free tokens should still be skipped using declared plan info")

        def invalidate(self, token_id: int) -> None:
            raise AssertionError("invalidate should never run when no request is attempted")

    app.state.oauth_manager = DummyOAuthManager()
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/images/generations",
            "headers": [(b"user-agent", b"yaak")],
            "client": ("127.0.0.1", 9000),
            "app": app,
        },
        receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
    )
    free_token = CodexToken(
        id=41,
        account_id="acct_free",
        refresh_token="refresh-free",
        is_active=True,
        raw_payload={"plan_type": "free"},
    )

    claim_calls: list[tuple[int, ...]] = []

    async def fake_get_token_counts():
        return SimpleNamespace(available=1)

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None):
        excluded = tuple(sorted(int(token_id) for token_id in (exclude_token_ids or ())))
        claim_calls.append(excluded)
        if 41 in excluded:
            return None
        return free_token

    async def fake_create_request_log(**kwargs):
        return SimpleNamespace(id=91)

    async def fake_finalize_request_log(*args, **kwargs) -> None:
        return None

    async def fake_mark_token_success(token_id: int) -> None:
        raise AssertionError("mark_token_success should not run when free token is skipped")

    async def fake_mark_token_error(*args, **kwargs) -> None:
        raise AssertionError("mark_token_error should not run when free token is skipped")

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        raise AssertionError("proxy_call should not run when free token is skipped")

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_error", fake_mark_token_error)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            _execute_proxy_request_with_failover(
                request,
                endpoint="/v1/images/generations",
                request_model="gpt-image-2",
                is_stream=False,
                proxy_call=fake_proxy_call,
            )
        )

    assert exc_info.value.status_code == 503
    assert str(exc_info.value.detail) == "No non-free Codex token available for gpt-image-2 requests"
    assert claim_calls == [(), (41,)]


def test_execute_proxy_request_with_failover_postpones_unknown_plan_tokens_until_known_non_free_found(
    monkeypatch,
) -> None:
    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=None,
            quota_service=None,
        )
    )

    class DummyOAuthManager:
        async def get_access_token(
            self,
            token_row,
            client,
            *,
            force_refresh: bool = False,
            reactivate_on_refresh: bool = True,
        ) -> tuple[str, bool]:
            return f"access-token-{token_row.id}", False

        def invalidate(self, token_id: int) -> None:
            raise AssertionError("invalidate should not run in successful unknown-plan fallback test")

    app.state.oauth_manager = DummyOAuthManager()
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": [(b"user-agent", b"yaak")],
            "client": ("127.0.0.1", 9000),
            "app": app,
        },
        receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
    )
    unknown_token = CodexToken(
        id=51,
        account_id="acct_unknown",
        refresh_token="refresh-unknown",
        is_active=True,
        raw_payload={},
    )
    paid_token = CodexToken(
        id=52,
        account_id="acct_paid",
        refresh_token="refresh-paid",
        is_active=True,
        raw_payload={},
        plan_type="team",
    )

    claim_calls: list[tuple[int, ...]] = []
    proxy_calls: list[tuple[str, str | None]] = []
    finalized: list[dict[str, object]] = []

    async def fake_get_token_counts():
        return SimpleNamespace(available=2)

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None):
        excluded = tuple(sorted(int(token_id) for token_id in (exclude_token_ids or ())))
        claim_calls.append(excluded)
        if 51 not in excluded:
            return unknown_token
        if 52 not in excluded:
            return paid_token
        return None

    async def fake_create_request_log(**kwargs):
        return SimpleNamespace(id=92)

    async def fake_finalize_request_log(request_log_id: int, **kwargs) -> None:
        finalized.append({"request_log_id": request_log_id, **kwargs})

    async def fake_mark_token_success(token_id: int) -> None:
        return None

    async def fake_mark_token_error(*args, **kwargs) -> None:
        raise AssertionError("mark_token_error should not run in successful unknown-plan fallback test")

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        proxy_calls.append((access_token, account_id))
        return SimpleNamespace(
            response=JSONResponse({"ok": True}),
            status_code=200,
            model_name="gpt-image-2",
            first_token_at=None,
            usage_metrics=None,
            on_success=None,
            stream_capture=None,
        )

    monkeypatch.setattr("oaix_gateway.api_server.get_token_counts", fake_get_token_counts)
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_error", fake_mark_token_error)

    response = asyncio.run(
        _execute_proxy_request_with_failover(
            request,
            endpoint="/v1/chat/completions",
            request_model="gpt-image-2",
            is_stream=False,
            proxy_call=fake_proxy_call,
        )
    )

    assert response.status_code == 200
    assert claim_calls == [(), (51,)]
    assert proxy_calls == [("access-token-52", "acct_paid")]
    assert len(finalized) == 1
    assert finalized[0]["request_log_id"] == 92
    assert finalized[0]["status_code"] == 200
    assert finalized[0]["success"] is True
    assert finalized[0]["attempt_count"] == 1
    assert finalized[0]["token_id"] == 52
    assert finalized[0]["account_id"] == "acct_paid"


def test_response_traffic_controller_waits_until_idle(monkeypatch) -> None:
    monkeypatch.setenv("IMPORT_RESPONSE_IDLE_GRACE_SECONDS", "0")
    controller = ResponseTrafficController()

    async def runner() -> None:
        lease = controller.start_response()
        waiter = asyncio.create_task(controller.wait_for_import_turn())
        await asyncio.sleep(0)
        assert waiter.done() is False

        await lease.release()
        assert await asyncio.wait_for(waiter, timeout=0.2) is True
        assert controller.active_responses == 0

    asyncio.run(runner())


def test_response_traffic_controller_keeps_waiting_until_all_responses_finish(monkeypatch) -> None:
    monkeypatch.setenv("IMPORT_RESPONSE_IDLE_GRACE_SECONDS", "0")
    controller = ResponseTrafficController()

    async def runner() -> None:
        lease_a = controller.start_response()
        lease_b = controller.start_response()
        waiter = asyncio.create_task(controller.wait_for_import_turn())
        await asyncio.sleep(0)
        assert waiter.done() is False

        await lease_a.release()
        await asyncio.sleep(0)
        assert waiter.done() is False

        await lease_b.release()
        assert await asyncio.wait_for(waiter, timeout=0.2) is True

    asyncio.run(runner())


def test_response_traffic_controller_returns_immediately_without_active_responses(monkeypatch) -> None:
    monkeypatch.setenv("IMPORT_RESPONSE_IDLE_GRACE_SECONDS", "0.25")
    controller = ResponseTrafficController()

    assert asyncio.run(controller.wait_for_import_turn()) is False


def test_response_traffic_controller_times_out_when_active_responses_do_not_drain(monkeypatch) -> None:
    monkeypatch.setenv("IMPORT_RESPONSE_IDLE_GRACE_SECONDS", "0")
    monkeypatch.setenv("IMPORT_WAIT_TIMEOUT_SECONDS", "0.01")
    controller = ResponseTrafficController()

    async def runner() -> None:
        lease = controller.start_response()
        try:
            with pytest.raises(TimeoutError, match="Timed out waiting for active /v1/responses traffic to drain"):
                await controller.wait_for_import_turn()
        finally:
            await lease.release()

    asyncio.run(runner())


def test_import_route_enqueues_background_job(monkeypatch) -> None:
    app = create_app()
    captured: dict[str, object] = {}

    class FakeWorker:
        def __init__(self) -> None:
            self.start_calls = 0
            self.submitted_jobs: list[TokenImportJobLease] = []

        async def start(self) -> None:
            self.start_calls += 1

        def submit(self, job: TokenImportJobLease) -> None:
            self.submitted_jobs.append(job)

    async def fake_create_token_import_job(
        payloads: list[dict[str, object]],
        *,
        start_immediately: bool = False,
        import_queue_position: str = "front",
    ) -> TokenImportJobLease:
        captured["payloads"] = payloads
        captured["start_immediately"] = start_immediately
        captured["import_queue_position"] = import_queue_position
        return TokenImportJobLease(
            id=42,
            status=IMPORT_JOB_STATUS_RUNNING if start_immediately else IMPORT_JOB_STATUS_QUEUED,
            import_queue_position=import_queue_position,
            total_count=len(payloads),
            processed_count=0,
            created_count=0,
            updated_count=0,
            skipped_count=0,
            failed_count=0,
            yielded_to_response_traffic_count=0,
            response_traffic_timeout_count=0,
            created=[],
            updated=[],
            skipped=[],
            failed=[],
            submitted_at=None,
            started_at=None,
            heartbeat_at=None,
            finished_at=None,
            last_error=None,
            payloads=list(payloads),
        )

    async def receive():
        return {
            "type": "http.request",
            "body": b'[{"refresh_token":"rt-123"}]',
            "more_body": False,
        }

    monkeypatch.setattr("oaix_gateway.api_server.create_token_import_job", fake_create_token_import_job)
    app.state.token_import_worker = FakeWorker()

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/admin/tokens/import",
            "headers": [(b"content-type", b"application/json")],
            "app": app,
        },
        receive=receive,
    )
    route = next(
        route
        for route in app.routes
        if getattr(route, "path", None) == "/admin/tokens/import" and "POST" in getattr(route, "methods", set())
    )

    assert getattr(route, "status_code", None) == 202

    result = asyncio.run(route.endpoint(request, None))

    assert captured["payloads"] == [{"refresh_token": "rt-123"}]
    assert captured["start_immediately"] is False
    assert captured["import_queue_position"] == "front"
    assert app.state.token_import_worker.start_calls == 1
    assert [job.id for job in app.state.token_import_worker.submitted_jobs] == [42]
    assert result["job"]["id"] == 42
    assert result["job"]["status"] == IMPORT_JOB_STATUS_QUEUED
    assert result["job"]["total_count"] == 1
    assert "payloads" not in result["job"]


def test_import_route_forwards_requested_import_queue_position(monkeypatch) -> None:
    app = create_app()
    captured: dict[str, object] = {}

    async def fake_create_token_import_job(
        payloads: list[dict[str, object]],
        *,
        start_immediately: bool = False,
        import_queue_position: str = "front",
    ) -> TokenImportJobLease:
        captured["payloads"] = payloads
        captured["start_immediately"] = start_immediately
        captured["import_queue_position"] = import_queue_position
        return TokenImportJobLease(
            id=43,
            status=IMPORT_JOB_STATUS_QUEUED,
            import_queue_position=import_queue_position,
            total_count=len(payloads),
            processed_count=0,
            created_count=0,
            updated_count=0,
            skipped_count=0,
            failed_count=0,
            yielded_to_response_traffic_count=0,
            response_traffic_timeout_count=0,
            created=[],
            updated=[],
            skipped=[],
            failed=[],
            submitted_at=None,
            started_at=None,
            heartbeat_at=None,
            finished_at=None,
            last_error=None,
            payloads=list(payloads),
        )

    async def receive():
        return {
            "type": "http.request",
            "body": b'{"tokens":[{"refresh_token":"rt-456"}],"import_queue_position":"back"}',
            "more_body": False,
        }

    monkeypatch.setattr("oaix_gateway.api_server.create_token_import_job", fake_create_token_import_job)

    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/admin/tokens/import",
            "headers": [(b"content-type", b"application/json")],
            "app": app,
        },
        receive=receive,
    )
    route = next(
        route
        for route in app.routes
        if getattr(route, "path", None) == "/admin/tokens/import" and "POST" in getattr(route, "methods", set())
    )

    result = asyncio.run(route.endpoint(request, None))

    assert captured == {
        "payloads": [{"refresh_token": "rt-456"}],
        "start_immediately": False,
        "import_queue_position": "back",
    }
    assert result["job"]["import_queue_position"] == "back"


def test_get_import_job_route_restarts_background_worker(monkeypatch) -> None:
    app = create_app()
    captured: dict[str, object] = {}

    class FakeWorker:
        def __init__(self) -> None:
            self.start_calls = 0

        async def start(self) -> None:
            self.start_calls += 1

    async def fake_get_token_import_job(job_id: int) -> TokenImportJobState:
        captured["job_id"] = job_id
        return TokenImportJobState(
            id=job_id,
            status=IMPORT_JOB_STATUS_RUNNING,
            import_queue_position="front",
            total_count=3,
            processed_count=1,
            created_count=1,
            updated_count=0,
            skipped_count=0,
            failed_count=0,
            yielded_to_response_traffic_count=0,
            response_traffic_timeout_count=0,
            created=[],
            updated=[],
            skipped=[],
            failed=[],
            submitted_at=None,
            started_at=None,
            heartbeat_at=None,
            finished_at=None,
            last_error=None,
        )

    monkeypatch.setattr("oaix_gateway.api_server.get_token_import_job", fake_get_token_import_job)
    app.state.token_import_worker = FakeWorker()

    route = next(
        route
        for route in app.routes
        if getattr(route, "path", None) == "/admin/tokens/import-jobs/{job_id}"
        and "GET" in getattr(route, "methods", set())
    )

    result = asyncio.run(route.endpoint(25, None))

    assert captured["job_id"] == 25
    assert app.state.token_import_worker.start_calls == 1
    assert result["job"]["id"] == 25
    assert result["job"]["status"] == IMPORT_JOB_STATUS_RUNNING


def test_get_token_import_job_reads_via_execute(monkeypatch) -> None:
    job_row = SimpleNamespace(
        id=25,
        status=IMPORT_JOB_STATUS_COMPLETED,
        total_count=12,
        processed_count=12,
        created_count=12,
        updated_count=0,
        skipped_count=0,
        failed_count=0,
        yielded_to_response_traffic_count=0,
        response_traffic_timeout_count=0,
        created_items=[],
        updated_items=[],
        skipped_items=[],
        failed_items=[],
        submitted_at=None,
        started_at=None,
        heartbeat_at=None,
        finished_at=None,
        last_error=None,
    )

    class FakeResult:
        def scalars(self) -> "FakeResult":
            return self

        def first(self):
            return job_row

    class FakeSession:
        def __init__(self) -> None:
            self.execute_calls = 0

        async def execute(self, stmt):
            self.execute_calls += 1
            return FakeResult()

    fake_session = FakeSession()

    @asynccontextmanager
    async def fake_get_read_session():
        yield fake_session

    monkeypatch.setattr("oaix_gateway.token_import_jobs.get_import_read_session", fake_get_read_session)

    result = asyncio.run(get_token_import_job(25))

    assert fake_session.execute_calls == 1
    assert result is not None
    assert result.id == 25
    assert result.status == IMPORT_JOB_STATUS_COMPLETED


def test_token_import_background_worker_recovers_after_claim_error(monkeypatch) -> None:
    processed_job_ids: list[int] = []
    processed_event = asyncio.Event()
    claim_attempts = {"count": 0}

    class FakeResponseTraffic:
        active_responses = 0

        async def wait_for_import_turn(self, *, timeout_seconds=None) -> bool:
            return False

    job = TokenImportJobLease(
        id=93,
        status=IMPORT_JOB_STATUS_RUNNING,
        import_queue_position="front",
        total_count=1,
        processed_count=0,
        created_count=0,
        updated_count=0,
        skipped_count=0,
        failed_count=0,
        yielded_to_response_traffic_count=0,
        response_traffic_timeout_count=0,
        created=[],
        updated=[],
        skipped=[],
        failed=[],
        submitted_at=None,
        started_at=None,
        heartbeat_at=None,
        finished_at=None,
        last_error=None,
        payloads=[{"refresh_token": "rt-93"}],
    )

    async def fake_requeue_stale_token_import_jobs(*, stale_after_seconds=None) -> int:
        return 0

    async def fake_claim_next_token_import_job(*, stale_after_seconds=None) -> TokenImportJobLease | None:
        claim_attempts["count"] += 1
        if claim_attempts["count"] == 1:
            raise RuntimeError("temporary db outage")
        if claim_attempts["count"] == 2:
            return job
        return None

    async def fake_process_token_import_job(
        claimed_job: TokenImportJobLease,
        *,
        response_traffic,
    ) -> None:
        processed_job_ids.append(claimed_job.id)
        processed_event.set()
        return None

    monkeypatch.setattr(
        "oaix_gateway.token_import_jobs.requeue_stale_token_import_jobs",
        fake_requeue_stale_token_import_jobs,
    )
    monkeypatch.setattr("oaix_gateway.token_import_jobs.claim_next_token_import_job", fake_claim_next_token_import_job)
    monkeypatch.setattr("oaix_gateway.token_import_jobs.process_token_import_job", fake_process_token_import_job)

    worker = TokenImportBackgroundWorker(response_traffic=FakeResponseTraffic(), poll_interval_seconds=0.25)

    async def runner() -> None:
        await worker.start()
        await asyncio.wait_for(processed_event.wait(), timeout=1.0)
        await worker.stop()

    asyncio.run(runner())

    assert claim_attempts["count"] >= 2
    assert processed_job_ids == [93]


def test_token_import_background_worker_applies_completed_import_queue_position(monkeypatch) -> None:
    calls: list[dict[str, object]] = []
    published: list[TokenSelectionSettings] = []

    class FakeResponseTraffic:
        active_responses = 0

        async def wait_for_import_turn(self, *, timeout_seconds=None) -> bool:
            return False

    async def fake_apply_imported_token_queue_position(token_ids, *, position: str) -> TokenSelectionSettings:
        calls.append({"token_ids": tuple(token_ids), "position": position})
        return TokenSelectionSettings(strategy=TOKEN_SELECTION_STRATEGY_FILL_FIRST, token_order=(17, 23))

    monkeypatch.setattr(
        "oaix_gateway.token_import_jobs.apply_imported_token_queue_position",
        fake_apply_imported_token_queue_position,
    )

    worker = TokenImportBackgroundWorker(
        response_traffic=FakeResponseTraffic(),
        on_token_selection_settings_updated=lambda selection: published.append(selection),
    )
    job = TokenImportJobLease(
        id=94,
        status=IMPORT_JOB_STATUS_RUNNING,
        import_queue_position="back",
        total_count=3,
        processed_count=0,
        created_count=0,
        updated_count=0,
        skipped_count=0,
        failed_count=0,
        yielded_to_response_traffic_count=0,
        response_traffic_timeout_count=0,
        created=[],
        updated=[],
        skipped=[],
        failed=[],
        submitted_at=None,
        started_at=None,
        heartbeat_at=None,
        finished_at=None,
        last_error=None,
        payloads=[],
    )
    result = TokenImportJobState(
        id=94,
        status=IMPORT_JOB_STATUS_COMPLETED,
        import_queue_position="back",
        total_count=3,
        processed_count=3,
        created_count=2,
        updated_count=1,
        skipped_count=0,
        failed_count=0,
        yielded_to_response_traffic_count=0,
        response_traffic_timeout_count=0,
        created=[{"id": 23, "index": 2}, {"id": 17, "index": 0}],
        updated=[{"id": 23, "index": 1}],
        skipped=[],
        failed=[],
        submitted_at=None,
        started_at=None,
        heartbeat_at=None,
        finished_at=None,
        last_error=None,
    )

    asyncio.run(worker._apply_imported_token_queue_position(job, result))

    assert calls == [{"token_ids": (17, 23), "position": "back"}]
    assert published == [TokenSelectionSettings(strategy=TOKEN_SELECTION_STRATEGY_FILL_FIRST, token_order=(17, 23))]


def _install_staged_import_fakes(monkeypatch, job_id: int, payloads: list[object]) -> list[SimpleNamespace]:
    staged_items = [
        SimpleNamespace(
            id=index + 1,
            job_id=job_id,
            index=index,
            status=IMPORT_ITEM_STATUS_QUEUED,
            payload=payload if isinstance(payload, dict) else {"_raw": payload},
            validated_payload=None,
            token_id=None,
            action=None,
            error_message=None,
        )
        for index, payload in enumerate(payloads)
    ]

    async def fake_list_token_import_items(requested_job_id: int):
        assert requested_job_id == job_id
        return list(staged_items)

    async def fake_mark_import_item_validating(item_id: int) -> None:
        staged_items[item_id - 1].status = "validating"

    async def fake_mark_import_item_validated(
        item_id: int,
        *,
        validated_payload: dict[str, object],
        validation_ms: int,
    ) -> None:
        del validation_ms
        staged_items[item_id - 1].status = IMPORT_ITEM_STATUS_VALIDATED
        staged_items[item_id - 1].validated_payload = dict(validated_payload)

    async def fake_mark_import_item_failed(
        item_id: int,
        *,
        error_message: str,
        validation_ms: int | None = None,
        publish_ms: int | None = None,
    ) -> None:
        del validation_ms, publish_ms
        staged_items[item_id - 1].status = IMPORT_ITEM_STATUS_FAILED
        staged_items[item_id - 1].error_message = error_message

    async def fake_mark_import_items_publish_pending(item_ids) -> None:
        for item_id in item_ids:
            staged_items[int(item_id) - 1].status = IMPORT_ITEM_STATUS_PUBLISH_PENDING

    async def fake_mark_import_item_published(
        item_id: int,
        *,
        token_id: int,
        action: str,
        publish_ms: int,
    ) -> None:
        del publish_ms
        staged_items[item_id - 1].status = IMPORT_ITEM_STATUS_PUBLISHED
        staged_items[item_id - 1].token_id = token_id
        staged_items[item_id - 1].action = action

    monkeypatch.setattr("oaix_gateway.token_import_jobs._list_token_import_items", fake_list_token_import_items)
    monkeypatch.setattr("oaix_gateway.token_import_jobs._mark_import_item_validating", fake_mark_import_item_validating)
    monkeypatch.setattr("oaix_gateway.token_import_jobs._mark_import_item_validated", fake_mark_import_item_validated)
    monkeypatch.setattr("oaix_gateway.token_import_jobs._mark_import_item_failed", fake_mark_import_item_failed)
    monkeypatch.setattr(
        "oaix_gateway.token_import_jobs._mark_import_items_publish_pending",
        fake_mark_import_items_publish_pending,
    )
    monkeypatch.setattr("oaix_gateway.token_import_jobs._mark_import_item_published", fake_mark_import_item_published)
    return staged_items


def test_process_token_import_job_enriches_missing_account_id(monkeypatch) -> None:
    completed: dict[str, object] = {}

    class IdleResponseTraffic:
        active_responses = 0

        async def wait_for_import_turn(self, *, timeout_seconds=None) -> bool:
            return False

    async def fake_refresh_codex_access_token(self, client, refresh_token: str) -> dict[str, object]:
        assert refresh_token == "rt-only"
        return {
            "access_token": "access-from-refresh",
            "refresh_token": "rt-only",
            "account_id": "acct_from_refresh",
            "email": None,
            "expires_at": None,
        }

    async def fake_publish_token_payload_batch(payloads, *, import_queue_position: str) -> TokenPublishBatchResult:
        assert import_queue_position == "front"
        payload = list(payloads)[0]
        assert payload["access_token"] == "access-from-refresh"
        token = CodexToken(
            id=41,
            email=None,
            account_id=payload["account_id"],
            refresh_token=payload["refresh_token"],
            token_type="codex",
            is_active=True,
        )
        return TokenPublishBatchResult(results=(TokenUpsertResult(token=token, action="created"),))

    async def fake_complete_token_import_job(
        job_id: int,
        *,
        processed_count: int,
        created: list[dict[str, object]],
        updated: list[dict[str, object]],
        skipped: list[dict[str, object]],
        failed: list[dict[str, object]],
        yielded_to_response_traffic_count: int,
        response_traffic_timeout_count: int,
    ) -> TokenImportJobState:
        completed.update(
            {
                "processed_count": processed_count,
                "created": list(created),
                "updated": list(updated),
                "skipped": list(skipped),
                "failed": list(failed),
            }
        )
        return TokenImportJobState(
            id=job_id,
            status=IMPORT_JOB_STATUS_COMPLETED,
            import_queue_position="front",
            total_count=1,
            processed_count=processed_count,
            created_count=len(created),
            updated_count=len(updated),
            skipped_count=len(skipped),
            failed_count=len(failed),
            yielded_to_response_traffic_count=yielded_to_response_traffic_count,
            response_traffic_timeout_count=response_traffic_timeout_count,
            created=list(created),
            updated=list(updated),
            skipped=list(skipped),
            failed=list(failed),
            submitted_at=None,
            started_at=None,
            heartbeat_at=None,
            finished_at=None,
            last_error=None,
        )

    _install_staged_import_fakes(monkeypatch, 95, [{"refresh_token": "rt-only"}])
    monkeypatch.setattr("oaix_gateway.token_import_jobs.CodexOAuthManager._refresh_codex_access_token", fake_refresh_codex_access_token)
    monkeypatch.setattr("oaix_gateway.token_import_jobs.publish_token_payload_batch", fake_publish_token_payload_batch)
    monkeypatch.setattr("oaix_gateway.token_import_jobs.complete_token_import_job", fake_complete_token_import_job)

    job = TokenImportJobLease(
        id=95,
        status=IMPORT_JOB_STATUS_RUNNING,
        import_queue_position="front",
        total_count=1,
        processed_count=0,
        created_count=0,
        updated_count=0,
        skipped_count=0,
        failed_count=0,
        yielded_to_response_traffic_count=0,
        response_traffic_timeout_count=0,
        created=[],
        updated=[],
        skipped=[],
        failed=[],
        submitted_at=None,
        started_at=None,
        heartbeat_at=None,
        finished_at=None,
        last_error=None,
        payloads=[{"refresh_token": "rt-only"}],
    )

    result = asyncio.run(process_token_import_job(job, response_traffic=IdleResponseTraffic()))

    assert completed["processed_count"] == 1
    assert completed["created"] == [{"id": 41, "email": None, "account_id": "acct_from_refresh", "index": 0}]
    assert completed["failed"] == []
    assert result is not None
    assert result.created_count == 1


def test_process_token_import_job_marks_refresh_only_tokens_failed_when_validation_fails(monkeypatch) -> None:
    completed: dict[str, object] = {}

    class IdleResponseTraffic:
        active_responses = 0

        async def wait_for_import_turn(self, *, timeout_seconds=None) -> bool:
            return False

    async def fake_refresh_codex_access_token(self, client, refresh_token: str) -> dict[str, object]:
        assert refresh_token == "rt-only"
        raise HTTPException(status_code=401, detail="refresh token revoked")

    async def fake_complete_token_import_job(
        job_id: int,
        *,
        processed_count: int,
        created: list[dict[str, object]],
        updated: list[dict[str, object]],
        skipped: list[dict[str, object]],
        failed: list[dict[str, object]],
        yielded_to_response_traffic_count: int,
        response_traffic_timeout_count: int,
    ) -> TokenImportJobState:
        completed.update(
            {
                "processed_count": processed_count,
                "created": list(created),
                "updated": list(updated),
                "skipped": list(skipped),
                "failed": list(failed),
            }
        )
        return TokenImportJobState(
            id=job_id,
            status=IMPORT_JOB_STATUS_COMPLETED,
            import_queue_position="front",
            total_count=1,
            processed_count=processed_count,
            created_count=len(created),
            updated_count=len(updated),
            skipped_count=len(skipped),
            failed_count=len(failed),
            yielded_to_response_traffic_count=yielded_to_response_traffic_count,
            response_traffic_timeout_count=response_traffic_timeout_count,
            created=list(created),
            updated=list(updated),
            skipped=list(skipped),
            failed=list(failed),
            submitted_at=None,
            started_at=None,
            heartbeat_at=None,
            finished_at=None,
            last_error=None,
        )

    _install_staged_import_fakes(monkeypatch, 96, [{"refresh_token": "rt-only"}])
    monkeypatch.setattr("oaix_gateway.token_import_jobs.CodexOAuthManager._refresh_codex_access_token", fake_refresh_codex_access_token)
    monkeypatch.setattr("oaix_gateway.token_import_jobs.complete_token_import_job", fake_complete_token_import_job)

    job = TokenImportJobLease(
        id=96,
        status=IMPORT_JOB_STATUS_RUNNING,
        import_queue_position="front",
        total_count=1,
        processed_count=0,
        created_count=0,
        updated_count=0,
        skipped_count=0,
        failed_count=0,
        yielded_to_response_traffic_count=0,
        response_traffic_timeout_count=0,
        created=[],
        updated=[],
        skipped=[],
        failed=[],
        submitted_at=None,
        started_at=None,
        heartbeat_at=None,
        finished_at=None,
        last_error=None,
        payloads=[{"refresh_token": "rt-only"}],
    )

    result = asyncio.run(process_token_import_job(job, response_traffic=IdleResponseTraffic()))

    assert completed["failed"] == [{"index": 0, "error": "refresh token revoked"}]
    assert result is not None
    assert result.failed_count == 1


def test_process_token_import_job_does_not_wait_for_response_traffic_by_default(monkeypatch) -> None:
    wait_calls: list[int] = []

    class BusyResponseTraffic:
        active_responses = 2

        async def wait_for_import_turn(self, *, timeout_seconds=None) -> bool:
            wait_calls.append(self.active_responses)
            return True

    async def fake_refresh_codex_access_token(self, client, refresh_token: str) -> dict[str, object]:
        return {
            "access_token": "access-43",
            "refresh_token": refresh_token,
            "account_id": "acct_123",
            "email": "user@example.com",
            "expires_at": None,
        }

    async def fake_publish_token_payload_batch(payloads, *, import_queue_position: str) -> TokenPublishBatchResult:
        payload = list(payloads)[0]
        token = CodexToken(
            id=43,
            email="user@example.com",
            account_id="acct_123",
            refresh_token=payload["refresh_token"],
            is_active=True,
        )
        return TokenPublishBatchResult(results=(TokenUpsertResult(token=token, action="created"),))

    async def fake_complete_token_import_job(
        job_id: int,
        *,
        processed_count: int,
        created: list[dict[str, object]],
        updated: list[dict[str, object]],
        skipped: list[dict[str, object]],
        failed: list[dict[str, object]],
        yielded_to_response_traffic_count: int,
        response_traffic_timeout_count: int,
    ) -> TokenImportJobState:
        return TokenImportJobState(
            id=job_id,
            status=IMPORT_JOB_STATUS_COMPLETED,
            import_queue_position="front",
            total_count=1,
            processed_count=processed_count,
            created_count=len(created),
            updated_count=len(updated),
            skipped_count=len(skipped),
            failed_count=len(failed),
            yielded_to_response_traffic_count=yielded_to_response_traffic_count,
            response_traffic_timeout_count=response_traffic_timeout_count,
            created=list(created),
            updated=list(updated),
            skipped=list(skipped),
            failed=list(failed),
            submitted_at=None,
            started_at=None,
            heartbeat_at=None,
            finished_at=None,
            last_error=None,
        )

    monkeypatch.delenv("IMPORT_JOB_RESPECT_RESPONSE_TRAFFIC", raising=False)
    _install_staged_import_fakes(monkeypatch, 97, [{"refresh_token": "rt-43"}])
    monkeypatch.setattr("oaix_gateway.token_import_jobs.CodexOAuthManager._refresh_codex_access_token", fake_refresh_codex_access_token)
    monkeypatch.setattr("oaix_gateway.token_import_jobs.publish_token_payload_batch", fake_publish_token_payload_batch)
    monkeypatch.setattr("oaix_gateway.token_import_jobs.complete_token_import_job", fake_complete_token_import_job)

    job = TokenImportJobLease(
        id=97,
        status=IMPORT_JOB_STATUS_RUNNING,
        import_queue_position="front",
        total_count=1,
        processed_count=0,
        created_count=0,
        updated_count=0,
        skipped_count=0,
        failed_count=0,
        yielded_to_response_traffic_count=0,
        response_traffic_timeout_count=0,
        created=[],
        updated=[],
        skipped=[],
        failed=[],
        submitted_at=None,
        started_at=None,
        heartbeat_at=None,
        finished_at=None,
        last_error=None,
        payloads=[{"refresh_token": "rt-43"}],
    )

    result = asyncio.run(process_token_import_job(job, response_traffic=BusyResponseTraffic()))

    assert wait_calls == []
    assert result is not None
    assert result.yielded_to_response_traffic_count == 0
    assert result.created_count == 1


def test_process_token_import_job_runs_payloads_in_parallel_without_response_traffic_wait(monkeypatch) -> None:
    progress_calls: list[dict[str, object]] = []
    completed: dict[str, object] = {}
    concurrency = {"active": 0, "max_active": 0}

    class BusyResponseTraffic:
        active_responses = 2

        def __init__(self) -> None:
            self.calls = 0

        async def wait_for_import_turn(self, *, timeout_seconds=None) -> bool:
            self.calls += 1
            raise AssertionError("wait_for_import_turn should not run when IMPORT_JOB_RESPECT_RESPONSE_TRAFFIC is disabled")

    response_traffic = BusyResponseTraffic()

    async def fake_refresh_codex_access_token(self, client, refresh_token: str) -> dict[str, object]:
        concurrency["active"] += 1
        concurrency["max_active"] = max(concurrency["max_active"], concurrency["active"])
        await asyncio.sleep(0.02)
        concurrency["active"] -= 1
        return {
            "access_token": f"access-{refresh_token}",
            "refresh_token": refresh_token,
            "account_id": "acct_123",
            "email": "user@example.com",
            "expires_at": None,
        }

    async def fake_publish_token_payload_batch(payloads, *, import_queue_position: str) -> TokenPublishBatchResult:
        results: list[TokenUpsertResult] = []
        for payload in payloads:
            refresh_token = str(payload["refresh_token"])
            token_id = int(refresh_token.split("-")[-1])
            token = CodexToken(
                id=token_id,
                email="user@example.com",
                account_id="acct_123",
                refresh_token=refresh_token,
                is_active=True,
            )
            results.append(TokenUpsertResult(token=token, action="created"))
        return TokenPublishBatchResult(results=tuple(results))

    async def fake_update_token_import_job_progress(
        job_id: int,
        *,
        processed_count: int,
        created: list[dict[str, object]],
        updated: list[dict[str, object]],
        skipped: list[dict[str, object]],
        failed: list[dict[str, object]],
        yielded_to_response_traffic_count: int,
        response_traffic_timeout_count: int,
    ) -> None:
        progress_calls.append(
            {
                "job_id": job_id,
                "processed_count": processed_count,
                "created": list(created),
                "updated": list(updated),
                "skipped": list(skipped),
                "failed": list(failed),
                "yielded_to_response_traffic_count": yielded_to_response_traffic_count,
                "response_traffic_timeout_count": response_traffic_timeout_count,
            }
        )
        return None

    async def fake_complete_token_import_job(
        job_id: int,
        *,
        processed_count: int,
        created: list[dict[str, object]],
        updated: list[dict[str, object]],
        skipped: list[dict[str, object]],
        failed: list[dict[str, object]],
        yielded_to_response_traffic_count: int,
        response_traffic_timeout_count: int,
    ) -> TokenImportJobState:
        completed.update(
            {
                "job_id": job_id,
                "processed_count": processed_count,
                "created": list(created),
                "updated": list(updated),
                "skipped": list(skipped),
                "failed": list(failed),
                "yielded_to_response_traffic_count": yielded_to_response_traffic_count,
                "response_traffic_timeout_count": response_traffic_timeout_count,
            }
        )
        return TokenImportJobState(
            id=job_id,
            status=IMPORT_JOB_STATUS_COMPLETED,
            import_queue_position="front",
            total_count=2,
            processed_count=processed_count,
            created_count=len(created),
            updated_count=len(updated),
            skipped_count=len(skipped),
            failed_count=len(failed),
            yielded_to_response_traffic_count=yielded_to_response_traffic_count,
            response_traffic_timeout_count=response_traffic_timeout_count,
            created=list(created),
            updated=list(updated),
            skipped=list(skipped),
            failed=list(failed),
            submitted_at=None,
            started_at=None,
            heartbeat_at=None,
            finished_at=None,
            last_error=None,
        )

    monkeypatch.setenv("IMPORT_JOB_MAX_CONCURRENCY", "4")
    monkeypatch.setenv("IMPORT_JOB_PROGRESS_FLUSH_EVERY", "1")
    monkeypatch.setenv("IMPORT_JOB_PROGRESS_FLUSH_INTERVAL_SECONDS", "0")
    monkeypatch.setenv("IMPORT_JOB_RESPECT_RESPONSE_TRAFFIC", "0")
    _install_staged_import_fakes(
        monkeypatch,
        7,
        [
            {"refresh_token": "rt-1"},
            {"refresh_token": "rt-2"},
            {"refresh_token": "rt-3"},
            "bad-payload",
        ],
    )
    monkeypatch.setattr("oaix_gateway.token_import_jobs.CodexOAuthManager._refresh_codex_access_token", fake_refresh_codex_access_token)
    monkeypatch.setattr("oaix_gateway.token_import_jobs.publish_token_payload_batch", fake_publish_token_payload_batch)
    monkeypatch.setattr(
        "oaix_gateway.token_import_jobs.update_token_import_job_progress",
        fake_update_token_import_job_progress,
    )
    monkeypatch.setattr("oaix_gateway.token_import_jobs.complete_token_import_job", fake_complete_token_import_job)

    job = TokenImportJobLease(
        id=7,
        status=IMPORT_JOB_STATUS_RUNNING,
        import_queue_position="front",
        total_count=2,
        processed_count=0,
        created_count=0,
        updated_count=0,
        skipped_count=0,
        failed_count=0,
        yielded_to_response_traffic_count=0,
        response_traffic_timeout_count=0,
        created=[],
        updated=[],
        skipped=[],
        failed=[],
        submitted_at=None,
        started_at=None,
        heartbeat_at=None,
        finished_at=None,
        last_error=None,
        payloads=[
            {"refresh_token": "rt-1"},
            {"refresh_token": "rt-2"},
            {"refresh_token": "rt-3"},
            "bad-payload",
        ],
    )

    result = asyncio.run(
        process_token_import_job(
            job,
            response_traffic=response_traffic,
        )
    )

    assert response_traffic.calls == 0
    assert concurrency["max_active"] > 1
    assert progress_calls == []
    assert completed["processed_count"] == 4
    assert completed["response_traffic_timeout_count"] == 0
    assert [item["index"] for item in completed["created"]] == [0, 1, 2]
    assert completed["failed"] == [{"index": 3, "error": "Token payload must be a JSON object"}]
    assert result is not None
    assert result.status == IMPORT_JOB_STATUS_COMPLETED
    assert result.created_count == 3
    assert result.failed_count == 1


def test_process_token_import_job_batches_progress_updates_for_small_fast_jobs(monkeypatch) -> None:
    progress_calls: list[int] = []

    class IdleResponseTraffic:
        active_responses = 0

        async def wait_for_import_turn(self, *, timeout_seconds=None) -> bool:
            raise AssertionError("wait_for_import_turn should stay disabled by default")

    async def fake_refresh_codex_access_token(self, client, refresh_token: str) -> dict[str, object]:
        return {
            "access_token": f"access-{refresh_token}",
            "refresh_token": refresh_token,
            "account_id": "acct_123",
            "email": "user@example.com",
            "expires_at": None,
        }

    async def fake_publish_token_payload_batch(payloads, *, import_queue_position: str) -> TokenPublishBatchResult:
        results = []
        for payload in payloads:
            refresh_token = str(payload["refresh_token"])
            token = CodexToken(
                id=int(refresh_token.split("-")[-1]),
                email="user@example.com",
                account_id="acct_123",
                refresh_token=refresh_token,
                is_active=True,
            )
            results.append(TokenUpsertResult(token=token, action="created"))
        return TokenPublishBatchResult(results=tuple(results))

    async def fake_update_token_import_job_progress(
        job_id: int,
        *,
        processed_count: int,
        created: list[dict[str, object]],
        updated: list[dict[str, object]],
        skipped: list[dict[str, object]],
        failed: list[dict[str, object]],
        yielded_to_response_traffic_count: int,
        response_traffic_timeout_count: int,
    ) -> None:
        progress_calls.append(processed_count)
        return None

    async def fake_complete_token_import_job(
        job_id: int,
        *,
        processed_count: int,
        created: list[dict[str, object]],
        updated: list[dict[str, object]],
        skipped: list[dict[str, object]],
        failed: list[dict[str, object]],
        yielded_to_response_traffic_count: int,
        response_traffic_timeout_count: int,
    ) -> TokenImportJobState:
        return TokenImportJobState(
            id=job_id,
            status=IMPORT_JOB_STATUS_COMPLETED,
            import_queue_position="front",
            total_count=17,
            processed_count=processed_count,
            created_count=len(created),
            updated_count=len(updated),
            skipped_count=len(skipped),
            failed_count=len(failed),
            yielded_to_response_traffic_count=yielded_to_response_traffic_count,
            response_traffic_timeout_count=response_traffic_timeout_count,
            created=list(created),
            updated=list(updated),
            skipped=list(skipped),
            failed=list(failed),
            submitted_at=None,
            started_at=None,
            heartbeat_at=None,
            finished_at=None,
            last_error=None,
        )

    monkeypatch.setenv("IMPORT_JOB_MAX_CONCURRENCY", "16")
    monkeypatch.setenv("IMPORT_JOB_PROGRESS_FLUSH_EVERY", "64")
    monkeypatch.setenv("IMPORT_JOB_PROGRESS_FLUSH_INTERVAL_SECONDS", "3600")
    monkeypatch.setenv("IMPORT_JOB_RESPECT_RESPONSE_TRAFFIC", "0")
    _install_staged_import_fakes(monkeypatch, 8, [{"refresh_token": f"rt-{index + 1}"} for index in range(17)])
    monkeypatch.setattr("oaix_gateway.token_import_jobs.CodexOAuthManager._refresh_codex_access_token", fake_refresh_codex_access_token)
    monkeypatch.setattr("oaix_gateway.token_import_jobs.publish_token_payload_batch", fake_publish_token_payload_batch)
    monkeypatch.setattr(
        "oaix_gateway.token_import_jobs.update_token_import_job_progress",
        fake_update_token_import_job_progress,
    )
    monkeypatch.setattr("oaix_gateway.token_import_jobs.complete_token_import_job", fake_complete_token_import_job)

    job = TokenImportJobLease(
        id=8,
        status=IMPORT_JOB_STATUS_RUNNING,
        import_queue_position="front",
        total_count=17,
        processed_count=0,
        created_count=0,
        updated_count=0,
        skipped_count=0,
        failed_count=0,
        yielded_to_response_traffic_count=0,
        response_traffic_timeout_count=0,
        created=[],
        updated=[],
        skipped=[],
        failed=[],
        submitted_at=None,
        started_at=None,
        heartbeat_at=None,
        finished_at=None,
        last_error=None,
        payloads=[{"refresh_token": f"rt-{index + 1}"} for index in range(17)],
    )

    result = asyncio.run(
        process_token_import_job(
            job,
            response_traffic=IdleResponseTraffic(),
        )
    )

    assert progress_calls == []
    assert result is not None
    assert result.status == IMPORT_JOB_STATUS_COMPLETED
    assert result.processed_count == 17
    assert result.created_count == 17
    assert [item["index"] for item in result.created] == list(range(17))


def test_wrap_streaming_body_iterator_runs_cleanup_on_close() -> None:
    events: list[str] = []

    async def upstream() -> AsyncIterator[bytes]:
        try:
            yield b"chunk-1"
            await asyncio.sleep(60)
        finally:
            events.append("upstream_closed")

    async def on_close() -> None:
        events.append("cleanup_ran")

    async def runner() -> None:
        wrapped = _wrap_streaming_body_iterator(upstream(), on_close=on_close)
        assert await wrapped.__anext__() == b"chunk-1"
        await wrapped.aclose()

    asyncio.run(runner())

    assert events == ["upstream_closed", "cleanup_ran"]


def test_http_phase_trace_recorder_splits_upstream_request_phases(monkeypatch) -> None:
    now = 100.0
    monkeypatch.setattr("oaix_gateway.api_server.time.perf_counter", lambda: now)
    timing = _RequestTimingRecorder()
    recorder = _HTTPPhaseTraceRecorder(timing)

    async def runner() -> None:
        nonlocal now
        now = 100.010
        await recorder.record("connection.connect_tcp.started", {})
        now = 100.030
        await recorder.record("connection.connect_tcp.complete", {})
        now = 100.040
        await recorder.record("connection.start_tls.started", {})
        now = 100.055
        await recorder.record("connection.start_tls.complete", {})
        now = 100.060
        await recorder.record("http11.send_request_headers.started", {})
        now = 100.062
        await recorder.record("http11.send_request_headers.complete", {})
        now = 100.063
        await recorder.record("http11.send_request_body.started", {})
        now = 100.067
        await recorder.record("http11.send_request_body.complete", {})
        now = 100.070
        await recorder.record("http11.receive_response_headers.started", {})
        now = 100.120
        await recorder.record("http11.receive_response_headers.complete", {})

    asyncio.run(runner())

    spans = timing.snapshot()
    assert spans["http_pool_wait_ms"] == 10
    assert spans["http_connect_ms"] == 20
    assert spans["http_tls_ms"] == 15
    assert spans["http_request_write_ms"] == 6
    assert spans["http_send_request_headers_ms"] == 2
    assert spans["http_send_request_body_ms"] == 4
    assert spans["upstream_headers_ms"] == 50


def test_stream_response_finalizes_499_when_client_disconnects_before_body(monkeypatch) -> None:
    app = SimpleNamespace(
        state=SimpleNamespace(
            response_traffic=ResponseTrafficController(),
            http_client=object(),
            oauth_manager=None,
        )
    )

    class DummyOAuthManager:
        async def get_access_token(self, token_row, client) -> tuple[str, bool]:
            return "access-token", False

        def invalidate(self, token_id: int) -> None:
            raise AssertionError("invalidate should not run for a disconnected stream")

    app.state.oauth_manager = DummyOAuthManager()
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/responses",
            "headers": [(b"user-agent", b"test-client")],
            "client": ("127.0.0.1", 9000),
            "app": app,
        },
        receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
    )
    token_row = CodexToken(
        id=51,
        account_id="acct_51",
        refresh_token="refresh-51",
        token_type="codex",
        is_active=True,
        plan_type="team",
    )
    finalized: list[dict[str, object]] = []
    marked_success: list[int] = []

    async def fake_create_request_log(**kwargs):
        return SimpleNamespace(id=909)

    async def fake_finalize_request_log(request_log_id: int, **kwargs):
        finalized.append({"request_log_id": request_log_id, **kwargs})
        return kwargs.get("timing_spans")

    async def fake_claim_next_active_token(*, selection_strategy: str, exclude_token_ids=None):
        return token_row

    async def fake_mark_token_success(token_id: int) -> None:
        marked_success.append(token_id)

    async def stalled_stream() -> AsyncIterator[bytes]:
        await asyncio.sleep(60)
        yield b"data: unreachable\n\n"

    async def fake_proxy_call(client, access_token: str, account_id: str | None):
        return SimpleNamespace(
            response=StreamingResponse(stalled_stream(), media_type="text/event-stream"),
            status_code=200,
            model_name="gpt-5.5",
            first_token_at=None,
            usage_metrics=None,
            on_success=None,
            stream_capture=_ProxyStreamCapture(initial_model_name="gpt-5.5", initial_first_token_at=None),
        )

    monkeypatch.setattr("oaix_gateway.api_server.create_request_log", fake_create_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.finalize_request_log", fake_finalize_request_log)
    monkeypatch.setattr("oaix_gateway.api_server.claim_next_active_token", fake_claim_next_active_token)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)

    async def run() -> None:
        response = await _execute_proxy_request_with_failover(
            request,
            endpoint="/v1/responses",
            request_model="gpt-5.5",
            is_stream=True,
            proxy_call=fake_proxy_call,
        )

        async def receive() -> dict[str, object]:
            return {"type": "http.disconnect"}

        async def send(message: dict[str, object]) -> None:
            del message

        await asyncio.wait_for(
            response(
                {
                    "type": "http",
                    "asgi": {"version": "3.0"},
                    "method": "POST",
                    "path": "/v1/responses",
                    "headers": [],
                },
                receive,
                send,
            ),
            timeout=0.2,
        )

    asyncio.run(run())

    assert finalized[0]["request_log_id"] == 909
    assert finalized[0]["status_code"] == 499
    assert finalized[0]["success"] is False
    assert finalized[0]["attempt_count"] == 1
    assert finalized[0]["token_id"] == 51
    assert finalized[0]["account_id"] == "acct_51"
    assert finalized[0]["error_message"] == "Client disconnected"
    assert marked_success == []
    assert app.state.response_traffic.active_responses == 0


def test_probe_token_with_latest_access_token_reactivates_on_success(monkeypatch) -> None:
    app = SimpleNamespace(state=SimpleNamespace(http_client=object(), oauth_manager=None))
    token_row = CodexToken(
        id=7,
        account_id="acct_123",
        refresh_token="refresh-token",
        is_active=False,
    )
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/admin/tokens/7/probe",
            "headers": [],
            "app": app,
        },
        receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
    )
    calls: dict[str, object] = {}

    class DummyOAuthManager:
        def __init__(self) -> None:
            self.invalidated: list[int] = []

        async def get_access_token(
            self,
            token,
            client,
            *,
            force_refresh: bool = False,
            reactivate_on_refresh: bool = True,
        ) -> tuple[str, bool]:
            calls["oauth"] = {
                "token_id": token.id,
                "force_refresh": force_refresh,
                "reactivate_on_refresh": reactivate_on_refresh,
            }
            return "fresh-access-token", True

        def invalidate(self, token_id: int) -> None:
            self.invalidated.append(token_id)

    app.state.oauth_manager = DummyOAuthManager()

    async def fake_proxy_request_with_token(
        client,
        http_request,
        request_data,
        *,
        access_token: str,
        account_id: str | None,
        compact: bool = False,
    ):
        calls["proxy"] = {
            "access_token": access_token,
            "account_id": account_id,
            "compact": compact,
            "model": request_data.model,
            "input": request_data.input,
            "payload": request_data.model_dump(exclude_unset=True),
        }
        return SimpleNamespace(
            status_code=200,
            model_name="gpt-5.4-mini",
        )

    async def fake_mark_token_success(token_id: int) -> None:
        calls["success_token_id"] = token_id

    async def fake_mark_token_error(
        token_id: int,
        message: str,
        *,
        deactivate: bool = False,
        cooldown_seconds: int | None = None,
        clear_access_token: bool = False,
    ) -> None:
        raise AssertionError("mark_token_error should not run on a successful probe")

    monkeypatch.setattr("oaix_gateway.api_server._proxy_request_with_token", fake_proxy_request_with_token)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_error", fake_mark_token_error)

    result = asyncio.run(
        _probe_token_with_latest_access_token(
            app,
            http_request=request,
            token_row=token_row,
            probe_model="gpt-5.5",
        )
    )

    assert calls["oauth"] == {
        "token_id": 7,
        "force_refresh": True,
        "reactivate_on_refresh": False,
    }
    assert calls["proxy"] == {
        "access_token": "fresh-access-token",
        "account_id": "acct_123",
        "compact": False,
        "model": "gpt-5.5",
        "input": [
            {
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": "say test",
                    }
                ],
            }
        ],
        "payload": {
            "model": "gpt-5.5",
            "input": [
                {
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": "say test",
                        }
                    ],
                }
            ],
            "stream": False,
            "store": False,
        },
    }
    assert calls["success_token_id"] == 7
    assert result["outcome"] == "reactivated"
    assert result["status_code"] == 200
    assert result["probe_model"] == "gpt-5.5"
    assert "已标记为可用" in result["message"]


def test_probe_token_with_latest_access_token_keeps_disabled_on_auth_failure(monkeypatch) -> None:
    app = SimpleNamespace(state=SimpleNamespace(http_client=object(), oauth_manager=None))
    token_row = CodexToken(
        id=8,
        account_id="acct_456",
        refresh_token="refresh-token",
        is_active=False,
    )
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/admin/tokens/8/probe",
            "headers": [],
            "app": app,
        },
        receive=lambda: {"type": "http.request", "body": b"", "more_body": False},
    )
    calls: dict[str, object] = {}

    class DummyOAuthManager:
        def __init__(self) -> None:
            self.invalidated: list[int] = []

        async def get_access_token(
            self,
            token,
            client,
            *,
            force_refresh: bool = False,
            reactivate_on_refresh: bool = True,
        ) -> tuple[str, bool]:
            calls["oauth"] = {
                "token_id": token.id,
                "force_refresh": force_refresh,
                "reactivate_on_refresh": reactivate_on_refresh,
            }
            return "fresh-access-token", True

        def invalidate(self, token_id: int) -> None:
            self.invalidated.append(token_id)

    app.state.oauth_manager = DummyOAuthManager()

    async def fake_proxy_request_with_token(
        client,
        http_request,
        request_data,
        *,
        access_token: str,
        account_id: str | None,
        compact: bool = False,
    ):
        raise HTTPException(status_code=403, detail='{"error":{"code":"account_deactivated"}}')

    async def fake_mark_token_success(token_id: int) -> None:
        raise AssertionError("mark_token_success should not run when the probe still auth-fails")

    async def fake_mark_token_error(
        token_id: int,
        message: str,
        *,
        deactivate: bool = False,
        cooldown_seconds: int | None = None,
        clear_access_token: bool = False,
    ) -> None:
        calls["error"] = {
            "token_id": token_id,
            "message": message,
            "deactivate": deactivate,
            "cooldown_seconds": cooldown_seconds,
            "clear_access_token": clear_access_token,
        }

    monkeypatch.setattr("oaix_gateway.api_server._proxy_request_with_token", fake_proxy_request_with_token)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_success", fake_mark_token_success)
    monkeypatch.setattr("oaix_gateway.api_server.mark_token_error", fake_mark_token_error)

    result = asyncio.run(
        _probe_token_with_latest_access_token(
            app,
            http_request=request,
            token_row=token_row,
        )
    )

    assert calls["oauth"] == {
        "token_id": 8,
        "force_refresh": True,
        "reactivate_on_refresh": False,
    }
    assert app.state.oauth_manager.invalidated == [8]
    assert calls["error"] == {
        "token_id": 8,
        "message": '{"error":{"code":"account_deactivated"}}',
        "deactivate": True,
        "cooldown_seconds": None,
        "clear_access_token": True,
    }
    assert result["outcome"] == "disabled"
    assert result["status_code"] == 403
    assert "已标记为禁用" in result["message"]


def test_codex_token_refresh_token_index_is_declared() -> None:
    assert "ix_codex_tokens_refresh_token" in {index.name for index in CodexToken.__table__.indexes}


def test_parse_admin_token_ids_dedupes_and_validates_positive_ids() -> None:
    assert _parse_admin_token_ids(" 7, 8\n7 9 ") == (7, 8, 9)
    assert _parse_admin_token_ids("1,2,3", limit=2) == (1, 2)
    assert _parse_admin_token_ids("") == ()

    with pytest.raises(ValueError):
        _parse_admin_token_ids("7,nope")
    with pytest.raises(ValueError):
        _parse_admin_token_ids("0")


def test_build_admin_token_items_skips_inactive_tokens_for_quota_fetch(monkeypatch) -> None:
    quota_token_ids: list[int] = []

    class DummyQuotaService:
        async def get_many(self, token_rows, *, client, oauth_manager, account_ids):
            quota_token_ids.extend(token_row.id for token_row in token_rows)
            return {
                token_row.id: CodexQuotaSnapshot(
                    fetched_at=datetime.now(timezone.utc),
                    error=None,
                    plan_type="plus",
                    windows=[],
                )
                for token_row in token_rows
            }

    async def fake_get_request_costs_by_token(token_ids):
        return {}

    async def fake_get_request_costs_by_account(account_ids):
        return {}

    async def fake_get_token_counts_by_account_ids(account_ids):
        return {}

    monkeypatch.setattr("oaix_gateway.api_server.get_request_costs_by_token", fake_get_request_costs_by_token)
    monkeypatch.setattr("oaix_gateway.api_server.get_request_costs_by_account", fake_get_request_costs_by_account)
    monkeypatch.setattr(
        "oaix_gateway.api_server.get_token_counts_by_account_ids",
        fake_get_token_counts_by_account_ids,
    )

    active_token = CodexToken(
        id=51,
        account_id="acct_active",
        refresh_token="refresh-active",
        token_type="codex",
        is_active=True,
    )
    disabled_token = CodexToken(
        id=52,
        account_id="acct_disabled",
        refresh_token="refresh-disabled",
        token_type="codex",
        is_active=False,
    )
    app = SimpleNamespace(
        state=SimpleNamespace(
            quota_service=DummyQuotaService(),
            http_client=object(),
            oauth_manager=object(),
        )
    )

    items = asyncio.run(
        _build_admin_token_items(
            app,
            token_rows=[active_token, disabled_token],
            include_quota=True,
        )
    )

    assert quota_token_ids == [51]
    quota_by_id = {item["id"]: item["quota"] for item in items}
    assert quota_by_id[51] is not None
    assert quota_by_id[52] is None


def test_build_admin_token_quota_items_skips_inactive_tokens_for_quota_fetch() -> None:
    quota_token_ids: list[int] = []

    class DummyQuotaService:
        async def get_many(self, token_rows, *, client, oauth_manager, account_ids):
            quota_token_ids.extend(token_row.id for token_row in token_rows)
            return {
                token_row.id: CodexQuotaSnapshot(
                    fetched_at=datetime.now(timezone.utc),
                    error=None,
                    plan_type="pro",
                    windows=[],
                )
                for token_row in token_rows
            }

    active_token = CodexToken(
        id=61,
        account_id="acct_active",
        refresh_token="refresh-active",
        token_type="codex",
        is_active=True,
        plan_type="plus",
    )
    disabled_token = CodexToken(
        id=62,
        account_id="acct_disabled",
        refresh_token="refresh-disabled",
        token_type="codex",
        is_active=False,
        plan_type="free",
    )
    app = SimpleNamespace(
        state=SimpleNamespace(
            quota_service=DummyQuotaService(),
            http_client=object(),
            oauth_manager=object(),
        )
    )

    items = asyncio.run(
        _build_admin_token_quota_items(
            app,
            token_rows=[active_token, disabled_token],
        )
    )

    assert quota_token_ids == [61]
    quota_by_id = {item["id"]: item["quota"] for item in items}
    plan_by_id = {item["id"]: item["plan_type"] for item in items}
    assert quota_by_id[61] is not None
    assert quota_by_id[62] is None
    assert plan_by_id[61] == "pro"
    assert plan_by_id[62] == "free"


def test_gateway_request_log_token_index_is_declared() -> None:
    assert "ix_gateway_request_logs_token_id" in {index.name for index in GatewayRequestLog.__table__.indexes}


def test_chat_image_checkpoint_indexes_are_declared() -> None:
    checkpoint_indexes = {index.name for index in ChatImageCheckpoint.__table__.indexes}
    image_indexes = {index.name for index in ChatImageCheckpointImage.__table__.indexes}

    assert "ix_chat_image_checkpoints_scope_hash" in checkpoint_indexes
    assert "ix_chat_image_checkpoints_client_model" in checkpoint_indexes
    assert "ix_chat_image_checkpoint_images_image_sha256" in image_indexes
    assert "ix_chat_image_checkpoint_images_image_call_id" in image_indexes
