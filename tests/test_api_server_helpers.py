import asyncio
import json
from collections.abc import AsyncIterator

import pytest
from fastapi import HTTPException
from starlette.requests import Request
from oaix_gateway.api_server import (
    _ProxyStreamCapture,
    ResponseTrafficController,
    _collect_responses_json_from_sse,
    _extract_usage_limit_cooldown_seconds,
    _get_compact_codex_server_error_cooling_time,
    _is_permanent_account_disable_error,
    _normalize_responses_compact_upstream_url,
    _proxy_request_with_token,
    _prime_responses_upstream_stream,
    _responses_failure_http_exception,
    ResponsesRequest,
    _sanitize_codex_payload,
    _should_retry_upstream_server_error,
)
from oaix_gateway.database import CodexToken


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
        yield b'event: response.created\ndata: {"type":"response.created","response":{"model":"gpt-5.4"}}\n\n'
        yield b'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"hi"}\n\n'

    buffered_chunks, stream_committed, model_name = asyncio.run(_prime_responses_upstream_stream(upstream()))

    assert stream_committed is True
    assert model_name == "gpt-5.4"
    assert buffered_chunks == [
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
    assert json.loads(stream_call["content"])["stream"] is True
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
    assert "stream" not in json.loads(stream_call["content"])
    assert result.first_token_at is not None
    assert result.status_code == 200
    assert result.model_name == "gpt-5.4-compact"
    assert result.usage_metrics is None
    body = json.loads(result.response.body)
    assert body["id"] == "resp_compact_123"
    assert body["model"] == "gpt-5.4-compact"
    assert body["status"] == "completed"
    assert body["output"][0]["content"][0]["text"] == "hello compact"


def test_should_retry_upstream_server_error_only_for_5xx() -> None:
    assert _should_retry_upstream_server_error(500) is True
    assert _should_retry_upstream_server_error(503) is True
    assert _should_retry_upstream_server_error(429) is False
    assert _should_retry_upstream_server_error(400) is False


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


def test_codex_token_refresh_token_index_is_declared() -> None:
    assert "ix_codex_tokens_refresh_token" in {index.name for index in CodexToken.__table__.indexes}
