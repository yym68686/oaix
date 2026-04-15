import asyncio
import json
from collections.abc import AsyncIterator
from datetime import datetime, timezone

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
    _serialize_admin_token_item,
    _sanitize_codex_payload,
    _should_retry_upstream_server_error,
    _wrap_streaming_body_iterator,
    create_app,
)
from oaix_gateway.database import CodexToken
from oaix_gateway.quota import CodexPlanInfo
from oaix_gateway.token_import_jobs import (
    IMPORT_JOB_STATUS_COMPLETED,
    IMPORT_JOB_STATUS_QUEUED,
    IMPORT_JOB_STATUS_RUNNING,
    TokenImportJobLease,
    TokenImportJobState,
    process_token_import_job,
)
from oaix_gateway.token_store import TokenUpsertResult


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


def test_serialize_admin_token_item_includes_created_at() -> None:
    created_at = datetime(2026, 4, 15, 9, 30, tzinfo=timezone.utc)
    token = CodexToken(
        id=7,
        email="user@example.com",
        account_id="acct_123",
        refresh_token="refresh_token",
        is_active=True,
    )
    token.created_at = created_at

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
            self.notified = False

        def notify(self) -> None:
            self.notified = True

    async def fake_create_token_import_job(payloads: list[dict[str, object]]) -> TokenImportJobState:
        captured["payloads"] = payloads
        return TokenImportJobState(
            id=42,
            status=IMPORT_JOB_STATUS_QUEUED,
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
    assert app.state.token_import_worker.notified is True
    assert result["job"]["id"] == 42
    assert result["job"]["status"] == IMPORT_JOB_STATUS_QUEUED
    assert result["job"]["total_count"] == 1


def test_process_token_import_job_updates_progress_and_completes(monkeypatch) -> None:
    progress_calls: list[dict[str, object]] = []
    touched: list[int] = []
    completed: dict[str, object] = {}

    class BusyResponseTraffic:
        active_responses = 2

        def __init__(self) -> None:
            self.calls = 0

        async def wait_for_import_turn(self, *, timeout_seconds=None) -> bool:
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError("Timed out waiting for active /v1/responses traffic to drain before import")
            return False

    async def fake_upsert_token_payload(payload: dict) -> TokenUpsertResult:
        token = CodexToken(
            id=99,
            email="user@example.com",
            account_id="acct_123",
            refresh_token=payload["refresh_token"],
            is_active=True,
        )
        return TokenUpsertResult(token=token, action="created")

    async def fake_touch_token_import_job(job_id: int) -> None:
        touched.append(job_id)
        return None

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

    monkeypatch.setattr("oaix_gateway.token_import_jobs.upsert_token_payload", fake_upsert_token_payload)
    monkeypatch.setattr("oaix_gateway.token_import_jobs.touch_token_import_job", fake_touch_token_import_job)
    monkeypatch.setattr(
        "oaix_gateway.token_import_jobs.update_token_import_job_progress",
        fake_update_token_import_job_progress,
    )
    monkeypatch.setattr("oaix_gateway.token_import_jobs.complete_token_import_job", fake_complete_token_import_job)

    job = TokenImportJobLease(
        id=7,
        status=IMPORT_JOB_STATUS_RUNNING,
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
        payloads=[{"refresh_token": "rt-123"}, "bad-payload"],
    )

    result = asyncio.run(
        process_token_import_job(
            job,
            response_traffic=BusyResponseTraffic(),
        )
    )

    assert touched == [7, 7]
    assert [call["processed_count"] for call in progress_calls] == [1, 2]
    assert progress_calls[0]["response_traffic_timeout_count"] == 1
    assert progress_calls[1]["failed"] == [{"index": 1, "error": "Token payload must be a JSON object"}]
    assert completed["processed_count"] == 2
    assert completed["response_traffic_timeout_count"] == 1
    assert len(completed["created"]) == 1
    assert completed["failed"] == [{"index": 1, "error": "Token payload must be a JSON object"}]
    assert result is not None
    assert result.status == IMPORT_JOB_STATUS_COMPLETED
    assert result.created_count == 1
    assert result.failed_count == 1


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


def test_codex_token_refresh_token_index_is_declared() -> None:
    assert "ix_codex_tokens_refresh_token" in {index.name for index in CodexToken.__table__.indexes}
