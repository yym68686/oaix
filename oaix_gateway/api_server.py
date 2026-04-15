import asyncio
import codecs
import copy
import json
import logging
import os
import re
import uuid
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import httpx
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict
from .database import close_database, init_db, utcnow
from .oauth import CodexOAuthManager, is_permanently_invalid_refresh_token_error
from .quota import CodexPlanInfo, CodexQuotaService, CodexQuotaSnapshot, extract_codex_plan_info
from .request_store import (
    create_request_log,
    finalize_request_log,
    get_request_log_analytics,
    get_request_costs_by_account,
    get_request_log_summary,
    list_request_logs,
)
from .token_import_jobs import (
    TokenImportBackgroundWorker,
    create_token_import_job,
    get_token_import_job,
)
from .token_store import (
    DEFAULT_TOKEN_SELECTION_STRATEGY,
    TOKEN_SELECTION_STRATEGY_FILL_FIRST,
    TokenSelectionSettings,
    claim_next_active_token,
    delete_token,
    get_token_counts,
    get_token_selection_settings,
    list_token_rows,
    mark_token_error,
    mark_token_success,
    repair_duplicate_token_histories,
    set_token_active_state,
    update_token_selection_settings,
)
from .usage_cost import UsageMetrics, extract_usage_metrics


logger = logging.getLogger("oaix.gateway")
WEB_DIR = Path(__file__).resolve().parent / "web"
RESPONSES_STREAM_PREFLIGHT_EVENTS = frozenset(
    {
        "response.created",
        "response.in_progress",
        "response.queued",
    }
)
RESPONSES_STREAM_NETWORK_ERRORS = (
    httpx.ReadError,
    httpx.RemoteProtocolError,
    httpx.LocalProtocolError,
    httpx.ReadTimeout,
    httpx.ConnectError,
)
RESPONSES_FAILURE_STATUS_BY_CODE = {
    "account_deactivated": 403,
    "account_disabled": 403,
    "account_suspended": 403,
    "authentication_error": 401,
    "billing_hard_limit_reached": 429,
    "context_length_exceeded": 400,
    "deactivated_workspace": 403,
    "incorrect_api_key_provided": 401,
    "insufficient_quota": 429,
    "invalid_api_key": 401,
    "invalid_request_error": 400,
    "invalid_type": 400,
    "model_not_found": 404,
    "not_found_error": 404,
    "permission_denied": 403,
    "rate_limit_exceeded": 429,
    "unsupported_parameter": 400,
    "user_deactivated": 403,
    "user_suspended": 403,
}
RESPONSES_FAILURE_STATUS_BY_TYPE = {
    "authentication_error": 401,
    "invalid_request_error": 400,
    "not_found_error": 404,
    "permission_error": 403,
    "rate_limit_error": 429,
    "tokens": 429,
}


class ResponsesRequest(BaseModel):
    model: str
    input: Any
    stream: bool | None = None

    model_config = ConfigDict(extra="allow")


class TokenSelectionUpdateRequest(BaseModel):
    strategy: str


class TokenActivationUpdateRequest(BaseModel):
    active: bool


@dataclass(frozen=True)
class ProxyRequestResult:
    response: Response
    status_code: int
    model_name: str | None = None
    first_token_at: datetime | None = None
    usage_metrics: UsageMetrics | None = None
    stream_capture: "_ProxyStreamCapture | None" = None


class _ProxyStreamCapture:
    def __init__(
        self,
        *,
        initial_model_name: str | None = None,
        initial_first_token_at: datetime | None = None,
    ) -> None:
        self.model_name = initial_model_name
        self.first_token_at = initial_first_token_at
        self.usage_metrics: UsageMetrics | None = None
        self._response_snapshot: dict[str, Any] = {}
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self._text_buffer = ""

    def feed(self, chunk: bytes) -> None:
        if not chunk:
            return
        try:
            self._text_buffer += self._decoder.decode(chunk)

            while True:
                match = re.search(r"\r?\n\r?\n", self._text_buffer)
                if not match:
                    break

                raw_event = self._text_buffer[:match.start()]
                self._text_buffer = self._text_buffer[match.end():]
                if not raw_event.strip():
                    continue

                event_type, event_payload = _extract_responses_stream_event(raw_event)
                extracted_model_name = _extract_response_model_name(event_payload)
                if extracted_model_name:
                    self.model_name = extracted_model_name

                if self.first_token_at is None and event_type and event_type not in RESPONSES_STREAM_PREFLIGHT_EVENTS:
                    self.first_token_at = utcnow()

                if not isinstance(event_payload, dict):
                    continue

                response_obj = event_payload.get("response")
                if isinstance(response_obj, dict):
                    _merge_mapping(self._response_snapshot, response_obj)

                usage_payload = self._response_snapshot or event_payload
                usage_metrics = extract_usage_metrics(usage_payload, model_name=self.model_name)
                if usage_metrics is not None:
                    self.usage_metrics = usage_metrics
        except Exception:
            logger.exception("Failed to observe streamed response usage")


class ResponseTrafficLease:
    def __init__(self, controller: "ResponseTrafficController") -> None:
        self._controller = controller
        self._released = False

    async def release(self) -> None:
        if self._released:
            return
        self._released = True
        self._controller.release_response()


class ResponseTrafficController:
    def __init__(self) -> None:
        self._active_responses = 0
        self._responses_idle = asyncio.Event()
        self._responses_idle.set()

    @property
    def active_responses(self) -> int:
        return self._active_responses

    def start_response(self) -> ResponseTrafficLease:
        self._active_responses += 1
        self._responses_idle.clear()
        return ResponseTrafficLease(self)

    def release_response(self) -> None:
        if self._active_responses <= 0:
            self._active_responses = 0
            self._responses_idle.set()
            return
        self._active_responses -= 1
        if self._active_responses == 0:
            self._responses_idle.set()

    async def wait_for_import_turn(self, *, timeout_seconds: float | None = None) -> bool:
        waited = False
        resolved_timeout = _import_wait_timeout_seconds() if timeout_seconds is None else float(timeout_seconds)
        deadline: float | None = None
        loop = asyncio.get_running_loop()
        if resolved_timeout > 0:
            deadline = loop.time() + resolved_timeout

        while True:
            while self._active_responses > 0:
                waited = True
                if deadline is None:
                    await self._responses_idle.wait()
                    continue

                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise TimeoutError("Timed out waiting for active /v1/responses traffic to drain before import")

                try:
                    await asyncio.wait_for(self._responses_idle.wait(), timeout=remaining)
                except asyncio.TimeoutError as exc:
                    raise TimeoutError(
                        "Timed out waiting for active /v1/responses traffic to drain before import"
                    ) from exc

            if not waited:
                return False

            grace_seconds = _import_response_idle_grace_seconds()
            if grace_seconds <= 0:
                return True

            if deadline is not None:
                remaining = deadline - loop.time()
                if remaining <= 0 or remaining < grace_seconds:
                    raise TimeoutError("Timed out waiting for active /v1/responses traffic to drain before import")

            await asyncio.sleep(grace_seconds)
            if self._active_responses == 0:
                return True


def _int_env(name: str, default: int, *, minimum: int = 0) -> int:
    raw = str(os.getenv(name, "") or "").strip()
    try:
        value = int(raw) if raw else int(default)
    except ValueError:
        value = int(default)
    return max(minimum, value)


def _float_env(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = str(os.getenv(name, "") or "").strip()
    try:
        value = float(raw) if raw else float(default)
    except ValueError:
        value = float(default)
    return max(minimum, value)


def _max_request_account_retries() -> int:
    return _int_env("MAX_REQUEST_ACCOUNT_RETRIES", 100, minimum=1)


def _default_usage_limit_cooldown_seconds() -> int:
    return _int_env("DEFAULT_USAGE_LIMIT_COOLDOWN_SECONDS", 300, minimum=1)


def _import_response_idle_grace_seconds() -> float:
    return _float_env("IMPORT_RESPONSE_IDLE_GRACE_SECONDS", 0.25, minimum=0.0)


def _import_wait_timeout_seconds() -> float:
    return _float_env("IMPORT_WAIT_TIMEOUT_SECONDS", 30.0, minimum=0.0)


def _compact_server_error_cooldown_seconds() -> int:
    return _int_env("COMPACT_SERVER_ERROR_COOLDOWN_SECONDS", 60, minimum=0)


def _admin_quota_cache_ttl_seconds() -> int:
    return _int_env("ADMIN_QUOTA_CACHE_TTL_SECONDS", 60, minimum=5)


def _admin_quota_max_concurrency() -> int:
    return _int_env("ADMIN_QUOTA_MAX_CONCURRENCY", 4, minimum=1)


def _get_service_api_keys() -> set[str]:
    raw = os.getenv("SERVICE_API_KEYS") or os.getenv("API_KEY") or ""
    return {item.strip() for item in raw.split(",") if item.strip()}


async def verify_service_api_key(http_request: Request) -> None:
    valid_keys = _get_service_api_keys()
    if not valid_keys:
        return

    header = (http_request.headers.get("Authorization") or "").strip()
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or token.strip() not in valid_keys:
        raise HTTPException(status_code=401, detail="Invalid or missing bearer token")


def _normalize_responses_upstream_url(base_url: str) -> str:
    base = (base_url or "").strip()
    if not base:
        return "https://chatgpt.com/backend-api/codex/responses"

    base = base.rstrip("/")
    if base.endswith("/v1/responses") or base.endswith("/responses"):
        return base
    return f"{base}/responses"


def _normalize_responses_compact_upstream_url(base_url: str) -> str:
    base = (base_url or "").strip()
    if not base:
        return "https://chatgpt.com/backend-api/codex/responses/compact"

    base = base.rstrip("/")
    if base.endswith("/v1/responses/compact") or base.endswith("/responses/compact"):
        return base

    base = _normalize_responses_upstream_url(base)
    if base.endswith("/v1/responses") or base.endswith("/responses"):
        return f"{base}/compact"
    if base.endswith("/compact"):
        return base
    return f"{base}/compact"


def _codex_responses_url(*, compact: bool = False) -> str:
    base = os.getenv("CODEX_BASE_URL") or ""
    if compact:
        return _normalize_responses_compact_upstream_url(base)
    return _normalize_responses_upstream_url(base)


def _responses_endpoint_path(*, compact: bool = False) -> str:
    if compact:
        return "/v1/responses/compact"
    return "/v1/responses"


def _build_upstream_headers(http_request: Request, access_token: str, account_id: str | None, stream: bool) -> dict[str, str]:
    session_id = (http_request.headers.get("Session_id") or "").strip() or str(uuid.uuid4())
    conversation_id = (http_request.headers.get("Conversation_id") or "").strip() or session_id

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Openai-Beta": (http_request.headers.get("Openai-Beta") or "responses=experimental").strip(),
        "Originator": (http_request.headers.get("Originator") or "codex_cli_rs").strip(),
        "Version": (http_request.headers.get("Version") or "0.21.0").strip(),
        "Session_id": session_id,
        "Conversation_id": conversation_id,
        "User-Agent": (http_request.headers.get("User-Agent") or "codex_cli_rs/0.50.0").strip(),
        "Connection": "Keep-Alive",
        "Accept": "text/event-stream" if stream else "application/json",
    }
    if account_id:
        headers["Chatgpt-Account-Id"] = account_id
    return headers


def _sanitize_codex_payload(payload: dict[str, Any], *, compact: bool = False) -> dict[str, Any]:
    payload.pop("max_output_tokens", None)
    payload.pop("response_format", None)
    payload.pop("previous_response_id", None)
    payload.pop("prompt_cache_retention", None)
    payload.pop("safety_identifier", None)
    if compact:
        payload.pop("store", None)
    payload.setdefault("instructions", "")
    return payload


def _decode_error_body(raw: bytes) -> str:
    try:
        return raw.decode("utf-8", errors="replace").strip()
    except Exception:
        return repr(raw)


def _mapping_get(payload: Any, *keys: str) -> Any | None:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _normalize_optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _extract_response_model_name(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None

    for candidate in (
        payload.get("model_name"),
        payload.get("model"),
        _mapping_get(payload, "response", "model_name"),
        _mapping_get(payload, "response", "model"),
    ):
        model_name = _normalize_optional_text(candidate)
        if model_name:
            return model_name
    return None


def _extract_responses_stream_event(raw_event: str) -> tuple[str, Any]:
    event_name = ""
    data_lines: list[str] = []
    for line in raw_event.splitlines():
        if line.startswith("event:"):
            event_name = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].strip())

    data_str = "\n".join(data_lines).strip()
    if data_str == "[DONE]":
        return "[DONE]", "[DONE]"

    parsed_payload: Any = data_str
    if data_str:
        try:
            parsed_payload = json.loads(data_str)
        except Exception:
            parsed_payload = data_str

    if not event_name and isinstance(parsed_payload, dict):
        event_name = str(parsed_payload.get("type") or "").strip()

    return event_name, parsed_payload


def _responses_error_status_code(error_obj: Any) -> int:
    if isinstance(error_obj, dict):
        raw_status = error_obj.get("status_code") or error_obj.get("status")
        try:
            status_code = int(raw_status)
        except (TypeError, ValueError):
            status_code = None
        if status_code is not None and 100 <= status_code <= 599:
            return status_code

        error_code = str(error_obj.get("code") or "").strip().lower()
        if error_code in RESPONSES_FAILURE_STATUS_BY_CODE:
            return RESPONSES_FAILURE_STATUS_BY_CODE[error_code]

        error_type = str(error_obj.get("type") or "").strip().lower()
        if error_type in RESPONSES_FAILURE_STATUS_BY_TYPE:
            return RESPONSES_FAILURE_STATUS_BY_TYPE[error_type]

        message = str(error_obj.get("message") or "").lower()
        if "rate limit" in message or "too many requests" in message:
            return 429
        if "invalid" in message or "unsupported" in message:
            return 400
        if "not found" in message:
            return 404
        if "permission" in message or "forbidden" in message:
            return 403
        if "auth" in message or "api key" in message or "unauthorized" in message:
            return 401

    return 500


def _responses_failure_http_exception(payload: Any) -> HTTPException | None:
    if not isinstance(payload, dict):
        return None

    error_obj = None
    response_status = str(_mapping_get(payload, "response", "status") or "").strip().lower()
    payload_status = str(payload.get("status") or "").strip().lower()
    payload_type = str(payload.get("type") or "").strip().lower()

    if payload_type == "error" and payload.get("error") is not None:
        error_obj = payload.get("error")
    elif payload_type == "response.failed":
        error_obj = _mapping_get(payload, "response", "error")
    elif payload_status == "failed":
        error_obj = payload.get("error")
    elif response_status == "failed":
        error_obj = _mapping_get(payload, "response", "error")
    elif isinstance(payload.get("error"), dict):
        error_obj = payload.get("error")

    if error_obj is None and (payload_status == "failed" or response_status == "failed"):
        error_obj = {"message": "Responses upstream returned status=failed"}

    if error_obj is None:
        return None

    return HTTPException(
        status_code=_responses_error_status_code(error_obj),
        detail=json.dumps({"error": error_obj}, ensure_ascii=False),
    )


def _merge_mapping(target: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _merge_mapping(target[key], value)
            continue
        target[key] = value
    return target


def _append_output_text_delta(
    output_text_parts: dict[tuple[int, int], list[str]],
    event_payload: dict[str, Any],
) -> None:
    delta = event_payload.get("delta")
    if delta is None:
        return

    try:
        output_index = int(event_payload.get("output_index", 0) or 0)
    except (TypeError, ValueError):
        output_index = 0
    try:
        content_index = int(event_payload.get("content_index", 0) or 0)
    except (TypeError, ValueError):
        content_index = 0

    output_text_parts.setdefault((output_index, content_index), []).append(str(delta))


def _build_responses_output_from_text_parts(
    output_text_parts: dict[tuple[int, int], list[str]],
) -> list[dict[str, Any]]:
    grouped: dict[int, dict[int, str]] = {}
    for (output_index, content_index), parts in sorted(output_text_parts.items()):
        grouped.setdefault(output_index, {})[content_index] = "".join(parts)

    output: list[dict[str, Any]] = []
    for output_index in sorted(grouped):
        content: list[dict[str, Any]] = []
        for content_index in sorted(grouped[output_index]):
            text = grouped[output_index][content_index]
            content.append(
                {
                    "type": "output_text",
                    "text": text,
                }
            )
        output.append(
            {
                "type": "message",
                "content": content,
            }
        )
    return output


def _finalize_collected_response(
    response_snapshot: dict[str, Any] | None,
    *,
    model: str,
    output_text_parts: dict[tuple[int, int], list[str]],
) -> dict[str, Any]:
    response_data = copy.deepcopy(response_snapshot or {})

    if not response_data.get("id"):
        response_data["id"] = f"resp_{uuid.uuid4().hex}"
    if not response_data.get("object"):
        response_data["object"] = "response"
    if response_data.get("created_at") is None:
        response_data["created_at"] = int(utcnow().timestamp())
    if not response_data.get("model"):
        response_data["model"] = model
    response_data["status"] = "completed"

    if output_text_parts and not response_data.get("output"):
        response_data["output"] = _build_responses_output_from_text_parts(output_text_parts)

    return response_data


async def _collect_responses_json_from_sse(
    upstream_iter: AsyncIterator[bytes],
    *,
    model: str,
) -> tuple[dict[str, Any], datetime | None]:
    response_snapshot: dict[str, Any] | None = None
    output_text_parts: dict[tuple[int, int], list[str]] = {}
    first_token_at: datetime | None = None

    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    text_buffer = ""

    while True:
        try:
            chunk = await upstream_iter.__anext__()
        except StopAsyncIteration:
            if text_buffer.strip():
                raise HTTPException(status_code=502, detail="Upstream closed stream with an incomplete SSE event")
            break

        text_buffer += decoder.decode(chunk)

        while True:
            match = re.search(r"\r?\n\r?\n", text_buffer)
            if not match:
                break

            raw_event = text_buffer[:match.start()]
            text_buffer = text_buffer[match.end():]
            if not raw_event.strip():
                continue

            event_type, event_payload = _extract_responses_stream_event(raw_event)

            if event_type == "[DONE]":
                return _finalize_collected_response(
                    response_snapshot,
                    model=model,
                    output_text_parts=output_text_parts,
                ), first_token_at

            semantic_failure = _responses_failure_http_exception(event_payload)
            if semantic_failure is not None:
                raise semantic_failure

            if first_token_at is None and event_type and event_type not in RESPONSES_STREAM_PREFLIGHT_EVENTS:
                first_token_at = utcnow()

            if not isinstance(event_payload, dict):
                continue

            response_obj = event_payload.get("response")
            if isinstance(response_obj, dict):
                if response_snapshot is None:
                    response_snapshot = {}
                _merge_mapping(response_snapshot, response_obj)

            if event_type == "response.output_text.delta":
                _append_output_text_delta(output_text_parts, event_payload)

            if event_type == "response.completed":
                return _finalize_collected_response(
                    response_snapshot,
                    model=model,
                    output_text_parts=output_text_parts,
                ), first_token_at

    if response_snapshot is None and not output_text_parts:
        raise HTTPException(status_code=502, detail="Upstream closed stream without data")

    return _finalize_collected_response(
        response_snapshot,
        model=model,
        output_text_parts=output_text_parts,
    ), first_token_at


async def _read_response_body_with_first_chunk_time(
    upstream_iter: AsyncIterator[bytes],
) -> tuple[bytes, datetime | None]:
    chunks: list[bytes] = []
    first_chunk_at: datetime | None = None

    async for chunk in upstream_iter:
        if chunk and first_chunk_at is None:
            first_chunk_at = utcnow()
        chunks.append(chunk)

    return b"".join(chunks), first_chunk_at


def _decode_responses_json_body(raw: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=502, detail="Upstream returned invalid JSON") from exc

    semantic_failure = _responses_failure_http_exception(payload)
    if semantic_failure is not None:
        raise semantic_failure
    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="Upstream returned a non-object JSON response")
    return payload


async def _prime_responses_upstream_stream(upstream_iter: AsyncIterator[bytes]) -> tuple[list[bytes], bool, str | None]:
    """
    Buffer only initial status events so semantic stream failures can still
    trigger key failover before the downstream response is committed.
    """
    buffered_chunks: list[bytes] = []
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    text_buffer = ""
    model_name: str | None = None

    while True:
        try:
            chunk = await upstream_iter.__anext__()
        except StopAsyncIteration as exc:
            if not buffered_chunks:
                raise HTTPException(status_code=502, detail="Upstream closed stream without data") from exc
            if text_buffer.strip():
                raise HTTPException(status_code=502, detail="Upstream closed stream with an incomplete SSE event")
            return buffered_chunks, False, model_name

        buffered_chunks.append(chunk)
        text_buffer += decoder.decode(chunk)

        while True:
            match = re.search(r"\r?\n\r?\n", text_buffer)
            if not match:
                break

            raw_event = text_buffer[:match.start()]
            text_buffer = text_buffer[match.end():]
            if not raw_event.strip():
                continue

            event_type, event_payload = _extract_responses_stream_event(raw_event)
            extracted_model_name = _extract_response_model_name(event_payload)
            if extracted_model_name:
                model_name = extracted_model_name
            if event_type == "[DONE]":
                return buffered_chunks, False, model_name

            semantic_failure = _responses_failure_http_exception(event_payload)
            if semantic_failure is not None:
                raise semantic_failure

            if not event_type or event_type not in RESPONSES_STREAM_PREFLIGHT_EVENTS:
                return buffered_chunks, True, model_name


def _extract_usage_limit_cooldown_seconds(status_code: int, error_text: str) -> int | None:
    if status_code != 429:
        return None

    try:
        payload = json.loads(error_text)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None

    error = payload.get("error")
    if not isinstance(error, dict):
        return None

    error_type = str(error.get("type") or "").strip()
    error_message = str(error.get("message") or "").strip().lower()
    if error_type != "usage_limit_reached" and "usage limit" not in error_message:
        return None

    resets_in_seconds = error.get("resets_in_seconds")
    if resets_in_seconds is not None:
        try:
            return max(0, int(float(resets_in_seconds)))
        except Exception:
            pass

    resets_at = error.get("resets_at")
    if resets_at is not None:
        try:
            reset_at_epoch = int(float(resets_at))
            now_epoch = int(datetime.now(timezone.utc).timestamp())
            return max(0, reset_at_epoch - now_epoch)
        except Exception:
            pass

    return _default_usage_limit_cooldown_seconds()


def _is_permanent_account_disable_error(status_code: int, error_text: str) -> bool:
    if status_code not in (401, 402):
        return False

    try:
        payload = json.loads(error_text)
    except Exception:
        return False
    if not isinstance(payload, dict):
        return False

    if status_code == 402:
        detail = payload.get("detail")
        if isinstance(detail, dict):
            detail_code = str(detail.get("code") or "").strip()
            if detail_code == "deactivated_workspace":
                return True

    if status_code == 401:
        error = payload.get("error")
        if isinstance(error, dict):
            error_code = str(error.get("code") or "").strip()
            if error_code == "account_deactivated":
                return True

    return False


def _get_compact_codex_server_error_cooling_time(
    *,
    compact: bool,
    status_code: int,
    error_text: str,
) -> int:
    if not compact:
        return 0
    if status_code < 500:
        return 0
    if _extract_usage_limit_cooldown_seconds(status_code, error_text) is not None:
        return 0
    return _compact_server_error_cooldown_seconds()


def _should_retry_upstream_server_error(status_code: int) -> bool:
    return 500 <= status_code <= 599


def _usage_finalize_kwargs(usage_metrics: UsageMetrics | None) -> dict[str, Any]:
    if usage_metrics is None:
        return {}
    return {
        "input_tokens": usage_metrics.input_tokens,
        "output_tokens": usage_metrics.output_tokens,
        "total_tokens": usage_metrics.total_tokens,
        "estimated_cost_usd": usage_metrics.estimated_cost_usd,
    }


async def _finalize_stream_request_log(
    *,
    request_log_id: int,
    status_code: int,
    attempt_count: int,
    account_id: str | None,
    fallback_model_name: str | None,
    fallback_first_token_at: datetime | None,
    stream_capture: _ProxyStreamCapture,
) -> None:
    try:
        await finalize_request_log(
            request_log_id,
            status_code=status_code,
            success=True,
            attempt_count=attempt_count,
            finished_at=utcnow(),
            first_token_at=stream_capture.first_token_at or fallback_first_token_at,
            account_id=account_id,
            model_name=stream_capture.model_name or fallback_model_name,
            **_usage_finalize_kwargs(stream_capture.usage_metrics),
        )
    except Exception:
        logger.exception("Failed to finalize streamed request log id=%s", request_log_id)


async def _wrap_streaming_body_iterator(
    body_iterator: AsyncIterator[bytes],
    *,
    on_close: Callable[[], Awaitable[None]],
) -> AsyncGenerator[bytes, None]:
    try:
        async for chunk in body_iterator:
            yield chunk
    finally:
        close_iterator = getattr(body_iterator, "aclose", None)
        if callable(close_iterator):
            try:
                await close_iterator()
            except Exception:
                logger.exception("Failed to close streamed response body iterator")

        try:
            await on_close()
        except Exception:
            logger.exception("Failed to run streamed response cleanup")


def _selection_strategy_label(strategy: str) -> str:
    if strategy == TOKEN_SELECTION_STRATEGY_FILL_FIRST:
        return "Fill-first"
    return "最旧未使用优先"


def _selection_strategy_description(strategy: str) -> str:
    if strategy == TOKEN_SELECTION_STRATEGY_FILL_FIRST:
        return "持续使用第一个可用 key，直到该 key 冷却或失效再切到下一个。"
    return "优先分配最久未使用的 key，把请求更均匀地摊在账号窗口上。"


def _serialize_token_selection_settings(settings: TokenSelectionSettings) -> dict[str, Any]:
    return {
        "strategy": settings.strategy,
        "label": _selection_strategy_label(settings.strategy),
        "description": _selection_strategy_description(settings.strategy),
        "updated_at": settings.updated_at,
        "options": [
            {
                "id": DEFAULT_TOKEN_SELECTION_STRATEGY,
                "label": _selection_strategy_label(DEFAULT_TOKEN_SELECTION_STRATEGY),
                "description": _selection_strategy_description(DEFAULT_TOKEN_SELECTION_STRATEGY),
            },
            {
                "id": TOKEN_SELECTION_STRATEGY_FILL_FIRST,
                "label": _selection_strategy_label(TOKEN_SELECTION_STRATEGY_FILL_FIRST),
                "description": _selection_strategy_description(TOKEN_SELECTION_STRATEGY_FILL_FIRST),
            },
        ],
    }


def _current_token_selection_settings(app: FastAPI) -> TokenSelectionSettings:
    selection = getattr(app.state, "token_selection_settings", None)
    if isinstance(selection, TokenSelectionSettings):
        return selection
    return TokenSelectionSettings(strategy=DEFAULT_TOKEN_SELECTION_STRATEGY)


def _serialize_admin_token_item(
    token_row,
    *,
    plan_info: CodexPlanInfo,
    quota_snapshot: CodexQuotaSnapshot | None,
    observed_cost_usd: float | None,
) -> dict[str, Any]:
    item = {
        "id": token_row.id,
        "email": token_row.email,
        "account_id": token_row.account_id,
        "chatgpt_account_id": plan_info.chatgpt_account_id or token_row.account_id,
        "is_active": token_row.is_active,
        "cooldown_until": token_row.cooldown_until,
        "last_refresh_at": token_row.last_refresh_at,
        "expires_at": token_row.expires_at,
        "last_used_at": token_row.last_used_at,
        "last_error": token_row.last_error,
        "source_file": token_row.source_file,
        "created_at": token_row.created_at,
        "updated_at": token_row.updated_at,
        "plan_type": quota_snapshot.plan_type if quota_snapshot and quota_snapshot.plan_type else plan_info.plan_type,
        "subscription_active_start": plan_info.subscription_active_start,
        "subscription_active_until": plan_info.subscription_active_until,
        "observed_cost_usd": observed_cost_usd,
        "quota": asdict(quota_snapshot) if quota_snapshot is not None else None,
    }
    return item


def _token_observed_account_ids(token_row, *, plan_info: CodexPlanInfo) -> set[str]:
    return {
        account_id
        for account_id in {
            token_row.account_id,
            plan_info.chatgpt_account_id,
        }
        if str(account_id or "").strip()
    }


def _resolve_token_observed_cost_usd(
    observed_costs_by_account: dict[str, float],
    *,
    token_row,
    plan_info: CodexPlanInfo,
) -> float | None:
    candidate_ids = _token_observed_account_ids(token_row, plan_info=plan_info)
    if not candidate_ids:
        return None
    return round(sum(observed_costs_by_account.get(account_id, 0.0) for account_id in candidate_ids), 6)


async def _build_admin_token_items(
    app: FastAPI,
    *,
    token_rows: list[Any],
    include_quota: bool,
) -> list[dict[str, Any]]:
    plan_info_by_id = {
        token_row.id: extract_codex_plan_info(
            token_row.id_token,
            account_id=token_row.account_id,
            raw_payload=token_row.raw_payload,
        )
        for token_row in token_rows
    }

    quota_by_id: dict[int, CodexQuotaSnapshot] = {}
    observed_costs_by_account: dict[str, float] = {}
    observed_account_ids = {
        account_id
        for token_row in token_rows
        for account_id in _token_observed_account_ids(token_row, plan_info=plan_info_by_id[token_row.id])
    }
    if observed_account_ids:
        try:
            observed_costs_by_account = await get_request_costs_by_account(observed_account_ids)
        except Exception:
            logger.exception("Failed to collect admin token observed costs")
    if include_quota and token_rows:
        quota_service: CodexQuotaService = app.state.quota_service
        client: httpx.AsyncClient = app.state.http_client
        oauth_manager: CodexOAuthManager = app.state.oauth_manager
        effective_account_ids = {
            token_row.id: plan_info_by_id[token_row.id].chatgpt_account_id or token_row.account_id
            for token_row in token_rows
        }
        try:
            quota_by_id = await quota_service.get_many(
                token_rows,
                client=client,
                oauth_manager=oauth_manager,
                account_ids=effective_account_ids,
            )
        except Exception:
            logger.exception("Failed to collect admin token quota snapshots")

    return [
        _serialize_admin_token_item(
            token_row,
            plan_info=plan_info_by_id[token_row.id],
            quota_snapshot=quota_by_id.get(token_row.id),
            observed_cost_usd=_resolve_token_observed_cost_usd(
                observed_costs_by_account,
                token_row=token_row,
                plan_info=plan_info_by_id[token_row.id],
            ),
        )
        for token_row in token_rows
    ]


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
    await init_db()
    repair_summary = await repair_duplicate_token_histories()
    if repair_summary.merged_row_count:
        logger.info(
            "Merged duplicate token histories on startup: groups=%s merged_rows=%s canonical_ids=%s shadow_ids=%s",
            repair_summary.duplicate_group_count,
            repair_summary.merged_row_count,
            list(repair_summary.canonical_ids),
            list(repair_summary.shadow_ids),
        )
    app.state.http_client = httpx.AsyncClient(
        follow_redirects=True,
        http2=False,
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
    )
    app.state.oauth_manager = CodexOAuthManager()
    app.state.quota_service = CodexQuotaService(
        ttl_seconds=_admin_quota_cache_ttl_seconds(),
        concurrency=_admin_quota_max_concurrency(),
    )
    app.state.token_selection_settings = await get_token_selection_settings()
    app.state.token_import_worker = TokenImportBackgroundWorker(response_traffic=app.state.response_traffic)
    await app.state.token_import_worker.start()
    try:
        yield
    finally:
        await app.state.token_import_worker.stop()
        await app.state.http_client.aclose()
        await close_database()


def create_app() -> FastAPI:
    app = FastAPI(title="oaix Gateway", version="0.1.0", lifespan=lifespan)
    app.state.response_traffic = ResponseTrafficController()
    app.state.token_import_worker = None
    app.mount("/assets", StaticFiles(directory=str(WEB_DIR)), name="assets")

    @app.get("/")
    async def index() -> HTMLResponse:
        html = (WEB_DIR / "index.html").read_text(encoding="utf-8")
        css_version = int((WEB_DIR / "styles.css").stat().st_mtime)
        js_version = int((WEB_DIR / "app.js").stat().st_mtime)
        html = html.replace("/assets/styles.css", f"/assets/styles.css?v={css_version}")
        html = html.replace("/assets/app.js", f"/assets/app.js?v={js_version}")
        return HTMLResponse(
            content=html,
            headers={"Cache-Control": "no-store, max-age=0"},
        )

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        counts = await get_token_counts()
        ok = counts.available > 0
        return JSONResponse(
            status_code=200 if ok else 503,
            content={
                "ok": ok,
                "upstream": _codex_responses_url(),
                "counts": asdict(counts),
                "service_key_protected": bool(_get_service_api_keys()),
            },
        )

    @app.get("/admin/token-selection")
    async def get_token_selection_route(
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        return _serialize_token_selection_settings(_current_token_selection_settings(app))

    @app.post("/admin/token-selection")
    async def update_token_selection_route(
        payload: TokenSelectionUpdateRequest,
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        try:
            selection = await update_token_selection_settings(strategy=payload.strategy)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        app.state.token_selection_settings = selection
        return _serialize_token_selection_settings(selection)

    @app.get("/admin/tokens")
    async def list_tokens_route(
        http_request: Request,
        limit: int = Query(100, ge=1, le=500),
        include_quota: bool = Query(False),
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        counts = await get_token_counts()
        token_rows = await list_token_rows(limit=limit)
        items = await _build_admin_token_items(
            http_request.app,
            token_rows=token_rows,
            include_quota=include_quota,
        )
        return {
            "counts": asdict(counts),
            "selection": _serialize_token_selection_settings(_current_token_selection_settings(http_request.app)),
            "items": items,
        }

    @app.post("/admin/tokens/{token_id}/activation")
    async def update_token_activation_route(
        token_id: int,
        payload: TokenActivationUpdateRequest,
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        token = await set_token_active_state(token_id, active=payload.active)
        if token is None:
            raise HTTPException(status_code=404, detail="Token not found")
        counts = await get_token_counts()
        return {
            "id": token.id,
            "is_active": token.is_active,
            "cooldown_until": token.cooldown_until,
            "counts": asdict(counts),
        }

    @app.delete("/admin/tokens/{token_id}")
    async def delete_token_route(
        token_id: int,
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        deleted = await delete_token(token_id)
        if deleted is None:
            raise HTTPException(status_code=404, detail="Token not found")
        counts = await get_token_counts()
        return {
            "id": deleted.canonical_id,
            "deleted_ids": list(deleted.deleted_ids),
            "counts": asdict(counts),
        }

    @app.get("/admin/requests")
    async def list_requests_route(
        limit: int = Query(100, ge=1, le=500),
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        summary = await get_request_log_summary()
        analytics = await get_request_log_analytics(hours=24, bucket_minutes=60, top_models=6)
        items = [asdict(item) for item in await list_request_logs(limit=limit)]
        return {
            "summary": asdict(summary),
            "analytics": asdict(analytics),
            "items": items,
        }

    @app.post("/admin/tokens/import", status_code=202)
    async def import_tokens_route(
        http_request: Request,
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        try:
            body = await http_request.json()
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="Request body must be valid JSON") from exc

        if isinstance(body, dict) and isinstance(body.get("tokens"), list):
            payloads = body["tokens"]
        elif isinstance(body, list):
            payloads = body
        elif isinstance(body, dict):
            payloads = [body]
        else:
            raise HTTPException(
                status_code=400,
                detail="Body must be a token object, an array of token objects, or {'tokens': [...]}",
            )
        job = await create_token_import_job(payloads)
        worker = getattr(http_request.app.state, "token_import_worker", None)
        if worker is not None:
            worker.notify()
        return {"job": asdict(job)}

    @app.get("/admin/tokens/import-jobs/{job_id}")
    async def get_import_job_route(
        job_id: int,
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        job = await get_token_import_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Import job not found")
        return {"job": asdict(job)}

    @app.post("/v1/responses")
    async def responses_route(
        http_request: Request,
        request_data: ResponsesRequest,
        _: None = Depends(verify_service_api_key),
    ) -> Response:
        return await _responses_route_common(http_request, request_data, compact=False)

    @app.post("/v1/responses/compact")
    async def responses_compact_route(
        http_request: Request,
        request_data: ResponsesRequest,
        _: None = Depends(verify_service_api_key),
    ) -> Response:
        return await _responses_route_common(http_request, request_data, compact=True)

    async def _responses_route_common(
        http_request: Request,
        request_data: ResponsesRequest,
        *,
        compact: bool,
    ) -> Response:
        response_traffic: ResponseTrafficController = http_request.app.state.response_traffic
        response_lease = response_traffic.start_response()
        release_response_traffic = True
        endpoint = _responses_endpoint_path(compact=compact)
        request_started_at = utcnow()
        request_log = await create_request_log(
            request_id=str(uuid.uuid4()),
            endpoint=endpoint,
            model=request_data.model,
            is_stream=bool(request_data.stream),
            started_at=request_started_at,
            client_ip=http_request.client.host if http_request.client is not None else None,
            user_agent=(http_request.headers.get("User-Agent") or "").strip() or None,
        )

        attempt_count = 0
        last_account_id: str | None = None

        try:
            counts = await get_token_counts()
            if counts.available <= 0:
                raise HTTPException(status_code=503, detail="No available Codex token in database")

            max_attempts = max(1, min(counts.available, _max_request_account_retries()))
            client: httpx.AsyncClient = http_request.app.state.http_client
            oauth_manager: CodexOAuthManager = http_request.app.state.oauth_manager
            selection_settings = _current_token_selection_settings(http_request.app)
            last_error: HTTPException | None = None

            for attempt in range(1, max_attempts + 1):
                attempt_count = attempt
                token_row = await claim_next_active_token(selection_strategy=selection_settings.strategy)
                if token_row is None:
                    break
                last_account_id = token_row.account_id

                try:
                    access_token, access_token_refreshed = await oauth_manager.get_access_token(token_row, client)
                except HTTPException as exc:
                    oauth_manager.invalidate(token_row.id)
                    permanent_refresh_failure = is_permanently_invalid_refresh_token_error(exc)
                    await mark_token_error(
                        token_row.id,
                        str(exc.detail),
                        deactivate=permanent_refresh_failure,
                        clear_access_token=True,
                    )
                    if exc.status_code in (401, 403):
                        last_error = exc
                        continue
                    raise

                try:
                    proxy_result = await _proxy_request_with_token(
                        client,
                        http_request,
                        request_data,
                        access_token=access_token,
                        account_id=token_row.account_id,
                        compact=compact,
                    )
                    await mark_token_success(token_row.id)
                    if isinstance(proxy_result.response, StreamingResponse):
                        async def on_stream_close() -> None:
                            try:
                                if proxy_result.stream_capture is not None:
                                    await _finalize_stream_request_log(
                                        request_log_id=request_log.id,
                                        status_code=proxy_result.status_code,
                                        attempt_count=attempt,
                                        account_id=token_row.account_id,
                                        fallback_model_name=proxy_result.model_name,
                                        fallback_first_token_at=proxy_result.first_token_at,
                                        stream_capture=proxy_result.stream_capture,
                                    )
                                else:
                                    await finalize_request_log(
                                        request_log.id,
                                        status_code=proxy_result.status_code,
                                        success=True,
                                        attempt_count=attempt,
                                        finished_at=utcnow(),
                                        first_token_at=proxy_result.first_token_at,
                                        account_id=token_row.account_id,
                                        model_name=proxy_result.model_name,
                                    )
                            finally:
                                await response_lease.release()

                        proxy_result.response.body_iterator = _wrap_streaming_body_iterator(
                            proxy_result.response.body_iterator,
                            on_close=on_stream_close,
                        )
                        release_response_traffic = False
                        return proxy_result.response
                    await finalize_request_log(
                        request_log.id,
                        status_code=proxy_result.status_code,
                        success=True,
                        attempt_count=attempt,
                        finished_at=utcnow(),
                        first_token_at=proxy_result.first_token_at,
                        account_id=token_row.account_id,
                        model_name=proxy_result.model_name,
                        **_usage_finalize_kwargs(proxy_result.usage_metrics),
                    )
                    return proxy_result.response
                except HTTPException as exc:
                    status_code = getattr(exc, "status_code", 500)
                    detail = str(getattr(exc, "detail", "") or exc)
                    cooldown_seconds = _extract_usage_limit_cooldown_seconds(status_code, detail)

                    if cooldown_seconds is not None:
                        await mark_token_error(
                            token_row.id,
                            detail,
                            cooldown_seconds=cooldown_seconds,
                        )
                        logger.warning(
                            "Codex account cooled down: token_id=%s account_id=%s cooldown_seconds=%s attempt=%s/%s",
                            token_row.id,
                            token_row.account_id,
                            cooldown_seconds,
                            attempt,
                            max_attempts,
                        )
                        last_error = exc
                        continue

                    permanently_disabled = _is_permanent_account_disable_error(status_code, detail)
                    auth_failed_after_refresh = status_code in (401, 403) and access_token_refreshed

                    if permanently_disabled or auth_failed_after_refresh:
                        oauth_manager.invalidate(token_row.id)
                        await mark_token_error(
                            token_row.id,
                            detail,
                            deactivate=True,
                            clear_access_token=True,
                        )
                        logger.warning(
                            "Codex account disabled after upstream auth failure: token_id=%s account_id=%s status=%s refreshed=%s attempt=%s/%s",
                            token_row.id,
                            token_row.account_id,
                            status_code,
                            access_token_refreshed,
                            attempt,
                            max_attempts,
                        )
                        last_error = exc
                        continue

                    if status_code in (401, 403):
                        oauth_manager.invalidate(token_row.id)
                        await mark_token_error(token_row.id, detail, clear_access_token=True)
                        logger.warning(
                            "Codex upstream auth failed; invalidated access token for retry: token_id=%s account_id=%s status=%s attempt=%s/%s",
                            token_row.id,
                            token_row.account_id,
                            status_code,
                            attempt,
                            max_attempts,
                        )
                        last_error = exc
                        continue

                    compact_server_error_cooling_time = _get_compact_codex_server_error_cooling_time(
                        compact=compact,
                        status_code=status_code,
                        error_text=detail,
                    )
                    if _should_retry_upstream_server_error(status_code) and attempt < max_attempts:
                        mark_kwargs: dict[str, Any] = {}
                        if compact_server_error_cooling_time > 0:
                            mark_kwargs["cooldown_seconds"] = compact_server_error_cooling_time
                        await mark_token_error(token_row.id, detail, **mark_kwargs)
                        logger.warning(
                            "Codex upstream server error before response commit: token_id=%s account_id=%s status=%s compact=%s cooldown_seconds=%s attempt=%s/%s",
                            token_row.id,
                            token_row.account_id,
                            status_code,
                            compact,
                            compact_server_error_cooling_time,
                            attempt,
                            max_attempts,
                        )
                        last_error = exc
                        continue

                    raise
                except httpx.HTTPError as exc:
                    message = f"Upstream request failed: {type(exc).__name__}: {exc}"
                    compact_server_error_cooling_time = _get_compact_codex_server_error_cooling_time(
                        compact=compact,
                        status_code=500,
                        error_text=message,
                    )
                    if attempt < max_attempts:
                        mark_kwargs: dict[str, Any] = {}
                        if compact_server_error_cooling_time > 0:
                            mark_kwargs["cooldown_seconds"] = compact_server_error_cooling_time
                        await mark_token_error(token_row.id, message, **mark_kwargs)
                        logger.warning(
                            "Codex upstream transport error before response commit: token_id=%s account_id=%s error_type=%s compact=%s cooldown_seconds=%s attempt=%s/%s",
                            token_row.id,
                            token_row.account_id,
                            type(exc).__name__,
                            compact,
                            compact_server_error_cooling_time,
                            attempt,
                            max_attempts,
                        )
                        last_error = HTTPException(status_code=502, detail=message)
                        continue
                    await mark_token_error(token_row.id, message)
                    raise HTTPException(status_code=502, detail=message) from exc

            if last_error is not None:
                raise HTTPException(
                    status_code=503,
                    detail=f"All available Codex accounts are exhausted or cooling down. Last error: {last_error.detail}",
                )
            raise HTTPException(status_code=503, detail="No available Codex token could satisfy this request")
        except HTTPException as exc:
            await finalize_request_log(
                request_log.id,
                status_code=getattr(exc, "status_code", 500),
                success=False,
                attempt_count=attempt_count,
                finished_at=utcnow(),
                account_id=last_account_id,
                error_message=str(getattr(exc, "detail", "") or exc),
            )
            raise
        except Exception as exc:
            await finalize_request_log(
                request_log.id,
                status_code=500,
                success=False,
                attempt_count=attempt_count,
                finished_at=utcnow(),
                account_id=last_account_id,
                error_message=str(exc),
            )
            raise
        finally:
            if release_response_traffic:
                await response_lease.release()

    return app


async def _stream_upstream_response(
    stream_cm,
    upstream_iter: AsyncIterator[bytes],
    buffered_chunks: list[bytes],
    *,
    stream_committed: bool,
    stream_capture: _ProxyStreamCapture | None = None,
) -> AsyncGenerator[bytes, None]:
    try:
        for chunk in buffered_chunks:
            if stream_capture is not None:
                stream_capture.feed(chunk)
            yield chunk
        async for chunk in upstream_iter:
            if stream_capture is not None:
                stream_capture.feed(chunk)
            yield chunk
    except RESPONSES_STREAM_NETWORK_ERRORS as exc:
        logger.warning(
            "Codex upstream stream aborted after response commit: error_type=%s detail=%s",
            type(exc).__name__,
            str(exc) or type(exc).__name__,
        )
        if stream_committed:
            yield b"data: [DONE]\n\n"
    finally:
        await stream_cm.__aexit__(None, None, None)


async def _proxy_request_with_token(
    client: httpx.AsyncClient,
    http_request: Request,
    request_data: ResponsesRequest,
    *,
    access_token: str,
    account_id: str | None,
    compact: bool = False,
) -> ProxyRequestResult:
    downstream_stream = bool(request_data.stream)
    upstream_stream = downstream_stream or not compact
    payload = _sanitize_codex_payload(request_data.model_dump(exclude_unset=True), compact=compact)
    if compact and not downstream_stream:
        payload.pop("stream", None)
    elif not downstream_stream:
        payload["stream"] = True

    headers = _build_upstream_headers(
        http_request,
        access_token=access_token,
        account_id=account_id,
        stream=upstream_stream,
    )
    upstream_url = _codex_responses_url(compact=compact)
    json_payload = json.dumps(payload, ensure_ascii=False)

    if downstream_stream:
        stream_cm = client.stream(
            "POST",
            upstream_url,
            headers=headers,
            content=json_payload,
            timeout=httpx.Timeout(connect=30.0, read=None, write=30.0, pool=30.0),
        )
        upstream_response = await stream_cm.__aenter__()
        if upstream_response.status_code < 200 or upstream_response.status_code >= 300:
            raw = await upstream_response.aread()
            await stream_cm.__aexit__(None, None, None)
            raise HTTPException(
                status_code=upstream_response.status_code,
                detail=_decode_error_body(raw),
            )

        upstream_iter = upstream_response.aiter_raw()
        try:
            buffered_chunks, stream_committed, model_name = await _prime_responses_upstream_stream(upstream_iter)
        except HTTPException:
            await stream_cm.__aexit__(None, None, None)
            raise
        except RESPONSES_STREAM_NETWORK_ERRORS:
            await stream_cm.__aexit__(None, None, None)
            raise

        initial_first_token_at = utcnow() if stream_committed else None
        stream_capture = _ProxyStreamCapture(
            initial_model_name=model_name or request_data.model,
            initial_first_token_at=initial_first_token_at,
        )

        return ProxyRequestResult(
            response=StreamingResponse(
                _stream_upstream_response(
                    stream_cm,
                    upstream_iter,
                    buffered_chunks,
                    stream_committed=stream_committed,
                    stream_capture=stream_capture,
                ),
                media_type="text/event-stream",
            ),
            status_code=upstream_response.status_code,
            model_name=model_name or request_data.model,
            first_token_at=initial_first_token_at,
            stream_capture=stream_capture,
        )

    if upstream_stream:
        stream_cm = client.stream(
            "POST",
            upstream_url,
            headers=headers,
            content=json_payload,
            timeout=httpx.Timeout(connect=30.0, read=None, write=30.0, pool=30.0),
        )
        upstream_response = await stream_cm.__aenter__()
        try:
            if upstream_response.status_code < 200 or upstream_response.status_code >= 300:
                raw = await upstream_response.aread()
                raise HTTPException(
                    status_code=upstream_response.status_code,
                    detail=_decode_error_body(raw),
                )

            data, first_token_at = await _collect_responses_json_from_sse(
                upstream_response.aiter_raw(),
                model=request_data.model,
            )
            effective_model_name = _extract_response_model_name(data) or request_data.model
            return ProxyRequestResult(
                response=JSONResponse(status_code=upstream_response.status_code, content=data),
                status_code=upstream_response.status_code,
                model_name=effective_model_name,
                first_token_at=first_token_at,
                usage_metrics=extract_usage_metrics(data, model_name=effective_model_name),
            )
        finally:
            await stream_cm.__aexit__(None, None, None)

    stream_cm = client.stream(
        "POST",
        upstream_url,
        headers=headers,
        content=json_payload,
        timeout=httpx.Timeout(connect=30.0, read=None, write=30.0, pool=30.0),
    )
    upstream_response = await stream_cm.__aenter__()
    try:
        if upstream_response.status_code < 200 or upstream_response.status_code >= 300:
            raw = await upstream_response.aread()
            raise HTTPException(
                status_code=upstream_response.status_code,
                detail=_decode_error_body(raw),
            )

        raw, first_token_at = await _read_response_body_with_first_chunk_time(upstream_response.aiter_raw())
        data = _decode_responses_json_body(raw)
        effective_model_name = _extract_response_model_name(data) or request_data.model
        return ProxyRequestResult(
            response=JSONResponse(status_code=upstream_response.status_code, content=data),
            status_code=upstream_response.status_code,
            model_name=effective_model_name,
            first_token_at=first_token_at,
            usage_metrics=extract_usage_metrics(data, model_name=effective_model_name),
        )
    finally:
        await stream_cm.__aexit__(None, None, None)


app = create_app()


def main() -> None:
    uvicorn.run(
        "oaix_gateway.api_server:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
    )
