import asyncio
import codecs
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
from typing import Any

import httpx
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict
from starlette.background import BackgroundTask, BackgroundTasks

from .database import close_database, init_db, utcnow
from .oauth import CodexOAuthManager
from .request_store import (
    create_request_log,
    finalize_request_log,
    get_request_log_summary,
    list_request_logs,
)
from .token_store import (
    claim_next_active_token,
    get_token_counts,
    list_tokens,
    mark_token_error,
    mark_token_success,
    upsert_token_payload,
)


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


@dataclass(frozen=True)
class ProxyRequestResult:
    response: Response
    status_code: int
    first_token_at: datetime | None = None


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

    async def wait_for_import_turn(self) -> bool:
        waited = False
        while True:
            while self._active_responses > 0:
                waited = True
                await self._responses_idle.wait()

            if not waited:
                return False

            grace_seconds = _import_response_idle_grace_seconds()
            if grace_seconds <= 0:
                return True

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


def _compact_server_error_cooldown_seconds() -> int:
    return _int_env("COMPACT_SERVER_ERROR_COOLDOWN_SECONDS", 60, minimum=0)


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


def _append_background_task(response: Response, func, *args, **kwargs) -> None:
    if response.background is None:
        response.background = BackgroundTask(func, *args, **kwargs)
        return
    if isinstance(response.background, BackgroundTasks):
        response.background.add_task(func, *args, **kwargs)
        return

    background_tasks = BackgroundTasks()
    background_tasks.tasks.append(response.background)
    background_tasks.add_task(func, *args, **kwargs)
    response.background = background_tasks


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


async def _prime_responses_upstream_stream(upstream_iter: AsyncIterator[bytes]) -> tuple[list[bytes], bool]:
    """
    Buffer only initial status events so semantic stream failures can still
    trigger key failover before the downstream response is committed.
    """
    buffered_chunks: list[bytes] = []
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    text_buffer = ""

    while True:
        try:
            chunk = await upstream_iter.__anext__()
        except StopAsyncIteration as exc:
            if not buffered_chunks:
                raise HTTPException(status_code=502, detail="Upstream closed stream without data") from exc
            if text_buffer.strip():
                raise HTTPException(status_code=502, detail="Upstream closed stream with an incomplete SSE event")
            return buffered_chunks, False

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
            if event_type == "[DONE]":
                return buffered_chunks, False

            semantic_failure = _responses_failure_http_exception(event_payload)
            if semantic_failure is not None:
                raise semantic_failure

            if not event_type or event_type not in RESPONSES_STREAM_PREFLIGHT_EVENTS:
                return buffered_chunks, True


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
    await init_db()
    app.state.http_client = httpx.AsyncClient(
        follow_redirects=True,
        http2=False,
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
    )
    app.state.oauth_manager = CodexOAuthManager()
    try:
        yield
    finally:
        await app.state.http_client.aclose()
        await close_database()


def create_app() -> FastAPI:
    app = FastAPI(title="oaix Gateway", version="0.1.0", lifespan=lifespan)
    app.state.response_traffic = ResponseTrafficController()
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

    @app.get("/admin/tokens")
    async def list_tokens_route(
        limit: int = Query(100, ge=1, le=500),
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        counts = await get_token_counts()
        items = [asdict(token) for token in await list_tokens(limit=limit)]
        return {
            "counts": asdict(counts),
            "items": items,
        }

    @app.get("/admin/requests")
    async def list_requests_route(
        limit: int = Query(100, ge=1, le=500),
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        summary = await get_request_log_summary()
        items = [asdict(item) for item in await list_request_logs(limit=limit)]
        return {
            "summary": asdict(summary),
            "items": items,
        }

    @app.post("/admin/tokens/import")
    async def import_tokens_route(
        http_request: Request,
        _: None = Depends(verify_service_api_key),
    ) -> dict[str, Any]:
        response_traffic: ResponseTrafficController = http_request.app.state.response_traffic
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

        created: list[dict[str, Any]] = []
        updated: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        yielded_to_response_traffic = 0
        for index, payload in enumerate(payloads):
            if await response_traffic.wait_for_import_turn():
                yielded_to_response_traffic += 1
            if not isinstance(payload, dict):
                failed.append({"index": index, "error": "Token payload must be a JSON object"})
                continue
            try:
                result = await upsert_token_payload(payload)
                item = {
                    "id": result.token.id,
                    "email": result.token.email,
                    "account_id": result.token.account_id,
                    "index": index,
                }
                if result.action == "created":
                    created.append(item)
                elif result.action == "updated":
                    updated.append(item)
                else:
                    skipped.append(
                        {
                            **item,
                            "reason": "duplicate_refresh_token_history",
                        }
                    )
            except Exception as exc:
                failed.append({"index": index, "error": str(exc)})

        return {
            "imported_count": len(created) + len(updated),
            "created_count": len(created),
            "updated_count": len(updated),
            "skipped_count": len(skipped),
            "failed_count": len(failed),
            "yielded_to_response_traffic_count": yielded_to_response_traffic,
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "failed": failed,
        }

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
            last_error: HTTPException | None = None

            for attempt in range(1, max_attempts + 1):
                attempt_count = attempt
                token_row = await claim_next_active_token()
                if token_row is None:
                    break
                last_account_id = token_row.account_id

                try:
                    access_token, access_token_refreshed = await oauth_manager.get_access_token(token_row, client)
                except HTTPException as exc:
                    oauth_manager.invalidate(token_row.id)
                    await mark_token_error(token_row.id, str(exc.detail), deactivate=exc.status_code in (401, 403))
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
                    await finalize_request_log(
                        request_log.id,
                        status_code=proxy_result.status_code,
                        success=True,
                        attempt_count=attempt,
                        finished_at=utcnow(),
                        first_token_at=proxy_result.first_token_at,
                        account_id=token_row.account_id,
                    )
                    if isinstance(proxy_result.response, StreamingResponse):
                        _append_background_task(proxy_result.response, response_lease.release)
                        release_response_traffic = False
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
) -> AsyncGenerator[bytes, None]:
    try:
        for chunk in buffered_chunks:
            yield chunk
        async for chunk in upstream_iter:
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
    payload = _sanitize_codex_payload(request_data.model_dump(exclude_unset=True), compact=compact)
    headers = _build_upstream_headers(
        http_request,
        access_token=access_token,
        account_id=account_id,
        stream=bool(request_data.stream),
    )
    upstream_url = _codex_responses_url(compact=compact)
    json_payload = json.dumps(payload, ensure_ascii=False)

    if request_data.stream:
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
            buffered_chunks, stream_committed = await _prime_responses_upstream_stream(upstream_iter)
        except HTTPException:
            await stream_cm.__aexit__(None, None, None)
            raise
        except RESPONSES_STREAM_NETWORK_ERRORS:
            await stream_cm.__aexit__(None, None, None)
            raise

        return ProxyRequestResult(
            response=StreamingResponse(
                _stream_upstream_response(
                    stream_cm,
                    upstream_iter,
                    buffered_chunks,
                    stream_committed=stream_committed,
                ),
                media_type="text/event-stream",
            ),
            status_code=upstream_response.status_code,
            first_token_at=utcnow() if stream_committed else None,
        )

    upstream_response = await client.post(
        upstream_url,
        headers=headers,
        content=json_payload,
        timeout=httpx.Timeout(connect=30.0, read=300.0, write=30.0, pool=30.0),
    )
    if upstream_response.status_code < 200 or upstream_response.status_code >= 300:
        raw = await upstream_response.aread()
        raise HTTPException(
            status_code=upstream_response.status_code,
            detail=_decode_error_body(raw),
        )

    try:
        data = upstream_response.json()
    except json.JSONDecodeError:
        return ProxyRequestResult(
            response=Response(
                content=upstream_response.content,
                status_code=upstream_response.status_code,
                media_type=upstream_response.headers.get("content-type", "application/octet-stream"),
            ),
            status_code=upstream_response.status_code,
        )
    semantic_failure = _responses_failure_http_exception(data)
    if semantic_failure is not None:
        raise semantic_failure
    return ProxyRequestResult(
        response=JSONResponse(status_code=upstream_response.status_code, content=data),
        status_code=upstream_response.status_code,
    )


app = create_app()


def main() -> None:
    uvicorn.run(
        "oaix_gateway.api_server:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
    )
