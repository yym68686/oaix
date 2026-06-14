import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from oaix_gateway.database import CodexToken
from oaix_gateway.token_import_jobs import _build_token_import_batch_summary, list_token_import_batch_summaries


def test_build_token_import_batch_summary_includes_average_observed_cost() -> None:
    submitted_at = datetime(2026, 6, 3, 12, 0, tzinfo=timezone.utc)
    job = SimpleNamespace(
        id=42,
        status="completed",
        import_queue_position="front",
        total_count=3,
        processed_count=3,
        created_count=2,
        updated_count=0,
        skipped_count=1,
        failed_count=0,
        yielded_to_response_traffic_count=0,
        response_traffic_timeout_count=0,
        created_items=[
            {"index": 0, "id": 7},
            {"index": 1, "id": 8},
        ],
        updated_items=[],
        skipped_items=[{"index": 2, "id": 999}],
        failed_items=[],
        submitted_at=submitted_at,
        started_at=submitted_at,
        heartbeat_at=submitted_at,
        finished_at=submitted_at,
        last_error=None,
    )
    tokens_by_id = {
        7: CodexToken(id=7, refresh_token="rt-7", token_type="codex", is_active=True),
        8: CodexToken(id=8, refresh_token="rt-8", token_type="codex", is_active=False),
    }

    summary = _build_token_import_batch_summary(
        job,
        tokens_by_id=tokens_by_id,
        observed_costs_by_token={7: 1.23456789, 8: 2.0, 999: 100.0},
        now=submitted_at,
    )

    assert summary.token_count == 2
    assert summary.missing == 1
    assert summary.observed_cost_usd == 3.234568
    assert summary.average_observed_cost_usd == 1.617284


def test_list_token_import_batch_summaries_skips_observed_cost_by_default(monkeypatch) -> None:
    job = SimpleNamespace(
        id=42,
        status="completed",
        import_queue_position="front",
        total_count=1,
        processed_count=1,
        created_count=1,
        updated_count=0,
        skipped_count=0,
        failed_count=0,
        yielded_to_response_traffic_count=0,
        response_traffic_timeout_count=0,
        created_items=[{"index": 0, "id": 7}],
        updated_items=[],
        skipped_items=[],
        failed_items=[],
        submitted_at=datetime(2026, 6, 3, 12, 0, tzinfo=timezone.utc),
        started_at=None,
        heartbeat_at=None,
        finished_at=None,
        last_error=None,
    )
    token = CodexToken(id=7, refresh_token="rt-7", token_type="codex", is_active=True)

    class _ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def all(self):
            return self._rows

    class _Result:
        def __init__(self, rows):
            self._rows = rows

        def all(self):
            return self._rows

        def scalars(self):
            return _ScalarResult(self._rows)

    class _Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def execute(self, stmt):
            text = str(stmt)
            if "token_import_jobs" in text:
                return _Result([job])
            if "codex_tokens" in text:
                return _Result([token])
            raise AssertionError(f"unexpected query: {text}")

    def fake_get_read_session():
        return _Session()

    async def fail_get_request_costs_by_token(token_ids):
        raise AssertionError("default import batch summaries must not query request-log costs")

    monkeypatch.setattr("oaix_gateway.token_import_jobs.get_read_session", fake_get_read_session)
    monkeypatch.setattr("oaix_gateway.token_import_jobs.get_request_costs_by_token", fail_get_request_costs_by_token)

    summaries = asyncio.run(list_token_import_batch_summaries(limit=30))

    assert len(summaries) == 1
    assert summaries[0].id == 42
    assert summaries[0].observed_cost_usd == 0.0
    assert summaries[0].average_observed_cost_usd is None


def test_list_token_import_batch_summaries_can_include_observed_cost(monkeypatch) -> None:
    job = SimpleNamespace(
        id=42,
        status="completed",
        import_queue_position="front",
        total_count=1,
        processed_count=1,
        created_count=1,
        updated_count=0,
        skipped_count=0,
        failed_count=0,
        yielded_to_response_traffic_count=0,
        response_traffic_timeout_count=0,
        created_items=[{"index": 0, "id": 7}],
        updated_items=[],
        skipped_items=[],
        failed_items=[],
        submitted_at=datetime(2026, 6, 3, 12, 0, tzinfo=timezone.utc),
        started_at=None,
        heartbeat_at=None,
        finished_at=None,
        last_error=None,
    )
    token = CodexToken(id=7, refresh_token="rt-7", token_type="codex", is_active=True)
    captured = {}

    class _ScalarResult:
        def __init__(self, rows):
            self._rows = rows

        def all(self):
            return self._rows

    class _Result:
        def __init__(self, rows):
            self._rows = rows

        def all(self):
            return self._rows

        def scalars(self):
            return _ScalarResult(self._rows)

    class _Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def execute(self, stmt):
            text = str(stmt)
            if "token_import_jobs" in text:
                return _Result([job])
            if "codex_tokens" in text:
                return _Result([token])
            raise AssertionError(f"unexpected query: {text}")

    def fake_get_read_session():
        return _Session()

    async def fake_get_request_costs_by_token(token_ids):
        captured["token_ids"] = tuple(token_ids)
        return {7: 2.5}

    monkeypatch.setattr("oaix_gateway.token_import_jobs.get_read_session", fake_get_read_session)
    monkeypatch.setattr("oaix_gateway.token_import_jobs.get_request_costs_by_token", fake_get_request_costs_by_token)

    summaries = asyncio.run(list_token_import_batch_summaries(limit=30, include_observed_cost=True))

    assert captured == {"token_ids": (7,)}
    assert summaries[0].observed_cost_usd == 2.5
    assert summaries[0].average_observed_cost_usd == 2.5


def test_list_token_import_batch_summaries_uses_lightweight_rows(monkeypatch) -> None:
    job = SimpleNamespace(
        id=42,
        status="completed",
        import_queue_position="front",
        total_count=1,
        processed_count=1,
        created_count=1,
        updated_count=0,
        skipped_count=0,
        failed_count=0,
        created_items=[{"index": 0, "id": 7}],
        updated_items=[],
        skipped_items=[],
        submitted_at=datetime(2026, 6, 3, 12, 0, tzinfo=timezone.utc),
        started_at=None,
        finished_at=None,
    )
    token = SimpleNamespace(id=7, is_active=True, cooldown_until=None)

    class _Result:
        def __init__(self, rows):
            self._rows = rows

        def all(self):
            return self._rows

    class _Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def execute(self, stmt):
            text = str(stmt)
            if "token_import_jobs" in text:
                assert "payloads" not in text
                assert "failed_items" not in text
                return _Result([job])
            if "codex_tokens" in text:
                assert "access_token" not in text
                assert "refresh_token" not in text
                return _Result([token])
            raise AssertionError(f"unexpected query: {text}")

    def fake_get_read_session():
        return _Session()

    monkeypatch.setattr("oaix_gateway.token_import_jobs.get_read_session", fake_get_read_session)

    summaries = asyncio.run(list_token_import_batch_summaries(limit=30))

    assert summaries[0].id == 42
    assert summaries[0].token_ids == (7,)
