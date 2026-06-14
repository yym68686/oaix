import asyncio
from datetime import datetime, timezone

from sqlalchemy.dialects import postgresql

from oaix_gateway.database import GatewayRequestLog
from oaix_gateway.request_store import (
    _build_request_bucket_analytics_stmt,
    _build_request_bucket_analytics_stats_stmt,
    _build_request_log_summary_stmt,
    _build_request_log_summary_stats_stmt,
    _build_request_account_costs_stmt,
    _build_request_token_costs_stmt,
    _build_request_model_analytics_stmt,
    _build_request_model_analytics_stats_stmt,
    _normalize_request_account_ids,
    _normalize_request_token_ids,
    _request_token_cost_deltas,
    _request_hourly_stat_deltas,
    backfill_request_token_costs_from_logs,
    get_request_costs_by_token,
    _upsert_request_logs_portable,
)


def test_build_request_log_summary_stmt_uses_single_aggregate_query() -> None:
    stmt = _build_request_log_summary_stmt()

    sql = str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "count(*) AS total" in sql
    assert "CASE WHEN (gateway_request_logs.success IS true)" in sql
    assert "sum(gateway_request_logs.estimated_cost_usd)" in sql
    assert sql.count("FROM gateway_request_logs") == 1


def test_build_request_log_summary_stmt_can_scope_to_recent_window() -> None:
    stmt = _build_request_log_summary_stmt(
        since=datetime(2026, 4, 14, tzinfo=timezone.utc),
    )

    sql = str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "WHERE gateway_request_logs.started_at >= '2026-04-14 00:00:00+00:00'" in sql


def test_build_request_model_analytics_stmt_groups_by_subquery_alias() -> None:
    stmt = _build_request_model_analytics_stmt(
        since=datetime(2026, 4, 14, tzinfo=timezone.utc),
        top_models=6,
    )

    sql = str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "FROM (SELECT" in sql
    assert "GROUP BY anon_1.model_name" in sql
    assert "GROUP BY coalesce(" not in sql


def test_build_request_bucket_analytics_stmt_groups_in_database() -> None:
    stmt = _build_request_bucket_analytics_stmt(
        since=datetime(2026, 4, 14, tzinfo=timezone.utc),
        bucket_minutes=60,
    )

    sql = str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "date_bin(INTERVAL '60 minutes'" in sql
    assert "GROUP BY date_bin(" in sql
    assert "ORDER BY bucket_start ASC" in sql


def test_build_request_log_summary_stats_stmt_uses_hourly_stats() -> None:
    stmt = _build_request_log_summary_stats_stmt(
        since=datetime(2026, 4, 14, 12, 34, tzinfo=timezone.utc),
    )

    sql = str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "FROM gateway_request_hourly_stats" in sql
    assert "FROM gateway_request_logs" not in sql
    assert "gateway_request_hourly_stats.bucket_start >= '2026-04-14 12:00:00+00:00'" in sql


def test_build_request_model_analytics_stats_stmt_groups_hourly_stats() -> None:
    stmt = _build_request_model_analytics_stats_stmt(
        since=datetime(2026, 4, 14, tzinfo=timezone.utc),
        top_models=6,
    )

    sql = str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "FROM gateway_request_hourly_stats" in sql
    assert "FROM (SELECT" in sql
    assert "GROUP BY anon_1.model_name" in sql
    assert "GROUP BY coalesce(" not in sql
    assert "FROM gateway_request_logs" not in sql


def test_build_request_bucket_analytics_stats_stmt_groups_hourly_stats() -> None:
    stmt = _build_request_bucket_analytics_stats_stmt(
        since=datetime(2026, 4, 14, tzinfo=timezone.utc),
        bucket_minutes=60,
    )

    sql = str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "FROM gateway_request_hourly_stats" in sql
    assert "date_bin(INTERVAL '60 minutes'" in sql
    assert "FROM gateway_request_logs" not in sql


def test_request_hourly_stat_deltas_group_by_utc_hour_and_model() -> None:
    rows = [
        GatewayRequestLog(
            request_id="one",
            endpoint="/v1/responses",
            model="gpt-5",
            model_name="gpt-5",
            is_stream=True,
            status_code=200,
            success=True,
            started_at=datetime(2026, 4, 14, 12, 30, tzinfo=timezone.utc),
            input_tokens=10,
            output_tokens=20,
            total_tokens=30,
            estimated_cost_usd=0.01,
            ttft_ms=100,
            duration_ms=200,
        ),
        GatewayRequestLog(
            request_id="two",
            endpoint="/v1/responses",
            model="gpt-5",
            model_name="gpt-5",
            is_stream=False,
            status_code=502,
            success=False,
            started_at=datetime(2026, 4, 14, 12, 45, tzinfo=timezone.utc),
            input_tokens=1,
            output_tokens=2,
            total_tokens=3,
            estimated_cost_usd=0.02,
            ttft_ms=300,
            duration_ms=400,
        ),
    ]

    deltas = _request_hourly_stat_deltas(rows)

    assert len(deltas) == 1
    assert deltas[0]["bucket_start"] == datetime(2026, 4, 14, 12, tzinfo=timezone.utc)
    assert deltas[0]["model_name"] == "gpt-5"
    assert deltas[0]["request_count"] == 2
    assert deltas[0]["success_count"] == 1
    assert deltas[0]["failure_count"] == 1
    assert deltas[0]["streaming_count"] == 1
    assert deltas[0]["input_tokens"] == 11
    assert deltas[0]["output_tokens"] == 22
    assert deltas[0]["total_tokens"] == 33
    assert deltas[0]["ttft_ms_sum"] == 400
    assert deltas[0]["duration_ms_sum"] == 600


def test_normalize_request_account_ids_deduplicates_blank_values() -> None:
    assert _normalize_request_account_ids([" acct_1 ", "", "acct_2", None, "acct_1"]) == ["acct_1", "acct_2"]


def test_normalize_request_token_ids_filters_invalid_values() -> None:
    assert _normalize_request_token_ids([7, "8", 0, -1, None, "abc", 7]) == [7, 8]


def test_build_request_account_costs_stmt_groups_by_account_id() -> None:
    stmt = _build_request_account_costs_stmt(account_ids=["acct_1", "acct_2"])

    sql = str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "sum(gateway_request_logs.estimated_cost_usd)" in sql
    assert "WHERE gateway_request_logs.account_id IN ('acct_1', 'acct_2')" in sql
    assert "GROUP BY gateway_request_logs.account_id" in sql


def test_build_request_token_costs_stmt_groups_by_canonical_token_id() -> None:
    stmt = _build_request_token_costs_stmt(token_ids=[7, 8])

    sql = str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "FROM gateway_request_token_costs" in sql
    assert "gateway_request_token_costs.token_id IN (7, 8)" in sql
    assert "gateway_request_logs" not in sql


def test_request_token_cost_deltas_group_by_token_id() -> None:
    rows = [
        GatewayRequestLog(
            request_id="one",
            endpoint="/v1/responses",
            token_id=7,
            estimated_cost_usd=0.01,
        ),
        GatewayRequestLog(
            request_id="two",
            endpoint="/v1/responses",
            token_id=7,
            estimated_cost_usd=0.02,
        ),
        GatewayRequestLog(
            request_id="three",
            endpoint="/v1/responses",
            token_id=None,
            estimated_cost_usd=0.03,
        ),
    ]

    deltas = _request_token_cost_deltas(rows)

    assert len(deltas) == 1
    assert deltas[0]["token_id"] == 7
    assert deltas[0]["request_count"] == 2
    assert deltas[0]["estimated_cost_usd"] == 0.03


def test_get_request_costs_by_token_uses_token_cost_summary_when_backfilled(monkeypatch) -> None:
    class _FakeResult:
        def __init__(self, rows) -> None:
            self._rows = rows

        def all(self):
            return list(self._rows)

    class _FakeSession:
        def __init__(self) -> None:
            self.executed = []

        def get_bind(self):
            class _Bind:
                class dialect:
                    name = "postgresql"

            return _Bind()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def execute(self, stmt):
            sql = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
            self.executed.append(sql)
            if "FROM codex_tokens" in sql:
                return _FakeResult([(7, None), (8, None)])
            if "gateway_request_token_costs" in sql:
                return _FakeResult([(7, 12, 3.5)])
            return _FakeResult([])

        async def get(self, model, key):
            if model.__tablename__ == "gateway_settings":
                return type("Setting", (), {"value": {"completed": True}})()
            return None

    fake_session = _FakeSession()

    class _Ctx:
        async def __aenter__(self):
            return fake_session

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr("oaix_gateway.request_store.get_read_session", lambda: _Ctx())

    costs = asyncio.run(get_request_costs_by_token([7, 8]))

    assert costs == {7: 3.5, 8: 0.0}
    assert any("gateway_request_token_costs" in sql for sql in fake_session.executed)


def test_backfill_request_token_costs_from_logs_updates_summary_table(monkeypatch) -> None:
    class _FakeResult:
        def __init__(self, rows) -> None:
            self._rows = rows

        def all(self):
            return list(self._rows)

    class _FakeSession:
        def __init__(self) -> None:
            self.executed = []
            self.added = []

        def get_bind(self):
            class _Bind:
                class dialect:
                    name = "postgresql"

            return _Bind()

        def begin(self):
            class _Tx:
                async def __aenter__(self_inner):
                    return None

                async def __aexit__(self_inner, exc_type, exc, tb):
                    return False

            return _Tx()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def execute(self, stmt):
            sql = str(stmt.compile(dialect=postgresql.dialect()))
            self.executed.append(sql)
            if "FROM gateway_request_logs" in sql and "GROUP BY token_id" in sql:
                return _FakeResult([(7, 2, 3.5)])
            return _FakeResult([])

        async def get(self, model, key):
            return None

        def add(self, item):
            self.added.append(item)

    fake_session = _FakeSession()

    class _Ctx:
        async def __aenter__(self):
            return fake_session

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr("oaix_gateway.request_store.get_request_log_session", lambda: _Ctx())

    asyncio.run(backfill_request_token_costs_from_logs())

    assert any("gateway_request_token_costs" in sql for sql in fake_session.executed)
    assert any("gateway_settings" in sql for sql in fake_session.executed)


def test_backfill_request_token_costs_from_logs_skips_when_already_backfilled(monkeypatch) -> None:
    class _FakeSession:
        def __init__(self) -> None:
            self.executed = []

        def get_bind(self):
            class _Bind:
                class dialect:
                    name = "postgresql"

            return _Bind()

        def begin(self):
            class _Tx:
                async def __aenter__(self_inner):
                    return None

                async def __aexit__(self_inner, exc_type, exc, tb):
                    return False

            return _Tx()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def execute(self, stmt):
            sql = str(stmt.compile(dialect=postgresql.dialect()))
            self.executed.append(sql)
            return type("_Result", (), {"all": lambda self_inner: []})()

        async def get(self, model, key):
            if model.__tablename__ == "gateway_settings":
                return type("Setting", (), {"value": {"completed": True}})()
            return None

    fake_session = _FakeSession()

    class _Ctx:
        async def __aenter__(self):
            return fake_session

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr("oaix_gateway.request_store.get_request_log_session", lambda: _Ctx())

    asyncio.run(backfill_request_token_costs_from_logs())

    assert fake_session.executed == []


def test_upsert_request_logs_portable_prefetches_existing_rows_once(monkeypatch) -> None:
    class _FakeScalarResult:
        def __init__(self, rows) -> None:
            self._rows = rows

        def all(self):
            return list(self._rows)

        def first(self):
            return self._rows[0] if self._rows else None

    class _FakeResult:
        def __init__(self, rows) -> None:
            self._rows = rows

        def scalars(self) -> _FakeScalarResult:
            return _FakeScalarResult(self._rows)

    class _FakeTransaction:
        async def __aenter__(self):
            return None

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _FakeSession:
        def __init__(self) -> None:
            self.executed_statements: list[str] = []
            self.added: list[GatewayRequestLog] = []
            self.flush_count = 0
            self._next_id = 101

        def begin(self) -> _FakeTransaction:
            return _FakeTransaction()

        async def execute(self, stmt) -> _FakeResult:
            self.executed_statements.append(
                str(
                    stmt.compile(
                        dialect=postgresql.dialect(),
                        compile_kwargs={"literal_binds": True},
                    )
                )
            )
            return _FakeResult(
                [
                    GatewayRequestLog(
                        id=77,
                        request_id="existing",
                        endpoint="/v1/responses",
                        is_stream=False,
                        attempt_count=1,
                    )
                ]
            )

        def add(self, item: GatewayRequestLog) -> None:
            self.added.append(item)

        async def flush(self) -> None:
            self.flush_count += 1
            for item in self.added:
                if item.id is None:
                    item.id = self._next_id
                    self._next_id += 1

    fake_session = _FakeSession()

    def fake_get_request_log_session():
        class _Ctx:
            async def __aenter__(self_inner):
                return fake_session

            async def __aexit__(self_inner, exc_type, exc, tb):
                return False

        return _Ctx()

    monkeypatch.setattr("oaix_gateway.request_store.get_request_log_session", fake_get_request_log_session)

    base_values = {
        "model": None,
        "model_name": None,
        "client_ip": None,
        "user_agent": None,
        "finished_at": None,
        "first_token_at": None,
        "ttft_ms": None,
        "duration_ms": None,
        "token_id": None,
        "account_id": None,
        "input_tokens": None,
        "output_tokens": None,
        "total_tokens": None,
        "estimated_cost_usd": None,
        "timing_spans": None,
        "error_message": None,
    }

    result = asyncio.run(
        _upsert_request_logs_portable(
            [
                {
                    **base_values,
                    "request_id": "existing",
                    "endpoint": "/v1/responses",
                    "is_stream": False,
                    "status_code": 200,
                    "success": True,
                    "attempt_count": 1,
                    "started_at": datetime(2026, 4, 14, tzinfo=timezone.utc),
                },
                {
                    **base_values,
                    "request_id": "new",
                    "endpoint": "/v1/responses",
                    "is_stream": True,
                    "status_code": 500,
                    "success": False,
                    "attempt_count": 2,
                    "started_at": datetime(2026, 4, 14, tzinfo=timezone.utc),
                },
            ]
        )
    )

    assert len(fake_session.executed_statements) == 1
    assert "WHERE gateway_request_logs.request_id IN ('existing', 'new')" in fake_session.executed_statements[0]
    assert fake_session.flush_count == 1
    assert [item["request_id"] for item in result] == ["existing", "new"]
    assert result[0]["id"] == 77
    assert result[1]["id"] == 101
