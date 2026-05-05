from datetime import datetime, timezone

from sqlalchemy.dialects import postgresql

from oaix_gateway.request_store import (
    _build_request_bucket_analytics_stmt,
    _build_request_log_summary_stmt,
    _build_request_account_costs_stmt,
    _build_request_token_costs_stmt,
    _build_request_model_analytics_stmt,
    _normalize_request_account_ids,
    _normalize_request_token_ids,
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

    assert "JOIN codex_tokens ON gateway_request_logs.token_id = codex_tokens.id" in sql
    assert "coalesce(codex_tokens.merged_into_token_id, codex_tokens.id)" in sql
    assert "IN (7, 8)" in sql
