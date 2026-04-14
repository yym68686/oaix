from datetime import datetime, timezone

from sqlalchemy.dialects import postgresql

from oaix_gateway.request_store import (
    _build_request_account_costs_stmt,
    _build_request_model_analytics_stmt,
    _normalize_request_account_ids,
)


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


def test_normalize_request_account_ids_deduplicates_blank_values() -> None:
    assert _normalize_request_account_ids([" acct_1 ", "", "acct_2", None, "acct_1"]) == ["acct_1", "acct_2"]


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
