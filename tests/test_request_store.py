from datetime import datetime, timezone

from sqlalchemy.dialects import postgresql

from oaix_gateway.request_store import _build_request_model_analytics_stmt


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
