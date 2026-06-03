from datetime import datetime, timezone
from types import SimpleNamespace

from oaix_gateway.database import CodexToken
from oaix_gateway.token_import_jobs import _build_token_import_batch_summary


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
