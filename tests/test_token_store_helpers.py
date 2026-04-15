from datetime import datetime, timedelta, timezone

from oaix_gateway.database import CodexToken
from oaix_gateway.token_store import _merge_duplicate_token_rows


def test_merge_duplicate_token_rows_marks_shadow_and_preserves_canonical_history() -> None:
    now = datetime(2026, 4, 15, 10, 0, tzinfo=timezone.utc)
    older = CodexToken(
        id=24,
        account_id="acc_shared",
        refresh_token="rt_old",
        refresh_token_aliases=["rt_seed"],
        token_type="codex",
        is_active=True,
        access_token="access_old",
        expires_at=now + timedelta(hours=1),
        cooldown_until=now + timedelta(hours=2),
        last_error="额度已用尽，等待窗口重置",
        last_refresh_at=now - timedelta(hours=3),
        last_used_at=now - timedelta(minutes=15),
        updated_at=now - timedelta(hours=2),
        source_file="/tmp/older.json",
    )
    newer = CodexToken(
        id=25,
        email="shared@example.com",
        account_id="acc_shared",
        refresh_token="rt_new",
        refresh_token_aliases=["rt_seed", "rt_new"],
        token_type="codex",
        is_active=True,
        access_token=None,
        expires_at=None,
        cooldown_until=None,
        last_error=None,
        last_refresh_at=now - timedelta(minutes=30),
        last_used_at=now - timedelta(minutes=5),
        updated_at=now - timedelta(minutes=1),
    )

    canonical, shadows = _merge_duplicate_token_rows([older, newer], now=now)

    assert canonical.id == 25
    assert canonical.merged_into_token_id is None
    assert canonical.email == "shared@example.com"
    assert canonical.source_file == "/tmp/older.json"
    assert canonical.last_error == "额度已用尽，等待窗口重置"
    assert canonical.cooldown_until == now + timedelta(hours=2)
    assert canonical.last_used_at == now - timedelta(minutes=5)
    assert canonical.refresh_token_aliases == ["rt_seed", "rt_old", "rt_new"]

    assert [shadow.id for shadow in shadows] == [24]
    shadow = shadows[0]
    assert shadow.merged_into_token_id == 25
    assert shadow.is_active is False
    assert shadow.access_token is None
    assert shadow.expires_at is None
    assert shadow.cooldown_until is None
    assert shadow.last_error == "Merged into token #25 due to duplicate refresh token history"


def test_merge_duplicate_token_rows_prefers_latest_state_even_if_it_is_inactive() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    older = CodexToken(
        id=10,
        account_id="acc_shared",
        refresh_token="rt_old",
        refresh_token_aliases=["rt_seed"],
        token_type="codex",
        is_active=True,
        access_token="access_old",
        expires_at=now + timedelta(hours=1),
        last_error=None,
        last_refresh_at=now - timedelta(hours=3),
        updated_at=now - timedelta(hours=3),
    )
    newer = CodexToken(
        id=11,
        account_id="acc_shared",
        refresh_token="rt_new",
        refresh_token_aliases=["rt_seed", "rt_new"],
        token_type="codex",
        is_active=False,
        access_token=None,
        expires_at=None,
        cooldown_until=now + timedelta(minutes=30),
        last_error="refresh_token_reused",
        last_refresh_at=now - timedelta(minutes=10),
        updated_at=now - timedelta(minutes=1),
    )

    canonical, shadows = _merge_duplicate_token_rows([older, newer], now=now)

    assert canonical.id == 11
    assert canonical.is_active is False
    assert canonical.cooldown_until is None
    assert canonical.last_error == "refresh_token_reused"
    assert [shadow.id for shadow in shadows] == [10]
