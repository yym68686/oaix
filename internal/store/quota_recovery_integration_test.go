package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
)

func TestPostgresQuotaRecoveryStateFences(t *testing.T) {
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set OAIX_TEST_DATABASE_URL to run Postgres integration fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := Connect(ctx, config.DatabaseConfig{
		URL:            configURL(dsn),
		MaxConns:       8,
		MinConns:       1,
		ConnectTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	var tokenIDs []int64
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = db.pool.Exec(cleanupCtx, `delete from token_state_events where token_id = any($1)`, tokenIDs)
		_, _ = db.pool.Exec(cleanupCtx, `delete from codex_tokens where id = any($1)`, tokenIDs)
	})

	insertToken := func(name string, access string, refresh string, cooldownUntil *time.Time, active bool) (int64, *time.Time) {
		t.Helper()
		var id int64
		var storedCooldown *time.Time
		err := db.pool.QueryRow(ctx, `
			insert into codex_tokens(email, account_id, access_token, refresh_token, is_active, cooldown_until, disabled_at, last_error)
			values ($1, $2, $3, $4, $5, $6, case when $5 then null else now() end, case when $6::timestamptz is null then null else 'upstream usage limit cooldown' end)
			returning id, cooldown_until
		`, name+"-"+suffix+"@example.test", "acct-"+name+"-"+suffix, access, refresh, active, cooldownUntil).Scan(&id, &storedCooldown)
		if err != nil {
			t.Fatalf("insert %s token: %v", name, err)
		}
		if _, err := db.pool.Exec(ctx, `
			insert into token_secrets(token_id, access_token, refresh_token)
			values ($1, $2, $3)
		`, id, access, refresh); err != nil {
			t.Fatalf("insert %s secret: %v", name, err)
		}
		tokenIDs = append(tokenIDs, id)
		return id, storedCooldown
	}

	future := time.Now().UTC().Add(time.Hour)
	conflictID, conflictCooldown := insertToken("conflict", "access-old", "refresh-old", &future, true)
	lastError := "upstream usage limit cooldown"
	conflictFence := ManualProbeStateFence{
		IsActive:      true,
		CooldownUntil: conflictCooldown,
		LastError:     &lastError,
		Credentials: QuotaRecoveryCredentialFence{
			AccessToken:  "access-old",
			RefreshToken: "refresh-old",
			AccountID:    "acct-conflict-" + suffix,
		},
	}
	if _, err := db.pool.Exec(ctx, `update codex_tokens set access_token = 'access-new', refresh_token = 'refresh-new' where id = $1`, conflictID); err != nil {
		t.Fatalf("rotate token credentials: %v", err)
	}
	if _, err := db.pool.Exec(ctx, `update token_secrets set access_token = 'access-new', refresh_token = 'refresh-new' where token_id = $1`, conflictID); err != nil {
		t.Fatalf("rotate secret credentials: %v", err)
	}
	if err := db.MarkManualProbeDisabled(ctx, conflictID, conflictFence, "deactivated_workspace", true, 402, QuotaRecoveryModel); !errors.Is(err, ErrTokenStateChanged) {
		t.Fatalf("stale disable error = %v, want ErrTokenStateChanged", err)
	}
	var conflictActive bool
	var conflictAccess string
	if err := db.pool.QueryRow(ctx, `select is_active, access_token from codex_tokens where id = $1`, conflictID).Scan(&conflictActive, &conflictAccess); err != nil {
		t.Fatalf("read conflicted token: %v", err)
	}
	if !conflictActive || conflictAccess != "access-new" {
		t.Fatalf("stale disable overwrote current credentials: active=%v access=%q", conflictActive, conflictAccess)
	}

	disabledID, disabledCooldown := insertToken("disabled", "access-disable", "refresh-disable", &future, true)
	disabledFence := ManualProbeStateFence{
		IsActive:      true,
		CooldownUntil: disabledCooldown,
		LastError:     &lastError,
		Credentials: QuotaRecoveryCredentialFence{
			AccessToken:  "access-disable",
			RefreshToken: "refresh-disable",
			AccountID:    "acct-disabled-" + suffix,
		},
	}
	if err := db.MarkManualProbeDisabled(ctx, disabledID, disabledFence, "deactivated_workspace", true, 402, QuotaRecoveryModel); err != nil {
		t.Fatalf("matching disable: %v", err)
	}
	var disabledActive bool
	var disabledAt *time.Time
	var disabledCooldownAfter *time.Time
	var disabledAccess *string
	var secretAccess *string
	if err := db.pool.QueryRow(ctx, `select is_active, disabled_at, cooldown_until, access_token from codex_tokens where id = $1`, disabledID).Scan(&disabledActive, &disabledAt, &disabledCooldownAfter, &disabledAccess); err != nil {
		t.Fatalf("read disabled token: %v", err)
	}
	if err := db.pool.QueryRow(ctx, `select access_token from token_secrets where token_id = $1`, disabledID).Scan(&secretAccess); err != nil {
		t.Fatalf("read disabled secret: %v", err)
	}
	if disabledActive || disabledAt == nil || disabledCooldownAfter != nil || disabledAccess != nil || secretAccess != nil {
		t.Fatalf("matching disable was not atomic: active=%v disabled_at=%v cooldown=%v token_access=%v secret_access=%v", disabledActive, disabledAt, disabledCooldownAfter, disabledAccess, secretAccess)
	}

	automaticID, automaticCooldown := insertToken("automatic-disabled", "access-automatic", "refresh-automatic", &future, true)
	var sourceEventID int64
	if err := db.pool.QueryRow(ctx, `
		insert into token_state_events(
			token_id, event_type, reason, cooldown_until, status_code,
			previous_is_active, next_is_active
		)
		values ($1, 'error', 'upstream usage limit cooldown', $2, 429, true, true)
		returning id
	`, automaticID, automaticCooldown).Scan(&sourceEventID); err != nil {
		t.Fatalf("insert automatic recovery source event: %v", err)
	}
	claim, _, err := db.BeginQuotaRecoveryProbe(ctx, QuotaRecoveryCandidate{
		TokenID: automaticID, CooldownUntil: *automaticCooldown, SourceEventID: sourceEventID,
	}, time.Minute, 0)
	if err != nil || claim == nil {
		t.Fatalf("begin automatic disable probe: claim=%+v err=%v", claim, err)
	}
	applied, err := db.DisableQuotaRecovery(ctx, *claim, QuotaRecoveryCredentialFence{
		AccessToken: "access-automatic", RefreshToken: "refresh-automatic", AccountID: "acct-automatic-disabled-" + suffix,
	}, "HTTP 401: token_expired", true, 401, map[string]any{"probe_outcome": "disabled"})
	if err != nil || !applied {
		t.Fatalf("apply automatic recovery disable: applied=%v err=%v", applied, err)
	}
	var automaticActive bool
	var automaticDisabledAt *time.Time
	var automaticCooldownAfter *time.Time
	var automaticAccess *string
	var automaticSecretAccess *string
	var automaticEventType string
	var automaticEventSource string
	var automaticPreviousActive bool
	var automaticNextActive bool
	if err := db.pool.QueryRow(ctx, `
		select t.is_active, t.disabled_at, t.cooldown_until, t.access_token,
		       s.access_token, e.event_type, e.metadata->>'source',
		       e.previous_is_active, e.next_is_active
		from codex_tokens t
		join token_secrets s on s.token_id = t.id
		join lateral (
			select event_type, metadata, previous_is_active, next_is_active
			from token_state_events
			where token_id = t.id
			order by id desc
			limit 1
		) e on true
		where t.id = $1
	`, automaticID).Scan(
		&automaticActive, &automaticDisabledAt, &automaticCooldownAfter, &automaticAccess,
		&automaticSecretAccess, &automaticEventType, &automaticEventSource,
		&automaticPreviousActive, &automaticNextActive,
	); err != nil {
		t.Fatalf("read automatic recovery disable: %v", err)
	}
	if automaticActive || automaticDisabledAt == nil || automaticCooldownAfter != nil || automaticAccess != nil || automaticSecretAccess != nil ||
		automaticEventType != QuotaRecoveryDisabledEvent || automaticEventSource != "automatic_quota_recovery" || !automaticPreviousActive || automaticNextActive {
		t.Fatalf("automatic recovery disable was not atomic or auditable: active=%v disabled_at=%v cooldown=%v token_access=%v secret_access=%v event=%q source=%q previous=%v next=%v",
			automaticActive, automaticDisabledAt, automaticCooldownAfter, automaticAccess, automaticSecretAccess,
			automaticEventType, automaticEventSource, automaticPreviousActive, automaticNextActive)
	}

	probeID, _ := insertToken("probe-reactivate", "access-probe", "refresh-probe", nil, false)
	var probeDisabledAt *time.Time
	if err := db.pool.QueryRow(ctx, `select disabled_at from codex_tokens where id = $1`, probeID).Scan(&probeDisabledAt); err != nil {
		t.Fatalf("read probe token state: %v", err)
	}
	probeFence := ManualProbeStateFence{
		IsActive:   false,
		DisabledAt: probeDisabledAt,
		Credentials: QuotaRecoveryCredentialFence{
			AccessToken:  "access-probe",
			RefreshToken: "refresh-probe",
			AccountID:    "acct-probe-reactivate-" + suffix,
		},
	}
	if err := db.MarkManualProbeSuccess(ctx, probeID, probeFence, 200, "gpt-5.4-mini"); err != nil {
		t.Fatalf("manual probe success: %v", err)
	}
	var probeActive bool
	var probeEventType string
	var probeModel string
	var probeStatus int
	var probePreviousActive bool
	var probeNextActive bool
	var probeSource string
	if err := db.pool.QueryRow(ctx, `
		select t.is_active, e.event_type, e.model, e.status_code,
		       e.previous_is_active, e.next_is_active, e.metadata->>'source'
		from codex_tokens t
		join lateral (
			select event_type, model, status_code, previous_is_active, next_is_active, metadata
			from token_state_events
			where token_id = t.id
			order by id desc
			limit 1
		) e on true
		where t.id = $1
	`, probeID).Scan(&probeActive, &probeEventType, &probeModel, &probeStatus, &probePreviousActive, &probeNextActive, &probeSource); err != nil {
		t.Fatalf("read manual probe event: %v", err)
	}
	if !probeActive || probeEventType != "reactivated_by_manual_probe" || probeModel != "gpt-5.4-mini" || probeStatus != 200 || probePreviousActive || !probeNextActive || probeSource != "manual_probe" {
		t.Fatalf("unexpected manual probe audit: active=%v type=%q model=%q status=%d previous=%v next=%v source=%q", probeActive, probeEventType, probeModel, probeStatus, probePreviousActive, probeNextActive, probeSource)
	}

	delayedDisabledID, _ := insertToken("delayed-disabled", "access-delayed-disabled", "refresh-delayed-disabled", nil, false)
	if _, err := db.pool.Exec(ctx, `
		insert into token_runtime_state(token_id, disabled_reason, failure_streak)
		values ($1, 'manual disable', 1)
	`, delayedDisabledID); err != nil {
		t.Fatalf("insert disabled runtime state: %v", err)
	}
	if err := db.MarkTokenSuccess(ctx, delayedDisabledID); err != nil {
		t.Fatalf("record delayed success for disabled token: %v", err)
	}
	var stillDisabled bool
	var disabledReason *string
	if err := db.pool.QueryRow(ctx, `
		select not t.is_active and t.disabled_at is not null, r.disabled_reason
		from codex_tokens t join token_runtime_state r on r.token_id = t.id
		where t.id = $1
	`, delayedDisabledID).Scan(&stillDisabled, &disabledReason); err != nil {
		t.Fatalf("read delayed-disabled state: %v", err)
	}
	if !stillDisabled || disabledReason == nil || *disabledReason != "manual disable" {
		t.Fatalf("delayed success undid manual disable: disabled=%v reason=%v", stillDisabled, disabledReason)
	}

	delayedFailureID, _ := insertToken("delayed-failure-after-disable", "access-delayed-failure", "refresh-delayed-failure", &future, true)
	permanentReason := "terminal upstream status 403: inactive selected workspace member"
	if err := db.MarkTokenError(ctx, delayedFailureID, permanentReason, true, nil); err != nil {
		t.Fatalf("permanently disable token: %v", err)
	}
	staleCooldown := time.Now().UTC().Add(5 * time.Second)
	if err := db.MarkTokenError(ctx, delayedFailureID, "stale non-terminal auth failure", false, &staleCooldown); err != nil {
		t.Fatalf("record delayed token failure: %v", err)
	}
	var delayedFailureActive bool
	var delayedFailureDisabledAt *time.Time
	var delayedFailureCooldown *time.Time
	var delayedFailureReason string
	var delayedRuntimeCooldown *time.Time
	var delayedRuntimeReason string
	if err := db.pool.QueryRow(ctx, `
		select t.is_active, t.disabled_at, t.cooldown_until, t.last_error, r.cooldown_until, r.disabled_reason
		from codex_tokens t join token_runtime_state r on r.token_id = t.id
		where t.id = $1
	`, delayedFailureID).Scan(&delayedFailureActive, &delayedFailureDisabledAt, &delayedFailureCooldown, &delayedFailureReason, &delayedRuntimeCooldown, &delayedRuntimeReason); err != nil {
		t.Fatalf("read delayed-failure token state: %v", err)
	}
	if delayedFailureActive || delayedFailureDisabledAt == nil || delayedFailureCooldown != nil || delayedRuntimeCooldown != nil || delayedFailureReason != permanentReason || delayedRuntimeReason != permanentReason {
		t.Fatalf("delayed failure overwrote permanent disable: active=%v disabled_at=%v token_cooldown=%v runtime_cooldown=%v reason=%q runtime_reason=%q",
			delayedFailureActive, delayedFailureDisabledAt, delayedFailureCooldown, delayedRuntimeCooldown, delayedFailureReason, delayedRuntimeReason)
	}

	newCooldown := time.Now().UTC().Add(2 * time.Hour)
	delayedCoolingID, storedNewCooldown := insertToken("delayed-cooling", "access-delayed-cooling", "refresh-delayed-cooling", &newCooldown, true)
	if _, err := db.pool.Exec(ctx, `
		insert into token_runtime_state(token_id, cooldown_until, disabled_reason, failure_streak)
		values ($1, $2, 'new cooldown', 1)
	`, delayedCoolingID, storedNewCooldown); err != nil {
		t.Fatalf("insert cooling runtime state: %v", err)
	}
	if err := db.MarkTokenSuccess(ctx, delayedCoolingID); err != nil {
		t.Fatalf("record delayed success for cooling token: %v", err)
	}
	var tokenCooldown *time.Time
	var runtimeCooldown *time.Time
	if err := db.pool.QueryRow(ctx, `
		select t.cooldown_until, r.cooldown_until
		from codex_tokens t join token_runtime_state r on r.token_id = t.id
		where t.id = $1
	`, delayedCoolingID).Scan(&tokenCooldown, &runtimeCooldown); err != nil {
		t.Fatalf("read delayed-cooling state: %v", err)
	}
	if tokenCooldown == nil || runtimeCooldown == nil || !tokenCooldown.Equal(*storedNewCooldown) || !runtimeCooldown.Equal(*storedNewCooldown) {
		t.Fatalf("delayed success undid new cooldown: token=%v runtime=%v want=%v", tokenCooldown, runtimeCooldown, storedNewCooldown)
	}
}
