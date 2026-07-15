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
	var disabledAccess *string
	var secretAccess *string
	if err := db.pool.QueryRow(ctx, `select is_active, disabled_at, access_token from codex_tokens where id = $1`, disabledID).Scan(&disabledActive, &disabledAt, &disabledAccess); err != nil {
		t.Fatalf("read disabled token: %v", err)
	}
	if err := db.pool.QueryRow(ctx, `select access_token from token_secrets where token_id = $1`, disabledID).Scan(&secretAccess); err != nil {
		t.Fatalf("read disabled secret: %v", err)
	}
	if disabledActive || disabledAt == nil || disabledAccess != nil || secretAccess != nil {
		t.Fatalf("matching disable was not atomic: active=%v disabled_at=%v token_access=%v secret_access=%v", disabledActive, disabledAt, disabledAccess, secretAccess)
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
