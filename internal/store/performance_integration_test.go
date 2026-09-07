package store

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/yym68686/oaix/internal/config"
)

func performanceFixture(t *testing.T) (*Store, context.Context, int64, int64) {
	t.Helper()
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set OAIX_TEST_DATABASE_URL for Postgres integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("performance_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `create schema `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, `drop schema `+pgx.Identifier{schema}.Sanitize()+` cascade`)
		_ = admin.Close(cleanupCtx)
	})
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	dsn = u.String()
	db, err := Connect(ctx, config.DatabaseConfig{URL: configURL(dsn), MaxConns: 8, MinConns: 1, ConnectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	var owner, token int64
	if err := db.pool.QueryRow(ctx, `insert into platform_users(email,display_name,role,status) values($1,'performance fixture','user','active') returning id`, fmt.Sprintf("perf-%d@example.invalid", time.Now().UnixNano())).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if err := db.pool.QueryRow(ctx, `insert into codex_tokens(refresh_token,access_token,owner_user_id) values($1,'fixture',$2) returning id`, fmt.Sprintf("perf-%d", owner), owner).Scan(&token); err != nil {
		t.Fatal(err)
	}
	return db, ctx, owner, token
}

func TestCurrentTokenCostsExactDeltasAndRetention(t *testing.T) {
	db, ctx, owner, token := performanceFixture(t)
	// Ensure the fixture account's entire lifetime is retained.
	if _, err := db.pool.Exec(ctx, `insert into gateway_request_logs(request_id,endpoint,started_at) values($1,'fixture',now()-interval '2 days')`, fmt.Sprintf("sentinel-%d", token)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReconcileRecordedTokenCosts(ctx); err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("cost-%d", token)
	finished := time.Now().UTC()
	log := RequestLog{RequestID: id, OwnerUserID: &owner, TokenID: &token, Endpoint: "fixture", StartedAt: finished, FinishedAt: &finished, EstimatedCostUSD: float64Ptr(2)}
	if err := db.UpsertRequestLogs(ctx, []RequestLog{log}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AggregateRequestHourlyStats(ctx); err != nil {
		t.Fatal(err)
	}
	if changed, err := db.initializeCurrentTokenCost(ctx, token); err != nil || !changed {
		t.Fatalf("seed: changed=%v err=%v", changed, err)
	}
	check := func(want float64) {
		t.Helper()
		got, err := db.TokenObservedCostsCurrentSnapshot(ctx, []Token{{ID: token}})
		if err != nil {
			t.Fatal(err)
		}
		assertApprox(t, valueOrZero(got[token]), want)
	}
	check(2)
	// A late update to an already drained ID must apply exactly once.
	log.EstimatedCostUSD = float64Ptr(7)
	late := finished.Add(time.Second)
	log.FinishedAt = &late
	for i := 0; i < 2; i++ {
		if err := db.UpsertRequestLogs(ctx, []RequestLog{log}); err != nil {
			t.Fatal(err)
		}
		check(7)
	}
	// Repricing and rollback exercise writers outside the Go upsert helper.
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `update gateway_request_logs set estimated_cost_usd=11 where request_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	check(7)
	if _, err := db.pool.Exec(ctx, `update gateway_request_logs set estimated_cost_usd=3.25 where request_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	check(3.25)
	if _, err := db.pool.Exec(ctx, `delete from gateway_request_logs where request_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	check(3.25) // Retention must not erase lifetime spend.
}

func TestCurrentTokenCostBackfillStartsAndCompletes(t *testing.T) {
	db, ctx, _, token := performanceFixture(t)
	if _, err := db.ReconcileRecordedTokenCosts(ctx); err != nil {
		t.Fatal(err)
	}
	cursor, count, err := db.BackfillCurrentTokenCosts(ctx, 0)
	if err != nil || count != 1 || cursor != token {
		t.Fatalf("first batch cursor=%d count=%d err=%v", cursor, count, err)
	}
	cursor, count, err = db.BackfillCurrentTokenCosts(ctx, cursor)
	if err != nil || count != 0 || cursor != 0 {
		t.Fatalf("completed cursor=%d count=%d err=%v", cursor, count, err)
	}
}

func TestCurrentTokenCostsConcurrentInitialization(t *testing.T) {
	db, ctx, _, token := performanceFixture(t)
	if _, err := db.pool.Exec(ctx, `insert into gateway_request_logs(request_id,endpoint,started_at) values($1,'fixture',now()-interval '2 days')`, fmt.Sprintf("concurrent-sentinel-%d", token)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				_, err := db.pool.Exec(ctx, `insert into gateway_request_logs(request_id,endpoint,started_at,token_id,estimated_cost_usd) values($1,'fixture',now(),$2,0.01)`, fmt.Sprintf("concurrent-%d-%d-%d", token, worker, i), token)
				if err != nil {
					errs <- err
					return
				}
			}
		}(worker)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if _, err := db.initializeCurrentTokenCost(ctx, token); err != nil {
				errs <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if _, err := db.initializeCurrentTokenCost(ctx, token); err != nil {
		t.Fatal(err)
	}
	got, err := db.TokenObservedCostsCurrentSnapshot(ctx, []Token{{ID: token}})
	if err != nil {
		t.Fatal(err)
	}
	assertApprox(t, valueOrZero(got[token]), 1)
}

func TestCurrentTokenCostsPreserveOlderHistoryAndCanonicalMerge(t *testing.T) {
	db, ctx, owner, token := performanceFixture(t)
	if _, err := db.pool.Exec(ctx, `update codex_tokens set created_at='2000-01-01' where id=$1`, token); err != nil {
		t.Fatal(err)
	}
	if _, err := db.pool.Exec(ctx, `insert into gateway_request_token_costs(token_id,estimated_cost_usd) values($1,100)`, token); err != nil {
		t.Fatal(err)
	}
	if _, err := db.pool.Exec(ctx, `insert into gateway_request_logs(request_id,endpoint,started_at,token_id,estimated_cost_usd) values($1,'fixture',now(),$2,2)`, fmt.Sprintf("history-%d", token), token); err != nil {
		t.Fatal(err)
	}
	if _, err := db.initializeCurrentTokenCost(ctx, token); err != nil {
		t.Fatal(err)
	}
	var child int64
	if err := db.pool.QueryRow(ctx, `insert into codex_tokens(refresh_token,owner_user_id,merged_into_token_id) values($1,$2,$3) returning id`, fmt.Sprintf("merged-%d", token), owner, token).Scan(&child); err != nil {
		t.Fatal(err)
	}
	if _, err := db.pool.Exec(ctx, `insert into gateway_request_logs(request_id,endpoint,started_at,token_id,estimated_cost_usd) values($1,'fixture',now(),$2,3)`, fmt.Sprintf("history-%d", child), child); err != nil {
		t.Fatal(err)
	}
	if _, err := db.initializeCurrentTokenCost(ctx, child); err != nil {
		t.Fatal(err)
	}
	got, err := db.TokenObservedCostsCurrentSnapshot(ctx, []Token{{ID: token}, {ID: token}})
	if err != nil {
		t.Fatal(err)
	}
	assertApprox(t, valueOrZero(got[token]), 105)
}

func TestTokenListMetadataMatchesExistingFilters(t *testing.T) {
	db, ctx, owner, token := performanceFixture(t)
	if _, err := db.pool.Exec(ctx, `update codex_tokens set plan_type=' Pro ',is_active=true where id=$1`, token); err != nil {
		t.Fatal(err)
	}
	if _, err := db.pool.Exec(ctx, `insert into codex_tokens(refresh_token,owner_user_id,plan_type,is_active,cooldown_until)
		select $1||i::text,$2,case when i%2=0 then 'plus' else null end,i%3<>0,case when i%4=0 then now()+interval '1 hour' end from generate_series(1,12) i`, fmt.Sprintf("facets-%d-", owner), owner); err != nil {
		t.Fatal(err)
	}
	for _, scope := range []ResourceScope{AllResources(), OwnerResources(owner)} {
		for _, opts := range []TokenListOptions{{}, {Status: "available"}, {Status: "disabled"}, {Status: "cooling"}, {Query: "no-match"}, {Plan: "pro"}, {OwnerUserID: owner}} {
			at := time.Now().UTC()
			counts, plans, err := db.TokenListMetadataScoped(ctx, scope, opts, at)
			if err != nil {
				t.Fatal(err)
			}
			wantCounts, err := db.TokenCountsScopedAt(ctx, scope, at)
			if err != nil {
				t.Fatal(err)
			}
			wantPlans, err := db.TokenPlanCountsScoped(ctx, scope, opts)
			if err != nil {
				t.Fatal(err)
			}
			if counts != wantCounts || !reflect.DeepEqual(plans, wantPlans) {
				t.Fatalf("opts=%+v got=%+v/%+v want=%+v/%+v", opts, counts, plans, wantCounts, wantPlans)
			}
		}
	}
}

func TestSub2APIRollupConcurrentChangesAndFreshness(t *testing.T) {
	db, ctx, owner, token := performanceFixture(t)
	var target int64
	if err := db.pool.QueryRow(ctx, `insert into sub2api_sync_targets(name,base_url,admin_key,owner_user_id) values($1,'https://example.invalid','fixture',$2) returning id`, fmt.Sprintf("concurrent-%d", token), owner).Scan(&target); err != nil {
		t.Fatal(err)
	}
	day := time.Now().UTC().Truncate(24 * time.Hour).Add(-48 * time.Hour)
	if err := db.SaveSub2APIUsageSnapshots(ctx, target, []Sub2APIUsageSnapshotInput{{TokenID: token, RemoteAccountID: 1, ThroughDate: &day, AccountCostUSD: 100}}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for writer := 1; writer <= 2; writer++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			for i := 1; i <= 20; i++ {
				if err := db.SaveSub2APIDailyUsageSnapshots(ctx, target, []Sub2APIDailyUsageSnapshotInput{{TokenID: token, RemoteAccountID: 1, UsageDate: day.AddDate(0, 0, writer), AccountCostUSD: float64(i), TotalRequests: int64(i)}}); err != nil {
					errs <- err
					return
				}
			}
		}(writer)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if _, err := db.BackfillSub2APIUsageRollups(ctx); err != nil {
				errs <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	got, err := db.Sub2APIUsageByTokens(ctx, []Token{{ID: token}})
	if err != nil {
		t.Fatal(err)
	}
	if got[token].AccountCostUSD != 140 || got[token].TotalRequests != 40 {
		t.Fatalf("concurrent rollup lost a change: %+v", got[token])
	}
	if _, err := db.BackfillSub2APIUsageRollups(ctx); err != nil {
		t.Fatal(err)
	}
	rolledUp, err := db.Sub2APIUsageByTokens(ctx, []Token{{ID: token}})
	if err != nil || rolledUp[token].AccountCostUSD != 140 || rolledUp[token].TotalRequests != 40 {
		t.Fatalf("rollup differs after concurrent writes: %+v %v", rolledUp[token], err)
	}
	// The same values advance freshness without rewriting the indexed daily row.
	var before time.Time
	if err := db.pool.QueryRow(ctx, `select updated_at from sub2api_usage_daily_snapshots where target_id=$1 and usage_date=$2`, target, day.AddDate(0, 0, 2)).Scan(&before); err != nil {
		t.Fatal(err)
	}
	computed := time.Now().UTC()
	if err := db.SaveSub2APIDailyUsageSnapshots(ctx, target, []Sub2APIDailyUsageSnapshotInput{{TokenID: token, RemoteAccountID: 1, UsageDate: day.AddDate(0, 0, 2), AccountCostUSD: 20, TotalRequests: 20, SourceComputedAt: &computed}}); err != nil {
		t.Fatal(err)
	}
	var after, synced, source time.Time
	if err := db.pool.QueryRow(ctx, `select d.updated_at,c.synced_at,c.source_computed_at from sub2api_usage_daily_snapshots d join sub2api_usage_daily_current c using(target_id,remote_account_id,usage_date) where d.target_id=$1 and d.usage_date=$2`, target, day.AddDate(0, 0, 2)).Scan(&after, &synced, &source); err != nil {
		t.Fatal(err)
	}
	if !after.Equal(before) || !synced.After(before) || source.Sub(computed).Abs() > time.Microsecond {
		t.Fatalf("freshness/value separation failed: before=%v after=%v synced=%v computed=%v", before, after, synced, source)
	}
	// Exact fallback during an interrupted initial backfill must be identical.
	if _, err := db.pool.Exec(ctx, `update sub2api_usage_rollups set ready=false where target_id=$1`, target); err != nil {
		t.Fatal(err)
	}
	fallback, err := db.Sub2APIUsageByTokens(ctx, []Token{{ID: token}})
	if err != nil {
		t.Fatal(err)
	}
	if fallback[token].AccountCostUSD != got[token].AccountCostUSD || fallback[token].TotalRequests != got[token].TotalRequests {
		t.Fatalf("fallback changed totals: before=%+v after=%+v", got[token], fallback[token])
	}
}
