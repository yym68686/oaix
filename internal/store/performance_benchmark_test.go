package store

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// Opt-in, isolated-schema workload matching the production history dimensions.
func TestPerformanceSettledHistoryBenchmark(t *testing.T) {
	if os.Getenv("OAIX_RUN_PERFORMANCE_BENCHMARK") != "1" {
		t.Skip("set OAIX_RUN_PERFORMANCE_BENCHMARK=1")
	}
	db, ctx, owner, token := performanceFixture(t)
	var target int64
	if err := db.pool.QueryRow(ctx, `insert into sub2api_sync_targets(name,base_url,admin_key,owner_user_id) values($1,'https://example.invalid','fixture',$2) returning id`, fmt.Sprintf("benchmark-%d", token), owner).Scan(&target); err != nil {
		t.Fatal(err)
	}
	// Simulate an existing database before migration: source rows first, then
	// rebuild the derived account rows with the same online helper.
	if _, err := db.pool.Exec(ctx, `alter table sub2api_usage_snapshots disable trigger user; alter table sub2api_usage_daily_snapshots disable trigger user`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.pool.Exec(ctx, `insert into codex_tokens(refresh_token,owner_user_id) select 'bench-token-'||i,$1 from generate_series(1,5000) i`, owner); err != nil {
		t.Fatal(err)
	}
	if _, err := db.pool.Exec(ctx, `insert into sub2api_sync_mappings(target_id,token_id,remote_account_id,status)
		select $1,id,replace(refresh_token,'bench-token-','')::bigint,'synced' from codex_tokens where refresh_token like 'bench-token-%'`, target); err != nil {
		t.Fatal(err)
	}
	if _, err := db.pool.Exec(ctx, `insert into sub2api_usage_snapshots(target_id,remote_account_id,token_id,through_date,status,synced_at)
		select target_id,remote_account_id,token_id,'2026-07-01','synced',now() from sub2api_sync_mappings where target_id=$1`, target); err != nil {
		t.Fatal(err)
	}
	if _, err := db.pool.Exec(ctx, `insert into sub2api_usage_daily_snapshots(target_id,remote_account_id,token_id,usage_date,account_cost_usd,status,finalized_at,synced_at)
		select $1,i,$2,date '2026-07-01'+d,1,'synced',now(),now() from generate_series(1,5000) i cross join generate_series(1,46) d`, target, token); err != nil {
		t.Fatal(err)
	}
	if _, err := db.pool.Exec(ctx, `alter table sub2api_usage_snapshots enable trigger user; alter table sub2api_usage_daily_snapshots enable trigger user;
		select oaix_refresh_usage_rollup(target_id,remote_account_id) from sub2api_usage_snapshots;
		analyze sub2api_usage_daily_snapshots; analyze sub2api_usage_rollups; analyze sub2api_usage_snapshots; analyze codex_tokens; analyze sub2api_sync_mappings; set work_mem='4MB'`); err != nil {
		t.Fatal(err)
	}
	queries := []struct{ name, sql string }{
		{"old_finalization", `select count(*) from sub2api_usage_snapshots b
		 cross join lateral generate_series(b.through_date+1,date '2026-08-16',interval '1 day') d(day)
		 left join sub2api_usage_daily_snapshots u on u.target_id=b.target_id and u.remote_account_id=b.remote_account_id and u.usage_date=d.day::date
		 where b.target_id=$1 and u.finalized_at is null`},
		{"new_finalization", `select count(*) from sub2api_usage_account_current b
		 cross join lateral oaix_usage_unsettled_dates(b.through_date,date '2026-08-17',b.finalized_dates) d(day)
		 where b.target_id=$1`},
		{"old_costs", `select sum(account_cost_usd)::float8 from sub2api_usage_daily_snapshots where target_id=$1`},
		{"new_costs", `select sum(account_cost_usd)::float8 from sub2api_usage_account_current where target_id=$1`},
	}
	for _, query := range queries {
		if os.Getenv("OAIX_EXPLAIN_PERFORMANCE") == "1" {
			rows, err := db.pool.Query(ctx, "explain (analyze, buffers) "+query.sql, target)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var line string
				if err := rows.Scan(&line); err != nil {
					t.Fatal(err)
				}
				t.Log(query.name + ": " + line)
			}
			rows.Close()
			if rows.Err() != nil {
				t.Fatal(rows.Err())
			}
		}
		var value float64
		start := time.Now()
		for i := 0; i < 3; i++ {
			if err := db.pool.QueryRow(ctx, query.sql, target).Scan(&value); err != nil {
				t.Fatal(err)
			}
		}
		t.Logf("%s: avg=%v value=%v", query.name, time.Since(start)/3, value)
		want := float64(0)
		if query.name == "old_costs" || query.name == "new_costs" {
			want = 230000
		}
		if value != want {
			t.Fatalf("%s result=%v want=%v", query.name, value, want)
		}
	}
	start := time.Now()
	before := time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC)
	items, err := db.ListPendingSub2APIDailyUsageFinalizations(ctx, Sub2APISyncTarget{ID: target, OwnerUserID: owner, Enabled: true, CheckIntervalSeconds: 300}, before, 32)
	if err != nil || len(items) != 0 {
		t.Fatalf("full finalization: items=%v err=%v", items, err)
	}
	t.Logf("full_finalization: elapsed=%v results=%d", time.Since(start), len(items))
	if os.Getenv("OAIX_EXPLAIN_PERFORMANCE") == "1" {
		rows, err := db.pool.Query(ctx, "explain (analyze,buffers) "+pendingSub2APIDailyFinalizationsSQL, target, owner, before, 300, 32)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			t.Log("full_finalization: " + line)
		}
		rows.Close()
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
	}
}
