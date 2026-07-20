package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
)

func TestSub2APIUsageDailySnapshotComposition(t *testing.T) {
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set OAIX_TEST_DATABASE_URL to run Postgres integration fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := Connect(ctx, config.DatabaseConfig{
		URL:            configURL(dsn),
		MaxConns:       4,
		MinConns:       1,
		ConnectTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Connect returned error: %v", err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("Migrate returned error: %v", err)
	}

	suffix := time.Now().UnixNano()
	var ownerID int64
	if err := db.pool.QueryRow(ctx, `
		insert into platform_users(email, display_name, role, status)
		values($1, 'usage fixture', 'admin', 'active')
		returning id
	`, fmt.Sprintf("sub2api-usage-%d@example.com", suffix)).Scan(&ownerID); err != nil {
		t.Fatalf("insert owner: %v", err)
	}
	var tokenID int64
	if err := db.pool.QueryRow(ctx, `
		insert into codex_tokens(refresh_token, is_active, owner_user_id)
		values($1, true, $2)
		returning id
	`, fmt.Sprintf("sub2api-usage-refresh-%d", suffix), ownerID).Scan(&tokenID); err != nil {
		t.Fatalf("insert token: %v", err)
	}
	var targetID int64
	if err := db.pool.QueryRow(ctx, `
		insert into sub2api_sync_targets(name, base_url, admin_key, owner_user_id)
		values($1, 'https://example.invalid', 'fixture', $2)
		returning id
	`, fmt.Sprintf("usage-fixture-%d", suffix), ownerID).Scan(&targetID); err != nil {
		t.Fatalf("insert target: %v", err)
	}
	const remoteAccountID = int64(424242)
	if _, err := db.pool.Exec(ctx, `
		insert into sub2api_sync_mappings(target_id, token_id, remote_account_id, status)
		values($1, $2, $3, 'synced')
	`, targetID, tokenID, remoteAccountID); err != nil {
		t.Fatalf("insert mapping: %v", err)
	}

	computedAt := time.Now().UTC()
	if err := db.SaveSub2APIUsageSnapshots(ctx, targetID, []Sub2APIUsageSnapshotInput{{
		TokenID: tokenID, RemoteAccountID: remoteAccountID, AccountCostUSD: 12,
		StandardCostUSD: 10, UserCostUSD: 14, TotalRequests: 12, TotalTokens: 120,
		SourceComputedAt: &computedAt,
	}}); err != nil {
		t.Fatalf("save legacy baseline: %v", err)
	}
	days := []time.Time{
		time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC),
	}
	if err := db.SaveSub2APIDailyUsageSnapshots(ctx, targetID, []Sub2APIDailyUsageSnapshotInput{
		{TokenID: tokenID, RemoteAccountID: remoteAccountID, UsageDate: days[0], AccountCostUSD: 2, StandardCostUSD: 1.5, UserCostUSD: 2.5, TotalRequests: 2, TotalTokens: 20, SourceComputedAt: &computedAt, Finalized: true},
		{TokenID: tokenID, RemoteAccountID: remoteAccountID, UsageDate: days[1], AccountCostUSD: 3, StandardCostUSD: 2.5, UserCostUSD: 3.5, TotalRequests: 3, TotalTokens: 30, SourceComputedAt: &computedAt},
	}); err != nil {
		t.Fatalf("save daily usage: %v", err)
	}

	legacy, err := db.Sub2APIUsageByTokens(ctx, []Token{{ID: tokenID}})
	if err != nil {
		t.Fatalf("load legacy usage: %v", err)
	}
	if got := legacy[tokenID].AccountCostUSD; got != 12 {
		t.Fatalf("legacy cost = %v, want 12 without daily double count", got)
	}

	throughDate := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	if err := db.SaveSub2APIUsageSnapshots(ctx, targetID, []Sub2APIUsageSnapshotInput{{
		TokenID: tokenID, RemoteAccountID: remoteAccountID, AccountCostUSD: 10,
		StandardCostUSD: 8, UserCostUSD: 11, TotalRequests: 10, TotalTokens: 100,
		SourceComputedAt: &computedAt, ThroughDate: &throughDate,
	}}); err != nil {
		t.Fatalf("save dated baseline: %v", err)
	}

	combined, err := db.Sub2APIUsageByTokens(ctx, []Token{{ID: tokenID}})
	if err != nil {
		t.Fatalf("load combined usage: %v", err)
	}
	got := combined[tokenID]
	if got.AccountCostUSD != 15 || got.StandardCostUSD != 12 || got.UserCostUSD != 17 || got.TotalRequests != 15 || got.TotalTokens != 150 {
		t.Fatalf("combined usage = %#v", got)
	}
	ownerCosts, err := db.Sub2APIUsageCostsByOwner(ctx, []int64{ownerID})
	if err != nil {
		t.Fatalf("load owner usage: %v", err)
	}
	if ownerCosts[ownerID] != 15 {
		t.Fatalf("owner cost = %v, want 15", ownerCosts[ownerID])
	}
	target := Sub2APISyncTarget{ID: targetID, Enabled: true, OwnerUserID: ownerID, CheckIntervalSeconds: 300}
	baselineMappings, err := db.ListSub2APIUsageBaselineMappings(ctx, target, 32)
	if err != nil {
		t.Fatalf("list baseline mappings: %v", err)
	}
	if len(baselineMappings) != 0 {
		t.Fatalf("baseline mappings after seed = %#v, want none", baselineMappings)
	}
	dueToday, err := db.ListDueSub2APITodayUsageSyncMappings(ctx, target, days[1], 500)
	if err != nil {
		t.Fatalf("list current daily mappings: %v", err)
	}
	if len(dueToday) != 0 {
		t.Fatalf("fresh current daily mappings = %#v, want none", dueToday)
	}
	pendingFinalization, err := db.ListPendingSub2APIDailyUsageFinalizations(ctx, target, days[1].AddDate(0, 0, 1), 8)
	if err != nil {
		t.Fatalf("list pending finalizations: %v", err)
	}
	if len(pendingFinalization) != 1 || !sameUsageDate(pendingFinalization[0].UsageDate, days[1]) {
		t.Fatalf("pending finalizations = %#v, want only %s", pendingFinalization, days[1].Format("2006-01-02"))
	}
	if _, err := db.pool.Exec(ctx, `
		delete from sub2api_usage_daily_snapshots
		where target_id = $1 and remote_account_id = $2 and usage_date = $3::date
	`, targetID, remoteAccountID, days[0]); err != nil {
		t.Fatalf("delete finalized day fixture: %v", err)
	}
	pendingFinalization, err = db.ListPendingSub2APIDailyUsageFinalizations(ctx, target, days[1].AddDate(0, 0, 1), 8)
	if err != nil {
		t.Fatalf("list recovered finalizations: %v", err)
	}
	if len(pendingFinalization) != 2 || !sameUsageDate(pendingFinalization[0].UsageDate, days[0]) || !sameUsageDate(pendingFinalization[1].UsageDate, days[1]) {
		t.Fatalf("gap recovery finalizations = %#v, want %s and %s", pendingFinalization, days[0].Format("2006-01-02"), days[1].Format("2006-01-02"))
	}
}

func sameUsageDate(left time.Time, right time.Time) bool {
	return left.Format("2006-01-02") == right.Format("2006-01-02")
}
