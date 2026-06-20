package tokens

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

type fakeSource struct {
	tokens      []store.Token
	err         error
	allCalls    atomic.Int64
	scopedCalls int
	scopedOwner int64
}

const testMaxAge = time.Second
const testRefreshInterval = time.Second

func (f *fakeSource) ListAvailableTokens(context.Context) ([]store.Token, error) {
	f.allCalls.Add(1)
	if f.err != nil {
		return nil, f.err
	}
	out := make([]store.Token, len(f.tokens))
	copy(out, f.tokens)
	return out, nil
}

func (f *fakeSource) ListAvailableTokensScoped(_ context.Context, scope store.ResourceScope) ([]store.Token, error) {
	f.scopedCalls++
	if scope.OwnerUserID != nil {
		f.scopedOwner = *scope.OwnerUserID
	}
	if f.err != nil {
		return nil, f.err
	}
	out := make([]store.Token, 0, len(f.tokens))
	for _, token := range f.tokens {
		if scope.AllowAll || (scope.OwnerUserID != nil && token.OwnerUserID == *scope.OwnerUserID) {
			out = append(out, token)
		}
	}
	return out, nil
}

func (f *fakeSource) TouchTokens(context.Context, []int64, time.Time) error { return nil }
func (f *fakeSource) MarkTokenSuccess(context.Context, int64) error         { return nil }
func (f *fakeSource) MarkTokenError(context.Context, int64, string, bool, *time.Time) error {
	return nil
}

func TestManagerClaimRelease(t *testing.T) {
	manager := NewManager(&fakeSource{tokens: makeTokens(3)}, slog.Default(), time.Second, time.Second, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	claim, err := manager.Claim(context.Background(), Intent{})
	if err != nil {
		t.Fatalf("Claim returned error: %v", err)
	}
	if claim.TokenID() == 0 {
		t.Fatal("claim returned empty token id")
	}
	if claim.Token.Active.Load() != 1 {
		t.Fatalf("active count = %d, want 1", claim.Token.Active.Load())
	}
	claim.Release()
	if claim.Token.Active.Load() != 0 {
		t.Fatalf("active count after release = %d, want 0", claim.Token.Active.Load())
	}
}

func TestSnapshotByIDLookup(t *testing.T) {
	manager := NewManager(&fakeSource{tokens: makeTokens(10_000)}, slog.Default(), time.Second, time.Second, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	snapshot := manager.Snapshot()
	token := snapshot.ByID[9999]
	if token == nil {
		t.Fatal("token 9999 not found")
	}
	if token.Token.AccessToken != "access-9999" {
		t.Fatalf("unexpected token: %q", token.Token.AccessToken)
	}
}

func TestSnapshotIndexes(t *testing.T) {
	pro := "pro"
	rows := makeTokens(2)
	rows[0].PlanType = &pro
	manager := NewManager(&fakeSource{tokens: rows}, slog.Default(), time.Second, time.Second, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	snapshot := manager.Snapshot()
	if len(snapshot.ByScope["default"]) != 2 {
		t.Fatalf("default scope index size = %d", len(snapshot.ByScope["default"]))
	}
	if len(snapshot.ByModel["*"]) != 2 {
		t.Fatalf("model wildcard index size = %d", len(snapshot.ByModel["*"]))
	}
	if len(snapshot.ByPlan["pro"]) != 1 {
		t.Fatalf("plan index size = %d", len(snapshot.ByPlan["pro"]))
	}
}

func TestManagerActiveStreamCapCanUpdateAtRuntime(t *testing.T) {
	manager := NewManager(&fakeSource{tokens: makeTokens(1)}, slog.Default(), time.Second, time.Second, 4)
	if manager.ActiveStreamCap() != 4 {
		t.Fatalf("ActiveStreamCap = %d, want 4", manager.ActiveStreamCap())
	}
	manager.SetActiveStreamCap(10)
	if manager.ActiveStreamCap() != 10 {
		t.Fatalf("ActiveStreamCap after update = %d, want 10", manager.ActiveStreamCap())
	}
	if stats := manager.Stats(); stats.ActiveCap != 10 {
		t.Fatalf("Stats.ActiveCap = %d, want 10", stats.ActiveCap)
	}
}

func TestManagerStatsIncludesOwnerSnapshotActiveStreams(t *testing.T) {
	rows := makeTokens(2)
	rows[0].OwnerUserID = 10
	rows[1].OwnerUserID = 10
	manager := NewManager(&fakeSource{tokens: rows}, nil, time.Second, time.Second, 10)

	claim, err := manager.Claim(context.Background(), Intent{OwnerUserID: 10})
	if err != nil {
		t.Fatalf("Claim returned error: %v", err)
	}
	defer claim.Release()

	if stats := manager.Stats(); stats.ActiveStreams != 1 {
		t.Fatalf("Stats.ActiveStreams = %d, want 1", stats.ActiveStreams)
	}
}

func TestManagerClaimLoadsOwnerSnapshotLazily(t *testing.T) {
	rows := makeTokens(4)
	rows[0].OwnerUserID = 10
	rows[1].OwnerUserID = 10
	rows[2].OwnerUserID = 20
	rows[3].OwnerUserID = 20
	source := &fakeSource{tokens: rows}
	manager := NewManager(source, slog.Default(), time.Second, time.Second, 1)

	claim, err := manager.Claim(context.Background(), Intent{OwnerUserID: 20})
	if err != nil {
		t.Fatalf("Claim returned error: %v", err)
	}
	defer claim.Release()
	if claim.Token.Token.OwnerUserID != 20 {
		t.Fatalf("claimed owner = %d, want 20", claim.Token.Token.OwnerUserID)
	}
	if source.scopedCalls == 0 || source.scopedOwner != 20 {
		t.Fatalf("scoped source calls=%d owner=%d, want owner scoped call for 20", source.scopedCalls, source.scopedOwner)
	}
	if got := len(manager.SnapshotForOwner(20).Ready); got != 2 {
		t.Fatalf("owner snapshot ready = %d, want 2", got)
	}
}

func TestManagerMarketplaceClaimUsesGlobalSnapshot(t *testing.T) {
	rows := makeTokens(3)
	rows[0].OwnerUserID = 1
	rows[0].ShareEnabled = false
	rows[1].OwnerUserID = 63910
	rows[1].ShareEnabled = false
	rows[2].OwnerUserID = 63911
	rows[2].ShareEnabled = true
	rows[2].ShareStatus = "active"
	source := &fakeSource{tokens: rows}
	manager := NewManager(source, slog.Default(), time.Second, time.Second, 1)
	manager.selector = FillFirstSelector{}
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	claim, err := manager.Claim(context.Background(), Intent{
		OwnerUserID:   1,
		SelectionMode: "marketplace",
	})
	if err != nil {
		t.Fatalf("Claim returned error: %v", err)
	}
	if claim.Token.Token.ID != 1 {
		t.Fatalf("claimed token = %d, want owner private token 1", claim.Token.Token.ID)
	}
	if claim.Token.Token.OwnerUserID != 1 {
		t.Fatalf("claimed owner = %d, want 1", claim.Token.Token.OwnerUserID)
	}
	claim.Release()

	claim, err = manager.Claim(context.Background(), Intent{
		OwnerUserID:        1,
		SelectionMode:      "marketplace",
		ExcludeOwnerUserID: 1,
	})
	if err != nil {
		t.Fatalf("Claim with owner exclusion returned error: %v", err)
	}
	defer claim.Release()
	if claim.Token.Token.ID != 3 {
		t.Fatalf("claimed token = %d, want shared non-owner token 3", claim.Token.Token.ID)
	}
	if source.scopedCalls != 0 {
		t.Fatalf("scoped source calls = %d, want marketplace to use global snapshot", source.scopedCalls)
	}
	if got := len(manager.SnapshotForOwner(1).Ready); got != 0 {
		t.Fatalf("owner snapshot ready = %d, want no owner snapshot for marketplace", got)
	}
}

func TestManagerRefreshActiveOwnersOnlyRecentOwners(t *testing.T) {
	rows := makeTokens(4)
	rows[0].OwnerUserID = 10
	rows[1].OwnerUserID = 10
	rows[2].OwnerUserID = 20
	rows[3].OwnerUserID = 20
	source := &fakeSource{tokens: rows}
	manager := NewManager(source, nil, time.Second, time.Second, 1)

	claim, err := manager.Claim(context.Background(), Intent{OwnerUserID: 10})
	if err != nil {
		t.Fatalf("Claim returned error: %v", err)
	}
	claim.Release()
	source.scopedCalls = 0
	refreshed, err := manager.RefreshActiveOwners(context.Background())
	if err != nil {
		t.Fatalf("RefreshActiveOwners returned error: %v", err)
	}
	if refreshed != 1 {
		t.Fatalf("refreshed owners = %d, want 1", refreshed)
	}
	if source.scopedCalls != 1 || source.scopedOwner != 10 {
		t.Fatalf("scoped source calls=%d owner=%d, want one refresh for owner 10", source.scopedCalls, source.scopedOwner)
	}
}

func TestManagerStartRefreshesGlobalSnapshotPeriodically(t *testing.T) {
	source := &fakeSource{tokens: makeTokens(2)}
	manager := NewManager(source, nil, time.Second, 10*time.Millisecond, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager.Start(ctx)
	defer manager.Stop()

	deadline := time.After(250 * time.Millisecond)
	for source.allCalls.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("global snapshot refresh calls = %d, want at least 2", source.allCalls.Load())
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func TestClaimRejectsStaleSnapshotWhenRefreshFails(t *testing.T) {
	manager := NewManager(&fakeSource{tokens: makeTokens(1)}, slog.Default(), time.Nanosecond, time.Second, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	time.Sleep(time.Millisecond)
	manager.source = &fakeSource{err: ErrNoToken}
	if _, err := manager.Claim(context.Background(), Intent{}); err != ErrSnapshotStale {
		t.Fatalf("Claim error = %v, want %v", err, ErrSnapshotStale)
	}
}

func BenchmarkSnapshotByIDLookup10000(b *testing.B) {
	manager := NewManager(&fakeSource{tokens: makeTokens(10_000)}, slog.Default(), time.Second, time.Second, 8)
	if err := manager.Refresh(context.Background()); err != nil {
		b.Fatalf("Refresh returned error: %v", err)
	}
	snapshot := manager.Snapshot()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if snapshot.ByID[int64((i%10_000)+1)] == nil {
			b.Fatal("missing token")
		}
	}
}

func BenchmarkLinearTokenLookup10000(b *testing.B) {
	rows := makeTokens(10_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := int64((i % 10_000) + 1)
		var found *store.Token
		for index := range rows {
			if rows[index].ID == id {
				found = &rows[index]
				break
			}
		}
		if found == nil {
			b.Fatal("missing token")
		}
	}
}

func BenchmarkClaim10000(b *testing.B) {
	manager := NewManager(&fakeSource{tokens: makeTokens(10_000)}, slog.Default(), time.Second, time.Second, 8)
	if err := manager.Refresh(context.Background()); err != nil {
		b.Fatalf("Refresh returned error: %v", err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		claim, err := manager.Claim(ctx, Intent{})
		if err != nil {
			b.Fatalf("Claim returned error: %v", err)
		}
		claim.Release()
	}
}

func makeTokens(count int) []store.Token {
	tokens := make([]store.Token, 0, count)
	now := time.Now().UTC()
	for i := 1; i <= count; i++ {
		tokens = append(tokens, store.Token{
			ID:           int64(i),
			AccessToken:  fmt.Sprintf("access-%d", i),
			RefreshToken: fmt.Sprintf("refresh-%d", i),
			IsActive:     true,
			CreatedAt:    now,
			UpdatedAt:    now,
		})
	}
	return tokens
}
