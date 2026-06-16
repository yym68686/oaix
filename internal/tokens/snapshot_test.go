package tokens

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

type fakeSource struct {
	tokens []store.Token
	err    error
}

const testMaxAge = time.Second
const testRefreshInterval = time.Second

func (f *fakeSource) ListAvailableTokens(context.Context) ([]store.Token, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := make([]store.Token, len(f.tokens))
	copy(out, f.tokens)
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
