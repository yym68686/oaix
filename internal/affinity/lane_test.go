package affinity

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/store"
)

func TestMemoryStoreRemoveTokenAndMetrics(t *testing.T) {
	s := NewMemoryStore()
	s.Put("prompt-a", Lane{
		PrimaryTokenID:    1,
		SecondaryTokenIDs: []int64{2, 3},
		MarketplacePriceLocks: map[int64]MarketplacePriceLock{
			1: {PriceBPS: 50, Source: "owner_default", LockedAt: time.Now().UTC()},
			2: {PriceBPS: 80, Source: "owner_default", LockedAt: time.Now().UTC()},
		},
	}, time.Hour)
	s.BindResponseOwner("response-a", ResponseOwnerBinding{TokenID: 1}, time.Hour)
	s.RemoveToken(1)
	lane, ok := s.Get("prompt-a")
	if !ok {
		t.Fatal("lane missing after removing primary with secondary fallback")
	}
	if lane.PrimaryTokenID != 2 || len(lane.SecondaryTokenIDs) != 1 || lane.SecondaryTokenIDs[0] != 3 {
		t.Fatalf("unexpected lane after remove: %+v", lane)
	}
	if _, ok := lane.MarketplacePriceLock(1); ok {
		t.Fatal("removed primary price lock should be removed")
	}
	if lock, ok := lane.MarketplacePriceLock(2); !ok || lock.PriceBPS != 80 {
		t.Fatalf("promoted secondary price lock was not retained: lock=%+v ok=%v", lock, ok)
	}
	if _, ok := s.GetResponseOwner("response-a"); ok {
		t.Fatal("response owner should be removed with token")
	}
	stats := s.Stats()
	if stats.Puts != 1 || stats.Removals != 1 || stats.Gets == 0 || stats.Hits == 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestPostgresLaneStoreMultiInstance(t *testing.T) {
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("OAIX_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	db, err := store.Connect(ctx, config.DatabaseConfig{
		URL:            dsn,
		MaxConns:       4,
		MinConns:       0,
		ConnectTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	writer := NewPostgresStore(db.Pool())
	reader := NewPostgresStore(db.Pool())
	key := "postgres-multi-instance-" + time.Now().Format(time.RFC3339Nano)
	writer.Put(key, Lane{PrimaryTokenID: 11, SecondaryTokenIDs: []int64{12}}, time.Hour)
	lane, ok := reader.Get(key)
	if !ok || lane.PrimaryTokenID != 11 {
		t.Fatalf("reader did not see writer lane: ok=%v lane=%+v", ok, lane)
	}
	writer.RemoveToken(11)
	lane, ok = reader.Get(key)
	if !ok || lane.PrimaryTokenID != 12 {
		t.Fatalf("reader did not see token removal: ok=%v lane=%+v", ok, lane)
	}
}

func TestRedisLaneStoreFixture(t *testing.T) {
	rawURL := os.Getenv("OAIX_TEST_REDIS_URL")
	if rawURL == "" {
		t.Skip("OAIX_TEST_REDIS_URL is not set")
	}
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(options)
	defer client.Close()
	s := NewRedisStore(client)
	key := "redis-fixture-" + time.Now().Format(time.RFC3339Nano)
	s.Put(key, Lane{PrimaryTokenID: 21, SecondaryTokenIDs: []int64{22}}, time.Minute)
	lane, ok := s.Get(key)
	if !ok || lane.PrimaryTokenID != 21 {
		t.Fatalf("missing redis lane: ok=%v lane=%+v", ok, lane)
	}
	s.RemoveToken(21)
	lane, ok = s.Get(key)
	if !ok || lane.PrimaryTokenID != 22 {
		t.Fatalf("redis removal did not promote secondary: ok=%v lane=%+v", ok, lane)
	}
}
