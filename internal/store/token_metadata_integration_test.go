package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
)

func TestPostgresUpdateTokenMetadataReturnsCompleteToken(t *testing.T) {
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set OAIX_TEST_DATABASE_URL to run Postgres integration fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := Connect(ctx, config.DatabaseConfig{URL: configURL(dsn), MaxConns: 4, MinConns: 1, ConnectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Connect returned error: %v", err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("Migrate returned error: %v", err)
	}

	var tokenID int64
	priceUpdatedAt := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	if err := db.pool.QueryRow(ctx, `
		insert into codex_tokens(email, refresh_token, is_active, codex_fingerprint_enabled,
		                        marketplace_price_bps, marketplace_price_updated_at, marketplace_price_source)
		values ($1, $2, true, true, 120, $3, 'token_override')
		returning id
	`, fmt.Sprintf("metadata-%d@example.test", time.Now().UnixNano()), "metadata-refresh", priceUpdatedAt).Scan(&tokenID); err != nil {
		t.Fatalf("insert token: %v", err)
	}
	defer func() {
		if _, err := db.pool.Exec(context.Background(), `delete from codex_tokens where id = $1`, tokenID); err != nil {
			t.Errorf("delete fixture token: %v", err)
		}
	}()

	for _, enabled := range []bool{false, true} {
		token, updateErr := db.UpdateTokenMetadata(ctx, TokenMetadataUpdate{TokenID: tokenID, CodexFingerprintEnabled: &enabled})
		// Read back even on error: a RETURNING scan failure can occur after the
		// autocommit UPDATE has already changed the setting.
		var persisted bool
		if err := db.pool.QueryRow(ctx, `select codex_fingerprint_enabled from codex_tokens where id = $1`, tokenID).Scan(&persisted); err != nil {
			t.Fatalf("read persisted setting: %v", err)
		}
		if updateErr != nil {
			t.Fatalf("UpdateTokenMetadata returned error: %v; persisted fingerprint=%v, requested=%v", updateErr, persisted, enabled)
		}
		if token.ID != tokenID || token.CodexFingerprintEnabled == nil || *token.CodexFingerprintEnabled != enabled || persisted != enabled {
			t.Fatalf("returned/persisted fingerprint mismatch: id=%d returned=%v persisted=%v want=%v", token.ID, token.CodexFingerprintEnabled, persisted, enabled)
		}
		if token.MarketplacePriceBPS == nil || *token.MarketplacePriceBPS != 120 ||
			token.MarketplacePriceUpdatedAt == nil || !token.MarketplacePriceUpdatedAt.Equal(priceUpdatedAt) ||
			token.MarketplacePriceSource != "token_override" {
			t.Fatalf("metadata update lost marketplace fields: price=%v updated=%v source=%q", token.MarketplacePriceBPS, token.MarketplacePriceUpdatedAt, token.MarketplacePriceSource)
		}
	}
}
