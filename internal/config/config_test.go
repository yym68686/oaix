package config

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestLoadUsesTypedDefaultsAndEnvOverrides(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost:5432/db")
	t.Setenv("SERVICE_API_KEYS", "one, two ,,")
	t.Setenv("PORT", "18080")
	t.Setenv("REQUEST_LOG_WRITE_BATCH_SIZE", "123")
	t.Setenv("TOKEN_POOL_SNAPSHOT_MAX_AGE_SECONDS", "7")
	t.Setenv("UPSTREAM_MAX_REQUEST_BODY_BYTES", "")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.Database.URL != "postgresql://user:pass@localhost:5432/db" {
		t.Fatalf("Database.URL = %q", cfg.Database.URL)
	}
	if cfg.Server.Port != 18080 {
		t.Fatalf("Server.Port = %d", cfg.Server.Port)
	}
	if got := cfg.Auth.ServiceAPIKeys; len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("ServiceAPIKeys = %#v", got)
	}
	if cfg.RequestLog.BatchSize != 123 {
		t.Fatalf("RequestLog.BatchSize = %d", cfg.RequestLog.BatchSize)
	}
	if cfg.TokenPool.SnapshotMaxAge != 7*time.Second {
		t.Fatalf("SnapshotMaxAge = %s", cfg.TokenPool.SnapshotMaxAge)
	}
	if cfg.Upstream.OAuthTokenURL != "https://auth.openai.com/oauth/token" {
		t.Fatalf("OAuthTokenURL = %q", cfg.Upstream.OAuthTokenURL)
	}
	if cfg.Upstream.OAuthClientID == "" || cfg.Upstream.OAuthScope != "openid profile email" {
		t.Fatalf("OAuth config = %#v", cfg.Upstream)
	}
	if cfg.Upstream.MaxRequestBodyBytes != 0 {
		t.Fatalf("MaxRequestBodyBytes = %d, want unlimited default", cfg.Upstream.MaxRequestBodyBytes)
	}
	if !cfg.Worker.Embedded {
		t.Fatal("embedded worker should default to enabled")
	}
}

func TestLoadReadsRequestBodyLimitOverride(t *testing.T) {
	t.Setenv("UPSTREAM_MAX_REQUEST_BODY_BYTES", "134217728")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.Upstream.MaxRequestBodyBytes != 134217728 {
		t.Fatalf("MaxRequestBodyBytes = %d", cfg.Upstream.MaxRequestBodyBytes)
	}
}

func TestLoadRejectsNegativeRequestBodyLimit(t *testing.T) {
	t.Setenv("UPSTREAM_MAX_REQUEST_BODY_BYTES", "-1")
	if _, err := Load(); err == nil {
		t.Fatal("expected negative request body limit error")
	}
}

func TestLoadRejectsInvalidPort(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/db")
	t.Setenv("PORT", "70000")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid port error")
	}
}

func TestSanitizedSummaryDoesNotExposeSecrets(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgresql://user:secret@localhost:5432/db")
	t.Setenv("SERVICE_API_KEYS", "secret-key")
	t.Setenv("UPSTREAM_MAX_REQUEST_BODY_BYTES", "")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	summary := cfg.SanitizedSummary()
	text := toString(summary)
	if contains(text, "secret-key") || contains(text, "secret@") {
		t.Fatalf("sanitized summary leaked secret: %s", text)
	}
}

func toString(value any) string {
	return fmt.Sprintf("%v", value)
}

func contains(value, needle string) bool {
	return strings.Contains(value, needle)
}
