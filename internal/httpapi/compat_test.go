package httpapi

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"log/slog"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/logs"
	"github.com/yym68686/oaix/internal/proxy"
	"github.com/yym68686/oaix/internal/store"
	"github.com/yym68686/oaix/internal/testkit"
	"github.com/yym68686/oaix/internal/tokens"
	"github.com/yym68686/oaix/internal/transport"
)

func TestBlackboxCompatibilityFixture(t *testing.T) {
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("OAIX_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	upstream := testkit.NewFakeUpstream()
	defer upstream.Close()
	cfg := config.Config{
		Server: config.ServerConfig{ShutdownTimeout: time.Second},
		Database: config.DatabaseConfig{
			URL:            dsn,
			MaxConns:       4,
			MinConns:       0,
			ConnectTimeout: time.Second,
		},
		Auth: config.AuthConfig{ServiceAPIKeys: []string{"compat-key"}},
		Upstream: config.UpstreamConfig{
			ResponsesURL:              upstream.URL(),
			MaxRetries:                2,
			ShardCount:                2,
			MaxIdleConns:              8,
			MaxIdleConnsPerHost:       8,
			MaxConnsPerHost:           8,
			IdleConnTimeout:           time.Second,
			ResponseHeaderTimeout:     time.Second,
			TLSHandshakeTimeout:       time.Second,
			NonStreamMaxResponseBytes: 1 << 20,
		},
		TokenPool: config.TokenPoolConfig{
			SnapshotMaxAge:  time.Minute,
			RefreshInterval: time.Minute,
			ActiveStreamCap: 2,
			DefaultCooldown: time.Second,
		},
		RequestLog: config.RequestLogConfig{
			Concurrency:     1,
			BatchSize:       10,
			QueueMaxSize:    100,
			FlushInterval:   10 * time.Millisecond,
			FinalSyncOnFull: true,
		},
	}
	db, err := store.Connect(ctx, cfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.UpsertAccessTokens(ctx, []string{"compat-access-token"}, "compat_test"); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	manager := tokens.NewManager(db, logger, cfg.TokenPool.SnapshotMaxAge, cfg.TokenPool.RefreshInterval, cfg.TokenPool.ActiveStreamCap)
	if err := manager.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	writer := logs.NewWriter(db, logger, cfg.RequestLog)
	writer.Start(ctx)
	defer writer.Stop(ctx)
	client := transport.New(cfg.Upstream)
	defer client.CloseIdleConnections()
	pipeline := proxy.New(cfg, logger, manager, client, writer, db)
	app := NewApp(cfg, logger, db, manager, writer, pipeline)
	server := httptest.NewServer(app.Handler())
	defer server.Close()

	postJSON := func(path, body string) *http.Response {
		req, err := http.NewRequest(http.MethodPost, server.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer compat-key")
		req.Header.Set("Content-Type", "application/json")
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	for _, tc := range []struct {
		path string
		body string
	}{
		{"/v1/responses", `{"model":"gpt-5","input":"hello"}`},
		{"/v1/responses", `{"model":"gpt-5","input":"hello","stream":true}`},
		{"/v1/chat/completions", `{"model":"gpt-5","messages":[{"role":"user","content":"hello"}]}`},
		{"/v1/images/generations", `{"model":"gpt-image-1","prompt":"logo"}`},
	} {
		resp := postJSON(tc.path, tc.body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s status = %d", tc.path, resp.StatusCode)
		}
	}
	var multipartBody bytes.Buffer
	mw := multipart.NewWriter(&multipartBody)
	_ = mw.WriteField("model", "gpt-image-1")
	_ = mw.WriteField("prompt", "edit")
	file, _ := mw.CreateFormFile("image", "input.png")
	_, _ = file.Write([]byte("png"))
	_ = mw.Close()
	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/images/edits", &multipartBody)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer compat-key")
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("image edits status = %d", resp.StatusCode)
	}
}
