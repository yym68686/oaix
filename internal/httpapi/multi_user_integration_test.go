package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/affinity"
	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/logs"
	"github.com/yym68686/oaix/internal/proxy"
	"github.com/yym68686/oaix/internal/store"
	"github.com/yym68686/oaix/internal/tokens"
	"github.com/yym68686/oaix/internal/transport"
)

type multiUserHarness struct {
	app      *App
	cfg      config.Config
	client   *http.Client
	db       *store.Store
	server   *httptest.Server
	upstream *recordingUpstream
	writer   *logs.Writer
}

type recordingUpstream struct {
	server *httptest.Server
	mu     sync.Mutex
	auths  []string
	bodies []map[string]any
	paths  []string
}

func newRecordingUpstream() *recordingUpstream {
	upstream := &recordingUpstream{}
	mux := http.NewServeMux()
	mux.HandleFunc("/", upstream.handle)
	upstream.server = httptest.NewServer(mux)
	return upstream
}

func (u *recordingUpstream) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	body, _ := io.ReadAll(r.Body)
	var payload map[string]any
	_ = json.Unmarshal(body, &payload)

	u.mu.Lock()
	u.auths = append(u.auths, r.Header.Get("Authorization"))
	u.paths = append(u.paths, r.URL.Path)
	u.bodies = append(u.bodies, payload)
	u.mu.Unlock()

	if r.Header.Get("Authorization") == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": "missing bearer"}})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	if containsImageTool(payload) {
		_, _ = io.WriteString(w, strings.Join([]string{
			`event: response.completed`,
			`data: {"type":"response.completed","response":{"id":"resp_image","created_at":1710000000,"output":[{"type":"image_generation_call","result":"QUJD","output_format":"png","size":"1024x1024"}],"tool_usage":{"image_gen":{"images":1}}}}`,
			``,
		}, "\n"))
		return
	}
	_, _ = io.WriteString(w, strings.Join([]string{
		`event: response.output_text.delta`,
		`data: {"type":"response.output_text.delta","delta":"ok"}`,
		``,
		`event: response.completed`,
		`data: {"type":"response.completed","response":{"id":"resp_text","object":"response","model":"gpt-5.5","status":"completed","output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"cached_input_tokens":0,"output_tokens":1,"total_tokens":2}}}`,
		``,
	}, "\n"))
}

func (u *recordingUpstream) URL() string {
	return u.server.URL
}

func (u *recordingUpstream) Close() {
	u.server.Close()
}

func (u *recordingUpstream) Auths() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.auths...)
}

func containsImageTool(payload map[string]any) bool {
	tools, _ := payload["tools"].([]any)
	for _, item := range tools {
		tool, _ := item.(map[string]any)
		if tool["type"] == "image_generation" {
			return true
		}
	}
	return false
}

func newMultiUserHarness(t *testing.T) *multiUserHarness {
	t.Helper()
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("OAIX_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	upstream := newRecordingUpstream()
	cfg := config.Config{
		Server: config.ServerConfig{ShutdownTimeout: time.Second},
		Database: config.DatabaseConfig{
			URL:            dsn,
			MaxConns:       4,
			MinConns:       0,
			ConnectTimeout: time.Second,
		},
		Auth: config.AuthConfig{ServiceAPIKeys: []string{"service-test-key"}},
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
			ActiveStreamCap: 10,
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
		upstream.Close()
		t.Fatal(err)
	}
	if err := db.Migrate(ctx); err != nil {
		db.Close()
		upstream.Close()
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	manager := tokens.NewManager(db, logger, cfg.TokenPool.SnapshotMaxAge, cfg.TokenPool.RefreshInterval, cfg.TokenPool.ActiveStreamCap)
	writer := logs.NewWriter(db, logger, cfg.RequestLog)
	writer.Start(ctx)
	client := transport.New(cfg.Upstream)
	pipeline := proxy.New(cfg, logger, manager, client, writer, db, affinity.NewMemoryStore())
	app := NewApp(cfg, logger, db, manager, writer, pipeline)
	server := httptest.NewServer(app.Handler())
	harness := &multiUserHarness{app: app, cfg: cfg, client: server.Client(), db: db, server: server, upstream: upstream, writer: writer}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = writer.Stop(stopCtx)
		server.Close()
		client.CloseIdleConnections()
		db.Close()
		upstream.Close()
	})
	return harness
}

func (h *multiUserHarness) createUser(t *testing.T, label string) (store.PlatformUser, store.CreatedAPIKey) {
	t.Helper()
	email := fmt.Sprintf("oaix-%s-%d@example.test", label, time.Now().UnixNano())
	user, key, err := h.db.CreateRegisteredUser(context.Background(), email, "password123", label)
	if err != nil {
		t.Fatal(err)
	}
	return user, key
}

func (h *multiUserHarness) createToken(t *testing.T, ownerID int64, label string) store.Token {
	t.Helper()
	result, err := h.db.UpsertTokenPayloadsForOwner(context.Background(), ownerID, []map[string]any{{
		"access_token":  "access-" + label + "-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		"refresh_token": "refresh-" + label + "-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		"account_id":    "account-" + label,
		"plan_type":     "pro",
	}}, "multi_user_test")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Tokens) != 1 {
		t.Fatalf("created tokens = %d, want 1", len(result.Tokens))
	}
	return result.Tokens[0]
}

func (h *multiUserHarness) request(t *testing.T, method string, path string, apiKey string, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, h.server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := h.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func decodeResponseBody(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, &payload); err != nil {
			t.Fatalf("decode response body: %v body=%s", err, data)
		}
	}
	return payload
}

func expectStatus(t *testing.T, resp *http.Response, want int) map[string]any {
	t.Helper()
	payload := decodeResponseBody(t, resp)
	if resp.StatusCode != want {
		t.Fatalf("status = %d, want %d, body=%v", resp.StatusCode, want, payload)
	}
	return payload
}

func TestMultiUserAPIIsolationWithDatabase(t *testing.T) {
	h := newMultiUserHarness(t)
	userA, keyA := h.createUser(t, "isolation-a")
	userB, _ := h.createUser(t, "isolation-b")
	_ = h.createToken(t, userA.ID, "owner-a")
	tokenB := h.createToken(t, userB.ID, "owner-b")
	jobB, err := h.db.CreateQueuedImportJobForOwner(context.Background(), userB.ID, []map[string]any{{"refresh_token": "rt-b-" + strconv.FormatInt(time.Now().UnixNano(), 10)}}, "front")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	success := true
	requestID := "req-b-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if err := h.db.UpsertRequestLogs(context.Background(), []store.RequestLog{{
		RequestID:   requestID,
		OwnerUserID: &userB.ID,
		Endpoint:    "/v1/responses",
		Model:       testStringPtr("gpt-5.5"),
		IsStream:    true,
		StatusCode:  testIntPtr(http.StatusOK),
		Success:     &success,
		StartedAt:   now,
		FinishedAt:  &now,
	}}); err != nil {
		t.Fatal(err)
	}

	expectStatus(t, h.request(t, http.MethodGet, "/api/tokens/"+strconv.FormatInt(tokenB.ID, 10), keyA.PlaintextKey, ""), http.StatusNotFound)
	expectStatus(t, h.request(t, http.MethodGet, "/api/import/jobs/"+strconv.FormatInt(jobB.ID, 10), keyA.PlaintextKey, ""), http.StatusNotFound)

	requestPayload := expectStatus(t, h.request(t, http.MethodGet, "/api/requests?include_total=true&request_id="+requestID, keyA.PlaintextKey, ""), http.StatusOK)
	if items, _ := requestPayload["items"].([]any); len(items) != 0 {
		t.Fatalf("user A saw user B request by request_id: %v", items)
	}
	overridePayload := expectStatus(t, h.request(t, http.MethodGet, "/api/requests?include_total=true&user_id="+strconv.FormatInt(userB.ID, 10), keyA.PlaintextKey, ""), http.StatusOK)
	if items, _ := overridePayload["items"].([]any); len(items) != 0 {
		t.Fatalf("user A bypassed owner scope via user_id query: %v", items)
	}
}

func TestMultiUserAdminAndReadonlyPermissionsWithDatabase(t *testing.T) {
	h := newMultiUserHarness(t)
	userB, _ := h.createUser(t, "admin-read-target")
	tokenOwner, _ := h.createUser(t, "admin-token-owner")
	caller, _ := h.createUser(t, "admin-caller")
	readonlyUser, err := h.db.CreatePlatformUserWithPassword(context.Background(), "oaix-ro-"+strconv.FormatInt(time.Now().UnixNano(), 10)+"@example.test", "password123", "readonly", "user")
	if err != nil {
		t.Fatal(err)
	}
	readonlyKey, err := h.db.CreateAPIKey(context.Background(), &readonlyUser.ID, "user", "readonly", "readonly_admin", &readonlyUser.ID)
	if err != nil {
		t.Fatal(err)
	}

	expectStatus(t, h.request(t, http.MethodGet, "/api/admin/users?limit=5", "service-test-key", ""), http.StatusOK)
	expectStatus(t, h.request(t, http.MethodGet, "/api/admin/pool-summary", "service-test-key", ""), http.StatusOK)
	expectStatus(t, h.request(t, http.MethodPatch, "/api/admin/users/"+strconv.FormatInt(userB.ID, 10), readonlyKey.PlaintextKey, `{"status":"disabled"}`), http.StatusForbidden)

	now := time.Now().UTC()
	success := true
	tokenOwnerRequestID := "req-token-owner-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if err := h.db.UpsertRequestLogs(context.Background(), []store.RequestLog{{
		RequestID:        tokenOwnerRequestID,
		OwnerUserID:      &caller.ID,
		TokenOwnerUserID: &tokenOwner.ID,
		Endpoint:         "/v1/responses",
		Model:            testStringPtr("gpt-5.5"),
		IsStream:         true,
		StatusCode:       testIntPtr(http.StatusOK),
		Success:          &success,
		StartedAt:        now,
		FinishedAt:       &now,
	}}); err != nil {
		t.Fatal(err)
	}
	usagePayload := expectStatus(t, h.request(t, http.MethodGet, "/api/admin/users/"+strconv.FormatInt(tokenOwner.ID, 10)+"/usage", "service-test-key", ""), http.StatusOK)
	usage, _ := usagePayload["usage"].(map[string]any)
	if got := int64(usage["request_count"].(float64)); got != 1 {
		t.Fatalf("token owner request_count = %d, want 1", got)
	}
	requestPayload := expectStatus(t, h.request(t, http.MethodGet, "/api/admin/users/"+strconv.FormatInt(tokenOwner.ID, 10)+"/requests?include_total=true", "service-test-key", ""), http.StatusOK)
	items, _ := requestPayload["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("token owner request items = %d, want 1 payload=%v", len(items), requestPayload)
	}
}

func TestAdminUserUsageIncludesObservedCostByTokenOwnerWithDatabase(t *testing.T) {
	h := newMultiUserHarness(t)
	tokenOwner, _ := h.createUser(t, "observed-token-owner")
	caller, _ := h.createUser(t, "observed-caller")
	token := h.createToken(t, tokenOwner.ID, "observed-owner")

	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour)
	success := true
	tokenID := token.ID
	statusOK := http.StatusOK
	inputTokens := 100
	cachedInputTokens := 25
	totalTokens := 110
	oldCost := 12.5
	recentCost := 1.25
	requestSuffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	if err := h.db.UpsertRequestLogs(context.Background(), []store.RequestLog{
		{
			RequestID:         "req-observed-old-" + requestSuffix,
			OwnerUserID:       &caller.ID,
			TokenOwnerUserID:  &tokenOwner.ID,
			Endpoint:          "/v1/responses",
			Model:             testStringPtr("gpt-5.5"),
			IsStream:          true,
			StatusCode:        &statusOK,
			Success:           &success,
			TokenID:           &tokenID,
			StartedAt:         old,
			FinishedAt:        &old,
			InputTokens:       &inputTokens,
			CachedInputTokens: &cachedInputTokens,
			TotalTokens:       &totalTokens,
			EstimatedCostUSD:  &oldCost,
		},
		{
			RequestID:         "req-observed-recent-" + requestSuffix,
			OwnerUserID:       &caller.ID,
			TokenOwnerUserID:  &tokenOwner.ID,
			Endpoint:          "/v1/responses",
			Model:             testStringPtr("gpt-5.5"),
			IsStream:          true,
			StatusCode:        &statusOK,
			Success:           &success,
			TokenID:           &tokenID,
			StartedAt:         now,
			FinishedAt:        &now,
			InputTokens:       &inputTokens,
			CachedInputTokens: &cachedInputTokens,
			TotalTokens:       &totalTokens,
			EstimatedCostUSD:  &recentCost,
		},
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := h.db.AggregateRequestHourlyStats(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := h.db.ReconcileRecordedTokenCosts(ctx); err != nil {
		t.Fatal(err)
	}

	usagePayload := expectStatus(t, h.request(t, http.MethodGet, "/api/admin/users/"+strconv.FormatInt(tokenOwner.ID, 10)+"/usage?hours=24", "service-test-key", ""), http.StatusOK)
	assertObservedUsageCost(t, usagePayload["usage"], recentCost, oldCost+recentCost)

	if tokenOwner.Email == nil {
		t.Fatal("token owner email is nil")
	}
	usersPayload := expectStatus(t, h.request(t, http.MethodGet, "/api/admin/analytics/users?limit=5&q="+url.QueryEscape(*tokenOwner.Email), "service-test-key", ""), http.StatusOK)
	items, _ := usersPayload["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("admin analytics users items = %d, want 1 payload=%v", len(items), usersPayload)
	}
	item, _ := items[0].(map[string]any)
	assertObservedUsageCost(t, item["usage"], recentCost, oldCost+recentCost)
}

func TestMultiUserAPIKeyRevocationAndDisabledUserWithDatabase(t *testing.T) {
	h := newMultiUserHarness(t)
	userA, keyA := h.createUser(t, "revoke-a")
	expectStatus(t, h.request(t, http.MethodGet, "/api/me", keyA.PlaintextKey, ""), http.StatusOK)
	if err := h.db.RevokeAPIKey(context.Background(), store.OwnerResources(userA.ID), keyA.ID); err != nil {
		t.Fatal(err)
	}
	expectStatus(t, h.request(t, http.MethodGet, "/api/me", keyA.PlaintextKey, ""), http.StatusUnauthorized)

	userB, keyB := h.createUser(t, "disabled-b")
	_ = h.createToken(t, userB.ID, "disabled-owner")
	disabled := "disabled"
	if _, err := h.db.UpdatePlatformUser(context.Background(), userB.ID, store.PlatformUserPatch{Status: &disabled}); err != nil {
		t.Fatal(err)
	}
	expectStatus(t, h.request(t, http.MethodGet, "/api/tokens", keyB.PlaintextKey, ""), http.StatusForbidden)
	expectStatus(t, h.request(t, http.MethodPost, "/v1/responses", keyB.PlaintextKey, `{"model":"gpt-5.5","input":"hello"}`), http.StatusForbidden)
}

func TestProxyUsesOwnerTokenPoolWithDatabase(t *testing.T) {
	h := newMultiUserHarness(t)
	userA, keyA := h.createUser(t, "proxy-a")
	userB, _ := h.createUser(t, "proxy-b")
	tokenA := h.createToken(t, userA.ID, "proxy-a")
	tokenB := h.createToken(t, userB.ID, "proxy-b")

	expectStatus(t, h.request(t, http.MethodPost, "/v1/responses", keyA.PlaintextKey, `{"model":"gpt-5.5","input":"hello"}`), http.StatusOK)
	expectStatus(t, h.request(t, http.MethodPost, "/v1/images/generations", keyA.PlaintextKey, `{"model":"gpt-image-2","prompt":"draw","response_format":"b64_json"}`), http.StatusOK)

	auths := h.upstream.Auths()
	if len(auths) != 2 {
		t.Fatalf("upstream calls = %d, want 2", len(auths))
	}
	for _, auth := range auths {
		if auth != "Bearer "+tokenA.AccessToken {
			t.Fatalf("upstream used wrong token auth=%q want owner A token %q; owner B token was %q", auth, tokenA.AccessToken, tokenB.AccessToken)
		}
	}
}

func testStringPtr(value string) *string {
	return &value
}

func testIntPtr(value int) *int {
	return &value
}

func assertObservedUsageCost(t *testing.T, raw any, wantRecent float64, wantObserved float64) {
	t.Helper()
	usage, _ := raw.(map[string]any)
	if usage == nil {
		t.Fatalf("usage payload missing: %#v", raw)
	}
	recent, _ := usage["estimated_cost_usd"].(float64)
	if math.Abs(recent-wantRecent) > 0.000001 {
		t.Fatalf("estimated_cost_usd = %f, want %f usage=%v", recent, wantRecent, usage)
	}
	observed, _ := usage["observed_cost_usd"].(float64)
	if math.Abs(observed-wantObserved) > 0.000001 {
		t.Fatalf("observed_cost_usd = %f, want %f usage=%v", observed, wantObserved, usage)
	}
}
