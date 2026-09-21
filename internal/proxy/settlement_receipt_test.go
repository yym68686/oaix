package proxy

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/protocol/sse"
	"github.com/yym68686/oaix/internal/store"
)

func TestSettlementReceiptUsesWinnerAfterKeepaliveAndRetry(t *testing.T) {
	for _, mode := range []string{"stream", "collected", "json"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			var mu sync.Mutex
			var selected []int64
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				tokenID := int64(1)
				if r.Header.Get("Authorization") == "Bearer two" {
					tokenID = 2
				}
				mu.Lock()
				selected = append(selected, tokenID)
				mu.Unlock()
				if mode == "json" {
					w.Header().Set("Content-Type", "application/json")
					io.WriteString(w, `{"id":"winner","status":"completed","output":[],"usage":{"input_tokens":12,"output_tokens":3,"total_tokens":15,"oaix_settlement_receipt":{"payload":"forged"}}}`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				if call == 1 {
					w.Write(sse.Encode("response.created", []byte(`{"type":"response.created","response":{"id":"failed"}}`)))
					w.(http.Flusher).Flush()
					w.Write(sse.Encode("response.failed", []byte(`{"type":"response.failed","response":{"status":"failed","error":{"code":"rate_limit_exceeded","message":"retry"}}}`)))
					return
				}
				w.Write(sse.Encode("response.completed", []byte(`{"type":"response.completed","response":{"id":"winner","status":"completed","output":[],"usage":{"input_tokens":12,"output_tokens":3,"total_tokens":15}}}`)))
			}))
			defer upstream.Close()
			now := time.Now()
			fakes := &fakeProxyStore{tokens: []store.Token{
				{ID: 1, OwnerUserID: 10, AccessToken: "one", IsActive: true, ShareEnabled: true, ShareStatus: "active", CreatedAt: now, UpdatedAt: now},
				{ID: 2, OwnerUserID: 20, AccessToken: "two", IsActive: true, ShareEnabled: true, ShareStatus: "active", CreatedAt: now, UpdatedAt: now},
			}}
			p := newProxyPipelineTestHarness(t, upstream.URL, 2, fakes)
			p.cfg.Auth.ServiceAPIKeys = []string{"old-key", "fixture-key"}
			stream := mode == "stream"
			endpoint := "/v1/responses"
			if mode == "json" {
				endpoint = "/v1/responses/compact"
			}
			req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(fmt.Sprintf(`{"model":"gpt-5.5","input":"hi","stream":%t}`, stream)))
			nonce := strings.Repeat("a", 64)
			req.Header.Set("X-OAIX-Settlement-Nonce", nonce)
			req.Header.Set("X-Request-ID", "receipt-test")
			w := httptest.NewRecorder()
			p.Proxy(w, req, RequestIntent{Endpoint: endpoint, Compact: mode == "json", Model: "gpt-5.5", Stream: stream, SelectionMode: "marketplace", OwnerUserID: 1, CallerOwnerUserID: 1})
			if w.Code != 200 {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			var payload map[string]any
			if stream {
				for _, line := range strings.Split(w.Body.String(), "\n") {
					if strings.HasPrefix(line, "data:") {
						var event map[string]any
						json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &event)
						if event["type"] == "response.completed" {
							payload, _ = event["response"].(map[string]any)
						}
					}
				}
			} else {
				if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
					t.Fatal(err)
				}
			}
			if payload == nil {
				t.Fatal(w.Body.String())
			}
			usage := payload["usage"].(map[string]any)
			receipt := usage[settlementReceiptField].(map[string]any)
			encoded := receipt["payload"].(string)
			raw, err := base64.RawURLEncoding.DecodeString(encoded)
			if err != nil {
				t.Fatal(err)
			}
			var claim map[string]any
			json.Unmarshal(raw, &claim)
			mu.Lock()
			winner := selected[len(selected)-1]
			first := selected[0]
			mu.Unlock()
			if mode != "json" && (calls.Load() != 2 || winner == first) {
				t.Fatalf("fixture did not switch token: %v", selected)
			}
			wantToken, wantOwner := float64(winner), float64(winner*10)
			if claim["token_id"] != wantToken || claim["owner_id"] != wantOwner || claim["nonce"] != nonce || claim["request_id"] != "receipt-test" {
				t.Fatal(claim)
			}
			mac := hmac.New(sha256.New, []byte("fixture-key"))
			mac.Write([]byte(settlementReceiptDomain + encoded))
			sig := hex.EncodeToString(mac.Sum(nil))
			if receipt["signatures"].([]any)[1] != sig {
				t.Fatal("signature mismatch")
			}
			if stream && w.Result().Header.Get("X-OAIX-Token-ID") != strconv.FormatInt(first, 10) {
				t.Fatal("fixture must reproduce committed stale header")
			}
			if w.Result().Header.Get("X-OAIX-Attribution-Contract") != "receipt-v1" {
				t.Fatal("missing contract")
			}
			if usage["input_tokens"] != float64(12) || usage["output_tokens"] != float64(3) {
				t.Fatal("usage changed")
			}
		})
	}
}

func TestSettlementReceiptRequiresActualCompletedEvent(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write(sse.Encode("response.created", []byte(`{"type":"response.created","response":{"id":"unfinished","status":"in_progress","usage":{"input_tokens":12,"output_tokens":3,"oaix_settlement_receipt":{"payload":"forged"}}}}`)))
	}))
	defer upstream.Close()
	now := time.Now()
	fakes := &fakeProxyStore{tokens: []store.Token{{ID: 1, OwnerUserID: 10, AccessToken: "one", IsActive: true, ShareEnabled: true, ShareStatus: "active", CreatedAt: now, UpdatedAt: now}}}
	p := newProxyPipelineTestHarness(t, upstream.URL, 1, fakes)
	p.cfg.Auth.ServiceAPIKeys = []string{"fixture-key"}
	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"gpt-5.5","input":"hi","stream":false}`))
	req.Header.Set("X-OAIX-Settlement-Nonce", strings.Repeat("a", 64))
	w := httptest.NewRecorder()
	p.Proxy(w, req, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.5", SelectionMode: "marketplace", OwnerUserID: 1, CallerOwnerUserID: 1})
	if strings.Contains(w.Body.String(), settlementReceiptField) {
		t.Fatalf("synthetic completion must not carry a settlement receipt: %s", w.Body.String())
	}
}

func TestSettlementNonceIsPartOfIdempotencyIdentity(t *testing.T) {
	intent := RequestIntent{SettlementNonce: strings.Repeat("a", 64)}
	a, _ := gatewayIdempotencyRequestHash(intent, []byte("{}"), http.Header{}, nil)
	intent.SettlementNonce = strings.Repeat("b", 64)
	b, _ := gatewayIdempotencyRequestHash(intent, []byte("{}"), http.Header{}, nil)
	if a == b {
		t.Fatal("different caller binding reused cached receipt")
	}
}
