package httpapi

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/egress"
	"github.com/yym68686/oaix/internal/store"
)

type probeDoerFunc func(context.Context, *http.Request) (*http.Response, error)

func (f probeDoerFunc) Do(ctx context.Context, r *http.Request) (*http.Response, error) {
	return f(ctx, r)
}

func TestProbeProxyFailurePreservesPersistedTokenAndBinding(t *testing.T) {
	h := newMultiUserHarness(t)
	owner, key := h.createUser(t, "probe-proxy")
	token := h.createToken(t, owner.ID, "probe-proxy")
	ctx := t.Context()
	channelID, err := h.db.SaveProxyChannel(ctx, owner.ID, 0, "fixture", "8.8.8.8:8080:fixture-user:fixture-password")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.db.SetTokenProxyChannel(ctx, store.ResourceScope{OwnerUserID: &owner.ID}, token.ID, channelID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.db.Pool().Exec(ctx, `update codex_tokens set is_active=false, disabled_at=now(), cooldown_until=now()+interval '7 days', last_error='existing state' where id=$1`, token.ID); err != nil {
		t.Fatal(err)
	}
	snapshot := func() string {
		t.Helper()
		var value string
		if err := h.db.Pool().QueryRow(ctx, `select jsonb_build_object('token',to_jsonb(t),'binding',to_jsonb(b))::text from codex_tokens t join token_proxy_bindings b on b.token_id=t.id where t.id=$1`, token.ID).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.Proxy = nil
	base.DialContext = func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("fixture connection refused")
	}
	client := &http.Client{Transport: egress.NewTransport(base), Timeout: time.Second}
	defer client.CloseIdleConnections()
	h.app.SetProbeRequestDoer(fallbackTokenProbeDoer{client: client})
	before := snapshot()
	for _, entry := range []struct{ prefix, auth string }{{"/api/tokens", key.PlaintextKey}, {"/admin/tokens", "service-test-key"}} {
		resp := h.request(t, http.MethodPost, fmt.Sprintf("%s/%d/probe", entry.prefix, token.ID), entry.auth, `{"model":"gpt-5.4-mini"}`)
		result := decodeResponseBody(t, resp)
		if resp.StatusCode != 200 || result["outcome"] != "inconclusive" || result["error_code"] != "proxy_unavailable" || result["message"] != "代理不可用，当前状态未改变。" {
			t.Fatalf("unexpected API response: %v", result)
		}
		if snapshot() != before || len(h.upstream.Auths()) != 0 {
			t.Fatal("failed probe changed persisted state or reached upstream")
		}
	}
}

type unavailableProbeProxyResolver struct{}

func (unavailableProbeProxyResolver) ResolveTokenProxy(context.Context, int64) (*url.URL, error) {
	return nil, errors.New("fixture-user:fixture-password")
}

// Exercise real HTTP CONNECT/TLS failures and upstream responses through the
// same transport as production. All sockets terminate on local fixtures.
func TestProbeProxyFailureMessages(t *testing.T) {
	for _, tc := range []struct {
		name, message, code string
		status              int
	}{
		{"connect_rejected", "代理不可用，当前状态未改变。", "proxy_unavailable", 502},
		{"tls_record", "代理不可用，当前状态未改变。", "proxy_unavailable", 502},
		{"tls_eof", "代理不可用，当前状态未改变。", "proxy_unavailable", 502},
		{"proxy_dial", "代理不可用，当前状态未改变。", "proxy_unavailable", 502},
		{"configuration", "测试未执行：无法读取代理配置，当前状态未改变。", "proxy_configuration_unavailable", 503},
		{"direct_dial", "连接上游失败，当前状态未改变。", "upstream_transport_error", 502},
		{"canceled", "测试在完整终止事件前被取消，当前状态未改变。", "request_canceled", 408},
		// Even an upstream error code matching a local code is not a proxy failure.
		{"upstream_502", "上游暂时不可用或限流，当前状态未改变。", "proxy_unavailable", 502},
		{"upstream_429", "上游暂时不可用或限流，当前状态未改变。", "rate_limit_exceeded", 429},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var connects, upstreamCalls atomic.Int64
			upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upstreamCalls.Add(1)
				if r.Header.Get("Proxy-Authorization") != "" {
					t.Error("proxy credentials reached upstream")
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": tc.code, "message": "fixture upstream failure"}})
			}))
			defer upstream.Close()
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				connects.Add(1)
				if r.Method != http.MethodConnect || r.Host != "probe-upstream.invalid:443" || r.Header.Get("Proxy-Authorization") == "" {
					t.Error("invalid proxy CONNECT")
				}
				if tc.name == "connect_rejected" {
					w.WriteHeader(http.StatusProxyAuthRequired)
					return
				}
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
				if tc.name == "tls_record" || tc.name == "tls_eof" {
					var hello [4096]byte
					_, _ = conn.Read(hello[:])
					if tc.name == "tls_record" {
						_, _ = io.WriteString(conn, "HTTP/1.1 502 Bad Gateway\r\n\r\n")
					}
					return
				}
				dest, err := net.DialTimeout("tcp", strings.TrimPrefix(upstream.URL, "https://"), time.Second)
				if err != nil {
					t.Error(err)
					return
				}
				defer dest.Close()
				done := make(chan struct{})
				go func() { defer close(done); _, _ = io.Copy(dest, conn); dest.Close() }()
				_, _ = io.Copy(conn, dest)
				conn.Close()
				<-done
			}))
			defer proxy.Close()
			base := http.DefaultTransport.(*http.Transport).Clone()
			base.Proxy = nil
			base.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // Local fixture only.
			base.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
				if tc.name == "proxy_dial" || tc.name == "direct_dial" {
					return nil, errors.New("fixture-user:fixture-password")
				}
				if address != "8.8.8.8:8080" {
					t.Errorf("unexpected direct dial: %s", address)
					return nil, errors.New("unexpected direct dial")
				}
				return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(proxy.URL, "http://"))
			}
			client := &http.Client{Transport: egress.NewTransport(base), Timeout: 5 * time.Second}
			defer client.CloseIdleConnections()
			proxyURL, _ := egress.Parse("8.8.8.8:8080:fixture-user:fixture-password")
			app := &App{
				cfg: config.Config{Upstream: config.UpstreamConfig{ResponsesURL: "https://probe-upstream.invalid/responses"}},
				probeDoer: probeDoerFunc(func(ctx context.Context, r *http.Request) (*http.Response, error) {
					if tc.name == "configuration" {
						ctx = egress.ForToken(ctx, unavailableProbeProxyResolver{}, 901)
					} else if tc.name != "direct_dial" {
						ctx = egress.WithProxy(ctx, proxyURL)
					}
					return client.Do(r.WithContext(ctx))
				}),
			}
			ctx := t.Context()
			if tc.name == "canceled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			result := app.probeTokenWithAccess(ctx, store.Token{ID: 901, AccessToken: "access", RefreshToken: "refresh"}, defaultAdminProbeModel)
			if result["outcome"] != "inconclusive" || result["status_code"] != tc.status || result["message"] != tc.message || result["error_code"] != tc.code {
				t.Fatalf("unexpected classification: %#v", result)
			}
			encoded, _ := json.Marshal(result)
			if strings.Contains(string(encoded), "fixture-user") || strings.Contains(string(encoded), "fixture-password") {
				t.Fatalf("probe leaked credentials: %s", encoded)
			}
			if strings.HasPrefix(tc.name, "upstream_") {
				if connects.Load() != 1 || upstreamCalls.Load() != 1 || result["probe_stage"] != probeStageUpstreamResponse || result["raw_response"] == nil {
					t.Fatalf("upstream response lost: connects=%d upstream=%d result=%v", connects.Load(), upstreamCalls.Load(), result)
				}
			} else if upstreamCalls.Load() != 0 {
				t.Fatalf("proxy failure fell back to upstream: %d", upstreamCalls.Load())
			}
			if tc.name == "configuration" && (result["upstream_attempted"] != false || result["probe_stage"] != probeStageLocalPreflight) {
				t.Fatalf("configuration failure reported an upstream request: %v", result)
			}
		})
	}
}
