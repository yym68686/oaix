package transport

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
)

func TestClientShardAndConnectionLifecycle(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()
	client := New(config.UpstreamConfig{
		ShardCount:                4,
		MaxIdleConns:              64,
		MaxIdleConnsPerHost:       64,
		MaxConnsPerHost:           64,
		IdleConnTimeout:           time.Second,
		ResponseHeaderTimeout:     time.Second,
		TLSHandshakeTimeout:       time.Second,
		NonStreamMaxResponseBytes: 1 << 20,
	})
	defer client.CloseIdleConnections()
	before := runtime.NumGoroutine()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
			if err != nil {
				t.Errorf("NewRequest: %v", err)
				return
			}
			req.Header.Set("X-Request-ID", fmt.Sprintf("req-%d", i))
			resp, err := client.Do(context.Background(), req)
			if err != nil {
				t.Errorf("Do: %v", err)
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}(i)
	}
	wg.Wait()
	if got := client.Stats().Active; got != 0 {
		t.Fatalf("active requests = %d, want 0", got)
	}
	client.CloseIdleConnections()
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()
	if after-before > 20 {
		t.Fatalf("goroutines grew from %d to %d", before, after)
	}
}

func TestClientResponseHeaderTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		_, _ = io.WriteString(w, "late")
	}))
	defer server.Close()
	client := New(config.UpstreamConfig{
		ShardCount:            1,
		MaxIdleConns:          2,
		MaxIdleConnsPerHost:   2,
		MaxConnsPerHost:       2,
		IdleConnTimeout:       time.Second,
		ResponseHeaderTimeout: 10 * time.Millisecond,
		TLSHandshakeTimeout:   time.Second,
	})
	defer client.CloseIdleConnections()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(context.Background(), req)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err == nil {
		t.Fatal("expected response header timeout")
	}
	if client.Stats().ErrorsTotal == 0 {
		t.Fatal("expected transport error metric")
	}
}
