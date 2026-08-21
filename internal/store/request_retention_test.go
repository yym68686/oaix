package store

import (
	"os"
	"strings"
	"testing"
)

func TestRequestLogRetentionRequiresFinalAnalyticsState(t *testing.T) {
	source, err := os.ReadFile("request_logs.go")
	if err != nil {
		t.Fatal(err)
	}
	text := strings.ToLower(strings.Join(strings.Fields(string(source)), " "))
	start := strings.Index(text, "delete from gateway_request_logs")
	if start < 0 {
		t.Fatal("request log retention delete not found")
	}
	end := strings.Index(text[start:], "delete from gateway_request_analytics_queue")
	if end < 0 {
		t.Fatal("request analytics queue cleanup boundary not found")
	}
	body := text[start : start+end]
	for _, fragment := range []string{"finished_at is not null", "analytics_recorded_at is not null", "analytics_recorded_at >= finished_at"} {
		if !strings.Contains(body, fragment) {
			t.Fatalf("request log retention missing final-state guard %q: %s", fragment, body)
		}
	}
	if !strings.Contains(body, "order by started_at, id") || !strings.Contains(body, "limit $2") {
		t.Fatalf("request log retention should use the bounded time-ordered batch: %s", body)
	}
}
