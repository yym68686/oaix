package admindiag

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestExporterPreservesQueryIdentityAndAcknowledgement(t *testing.T) {
	var payload map[string]any
	reply := `{}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/traces" {
			t.Error(r.URL.Path)
		}
		json.NewDecoder(r.Body).Decode(&payload)
		w.Write([]byte(reply))
	}))
	defer server.Close()
	e := NewExporter(server.URL, map[string]string{"service.name": "oaix"})
	defer e.Close()
	end := int64(123)
	d := Record{ID: strings.Repeat("1", 32), TraceID: strings.Repeat("2", 32), RequestID: "test", StartedAt: time.Now(), DurationUS: 123, Status: 503, Events: []Event{{ID: "1", Kind: "query", Stage: "pending", EndUS: &end, Error: "deadline"}}, Samples: []Sample{{QueryID: "1", State: "active", WaitType: "IO", Wait: "DataFileRead"}}}
	if err := e.Export(context.Background(), d, nil); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(payload)
	for _, s := range []string{"pending.query", "postgres_wait_sample", "DataFileRead", "observation_id"} {
		if !strings.Contains(string(raw), s) {
			t.Fatal(string(raw))
		}
	}
	reply = `{"partialSuccess":{"rejectedSpans":"1"}}`
	if e.Export(context.Background(), d, nil) == nil {
		t.Fatal("partial ack accepted")
	}
	reply = `not-json`
	if e.Export(context.Background(), d, nil) == nil {
		t.Fatal("invalid ack accepted")
	}
}
