package testkit

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"time"
)

type FakeUpstream struct {
	Server *httptest.Server
}

func NewFakeUpstream() *FakeUpstream {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") == "" {
			writeFakeJSON(w, http.StatusUnauthorized, map[string]any{"error": map[string]any{"message": "missing bearer"}})
			return
		}
		var payload map[string]any
		_ = json.NewDecoder(r.Body).Decode(&payload)
		if payload["stream"] == true {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.WriteHeader(http.StatusOK)
			flusher, _ := w.(http.Flusher)
			for _, chunk := range []string{
				"event: response.output_text.delta\ndata: {\"delta\":\"ok\"}\n\n",
				"event: response.completed\ndata: {\"id\":\"resp_fake\"}\n\n",
			} {
				_, _ = w.Write([]byte(chunk))
				if flusher != nil {
					flusher.Flush()
				}
				time.Sleep(time.Millisecond)
			}
			return
		}
		writeFakeJSON(w, http.StatusOK, map[string]any{
			"id":          "resp_fake",
			"object":      "response",
			"model":       payload["model"],
			"output_text": "ok",
		})
	})
	return &FakeUpstream{Server: httptest.NewServer(mux)}
}

func (f *FakeUpstream) Close() {
	if f != nil && f.Server != nil {
		f.Server.Close()
	}
}

func (f *FakeUpstream) URL() string {
	if f == nil || f.Server == nil {
		return ""
	}
	return f.Server.URL
}

func writeFakeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
