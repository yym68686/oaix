package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestObserveHTTPRecordsTemplatedRoute(t *testing.T) {
	app := &App{httpRoutes: newHTTPRouteMetrics()}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/items/{item_id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("unavailable"))
	})
	request := httptest.NewRequest(http.MethodGet, "/api/items/42", nil)
	response := httptest.NewRecorder()

	app.observeHTTP(mux).ServeHTTP(response, request)

	items := app.httpRoutes.snapshot()
	if len(items) != 1 {
		t.Fatalf("metrics = %#v", items)
	}
	if items[0].Route != "/api/items/{item_id}" || items[0].Requests != 1 || items[0].Errors != 1 {
		t.Fatalf("metric = %#v", items[0])
	}
	if items[0].ResponseBytes != uint64(len("unavailable")) {
		t.Fatalf("response bytes = %d", items[0].ResponseBytes)
	}
	if got := response.Header().Get("X-Request-ID"); len(got) != 32 {
		t.Fatalf("generated request id = %q", got)
	}
}

func TestObservedResponseWriterPreservesFlush(t *testing.T) {
	recorder := httptest.NewRecorder()
	writer := &observedResponseWriter{ResponseWriter: recorder}
	if _, ok := any(writer).(http.Flusher); !ok {
		t.Fatal("observed writer must preserve streaming flush support")
	}
	writer.Flush()
	if !recorder.Flushed {
		t.Fatal("flush was not forwarded")
	}
}

func TestWriteHTTPRouteMetricsUsesBoundedRouteLabels(t *testing.T) {
	app := &App{httpRoutes: newHTTPRouteMetrics()}
	app.httpRoutes.observe(http.MethodGet, "/api/items/{item_id}", http.StatusOK, slowOwnedAPIDuration, 0, 10)
	var output strings.Builder
	app.writeHTTPRouteMetrics(&output)
	text := output.String()
	if !strings.Contains(text, `route="/api/items/{item_id}"`) {
		t.Fatalf("metrics missing route label: %s", text)
	}
	if !strings.Contains(text, "oaix_http_request_slow_total") {
		t.Fatalf("metrics missing slow counter: %s", text)
	}
	if !strings.Contains(text, "oaix_http_request_first_byte_observations_total") {
		t.Fatalf("metrics missing first-byte observation counter: %s", text)
	}
}
