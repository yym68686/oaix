package proxy

import (
	"net/http"
	"testing"
)

func TestPromptCacheExperimentSessionOverridesDerivedSession(t *testing.T) {
	headers := make(http.Header)
	headers.Set("Session_id", "experiment-session")
	headers.Set("X-OAIX-Experiment", "true")
	got, source := promptCacheSession(headers, "client", "prompt", "affinity", false)
	if got != "experiment-session" || source != "header" {
		t.Fatalf("session = %q, source = %q; want experiment-session/header", got, source)
	}
}
