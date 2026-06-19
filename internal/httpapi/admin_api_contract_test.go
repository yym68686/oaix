package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAdminOpenAPISpecContainsAPIizedResources(t *testing.T) {
	spec := adminOpenAPISpec()
	paths, ok := spec["paths"].(map[string]any)
	if !ok {
		t.Fatalf("paths missing: %#v", spec["paths"])
	}
	required := []string{
		"/admin/import/parse",
		"/admin/import/jobs",
		"/admin/oauth/openai/sessions/{session_id}",
		"/admin/analytics/cache",
		"/admin/prompt-cache/lanes",
		"/admin/audit-logs",
		"/admin/api-keys",
		"/api/auth/register",
		"/api/me/settings/{key}",
		"/api/tokens/{token_id}",
		"/api/import/jobs/{job_id}/tokens",
		"/api/admin/users/{user_id}/tokens",
		"/api/admin/pool-summary/by-user",
		"/api/admin/requests/export",
	}
	for _, path := range required {
		if _, ok := paths[path]; !ok {
			t.Fatalf("OpenAPI path %s missing", path)
		}
	}
}

func TestWriteErrorIncludesStableEnvelope(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeError(recorder, http.StatusConflict, errors.New("duplicate request"))
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d", recorder.Code)
	}
	var payload map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["detail"] != "duplicate request" {
		t.Fatalf("detail = %#v", payload["detail"])
	}
	errObj, _ := payload["error"].(map[string]any)
	if errObj["code"] != "conflict" || errObj["message"] != "duplicate request" {
		t.Fatalf("error envelope = %#v", errObj)
	}
}

func TestParseImportTextPayloadsSupportsDocumentedFormats(t *testing.T) {
	payloads, err := parseImportTextPayloads(`
# comment
account-a,rt-a
email@example.com----x----y----rt-b
rt-c
eyJhbGciOi.access.signature
`)
	if err != nil {
		t.Fatal(err)
	}
	if len(payloads) != 4 {
		t.Fatalf("payload count = %d: %#v", len(payloads), payloads)
	}
	if payloads[0]["account_id"] != "account-a" || payloads[0]["refresh_token"] != "rt-a" {
		t.Fatalf("account csv payload = %#v", payloads[0])
	}
	if payloads[1]["email"] != "email@example.com" || payloads[1]["refresh_token"] != "rt-b" {
		t.Fatalf("sub2api payload = %#v", payloads[1])
	}
	if payloads[2]["refresh_token"] != "rt-c" {
		t.Fatalf("refresh payload = %#v", payloads[2])
	}
	if payloads[3]["access_token"] == "" {
		t.Fatalf("access payload = %#v", payloads[3])
	}
}
