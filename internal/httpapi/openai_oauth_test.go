package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/yym68686/oaix/internal/config"
)

func TestStartOpenAIOAuthBuildsAuthorizationURL(t *testing.T) {
	app := NewApp(config.Config{
		Upstream: config.UpstreamConfig{
			OAuthClientID: "client-fixture",
			OAuthScope:    "openid profile email",
		},
	}, nil, nil, nil, nil, nil)
	body := strings.NewReader(`{"import_queue_position":"back"}`)
	request := httptest.NewRequest(http.MethodPost, "/admin/oauth/openai/start", body)
	request.Host = "oaix.example"
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Forwarded-Proto", "https")
	recorder := httptest.NewRecorder()

	app.startOpenAIOAuth(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	authURL, _ := url.Parse(payload["auth_url"].(string))
	if authURL.Scheme != "https" || authURL.Host != "auth.openai.com" || authURL.Path != "/oauth/authorize" {
		t.Fatalf("unexpected auth url: %s", authURL.String())
	}
	query := authURL.Query()
	if query.Get("client_id") != "client-fixture" || query.Get("response_type") != "code" {
		t.Fatalf("unexpected auth query: %s", authURL.RawQuery)
	}
	if query.Get("redirect_uri") != "https://oaix.example/auth/callback" {
		t.Fatalf("redirect_uri = %q", query.Get("redirect_uri"))
	}
	if !strings.Contains(query.Get("scope"), "offline_access") {
		t.Fatalf("scope missing offline_access: %q", query.Get("scope"))
	}
	if query.Get("code_challenge") == "" || query.Get("code_challenge_method") != "S256" || query.Get("state") == "" {
		t.Fatalf("missing pkce fields: %s", authURL.RawQuery)
	}
}
