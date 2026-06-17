package oauth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseRefreshResponse(t *testing.T) {
	result, err := ParseRefreshResponse([]byte(`{"access_token":"at","refresh_token":"rt2","expires_in":3600,"account_id":"acct","email":"a@example.com","plan_type":"pro"}`))
	if err != nil {
		t.Fatalf("ParseRefreshResponse returned error: %v", err)
	}
	if result.AccessToken != "at" || result.RefreshToken != "rt2" || result.ExpiresIn != 3600 {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func TestParseRefreshResponseReadsIDTokenIdentity(t *testing.T) {
	idToken := testJWT(t, map[string]any{
		"email": "jwt@example.com",
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_account_id": "acct-jwt",
			"chatgpt_plan_type":  "pro",
		},
	})
	result, err := ParseRefreshResponse([]byte(`{"access_token":"at","refresh_token":"rt2","id_token":"` + idToken + `"}`))
	if err != nil {
		t.Fatalf("ParseRefreshResponse returned error: %v", err)
	}
	if result.AccountID != "acct-jwt" || result.Email != "jwt@example.com" || result.PlanType != "pro" {
		t.Fatalf("unexpected id_token identity: %#v", result)
	}
}

func TestClassifyStatus(t *testing.T) {
	if ClassifyStatus(http.StatusTooManyRequests) != ErrorClassRetryable {
		t.Fatal("429 should be retryable")
	}
	if ClassifyStatus(http.StatusUnauthorized) != ErrorClassInvalid {
		t.Fatal("401 should be invalid")
	}
	if ClassifyStatus(http.StatusTeapot) != ErrorClassTerminal {
		t.Fatal("418 should be terminal")
	}
}

func TestHTTPClientRefreshRetries(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}
		if r.Form.Get("client_id") != "client-fixture" || r.Form.Get("scope") != "scope-fixture" {
			t.Fatalf("missing oauth client metadata: %v", r.Form)
		}
		if r.Form.Get("grant_type") != "refresh_token" || r.Form.Get("refresh_token") != "rt" {
			t.Fatalf("unexpected form: %v", r.Form)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"at","refresh_token":"rt2","expires_in":3600}`))
	}))
	defer server.Close()
	client := NewHTTPClient(server.URL)
	client.ClientID = "client-fixture"
	client.Scope = "scope-fixture"
	client.MaxRetries = 2
	result, err := client.Refresh(context.Background(), "rt")
	if err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	if result.AccessToken != "at" || attempts != 2 {
		t.Fatalf("result=%#v attempts=%d", result, attempts)
	}
	stats := client.Stats()
	if stats.RefreshAttempts != 2 || stats.RefreshRetries != 1 || stats.RefreshSuccess != 1 || stats.RefreshFailures != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestHTTPClientExchangeAuthorizationCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}
		if r.Form.Get("grant_type") != "authorization_code" || r.Form.Get("client_id") != "client-fixture" {
			t.Fatalf("unexpected oauth form: %v", r.Form)
		}
		if r.Form.Get("code") != "code-fixture" || r.Form.Get("redirect_uri") != "https://oaix.example/auth/callback" || r.Form.Get("code_verifier") != "verifier-fixture" {
			t.Fatalf("unexpected oauth exchange params: %v", r.Form)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"at","refresh_token":"rt2","expires_in":3600}`))
	}))
	defer server.Close()
	client := NewHTTPClient(server.URL)
	client.ClientID = "client-fixture"
	result, err := client.ExchangeAuthorizationCode(context.Background(), AuthorizationCodeRequest{
		Code:         "code-fixture",
		RedirectURI:  "https://oaix.example/auth/callback",
		CodeVerifier: "verifier-fixture",
	})
	if err != nil {
		t.Fatalf("ExchangeAuthorizationCode returned error: %v", err)
	}
	if result.AccessToken != "at" || result.RefreshToken != "rt2" {
		t.Fatalf("unexpected exchange result: %#v", result)
	}
}

func TestDedupRefreshHistory(t *testing.T) {
	got := DedupRefreshHistory([]string{"rt1", "rt2", "rt1", "", "rt3"})
	want := []string{"rt1", "rt2", "rt3"}
	if len(got) != len(want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %#v want %#v", got, want)
		}
	}
}

func testJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]any{"alg": "none"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload) + "."
}
