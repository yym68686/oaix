package recovery

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testJWT(email, account, plan string) string {
	raw, _ := json.Marshal(map[string]any{"exp": time.Now().Add(time.Hour).Unix(), "https://api.openai.com/auth": map[string]any{"chatgpt_account_id": account, "chatgpt_plan_type": plan}, "https://api.openai.com/profile": map[string]any{"email": email}})
	return "e30." + base64.RawURLEncoding.EncodeToString(raw) + ".signature"
}

func TestRecoverProtocol(t *testing.T) {
	for _, mode := range []string{"sync", "async", "merged"} {
		t.Run(mode, func(t *testing.T) {
			email, account := "owner@example.test", "acct-workspace"
			payload := map[string]any{"accounts": []any{map[string]any{"credentials": map[string]any{"access_token": testJWT(email, account, EligiblePlan), "refresh_token": "new-refresh", "chatgpt_account_id": account}}}}
			result := map[string]any{"results": []any{map[string]any{"success": true, "payload": payload}}}
			if mode == "merged" {
				result = map[string]any{"mergedPayload": payload}
			}
			posts, gets := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodPost {
					posts++
					var input map[string]any
					if json.NewDecoder(r.Body).Decode(&input) != nil {
						t.Error("invalid submitted JSON")
					}
					if len(input) != 1 || len(input["accountNames"].([]any)) != 1 || input["accountNames"].([]any)[0] != email+"----workspace" {
						t.Errorf("unexpected payload: %v", input)
					}
					if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
						t.Error("credentials leaked into website request")
					}
					if mode == "sync" {
						result["success"] = true
						result["status"] = "success"
						_ = json.NewEncoder(w).Encode(result)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "status": "running", "jobKey": "cdk-replenish:test"})
					return
				}
				gets++
				if !strings.HasSuffix(r.URL.Path, "/status/cdk-replenish:test") {
					t.Errorf("status path: %s", r.URL.Path)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": map[string]any{"status": "success", "success": 1, "result": result}})
			}))
			defer server.Close()
			client := New(server.URL)
			client.PollEvery = time.Millisecond
			got, err := client.Recover(context.Background(), email+"----workspace")
			if err != nil {
				t.Fatal(err)
			}
			if got.RefreshToken != "new-refresh" {
				t.Fatal("missing fresh credential")
			}
			if err := Validate(got, email, account); err != nil {
				t.Fatal(err)
			}
			if posts != 1 || (mode != "sync" && gets != 1) {
				t.Fatalf("posts=%d gets=%d", posts, gets)
			}
		})
	}
}

func TestRecoverHTTP200IsNotAccountSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"status":"success","results":[{"success":false,"message":"workspace selection failed: private server details"}]}`))
	}))
	defer server.Close()
	_, err := New(server.URL).Recover(context.Background(), "owner@example.test----workspace")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "workspace_unavailable" {
		t.Fatalf("err=%v", err)
	}
	if strings.Contains(err.Error(), "private server details") {
		t.Fatal("server data leaked into diagnostic error")
	}
}

func TestRecoveredCredentialIdentityMustMatch(t *testing.T) {
	base := Result{Email: "owner@example.test", AccountID: "workspace", PlanType: EligiblePlan, ExpiresAt: time.Now().Add(time.Hour)}
	for _, field := range []string{"email", "workspace", "business", "team", "free", "expired"} {
		t.Run(field, func(t *testing.T) {
			result := base
			switch field {
			case "email":
				result.Email = "other@example.test"
			case "workspace":
				result.AccountID = "other"
			case "expired":
				result.ExpiresAt = time.Now().Add(-time.Minute)
			default:
				result.PlanType = field
			}
			if Validate(result, base.Email, base.AccountID) == nil {
				t.Fatalf("accepted %s mismatch", field)
			}
		})
	}
}

func TestRecoverCanceledPollAndSingleSubmission(t *testing.T) {
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts++
			_, _ = w.Write([]byte(`{"success":true,"status":"running","jobKey":"job"}`))
			return
		}
		_, _ = w.Write([]byte(`{"success":true,"data":{"status":"running","success":0}}`))
	}))
	defer server.Close()
	client := New(server.URL)
	client.PollEvery = time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := client.Recover(ctx, "owner@example.test----workspace")
	if !errors.Is(err, context.DeadlineExceeded) || posts != 1 {
		t.Fatalf("err=%v posts=%d", err, posts)
	}
}
