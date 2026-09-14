package httpapi

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/agentidentity"
	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/store"
)

const revokedProbeBody = `{"error":{"message":"Encountered invalidated oauth token for user, failing request","type":null,"code":"token_revoked","param":null},"status":401}`

func TestProbeTokenRevokedWithoutCredentialRefresh(t *testing.T) {
	for _, identity := range []bool{false, true} {
		t.Run(fmt.Sprintf("agent_identity=%v", identity), func(t *testing.T) {
			token := store.Token{ID: 7, AccessToken: "access", RefreshToken: "refresh"}
			if identity {
				credentials := probeAgentIdentityCredentials(t, "task-current")
				token.RefreshToken, token.AgentIdentity = credentials.IdentityToken(), &credentials
			}
			calls := 0
			app := &App{
				cfg: config.Config{Upstream: config.UpstreamConfig{ResponsesURL: "https://probe.invalid/responses"}},
				probeDoer: probeDoerFunc(func(_ context.Context, r *http.Request) (*http.Response, error) {
					calls++
					if r.URL.Path != "/responses" {
						t.Errorf("unexpected credential refresh: %s", r.URL.Path)
					}
					return &http.Response{StatusCode: 401, Body: io.NopCloser(strings.NewReader(revokedProbeBody))}, nil
				}),
			}
			result := app.probeTokenWithAccess(t.Context(), token, defaultAdminProbeModel)
			if calls != 1 || result["outcome"] != "disabled" || result["status_code"] != 401 || result["error_code"] != "token_revoked" || result["raw_response"] != revokedProbeBody {
				t.Fatalf("calls=%d, result=%#v", calls, result)
			}
			if !strings.Contains(fmt.Sprint(result["message"]), "token_revoked") {
				t.Fatal("missing revocation explanation")
			}
		})
	}
}

func TestProbeAgentIdentityRegistrationFailureClassification(t *testing.T) {
	for _, taskID := range []string{"task-old", ""} {
		for _, tc := range []struct {
			name     string
			status   int
			body     string
			disabled bool
		}{
			{"revoked", 401, revokedProbeBody, true},
			{"ordinary unauthorized", 401, `{"error":{"code":"unauthorized"}}`, false},
			{"message only", 401, `{"error":{"message":"token_revoked"}}`, false},
			{"server error", 502, revokedProbeBody, false},
		} {
			t.Run(taskID+"/"+tc.name, func(t *testing.T) {
				credentials := probeAgentIdentityCredentials(t, taskID)
				credentialStore := &fakeAgentIdentityProbeStore{credentials: credentials}
				registrations, probes := 0, 0
				app := &App{
					cfg:                config.Config{Upstream: config.UpstreamConfig{ResponsesURL: "https://probe.invalid/responses"}},
					probeIdentityStore: credentialStore,
					probeDoer: probeDoerFunc(func(_ context.Context, r *http.Request) (*http.Response, error) {
						status, body := 401, `{"error":{"code":"invalid_task_id"}}`
						if strings.HasSuffix(r.URL.Path, "/task/register") {
							registrations++
							status, body = tc.status, tc.body
						} else {
							probes++
						}
						return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}, nil
					}),
				}
				result := app.probeTokenWithAccess(t.Context(), store.Token{ID: 7, RefreshToken: credentials.IdentityToken(), AgentIdentity: &credentials}, defaultAdminProbeModel)
				want := "inconclusive"
				if tc.disabled {
					want = "disabled"
				}
				wantProbes := 1
				if taskID == "" {
					wantProbes = 0
				}
				if result["outcome"] != want || result["status_code"] != tc.status || result["raw_response"] != tc.body || result["upstream_attempted"] != true || result["probe_stage"] != probeStageCredentialPreparation {
					t.Fatalf("result=%#v", result)
				}
				if registrations != 1 || probes != wantProbes || credentialStore.updateCalls != 0 {
					t.Fatalf("registrations=%d probes=%d updates=%d", registrations, probes, credentialStore.updateCalls)
				}
			})
		}
	}
}

func TestProbeOAuthRefreshFailureDoesNotClaimAgentIdentity(t *testing.T) {
	app := &App{cfg: config.Config{Upstream: config.UpstreamConfig{ResponsesURL: "https://probe.invalid/responses"}}, probeDoer: probeDoerFunc(func(context.Context, *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 401, Body: io.NopCloser(strings.NewReader(`{"error":{"code":"unauthorized"}}`))}, nil
	})}
	// With no OAuth endpoint configured the refresh fails locally after the 401.
	result := app.probeTokenWithAccess(t.Context(), store.Token{ID: 7, AccessToken: "access", RefreshToken: "refresh"}, defaultAdminProbeModel)
	if result["outcome"] != "inconclusive" || !strings.Contains(fmt.Sprint(result["message"]), "OAuth") || strings.Contains(fmt.Sprint(result["message"]), "Agent Identity") {
		t.Fatalf("result=%#v", result)
	}
}

func TestProbeRevokedPersistsDisabledStateWithDatabase(t *testing.T) {
	h := newMultiUserHarness(t)
	owner, key := h.createUser(t, "revoked-probe")
	for _, prefix := range []string{"/api/tokens", "/admin/tokens"} {
		for _, mode := range []string{"oauth", "agent-direct", "agent-recovery", "agent-register"} {
			t.Run(prefix+"/"+mode, func(t *testing.T) {
				var token store.Token
				if mode == "oauth" {
					token = h.createToken(t, owner.ID, "revoked-probe")
				} else {
					credentials := probeAgentIdentityCredentials(t, "task-old")
					credentials.RuntimeID = fmt.Sprintf("runtime-revoked-%d", time.Now().UnixNano())
					if mode == "agent-register" {
						credentials.TaskID = ""
					}
					result, err := h.db.UpsertTokenPayloadsForOwner(t.Context(), owner.ID, []map[string]any{{
						"auth_mode": agentidentity.AuthMode, "agent_runtime_id": credentials.RuntimeID,
						"agent_private_key": credentials.PrivateKey, "task_id": credentials.TaskID,
						"account_id": credentials.AccountID, "chatgpt_user_id": credentials.UserID, "plan_type": "pro",
					}}, "revoked-test")
					if err != nil || len(result.Tokens) != 1 {
						t.Fatalf("import failed: %v", err)
					}
					token = result.Tokens[0]
				}
				if _, err := h.db.SetTokenCooldown(t.Context(), token.ID, time.Now().Add(time.Hour), "upstream usage limit cooldown"); err != nil {
					t.Fatal(err)
				}
				h.app.SetProbeRequestDoer(probeDoerFunc(func(_ context.Context, r *http.Request) (*http.Response, error) {
					body := revokedProbeBody
					if mode == "agent-recovery" && !strings.HasSuffix(r.URL.Path, "/task/register") {
						body = `{"error":{"code":"invalid_task_id"}}`
					}
					return &http.Response{StatusCode: 401, Body: io.NopCloser(strings.NewReader(body))}, nil
				}))
				// Each fixture uses the current transport, including task registration.
				h.app.agentIdentityTasks = nil
				auth := key.PlaintextKey
				if prefix == "/admin/tokens" {
					auth = "service-test-key"
				}
				response := expectStatus(t, h.request(t, http.MethodPost, fmt.Sprintf("%s/%d/probe", prefix, token.ID), auth, `{}`), 200)
				if response["outcome"] != "disabled" || response["error_code"] != "token_revoked" {
					t.Fatalf("result=%#v", response)
				}
				current, err := h.db.GetToken(t.Context(), token.ID)
				if err != nil {
					t.Fatal(err)
				}
				if current.IsActive || current.DisabledAt == nil || current.CooldownUntil != nil || current.AccessToken != "" {
					t.Fatal("disable did not clear cooling/credentials and set disabled_at")
				}
				var persisted bool
				err = h.db.Pool().QueryRow(t.Context(), `select exists (
					select 1 from token_runtime_state r join token_state_events e on e.token_id=r.token_id
					where r.token_id=$1 and r.cooldown_until is null and r.disabled_reason is not null
					and e.event_type='disabled' and e.status_code=401 and e.previous_is_active and not e.next_is_active
				) and not exists(select 1 from token_secrets where token_id=$1 and access_token is not null)`, token.ID).Scan(&persisted)
				if err != nil || !persisted {
					t.Fatalf("runtime/event/secret persistence: %v, err=%v", persisted, err)
				}
			})
		}
	}
}
