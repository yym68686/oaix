package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func signedTestDocument(email, account string) []byte {
	raw, _ := json.Marshal(map[string]any{"accounts": []any{map[string]any{"name": email, "platform": "openai", "type": "oauth", "credentials": map[string]any{"access_token": testJWT(email, account, EligiblePlan), "refresh_token": "fixture-refresh", "email": email, "chatgpt_account_id": account}}}, "x_revive_manifest": map[string]any{"issuer": "signed-recovery", "signature": "fixture-signature", "records": []any{map[string]any{"index": 0}}}})
	return append([]byte("\n  "), append(raw, '\n')...)
}
func TestSignedRecoveryProtocolAndResume(t *testing.T) {
	doc := signedTestDocument("owner@example.test", "workspace")
	for _, resume := range []bool{false, true} {
		t.Run(fmt.Sprint(resume), func(t *testing.T) {
			posts := 0
			saved := []string{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Has("task_token") {
					t.Error("task token in URL")
				}
				switch r.URL.Path {
				case "/api/revive/v1/verify/start":
					posts++
					if saved[len(saved)-1] != "verify_submitting" {
						t.Error("POST not fenced")
					}
					fmt.Fprint(w, `{"ok":true,"job":{"job_id":"job","task_token":"private-task-token","status":"queued"}}`)
				case "/api/revive/v1/verify/job":
					if r.Header.Get("X-Revive-Task-Token") != "private-task-token" {
						t.Error("missing task authorization")
					}
					fmt.Fprint(w, `{"ok":true,"job":{"status":"completed","preflight_id":"preflight","normal_count":0,"unauthorized_count":1}}`)
				case "/api/revive/v1/tasks":
					posts++
					if r.URL.Query().Get("preflight_id") != "preflight" || r.URL.Query().Get("auto_start") != "1" {
						t.Error("wrong task parameters")
					}
					fmt.Fprint(w, `{"ok":true,"task":{"task_id":"task","status":"running"}}`)
				case "/api/revive/v1/tasks/task":
					fmt.Fprint(w, `{"ok":true,"task":{"status":"recovered","all_download_ready":true}}`)
				case "/api/revive/v1/tasks/task/download":
					if r.URL.Query().Get("scope") != "all" {
						t.Error("lost normal accounts")
					}
					w.Write(doc)
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer server.Close()
			c := NewSigned(server.URL)
			c.PollEvery = time.Millisecond
			s := SignedSession{}
			if resume {
				s = SignedSession{Stage: "recovering", JobID: "job", TaskToken: "private-task-token", PreflightID: "preflight", TaskID: "task"}
			}
			raw, err := c.Recover(context.Background(), doc, &s, func(x SignedSession) error { saved = append(saved, x.Stage); return nil })
			if err != nil {
				t.Fatal(err)
			}
			if string(raw) != string(doc) {
				t.Fatal("signed bytes changed")
			}
			want := 2
			if resume {
				want = 0
			}
			if posts != want {
				t.Fatalf("POST count %d expected %d", posts, want)
			}
		})
	}
}
func TestSignedRecoveryAmbiguousPOSTCannotRepeat(t *testing.T) {
	calls := 0
	s := SignedSession{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(502) }))
	defer server.Close()
	c := NewSigned(server.URL)
	_, err := c.Recover(context.Background(), signedTestDocument("owner@example.test", "workspace"), &s, func(SignedSession) error { return nil })
	if err == nil || calls != 1 || s.Stage != "verify_submitting" {
		t.Fatalf("err=%v calls=%d stage=%s", err, calls, s.Stage)
	}
	_, err = c.Recover(context.Background(), signedTestDocument("owner@example.test", "workspace"), &s, func(SignedSession) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "submission_uncertain") || calls != 1 {
		t.Fatal("ambiguous submission repeated")
	}
}
func TestSignedRecoveryPersistenceFailureStopsSubmission(t *testing.T) {
	c := NewSigned("http://127.0.0.1:1")
	_, err := c.Recover(context.Background(), signedTestDocument("owner@example.test", "workspace"), &SignedSession{}, func(SignedSession) error { return errors.New("storage unavailable") })
	if err == nil || err.Error() != "storage unavailable" {
		t.Fatalf("err=%v", err)
	}
}
func TestSignedRecoveryDoesNotFollowRedirect(t *testing.T) {
	calls := 0
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++ }))
	defer other.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, other.URL, 307) }))
	defer server.Close()
	_, err := NewSigned(server.URL).Recover(context.Background(), signedTestDocument("owner@example.test", "workspace"), &SignedSession{}, func(SignedSession) error { return nil })
	if err == nil || calls != 0 {
		t.Fatal("signed document followed external redirect")
	}
}

func TestSignedDocumentWithOtherPlanNeverSubmitted(t *testing.T) {
	raw := signedTestDocument("owner@example.test", "workspace")
	var root map[string]any
	json.Unmarshal(raw, &root)
	account := root["accounts"].([]any)[0].(map[string]any)
	account["credentials"].(map[string]any)["access_token"] = testJWT("owner@example.test", "workspace", "business")
	raw, _ = json.Marshal(root)
	writes := 0
	_, err := NewSigned("http://127.0.0.1:1").Recover(context.Background(), raw, &SignedSession{}, func(SignedSession) error { writes++; return nil })
	if err == nil || !strings.Contains(err.Error(), "ineligible_plan") || writes != 0 {
		t.Fatal("other subscription reached submission stage")
	}
}
