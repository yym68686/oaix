package httpapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/recovery"
	"github.com/yym68686/oaix/internal/store"
)

func signedFixture(t *testing.T, email, account string) []byte {
	t.Helper()
	claims, _ := json.Marshal(map[string]any{"exp": time.Now().Add(time.Hour).Unix(), "https://api.openai.com/auth": map[string]any{"chatgpt_account_id": account, "chatgpt_plan_type": recovery.EligiblePlan}, "https://api.openai.com/profile": map[string]any{"email": email}})
	at := "e30." + base64.RawURLEncoding.EncodeToString(claims) + ".fixture"
	raw, _ := json.Marshal(map[string]any{"accounts": []any{map[string]any{"name": email, "platform": "openai", "type": "oauth", "credentials": map[string]any{"access_token": at, "refresh_token": "fixture-rt-" + email, "email": email, "chatgpt_account_id": account, "plan_type": recovery.EligiblePlan}}}, "proxies": []any{}, "x_revive_manifest": map[string]any{"issuer": "signed-recovery", "signature": "fixture", "records": []any{map[string]any{"index": 0}}}})
	return append([]byte("\n  "), append(raw, '\n')...)
}
func TestSignedRecoveryImportAndWorkerIntegration(t *testing.T) {
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires isolated test DB")
	}
	ctx := context.Background()
	db, err := store.Connect(ctx, config.DatabaseConfig{URL: dsn, MaxConns: 8, ConnectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	owner, err := db.BootstrapUserID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	account := "12345678-1234-4234-8234-123456abcdef"
	email := fmt.Sprintf("signed-%d@example.test", time.Now().UnixNano())
	raw := signedFixture(t, email, account)
	a := NewApp(config.Config{Recovery401: config.Recovery401Config{Enabled: true}}, nil, db, nil, nil, nil)
	// Exercise actual upload -> preview reference -> normalized import path.
	var form bytes.Buffer
	mp := multipart.NewWriter(&form)
	part, _ := mp.CreateFormFile("files", "signed.json")
	part.Write(raw)
	mp.Close()
	req := httptest.NewRequest("POST", "/admin/import/upload", &form)
	req.Header.Set("Content-Type", mp.FormDataContentType())
	req = withAuthContext(req, &AuthContext{IsService: true})
	rec := httptest.NewRecorder()
	a.uploadImport(rec, req)
	if rec.Code != 200 {
		t.Fatalf("upload status=%d", rec.Code)
	}
	var preview struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &preview); err != nil {
		t.Fatal(err)
	}
	ref := preview.Items[0][recovery.DocumentIDField]
	if ref == nil {
		t.Fatal("upload lost signed file")
	}
	normalized, _, err := parseImportPayload(map[string]any{"tokens": []any{preview.Items[0]}})
	if err != nil {
		t.Fatal(err)
	}
	id, _ := strconv.ParseInt(fmt.Sprint(ref), 10, 64)
	var encrypted string
	if err := db.Pool().QueryRow(ctx, `select original_ciphertext from recovery_documents where id=$1`, id).Scan(&encrypted); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(encrypted, email) || strings.Contains(encrypted, "fixture-rt") {
		t.Fatal("document persisted as plaintext")
	}
	// Publish already validated access credentials with the staged reference.
	doc, _ := recovery.ParseSignedDocument(raw)
	p := normalized[0]
	p["access_token"] = doc.Accounts[0].Credentials.AccessToken
	result, err := db.UpsertTokenPayloadsForOwner(ctx, owner, normalized, "signed-test")
	if err != nil {
		t.Fatal(err)
	}
	token := result.Tokens[0]
	saved, err := db.RecoveryDocumentForToken(ctx, owner, token.ID)
	if err != nil || saved == nil || !bytes.Equal(saved.Raw, raw) {
		t.Fatalf("original signed bytes were lost: %v", err)
	}
	if other, err := db.RecoveryDocumentForToken(ctx, owner+999, token.ID); err != nil || other != nil {
		t.Fatal("cross owner recovery document leaked")
	}
	// An arbitrary reference from another owner cannot bind or update a token.
	otherID, err := db.SaveRecoveryDocument(ctx, owner, signedFixture(t, "different@example.test", account))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.BindRecoveryDocument(ctx, owner, token.ID, otherID); err == nil {
		t.Fatal("foreign account identity bound")
	}
	if err := db.BindRecoveryDocument(ctx, owner+999, token.ID, id); err == nil {
		t.Fatal("cross owner binding accepted")
	}
	saved, err = db.RecoveryDocumentForToken(ctx, owner, token.ID)
	if err != nil || saved.ID != id {
		t.Fatal("failed binding changed original mapping")
	}
	// The session bearer is encrypted and can be resumed after process restart.
	session := recovery.SignedSession{Stage: "verified", JobID: "job-stored", TaskToken: "session-private", PreflightID: "preflight-stored"}
	if err := db.SaveRecoverySession(ctx, saved, session); err != nil {
		t.Fatal(err)
	}
	var sessionCipher string
	if err := db.Pool().QueryRow(ctx, `select session_ciphertext from recovery_documents where id=$1`, id).Scan(&sessionCipher); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sessionCipher, session.TaskToken) {
		t.Fatal("session bearer stored in plaintext")
	}
	resumed, err := db.RecoveryDocumentForToken(ctx, owner, token.ID)
	if err != nil || resumed.Session.TaskToken != session.TaskToken {
		t.Fatal("session did not survive reload")
	}
	if err := db.SaveRecoverySession(ctx, saved, recovery.SignedSession{}); err != nil {
		t.Fatal(err)
	}
	// An existing legacy provider failure must not block the new signed provider.
	status := 401
	if err := db.MarkTokenErrorWithContext(ctx, token.ID, "upstream 401", true, nil, store.TokenStateEventContext{StatusCode: &status}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool().Exec(ctx, `insert into token_state_events(token_id,owner_user_id,event_type,reason,metadata) values($1,$2,'oauth_401_recovery_result','workspace_record_not_found','{"provider":"5xteam"}')`, token.ID, owner); err != nil {
		t.Fatal(err)
	}
	ids, err := db.ListRecovery401Candidates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, i := range ids {
		if i == token.ID {
			found = true
		}
	}
	if !found {
		t.Fatal("old provider backoff blocked signed recovery")
	}
	calls := 0
	website := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/revive/v1/verify/start":
			calls++
			got, _ := io.ReadAll(r.Body)
			if !bytes.Equal(got, raw) {
				t.Error("uploaded bytes differ from signed source")
			}
			fmt.Fprint(w, `{"ok":true,"job":{"job_id":"job","task_token":"secret-session","status":"running"}}`)
		case "/api/revive/v1/verify/job":
			fmt.Fprint(w, `{"ok":true,"job":{"status":"completed","preflight_id":"preflight"}}`)
		case "/api/revive/v1/tasks":
			calls++
			fmt.Fprint(w, `{"ok":true,"task":{"task_id":"task","status":"normal"}}`)
		case "/api/revive/v1/tasks/task":
			fmt.Fprint(w, `{"ok":true,"task":{"status":"normal","all_download_ready":true}}`)
		default:
			w.Write(raw)
		}
	}))
	defer website.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"test\"}]}]}}\n\n")
	}))
	defer upstream.Close()
	a.cfg.Recovery401.SignedURL = website.URL
	a.cfg.Upstream.ResponsesURL = upstream.URL
	a.run401Recovery(ctx, token.ID)
	current, err := db.GetToken(ctx, token.ID)
	if err != nil || !current.IsActive || current.LastError != nil {
		t.Fatalf("signed recovery failed: %v", err)
	}
	if calls != 2 {
		t.Fatalf("unexpected submission count %d", calls)
	}
	statuses, err := db.RecoveryStatuses(ctx, []store.Token{*current})
	if err != nil {
		t.Fatal(err)
	}
	if statuses[token.ID].Status != "recovered" || !statuses[token.ID].HasSignedFile {
		t.Fatal("status missing signed recovery outcome")
	}
}
