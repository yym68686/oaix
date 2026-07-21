package agentidentity

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha512"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/nacl/box"
)

func testCredentials(t *testing.T) (Credentials, ed25519.PrivateKey) {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return Credentials{
		RuntimeID:  "runtime-test",
		PrivateKey: base64.StdEncoding.EncodeToString(der),
		TaskID:     "task-test",
		AccountID:  "account-test",
		UserID:     "user-test",
		Email:      "agent@example.invalid",
		PlanType:   "pro",
	}, privateKey
}

func TestNormalizePayloadReadsSub2APIAccountCredentials(t *testing.T) {
	credentials, _ := testCredentials(t)
	payload, ok := NormalizePayload(map[string]any{
		"name":     credentials.Email,
		"platform": "openai",
		"type":     "oauth",
		"credentials": map[string]any{
			"auth_mode":         AuthMode,
			"agent_runtime_id":  credentials.RuntimeID,
			"agent_private_key": credentials.PrivateKey,
			"task_id":           credentials.TaskID,
			"account_id":        credentials.AccountID,
			"chatgpt_user_id":   credentials.UserID,
			"email":             credentials.Email,
			"plan_type":         credentials.PlanType,
			"workspace_id":      "workspace-test",
		},
	})
	if !ok {
		t.Fatal("agent identity payload was not detected")
	}
	parsed, detected, err := Parse(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !detected || parsed.RuntimeID != credentials.RuntimeID || parsed.AccountID != credentials.AccountID || parsed.UserID != credentials.UserID {
		t.Fatalf("parsed credentials = %#v", parsed)
	}
	if parsed.IdentityToken() == "" || !strings.HasPrefix(parsed.IdentityToken(), SyntheticRefreshPrefix) {
		t.Fatalf("identity token = %q", parsed.IdentityToken())
	}
	if strings.Contains(string(mustJSON(t, SanitizedPayload(payload))), credentials.PrivateKey) {
		t.Fatal("sanitized payload leaked the private key")
	}
}

func TestBuildAssertionMatchesCodexEnvelopeAndSignature(t *testing.T) {
	credentials, privateKey := testCredentials(t)
	now := time.Date(2026, 7, 14, 8, 9, 10, 0, time.FixedZone("UTC+8", 8*60*60))
	assertion, err := credentials.BuildAssertion(now)
	if err != nil {
		t.Fatal(err)
	}
	encoded := strings.TrimPrefix(assertion, "AgentAssertion ")
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		RuntimeID string `json:"agent_runtime_id"`
		TaskID    string `json:"task_id"`
		Timestamp string `json:"timestamp"`
		Signature string `json:"signature"`
	}
	if err := json.Unmarshal(decoded, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.RuntimeID != credentials.RuntimeID || envelope.TaskID != credentials.TaskID || envelope.Timestamp != "2026-07-14T00:09:10Z" {
		t.Fatalf("assertion envelope = %#v", envelope)
	}
	signature, err := base64.StdEncoding.DecodeString(envelope.Signature)
	if err != nil {
		t.Fatal(err)
	}
	publicKey := privateKey.Public().(ed25519.PublicKey)
	if !ed25519.Verify(publicKey, []byte("runtime-test:task-test:2026-07-14T00:09:10Z"), signature) {
		t.Fatal("assertion signature did not verify")
	}
}

func TestRegisterTaskAcceptsPlaintextAndEncryptedResponses(t *testing.T) {
	credentials, privateKey := testCredentials(t)
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/agent/runtime-test/task/register" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["timestamp"] == "" || body["signature"] == "" {
			t.Errorf("registration body = %#v", body)
		}
		requestCount++
		if requestCount == 1 {
			_, _ = w.Write([]byte(`{"task_id":"task-plain"}`))
			return
		}
		digest := sha512.Sum512(privateKey.Seed())
		var curvePrivate [32]byte
		copy(curvePrivate[:], digest[:32])
		curvePrivate[0] &= 248
		curvePrivate[31] &= 127
		curvePrivate[31] |= 64
		publicBytes, err := curve25519.X25519(curvePrivate[:], curve25519.Basepoint)
		if err != nil {
			t.Error(err)
			return
		}
		var curvePublic [32]byte
		copy(curvePublic[:], publicBytes)
		ciphertext, err := box.SealAnonymous(nil, []byte("task-encrypted"), &curvePublic, rand.Reader)
		if err != nil {
			t.Error(err)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"encrypted_task_id": base64.StdEncoding.EncodeToString(ciphertext)})
	}))
	defer server.Close()
	doer := httpDoer{client: server.Client()}
	taskID, err := RegisterTask(context.Background(), doer, server.URL, credentials)
	if err != nil || taskID != "task-plain" {
		t.Fatalf("plaintext task = %q, err = %v", taskID, err)
	}
	taskID, err = RegisterTask(context.Background(), doer, server.URL, credentials)
	if err != nil || taskID != "task-encrypted" {
		t.Fatalf("encrypted task = %q, err = %v", taskID, err)
	}
}

func TestInvalidTaskDetectionAndRedaction(t *testing.T) {
	credentials, _ := testCredentials(t)
	if !IsInvalidTaskResponse(http.StatusUnauthorized, []byte(`{"error":{"code":"invalid_task_id"}}`)) {
		t.Fatal("invalid task response was not detected")
	}
	if IsInvalidTaskResponse(http.StatusUnauthorized, []byte(`{"error":{"code":"invalid_signature"}}`)) {
		t.Fatal("invalid signature was incorrectly treated as an invalid task")
	}
	body := []byte(credentials.RuntimeID + " " + credentials.TaskID + " " + credentials.PrivateKey + " AgentAssertion abc123")
	redacted := string(RedactSensitiveBody(body, credentials))
	for _, secret := range []string{credentials.RuntimeID, credentials.TaskID, credentials.PrivateKey, "AgentAssertion abc123"} {
		if strings.Contains(redacted, secret) {
			t.Fatalf("redacted body leaked %q", secret)
		}
	}
}

func TestRegisterTaskDoesNotLeakRuntimeInTransportErrors(t *testing.T) {
	credentials, _ := testCredentials(t)
	_, err := RegisterTask(context.Background(), failingDoer{}, "https://auth.example.invalid/api/accounts", credentials)
	if err == nil {
		t.Fatal("expected task registration to fail")
	}
	if strings.Contains(err.Error(), credentials.RuntimeID) || strings.Contains(err.Error(), "auth.example.invalid") {
		t.Fatalf("task registration error leaked request details: %q", err)
	}
}

type httpDoer struct {
	client *http.Client
}

func (d httpDoer) Do(ctx context.Context, request *http.Request) (*http.Response, error) {
	return d.client.Do(request.WithContext(ctx))
}

type failingDoer struct{}

func (failingDoer) Do(_ context.Context, request *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("post %s: network failed", request.URL)
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
