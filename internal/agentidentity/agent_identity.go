package agentidentity

import (
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/nacl/box"
)

const (
	AuthMode               = "agentIdentity"
	SyntheticRefreshPrefix = "__oaix_agent_identity__:"
	DefaultAuthAPIBaseURL  = "https://auth.openai.com/api/accounts"
)

type Credentials struct {
	RuntimeID   string
	PrivateKey  string
	TaskID      string
	AccountID   string
	UserID      string
	Email       string
	PlanType    string
	WorkspaceID string
	FedRAMP     bool
}

type RequestDoer interface {
	Do(context.Context, *http.Request) (*http.Response, error)
}

type taskRegistrationResponse struct {
	TaskID               string `json:"task_id"`
	TaskIDCamel          string `json:"taskId"`
	EncryptedTaskID      string `json:"encrypted_task_id"`
	EncryptedTaskIDCamel string `json:"encryptedTaskId"`
}

func NormalizePayload(payload map[string]any) (map[string]any, bool) {
	if len(payload) == 0 {
		return nil, false
	}
	source := payload
	if nested, ok := mapValue(payload, "agent_identity", "agentIdentity"); ok {
		source = nested
	} else if credentials, ok := mapValue(payload, "credentials"); ok {
		source = credentials
		if nested, nestedOK := mapValue(credentials, "agent_identity", "agentIdentity"); nestedOK {
			source = nested
		}
	}
	authMode := firstString(source, "auth_mode", "authMode")
	if authMode == "" {
		authMode = firstString(payload, "auth_mode", "authMode")
	}
	isAgent := strings.EqualFold(authMode, AuthMode) ||
		firstString(source, "agent_runtime_id", "agentRuntimeId") != "" ||
		firstString(source, "agent_private_key", "agentPrivateKey") != ""
	if !isAgent {
		return nil, false
	}

	out := make(map[string]any, len(payload)+12)
	for key, value := range payload {
		if key == "credentials" || key == "agent_identity" || key == "agentIdentity" {
			continue
		}
		out[key] = value
	}
	out["auth_mode"] = AuthMode
	copyCanonicalString(out, source, "agent_runtime_id", "agent_runtime_id", "agentRuntimeId")
	copyCanonicalString(out, source, "agent_private_key", "agent_private_key", "agentPrivateKey")
	copyCanonicalString(out, source, "task_id", "task_id", "taskId")
	copyCanonicalString(out, source, "account_id", "account_id", "accountId", "chatgpt_account_id", "chatgptAccountId")
	copyCanonicalString(out, source, "chatgpt_account_id", "chatgpt_account_id", "chatgptAccountId", "account_id", "accountId")
	copyCanonicalString(out, source, "chatgpt_user_id", "chatgpt_user_id", "chatgptUserId", "user_id", "userId")
	copyCanonicalString(out, source, "email", "email")
	copyCanonicalString(out, source, "plan_type", "plan_type", "planType")
	copyCanonicalString(out, source, "workspace_id", "workspace_id", "workspaceId")
	copyCanonicalString(out, source, "id_token", "id_token", "idToken")
	if value, ok := firstBool(source, "chatgpt_account_is_fedramp", "chatgptAccountIsFedramp"); ok {
		out["chatgpt_account_is_fedramp"] = value
	}
	return out, true
}

func Parse(payload map[string]any) (Credentials, bool, error) {
	normalized, ok := NormalizePayload(payload)
	if !ok {
		return Credentials{}, false, nil
	}
	credentials := Credentials{
		RuntimeID:   firstString(normalized, "agent_runtime_id"),
		PrivateKey:  firstString(normalized, "agent_private_key"),
		TaskID:      firstString(normalized, "task_id"),
		AccountID:   firstString(normalized, "chatgpt_account_id", "account_id"),
		UserID:      firstString(normalized, "chatgpt_user_id"),
		Email:       firstString(normalized, "email"),
		PlanType:    firstString(normalized, "plan_type"),
		WorkspaceID: firstString(normalized, "workspace_id"),
	}
	credentials.FedRAMP, _ = firstBool(normalized, "chatgpt_account_is_fedramp")
	if err := credentials.Validate(); err != nil {
		return Credentials{}, true, err
	}
	return credentials, true, nil
}

func (c Credentials) Validate() error {
	switch {
	case strings.TrimSpace(c.RuntimeID) == "":
		return errors.New("agent identity is missing agent_runtime_id")
	case strings.TrimSpace(c.PrivateKey) == "":
		return errors.New("agent identity is missing agent_private_key")
	case strings.TrimSpace(c.AccountID) == "":
		return errors.New("agent identity is missing account_id")
	case strings.TrimSpace(c.UserID) == "":
		return errors.New("agent identity is missing chatgpt_user_id")
	}
	_, err := c.privateKey()
	return err
}

func (c Credentials) IdentityToken() string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(c.RuntimeID)))
	return SyntheticRefreshPrefix + hex.EncodeToString(digest[:])
}

func (c Credentials) BuildAssertion(now time.Time) (string, error) {
	privateKey, err := c.privateKey()
	if err != nil {
		return "", err
	}
	runtimeID := strings.TrimSpace(c.RuntimeID)
	taskID := strings.TrimSpace(c.TaskID)
	if runtimeID == "" || taskID == "" {
		return "", errors.New("agent identity runtime or task id is missing")
	}
	timestamp := now.UTC().Format(time.RFC3339)
	signature, err := privateKey.Sign(nil, []byte(runtimeID+":"+taskID+":"+timestamp), crypto.Hash(0))
	if err != nil {
		return "", errors.New("failed to sign agent assertion")
	}
	envelope, err := json.Marshal(map[string]string{
		"agent_runtime_id": runtimeID,
		"task_id":          taskID,
		"timestamp":        timestamp,
		"signature":        base64.StdEncoding.EncodeToString(signature),
	})
	if err != nil {
		return "", errors.New("failed to serialize agent assertion")
	}
	return "AgentAssertion " + base64.RawURLEncoding.EncodeToString(envelope), nil
}

func RegisterTask(ctx context.Context, doer RequestDoer, baseURL string, credentials Credentials) (string, error) {
	if doer == nil {
		return "", errors.New("agent task registration transport is unavailable")
	}
	privateKey, err := credentials.privateKey()
	if err != nil {
		return "", err
	}
	runtimeID := strings.TrimSpace(credentials.RuntimeID)
	if runtimeID == "" {
		return "", errors.New("agent identity runtime id is missing")
	}
	timestamp := time.Now().UTC().Format(time.RFC3339)
	signature, err := privateKey.Sign(nil, []byte(runtimeID+":"+timestamp), crypto.Hash(0))
	if err != nil {
		return "", errors.New("failed to sign agent task registration")
	}
	body, err := json.Marshal(map[string]string{
		"timestamp": timestamp,
		"signature": base64.StdEncoding.EncodeToString(signature),
	})
	if err != nil {
		return "", errors.New("failed to serialize agent task registration")
	}
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = DefaultAuthAPIBaseURL
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/agent/"+runtimeID+"/task/register", strings.NewReader(string(body)))
	if err != nil {
		return "", errors.New("failed to build agent task registration request")
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := doer.Do(ctx, request)
	if err != nil {
		// Transport errors often include the request URL. The runtime ID is part of
		// that URL, so keep the externally persisted/logged error generic.
		return "", errors.New("agent task registration request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64*1024))
		return "", fmt.Errorf("agent task registration returned status %d", response.StatusCode)
	}
	var result taskRegistrationResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, 64*1024)).Decode(&result); err != nil {
		return "", errors.New("agent task registration response is invalid")
	}
	if taskID := strings.TrimSpace(result.TaskID); taskID != "" {
		return taskID, nil
	}
	if taskID := strings.TrimSpace(result.TaskIDCamel); taskID != "" {
		return taskID, nil
	}
	encrypted := strings.TrimSpace(result.EncryptedTaskID)
	if encrypted == "" {
		encrypted = strings.TrimSpace(result.EncryptedTaskIDCamel)
	}
	if encrypted == "" {
		return "", errors.New("agent task registration response omitted task id")
	}
	return decryptTaskID(privateKey, encrypted)
}

func IsInvalidTaskResponse(statusCode int, body []byte) bool {
	if statusCode != http.StatusUnauthorized {
		return false
	}
	lower := strings.ToLower(string(body))
	compact := strings.NewReplacer(" ", "", "\t", "", "\r", "", "\n", "").Replace(lower)
	for _, marker := range []string{`"code":"invalid_task_id"`, `"code":"task_not_found"`, `"code":"task_expired"`, `"error":"invalid_task_id"`} {
		if strings.Contains(compact, marker) {
			return true
		}
	}
	for _, marker := range []string{"invalid task_id", "invalid task id", "task_id is invalid", "task id is invalid", "task not found", "task expired", "unknown task_id", "unknown task id"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func SanitizedPayload(payload map[string]any) map[string]any {
	if payload == nil {
		return nil
	}
	out := cloneMap(payload)
	delete(out, "agent_private_key")
	delete(out, "agentPrivateKey")
	for _, key := range []string{"credentials", "agent_identity", "agentIdentity"} {
		if nested, ok := out[key].(map[string]any); ok {
			clean := cloneMap(nested)
			delete(clean, "agent_private_key")
			delete(clean, "agentPrivateKey")
			out[key] = clean
		}
	}
	return out
}

func RedactSensitiveBody(body []byte, credentials Credentials) []byte {
	if len(body) == 0 {
		return body
	}
	redacted := string(body)
	for _, value := range []string{credentials.PrivateKey, credentials.RuntimeID, credentials.TaskID} {
		if value = strings.TrimSpace(value); value != "" {
			redacted = strings.ReplaceAll(redacted, value, "[redacted]")
		}
	}
	const prefix = "AgentAssertion "
	for offset := 0; offset < len(redacted); {
		relative := strings.Index(redacted[offset:], prefix)
		if relative < 0 {
			break
		}
		start := offset + relative + len(prefix)
		end := start
		for end < len(redacted) && !strings.ContainsRune(" \t\r\n\"',}", rune(redacted[end])) {
			end++
		}
		redacted = redacted[:start] + "[redacted]" + redacted[end:]
		offset = start + len("[redacted]")
	}
	return []byte(redacted)
}

func (c Credentials) privateKey() (ed25519.PrivateKey, error) {
	der, err := base64.StdEncoding.DecodeString(strings.TrimSpace(c.PrivateKey))
	if err != nil {
		return nil, errors.New("agent identity private key is not valid base64")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(der)
	if err != nil {
		return nil, errors.New("agent identity private key is not valid PKCS#8")
	}
	privateKey, ok := parsed.(ed25519.PrivateKey)
	if !ok || len(privateKey) != ed25519.PrivateKeySize {
		return nil, errors.New("agent identity private key is not Ed25519")
	}
	return privateKey, nil
}

func decryptTaskID(privateKey ed25519.PrivateKey, encoded string) (string, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil {
		return "", errors.New("encrypted agent task id is not valid base64")
	}
	digest := sha512.Sum512(privateKey.Seed())
	var curvePrivate [32]byte
	copy(curvePrivate[:], digest[:32])
	curvePrivate[0] &= 248
	curvePrivate[31] &= 127
	curvePrivate[31] |= 64
	curvePublicBytes, err := curve25519.X25519(curvePrivate[:], curve25519.Basepoint)
	if err != nil {
		return "", errors.New("failed to derive agent identity decryption key")
	}
	var curvePublic [32]byte
	copy(curvePublic[:], curvePublicBytes)
	plaintext, ok := box.OpenAnonymous(nil, ciphertext, &curvePublic, &curvePrivate)
	if !ok {
		return "", errors.New("failed to decrypt encrypted agent task id")
	}
	taskID := strings.TrimSpace(string(plaintext))
	if taskID == "" {
		return "", errors.New("decrypted agent task id is empty")
	}
	return taskID, nil
}

func mapValue(payload map[string]any, keys ...string) (map[string]any, bool) {
	for _, key := range keys {
		if value, ok := payload[key].(map[string]any); ok {
			return value, true
		}
	}
	return nil, false
}

func firstString(payload map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := payload[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstBool(payload map[string]any, keys ...string) (bool, bool) {
	for _, key := range keys {
		if value, ok := payload[key].(bool); ok {
			return value, true
		}
	}
	return false, false
}

func copyCanonicalString(dst map[string]any, src map[string]any, dstKey string, srcKeys ...string) {
	if value := firstString(src, srcKeys...); value != "" {
		dst[dstKey] = value
	}
}

func cloneMap(payload map[string]any) map[string]any {
	out := make(map[string]any, len(payload))
	for key, value := range payload {
		out[key] = value
	}
	return out
}
