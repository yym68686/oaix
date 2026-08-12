package proxy

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/yym68686/oaix/internal/protocol/openai"
	"github.com/yym68686/oaix/internal/tokens"
)

// CodexFingerprintContext contains the request-level inputs shared by every
// account attempt. Account-level IDs are resolved only after token selection.
type CodexFingerprintContext struct {
	ClientSessionID string
	TurnID          string
	TurnStartedAt   int64
}

type codexFingerprintIDs struct {
	InstallationID string
	SessionID      string
	ThreadID       string
	TurnID         string
	WindowID       string
	TurnStartedAt  int64
}

var fallbackTurnSequence atomic.Uint64

func buildCodexFingerprintContext(headers http.Header, intent RequestIntent) *CodexFingerprintContext {
	if !codexFingerprintEligible(intent) {
		return nil
	}
	return &CodexFingerprintContext{
		ClientSessionID: extractClientSessionID(headers),
		TurnID:          newUUIDv7(),
		TurnStartedAt:   time.Now().UnixMilli(),
	}
}

func codexFingerprintEligible(intent RequestIntent) bool {
	if intent.Compact || intent.Endpoint == "/v1/responses/compact" || intent.UpstreamEndpoint == "/v1/responses/compact" {
		return false
	}
	return intent.Endpoint == "/v1/responses" || intent.UpstreamEndpoint == "/v1/responses"
}

func extractClientSessionID(headers http.Header) string {
	if headers == nil {
		return ""
	}
	if value := strings.TrimSpace(headers.Get("session-id")); value != "" {
		return value
	}
	return strings.TrimSpace(headers.Get("session_id"))
}

func resolveCodexFingerprintIDs(claim *tokens.Claim, context *CodexFingerprintContext) *codexFingerprintIDs {
	if claim == nil || context == nil || claim.TokenID() <= 0 {
		return nil
	}
	accountSeed := codexFingerprintAccountSeed(claim)
	installationID := deterministicUUID("oaix:codex-install-id:v1:" + accountSeed)
	sessionID := deterministicUUID("oaix:codex-session-id:v1:" + accountSeed)
	threadID := sessionID
	if context.ClientSessionID != "" {
		threadID = deterministicUUID("oaix:codex-thread-id:v1:" + accountSeed + ":" + context.ClientSessionID)
	}
	return &codexFingerprintIDs{
		InstallationID: installationID,
		SessionID:      sessionID,
		ThreadID:       threadID,
		TurnID:         context.TurnID,
		WindowID:       threadID + ":0",
		TurnStartedAt:  context.TurnStartedAt,
	}
}

func codexFingerprintAccountSeed(claim *tokens.Claim) string {
	if accountID := claim.AccountID(); accountID != nil {
		if value := strings.TrimSpace(*accountID); value != "" {
			return "account-id:" + value
		}
	}
	return fmt.Sprintf("token-id:%d", claim.TokenID())
}

func codexFingerprintLogFields(claim *tokens.Claim, context *CodexFingerprintContext) (*string, *string) {
	ids := resolveCodexFingerprintIDs(claim, context)
	if ids == nil {
		return nil, nil
	}
	return nullable(shortHash(ids.SessionID, 64)), nullable("codex_account_session")
}

func applyCodexFingerprintHeaders(headers http.Header, ids *codexFingerprintIDs) {
	if headers == nil || ids == nil {
		return
	}
	headers.Set("x-codex-installation-id", ids.InstallationID)
	headers.Set("x-codex-window-id", ids.WindowID)
	headers.Set("x-client-request-id", ids.ThreadID)
	headers.Set("session-id", ids.SessionID)
	headers.Set("session_id", ids.SessionID)
	headers.Set("thread-id", ids.ThreadID)
	rewriteCodexTurnMetadata(headers, map[string]any{
		"installation_id":         ids.InstallationID,
		"session_id":              ids.SessionID,
		"thread_id":               ids.ThreadID,
		"turn_id":                 ids.TurnID,
		"window_id":               ids.WindowID,
		"turn_started_at_unix_ms": ids.TurnStartedAt,
	})
}

func rewriteCodexTurnMetadata(headers http.Header, fields map[string]any) {
	raw := strings.TrimSpace(headers.Get("x-codex-turn-metadata"))
	if raw == "" {
		return
	}
	var metadata map[string]any
	if err := json.Unmarshal([]byte(raw), &metadata); err != nil || metadata == nil {
		return
	}
	for key, value := range fields {
		metadata[key] = value
	}
	if encoded, err := json.Marshal(metadata); err == nil {
		headers.Set("x-codex-turn-metadata", string(encoded))
	}
}

func applyCodexFingerprintBody(body []byte, ids *codexFingerprintIDs) ([]byte, error) {
	if ids == nil {
		return body, nil
	}
	var payload map[string]any
	decoder := json.NewDecoder(bytes.NewReader(bytes.TrimSpace(body)))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil || payload == nil {
		return nil, fmt.Errorf("apply Codex fingerprint: request body must be a JSON object")
	}
	metadata, _ := payload["client_metadata"].(map[string]any)
	if metadata == nil {
		metadata = make(map[string]any)
	}
	metadata["x-codex-installation-id"] = ids.InstallationID
	metadata["session_id"] = ids.SessionID
	metadata["thread_id"] = ids.ThreadID
	metadata["turn_id"] = ids.TurnID
	metadata["x-codex-window-id"] = ids.WindowID
	rewriteEmbeddedCodexTurnMetadata(metadata, map[string]any{
		"installation_id":         ids.InstallationID,
		"session_id":              ids.SessionID,
		"thread_id":               ids.ThreadID,
		"turn_id":                 ids.TurnID,
		"window_id":               ids.WindowID,
		"turn_started_at_unix_ms": ids.TurnStartedAt,
	})
	payload["client_metadata"] = metadata
	encoded, err := openai.EncodeJSON(payload)
	if err != nil {
		return nil, fmt.Errorf("apply Codex fingerprint: %w", err)
	}
	return encoded, nil
}

func rewriteEmbeddedCodexTurnMetadata(metadata map[string]any, fields map[string]any) {
	raw, ok := metadata["x-codex-turn-metadata"].(string)
	if !ok || strings.TrimSpace(raw) == "" {
		return
	}
	var embedded map[string]any
	if err := json.Unmarshal([]byte(raw), &embedded); err != nil || embedded == nil {
		return
	}
	for key, value := range fields {
		embedded[key] = value
	}
	if encoded, err := json.Marshal(embedded); err == nil {
		metadata["x-codex-turn-metadata"] = string(encoded)
	}
}

func newUUIDv7() string {
	raw := make([]byte, 16)
	now := time.Now().UnixMilli()
	if _, err := rand.Read(raw); err != nil {
		sequence := fallbackTurnSequence.Add(1)
		sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%d", time.Now().UnixNano(), sequence)))
		copy(raw, sum[:16])
	}
	raw[0] = byte(now >> 40)
	raw[1] = byte(now >> 32)
	raw[2] = byte(now >> 24)
	raw[3] = byte(now >> 16)
	raw[4] = byte(now >> 8)
	raw[5] = byte(now)
	raw[6] = (raw[6] & 0x0f) | 0x70
	raw[8] = (raw[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		binary.BigEndian.Uint32(raw[0:4]),
		binary.BigEndian.Uint16(raw[4:6]),
		binary.BigEndian.Uint16(raw[6:8]),
		binary.BigEndian.Uint16(raw[8:10]),
		raw[10:16],
	)
}
