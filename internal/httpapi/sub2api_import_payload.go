package httpapi

// This file is a wire-format adapter. Only the allowlisted native credential
// fields leave this boundary; foreign scheduling/billing/group settings do not.
import (
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/yym68686/oaix/internal/agentidentity"
	"github.com/yym68686/oaix/internal/importpayload"
	"github.com/yym68686/oaix/internal/oauth"
)

type sub2APIAccountInput struct {
	Name        string         `json:"name"`
	Notes       *string        `json:"notes"`
	Platform    string         `json:"platform"`
	Type        string         `json:"type"`
	Credentials map[string]any `json:"credentials"`
	ProxyID     *int64         `json:"proxy_id"`
	ProxyKey    *string        `json:"proxy_key"`
	// These fields describe foreign policies. Accept them without applying them.
	Extra              json.RawMessage `json:"extra"`
	Concurrency        json.RawMessage `json:"concurrency"`
	Priority           json.RawMessage `json:"priority"`
	RateMultiplier     json.RawMessage `json:"rate_multiplier"`
	LoadFactor         json.RawMessage `json:"load_factor"`
	ExpiresAt          json.RawMessage `json:"expires_at"`
	AutoPauseOnExpired json.RawMessage `json:"auto_pause_on_expired"`
}

type sub2APISessionRequest struct {
	Content          string          `json:"content"`
	Contents         []string        `json:"contents"`
	Name             string          `json:"name"`
	Notes            *string         `json:"notes"`
	ProxyID          *int64          `json:"proxy_id"`
	CredentialExtras json.RawMessage `json:"credential_extras"`
	// update_existing and other foreign policy options intentionally have no
	// native interpretation. OAIX keeps its existing identity/upsert rules.
}

type sub2APISessionItem struct {
	Index     int    `json:"index"`
	Name      string `json:"name,omitempty"`
	Action    string `json:"action"`
	AccountID int64  `json:"account_id,omitempty"`
	Message   string `json:"message,omitempty"`
}

type sub2APIImportMessage struct {
	Index   int    `json:"index"`
	Name    string `json:"name,omitempty"`
	Message string `json:"message"`
}

type sub2APISessionResult struct {
	Total    int                    `json:"total"`
	Created  int                    `json:"created"`
	Updated  int                    `json:"updated"`
	Skipped  int                    `json:"skipped"`
	Failed   int                    `json:"failed"`
	Items    []sub2APISessionItem   `json:"items,omitempty"`
	Warnings []sub2APIImportMessage `json:"warnings,omitempty"`
	Errors   []sub2APIImportMessage `json:"errors,omitempty"`
}

func sub2APINativeCredentials(raw map[string]any) map[string]any {
	source := raw
	if nested, ok := raw["tokens"].(map[string]any); ok {
		source = clonePayload(raw)
		for k, v := range nested {
			source[k] = v
		}
	}
	if normalized, ok := agentidentity.NormalizePayload(source); ok {
		source = normalized
	}
	payload := map[string]any{"type": "codex"}
	aliases := map[string][]string{
		"access_token":    {"access_token", "accessToken", "token"},
		"refresh_token":   {"refresh_token", "refreshToken"},
		"id_token":        {"id_token", "idToken"},
		"account_id":      {"chatgpt_account_id", "chatgptAccountId", "account_id", "accountId"},
		"chatgpt_user_id": {"chatgpt_user_id", "chatgptUserId", "user_id", "userId"},
		"email":           {"email"}, "plan_type": {"plan_type", "planType", "chatgpt_plan_type"},
		"organization_id": {"organization_id", "organizationId", "org_id", "orgId"},
		"client_id":       {"client_id", "clientId", "oauth_client_id"},
		"last_refresh":    {"last_refresh"}, "auth_mode": {"auth_mode", "authMode"},
		"agent_runtime_id":  {"agent_runtime_id", "agentRuntimeId"},
		"agent_private_key": {"agent_private_key", "agentPrivateKey"},
		"task_id":           {"task_id", "taskId"}, "workspace_id": {"workspace_id", "workspaceId"},
	}
	for key, keys := range aliases {
		if value := importpayload.String(source, keys...); value != "" {
			payload[key] = value
		}
	}
	if account, ok := raw["account"].(map[string]any); ok {
		for key, keys := range map[string][]string{"account_id": {"id", "account_id", "chatgpt_account_id"}, "plan_type": {"plan_type", "planType"}} {
			if payload[key] == nil {
				if value := importpayload.String(account, keys...); value != "" {
					payload[key] = value
				}
			}
		}
	}
	if user, ok := raw["user"].(map[string]any); ok {
		for key, k := range map[string]string{"email": "email", "chatgpt_user_id": "id"} {
			if payload[key] == nil {
				if value := importpayload.String(user, k); value != "" {
					payload[key] = value
				}
			}
		}
	}
	if flag, ok := source["chatgpt_account_is_fedramp"].(bool); ok {
		payload["chatgpt_account_is_fedramp"] = flag
	}
	if identity, err := oauth.ParseIDTokenIdentity(importpayload.String(payload, "id_token")); err == nil {
		for key, value := range map[string]string{"account_id": identity.AccountID, "email": identity.Email, "plan_type": identity.PlanType, "chatgpt_user_id": identity.UserID, "organization_id": identity.OrganizationID} {
			if payload[key] == nil && value != "" {
				payload[key] = value
			}
		}
	}
	if disabled, ok := source["disabled"].(bool); ok {
		payload["is_active"] = !disabled
	}
	return payload
}

func (item sub2APIAccountInput) nativePayload() (map[string]any, error) {
	if strings.TrimSpace(item.Name) == "" {
		return nil, errors.New("name is required")
	}
	if item.Platform != "openai" || item.Type != "oauth" {
		return nil, errors.New("only Codex accounts (platform=openai, type=oauth) are supported")
	}
	payload := sub2APINativeCredentials(item.Credentials)
	if _, agent, err := agentidentity.Parse(payload); agent {
		if err != nil {
			return nil, errors.New("invalid Codex agent identity credentials")
		}
	} else if importpayload.String(payload, "access_token", "refresh_token") == "" {
		return nil, errors.New("missing usable Codex access_token or refresh_token")
	}
	if item.Notes != nil {
		payload["remark"] = *item.Notes
	} else if item.Name != "" {
		payload["remark"] = item.Name
	}
	if item.ProxyID != nil {
		payload["proxy_channel_id"] = *item.ProxyID
	}
	return payload, nil
}

func sub2APISessionEntries(req sub2APISessionRequest) ([]any, error) {
	entries := []any{}
	var flatten func(any)
	flatten = func(value any) {
		if list, ok := value.([]any); ok {
			for _, item := range list {
				flatten(item)
			}
		} else {
			entries = append(entries, value)
		}
	}
	parse := func(content string) error {
		decoder := json.NewDecoder(strings.NewReader(content))
		decoder.UseNumber()
		for {
			var v any
			err := decoder.Decode(&v)
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return errors.New("invalid Codex session JSON")
			}
			flatten(v)
		}
	}
	for _, content := range append([]string{req.Content}, req.Contents...) {
		content = strings.TrimSpace(content)
		if content == "" {
			continue
		}
		// Support JSON streams, nested arrays, JSONL, and mixtures of token lines
		// and single-line JSON, as accepted by the source importer.
		start := len(entries)
		if strings.ContainsAny(content[:1], "[{\"") {
			if err := parse(content); err == nil {
				continue
			}
			entries = entries[:start]
		}
		for _, line := range strings.Split(content, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			if strings.ContainsAny(line[:1], "[{\"") {
				if err := parse(line); err != nil {
					return nil, err
				}
			} else {
				flatten(line)
			}
		}
	}
	if len(entries) == 0 {
		return nil, errors.New("content or contents must contain Codex credentials")
	}
	return entries, nil
}
