// Package recovery implements the 5xteam 401 recovery protocol. Only account
// names are submitted; existing access/refresh tokens never leave OAIX.
package recovery

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const DefaultURL = "https://5xteam.shop"
const EligiblePlan = "self_serve_business_prolite"

type Result struct {
	AccessToken  string
	RefreshToken string
	IDToken      string
	ExpiresAt    time.Time
	AccountID    string
	Email        string
	PlanType     string
}

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
	PollEvery  time.Duration
}

type APIError struct {
	Code   string
	Detail string
}

func (e *APIError) Error() string { return "401 recovery: " + e.Code }

func New(baseURL string) *Client {
	if baseURL == "" {
		baseURL = DefaultURL
	}
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), HTTPClient: &http.Client{
		Timeout:       30 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse },
	}, PollEvery: time.Second}
}

type apiResponse struct {
	Success       json.RawMessage `json:"success"`
	Status        string          `json:"status"`
	JobKey        string          `json:"jobKey"`
	Message       string          `json:"message"`
	Results       []apiItem       `json:"results"`
	Data          *apiResponse    `json:"data"`
	Result        json.RawMessage `json:"result"`
	Payload       json.RawMessage `json:"payload"`
	MergedPayload json.RawMessage `json:"mergedPayload"`
}
type apiItem struct {
	Success            bool   `json:"success"`
	Message            string `json:"message"`
	AccountDeactivated bool   `json:"accountDeactivated"`
	FailureKind        string `json:"failureKind"`
	Payload            struct {
		Accounts []struct {
			Name        string `json:"name"`
			Credentials struct {
				AccessToken  string `json:"access_token"`
				RefreshToken string `json:"refresh_token"`
				IDToken      string `json:"id_token"`
				AccountID    string `json:"chatgpt_account_id"`
			} `json:"credentials"`
		} `json:"accounts"`
	} `json:"payload"`
}

// Recover performs one submission, followed by bounded, read-only polling.
// A POST with an uncertain outcome is never blindly retried.
func (c *Client) Recover(parent context.Context, name string) (Result, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Minute)
	defer cancel()
	if !strings.Contains(name, "----") {
		return Result{}, &APIError{Code: "missing_workspace_name"}
	}
	body, _ := json.Marshal(map[string]any{"accountNames": []string{name}})
	result, err := c.do(ctx, http.MethodPost, "/api/cdk/replenish-email-gpt", body)
	if err != nil {
		return Result{}, err
	}
	if result.Status == "success" {
		return parseResult(result, result.Result, result.Payload, result.MergedPayload)
	}
	if result.JobKey == "" {
		return Result{}, &APIError{Code: "missing_job_key"}
	}
	path := "/api/cdk/replenish-email-gpt/status/" + url.PathEscape(result.JobKey)
	interval := c.PollEvery
	if interval <= 0 {
		interval = time.Second
	}
	for {
		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		case <-time.After(interval):
		}
		polled, err := c.do(ctx, http.MethodGet, path, nil)
		if err != nil {
			var apiErr *APIError
			if errors.As(err, &apiErr) {
				return Result{}, err
			}
			// Transient GET failures can be retried without starting another OAuth job.
			continue
		}
		if polled.Data == nil {
			return Result{}, &APIError{Code: "missing_status_data"}
		}
		switch polled.Data.Status {
		case "success":
			return parseResult(polled.Data, polled.Data.Result, polled.Data.Payload, polled.Data.MergedPayload)
		case "failed":
			return Result{}, &APIError{Code: "job_failed", Detail: polled.Data.Message}
		case "running", "pending", "queued":
		default:
			return Result{}, &APIError{Code: "unknown_job_status"}
		}
	}
}

func (c *Client) do(ctx context.Context, method, path string, body []byte) (*apiResponse, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "oaix-401-recovery/1.0")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, errors.New("401 recovery network failure")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &APIError{Code: fmt.Sprintf("http_%d", resp.StatusCode)}
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (2<<20)+1))
	if err != nil {
		return nil, errors.New("401 recovery response read failure")
	}
	if len(raw) > 2<<20 {
		return nil, &APIError{Code: "response_too_large"}
	}
	var out apiResponse
	if json.Unmarshal(raw, &out) != nil {
		return nil, &APIError{Code: "invalid_json"}
	}
	if string(out.Success) != "true" {
		return nil, &APIError{Code: "api_rejected", Detail: out.Message}
	}
	return &out, nil
}

func parseResult(data *apiResponse, resultRaw, payloadRaw, mergedRaw json.RawMessage) (Result, error) {
	if data == nil {
		return Result{}, &APIError{Code: "unexpected_result_count"}
	}
	items := data.Results
	if len(items) == 0 && len(resultRaw) > 0 {
		var nested struct {
			Results       []apiItem       `json:"results"`
			Payload       json.RawMessage `json:"payload"`
			MergedPayload json.RawMessage `json:"mergedPayload"`
		}
		if json.Unmarshal(resultRaw, &nested) == nil {
			items = nested.Results
			payloadRaw, mergedRaw = nested.Payload, nested.MergedPayload
		}
	}
	if len(items) != 1 {
		for _, raw := range []json.RawMessage{mergedRaw, payloadRaw} {
			if len(raw) == 0 {
				continue
			}
			var payload map[string]any
			if json.Unmarshal(raw, &payload) == nil {
				return parsePayloadMap(payload)
			}
		}
		return Result{}, &APIError{Code: "unexpected_result_count"}
	}
	item := items[0]
	if !item.Success {
		code := "oauth_failed"
		if item.AccountDeactivated || item.FailureKind == "account_deactivated" {
			code = "account_deactivated"
		}
		if strings.Contains(item.Message, "workspace selection failed") {
			code = "workspace_unavailable"
		}
		if strings.Contains(item.Message, "RT记录") {
			code = "workspace_record_not_found"
		}
		return Result{}, &APIError{Code: code, Detail: item.Message}
	}
	if len(item.Payload.Accounts) != 1 {
		return Result{}, &APIError{Code: "unexpected_account_count"}
	}
	a := item.Payload.Accounts[0]
	c := a.Credentials
	if c.AccessToken == "" || c.RefreshToken == "" {
		return Result{}, &APIError{Code: "missing_credentials"}
	}
	identity, err := TokenIdentity(c.AccessToken)
	if err != nil {
		return Result{}, err
	}
	if c.AccountID != "" && c.AccountID != identity.AccountID {
		return Result{}, &APIError{Code: "workspace_mismatch"}
	}
	identity.AccessToken = c.AccessToken
	identity.RefreshToken = c.RefreshToken
	identity.IDToken = c.IDToken
	return identity, nil
}

func parsePayloadMap(payload map[string]any) (Result, error) {
	raw, _ := json.Marshal(map[string]any{"payload": payload})
	var item apiItem
	if err := json.Unmarshal(raw, &item); err != nil || len(item.Payload.Accounts) != 1 {
		return Result{}, &APIError{Code: "invalid_payload"}
	}
	a := item.Payload.Accounts[0]
	c := a.Credentials
	if c.AccessToken == "" || c.RefreshToken == "" {
		return Result{}, &APIError{Code: "missing_credentials"}
	}
	identity, err := TokenIdentity(c.AccessToken)
	if err != nil {
		return Result{}, err
	}
	if c.AccountID != "" && c.AccountID != identity.AccountID {
		return Result{}, &APIError{Code: "workspace_mismatch"}
	}
	identity.AccessToken, identity.RefreshToken, identity.IDToken = c.AccessToken, c.RefreshToken, c.IDToken
	return identity, nil
}

// TokenIdentity reads the raw plan and workspace claims, never broad display
// aliases such as business/team. The actual credential is validated by a full
// upstream model probe before persistence, not by trusting unverified claims.
func TokenIdentity(token string) (Result, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Result{}, &APIError{Code: "invalid_access_token"}
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Result{}, &APIError{Code: "invalid_access_token"}
	}
	var claims struct {
		Auth struct {
			AccountID string `json:"chatgpt_account_id"`
			PlanType  string `json:"chatgpt_plan_type"`
		} `json:"https://api.openai.com/auth"`
		Profile struct {
			Email string `json:"email"`
		} `json:"https://api.openai.com/profile"`
		Email   string `json:"email"`
		Expires int64  `json:"exp"`
	}
	if json.Unmarshal(raw, &claims) != nil {
		return Result{}, &APIError{Code: "invalid_access_token"}
	}
	email := claims.Profile.Email
	if email == "" {
		email = claims.Email
	}
	return Result{Email: email, AccountID: claims.Auth.AccountID, PlanType: claims.Auth.PlanType, ExpiresAt: time.Unix(claims.Expires, 0)}, nil
}

func Validate(result Result, email, accountID string) error {
	if !strings.EqualFold(result.Email, strings.TrimSpace(email)) || result.Email == "" {
		return &APIError{Code: "email_mismatch"}
	}
	if result.AccountID == "" || result.AccountID != strings.TrimSpace(accountID) {
		return &APIError{Code: "workspace_mismatch"}
	}
	if result.PlanType != EligiblePlan {
		return &APIError{Code: "subscription_ineligible"}
	}
	if !result.ExpiresAt.After(time.Now().Add(30 * time.Second)) {
		return &APIError{Code: "credential_expired"}
	}
	return nil
}
