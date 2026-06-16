package oauth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

type RefreshResult struct {
	AccessToken  string
	RefreshToken string
	ExpiresIn    int
	AccountID    string
	Email        string
	PlanType     string
}

type Client interface {
	Refresh(ctx context.Context, refreshToken string) (RefreshResult, error)
}

type HTTPClient struct {
	TokenURL   string
	HTTPClient *http.Client
	MaxRetries int
	metrics    Metrics
}

type Metrics struct {
	RefreshAttempts int64 `json:"refresh_attempts"`
	RefreshSuccess  int64 `json:"refresh_success"`
	RefreshFailures int64 `json:"refresh_failures"`
	RefreshRetries  int64 `json:"refresh_retries"`
}

type ErrorClass string

const (
	ErrorClassRetryable ErrorClass = "retryable"
	ErrorClassInvalid   ErrorClass = "invalid"
	ErrorClassTerminal  ErrorClass = "terminal"
)

func NewHTTPClient(tokenURL string) *HTTPClient {
	return &HTTPClient{
		TokenURL: tokenURL,
		HTTPClient: &http.Client{
			Timeout: 15 * time.Second,
		},
		MaxRetries: 3,
	}
}

func (c *HTTPClient) Refresh(ctx context.Context, refreshToken string) (RefreshResult, error) {
	if c == nil || c.TokenURL == "" {
		return RefreshResult{}, errors.New("oauth token url is not configured")
	}
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: 15 * time.Second}
	}
	maxRetries := c.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 1
	}
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		atomic.AddInt64(&c.metrics.RefreshAttempts, 1)
		result, status, err := c.refreshOnce(ctx, refreshToken)
		if err == nil {
			atomic.AddInt64(&c.metrics.RefreshSuccess, 1)
			return result, nil
		}
		lastErr = err
		if ClassifyStatus(status) != ErrorClassRetryable || attempt == maxRetries {
			break
		}
		atomic.AddInt64(&c.metrics.RefreshRetries, 1)
		select {
		case <-ctx.Done():
			return RefreshResult{}, ctx.Err()
		case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
		}
	}
	atomic.AddInt64(&c.metrics.RefreshFailures, 1)
	return RefreshResult{}, lastErr
}

func (c *HTTPClient) Stats() Metrics {
	if c == nil {
		return Metrics{}
	}
	return Metrics{
		RefreshAttempts: atomic.LoadInt64(&c.metrics.RefreshAttempts),
		RefreshSuccess:  atomic.LoadInt64(&c.metrics.RefreshSuccess),
		RefreshFailures: atomic.LoadInt64(&c.metrics.RefreshFailures),
		RefreshRetries:  atomic.LoadInt64(&c.metrics.RefreshRetries),
	}
}

func (c *HTTPClient) refreshOnce(ctx context.Context, refreshToken string) (RefreshResult, int, error) {
	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", refreshToken)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.TokenURL, bytes.NewBufferString(form.Encode()))
	if err != nil {
		return RefreshResult{}, 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return RefreshResult{}, 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return RefreshResult{}, resp.StatusCode, fmt.Errorf("oauth refresh failed with status %d: %s", resp.StatusCode, string(body))
	}
	result, err := ParseRefreshResponse(body)
	if err != nil {
		return RefreshResult{}, resp.StatusCode, err
	}
	return result, resp.StatusCode, nil
}

func ParseRefreshResponse(body []byte) (RefreshResult, error) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return RefreshResult{}, err
	}
	result := RefreshResult{
		AccessToken:  stringField(payload, "access_token"),
		RefreshToken: stringField(payload, "refresh_token"),
		AccountID:    stringField(payload, "account_id"),
		Email:        stringField(payload, "email"),
		PlanType:     stringField(payload, "plan_type"),
	}
	if expires, ok := payload["expires_in"].(float64); ok {
		result.ExpiresIn = int(expires)
	}
	if result.AccessToken == "" {
		return RefreshResult{}, errors.New("oauth response missing access_token")
	}
	return result, nil
}

func ClassifyStatus(status int) ErrorClass {
	switch {
	case status == http.StatusTooManyRequests || status >= 500 || status == 0:
		return ErrorClassRetryable
	case status == http.StatusBadRequest || status == http.StatusUnauthorized || status == http.StatusForbidden:
		return ErrorClassInvalid
	default:
		return ErrorClassTerminal
	}
}

func DedupRefreshHistory(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func stringField(payload map[string]any, key string) string {
	if value, ok := payload[key].(string); ok {
		return value
	}
	return ""
}

type DisabledClient struct{}

func (DisabledClient) Refresh(context.Context, string) (RefreshResult, error) {
	return RefreshResult{}, errors.New("oauth refresh worker is not configured")
}
