package sub2api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const apiPrefix = "/api/v1"

type Client struct {
	HTTPClient *http.Client
}

type Group struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	Platform string `json:"platform"`
	Status   string `json:"status"`
}

type Proxy struct {
	ID           int64  `json:"id"`
	Name         string `json:"name"`
	Protocol     string `json:"protocol"`
	Host         string `json:"host,omitempty"`
	Port         int    `json:"port,omitempty"`
	Status       string `json:"status"`
	AccountCount int64  `json:"account_count,omitempty"`
}

type Account struct {
	ID          int64          `json:"id"`
	Name        string         `json:"name"`
	Platform    string         `json:"platform"`
	Type        string         `json:"type"`
	Status      string         `json:"status"`
	Schedulable *bool          `json:"schedulable,omitempty"`
	Extra       map[string]any `json:"extra,omitempty"`
	GroupIDs    []int64        `json:"group_ids,omitempty"`
}

type AccountPage struct {
	Items    []Account `json:"items"`
	Total    int64     `json:"total"`
	Page     int       `json:"page"`
	PageSize int       `json:"page_size"`
	Pages    int       `json:"pages"`
}

type CreateAccountRequest struct {
	Name                    string         `json:"name"`
	Notes                   *string        `json:"notes,omitempty"`
	Platform                string         `json:"platform"`
	Type                    string         `json:"type"`
	Credentials             map[string]any `json:"credentials"`
	Extra                   map[string]any `json:"extra,omitempty"`
	ProxyID                 *int64         `json:"proxy_id,omitempty"`
	Concurrency             int            `json:"concurrency"`
	Priority                int            `json:"priority"`
	GroupIDs                []int64        `json:"group_ids"`
	AutoPauseOnExpired      *bool          `json:"auto_pause_on_expired,omitempty"`
	ConfirmMixedChannelRisk *bool          `json:"confirm_mixed_channel_risk,omitempty"`
}

type BatchCreateResult struct {
	Success int                  `json:"success"`
	Failed  int                  `json:"failed"`
	Results []BatchAccountResult `json:"results"`
}

type BatchAccountResult struct {
	Name    string `json:"name"`
	ID      int64  `json:"id"`
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

func NewClient(httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 60 * time.Second}
	}
	return &Client{HTTPClient: httpClient}
}

func (c *Client) ListGroups(ctx context.Context, baseURL string, adminKey string, platform string) ([]Group, error) {
	values := url.Values{}
	if strings.TrimSpace(platform) != "" {
		values.Set("platform", strings.TrimSpace(platform))
	}
	path := "/admin/groups/all"
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var groups []Group
	if err := c.doJSON(ctx, http.MethodGet, baseURL, adminKey, path, nil, &groups); err != nil {
		return nil, err
	}
	return groups, nil
}

func (c *Client) ListProxies(ctx context.Context, baseURL string, adminKey string) ([]Proxy, error) {
	var proxies []Proxy
	if err := c.doJSON(ctx, http.MethodGet, baseURL, adminKey, "/admin/proxies/all?with_count=true", nil, &proxies); err != nil {
		return nil, err
	}
	return proxies, nil
}

func (c *Client) ListAccountsPage(ctx context.Context, baseURL string, adminKey string, groupID int64, page int, pageSize int) (AccountPage, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}
	values := url.Values{}
	values.Set("page", strconv.Itoa(page))
	values.Set("page_size", strconv.Itoa(pageSize))
	values.Set("group", strconv.FormatInt(groupID, 10))
	values.Set("platform", "openai")
	values.Set("type", "oauth")
	values.Set("status", "active")
	values.Set("lite", "true")
	var payload AccountPage
	err := c.doJSON(ctx, http.MethodGet, baseURL, adminKey, "/admin/accounts?"+values.Encode(), nil, &payload)
	return payload, err
}

func (c *Client) CountActiveOAuthAccounts(ctx context.Context, baseURL string, adminKey string, groupIDs []int64) (int, error) {
	seen := map[int64]struct{}{}
	for _, groupID := range groupIDs {
		if groupID <= 0 {
			continue
		}
		page := 1
		for {
			payload, err := c.ListAccountsPage(ctx, baseURL, adminKey, groupID, page, 1000)
			if err != nil {
				return 0, err
			}
			for _, account := range payload.Items {
				if account.ID <= 0 {
					continue
				}
				if account.Schedulable != nil && !*account.Schedulable {
					continue
				}
				seen[account.ID] = struct{}{}
			}
			if len(payload.Items) == 0 || int64(page*payload.PageSize) >= payload.Total || page >= payload.Pages {
				break
			}
			page++
		}
	}
	return len(seen), nil
}

func (c *Client) CreateAccounts(ctx context.Context, baseURL string, adminKey string, accounts []CreateAccountRequest) (BatchCreateResult, error) {
	var result BatchCreateResult
	if len(accounts) == 0 {
		return result, nil
	}
	body := map[string]any{"accounts": accounts}
	err := c.doJSON(ctx, http.MethodPost, baseURL, adminKey, "/admin/accounts/batch", body, &result)
	return result, err
}

func (c *Client) doJSON(ctx context.Context, method string, baseURL string, adminKey string, path string, body any, dest any) error {
	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	endpoint, err := buildURL(baseURL, path)
	if err != nil {
		return err
	}
	var reader io.Reader
	if body != nil {
		data, marshalErr := json.Marshal(body)
		if marshalErr != nil {
			return marshalErr
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "oaix-sub2api-sync/1.0")
	req.Header.Set("X-API-Key", strings.TrimSpace(adminKey))
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16*1024*1024))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s failed: HTTP %d: %s", method, endpoint, resp.StatusCode, truncateBody(raw))
	}
	payload, err := unwrapResponse(raw)
	if err != nil {
		return fmt.Errorf("%s %s failed: %w", method, endpoint, err)
	}
	if dest == nil {
		return nil
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("%s %s returned unexpected response: %w", method, endpoint, err)
	}
	return nil
}

func buildURL(baseURL string, path string) (string, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		return "", fmt.Errorf("sub2api base_url is required")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("invalid sub2api base_url")
	}
	path = "/" + strings.TrimLeft(path, "/")
	if strings.HasPrefix(path, apiPrefix+"/") {
		return baseURL + path, nil
	}
	return baseURL + apiPrefix + path, nil
}

func unwrapResponse(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var payload any
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("non-json response")
	}
	envelope, ok := payload.(map[string]any)
	if !ok {
		return payload, nil
	}
	code, hasCode := envelope["code"]
	if !hasCode {
		return payload, nil
	}
	if !zeroCode(code) {
		message := strings.TrimSpace(fmt.Sprint(envelope["message"]))
		if message == "" || message == "<nil>" {
			message = strings.TrimSpace(fmt.Sprint(envelope))
		}
		return nil, errors.New(message)
	}
	if data, ok := envelope["data"]; ok {
		return data, nil
	}
	return map[string]any{}, nil
}

func zeroCode(value any) bool {
	switch typed := value.(type) {
	case int:
		return typed == 0
	case float64:
		return typed == 0
	case json.Number:
		parsed, err := typed.Int64()
		return err == nil && parsed == 0
	case string:
		return strings.TrimSpace(typed) == "0"
	default:
		return false
	}
}

func truncateBody(raw []byte) string {
	text := strings.TrimSpace(string(raw))
	if len(text) > 400 {
		return text[:400]
	}
	return text
}
