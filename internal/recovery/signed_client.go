package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Session is encrypted at rest. TaskToken must never be returned in admin JSON
// or logs. Persisting before POST makes ambiguous submissions fail closed.
type SignedSession struct {
	Stage       string `json:"stage"`
	JobID       string `json:"job_id,omitempty"`
	TaskToken   string `json:"task_token,omitempty"`
	PreflightID string `json:"preflight_id,omitempty"`
	TaskID      string `json:"task_id,omitempty"`
}

type SignedClient struct {
	BaseURL    string
	HTTPClient *http.Client
	PollEvery  time.Duration
}

func NewSigned(base string) *SignedClient {
	if base == "" {
		base = SignedDefaultURL
	}
	return &SignedClient{BaseURL: strings.TrimRight(base, "/"), HTTPClient: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, PollEvery: time.Second}
}

type signedResource struct {
	JobID            string `json:"job_id"`
	TaskID           string `json:"task_id"`
	TaskToken        string `json:"task_token"`
	PreflightID      string `json:"preflight_id"`
	Status           string `json:"status"`
	DownloadReady    bool   `json:"download_ready"`
	AllDownloadReady bool   `json:"all_download_ready"`
}
type signedEnvelope struct {
	OK    bool           `json:"ok"`
	Code  string         `json:"code"`
	Error string         `json:"error"`
	Job   signedResource `json:"job"`
	Task  signedResource `json:"task"`
}

func (c *SignedClient) Recover(ctx context.Context, raw []byte, state *SignedSession, save func(SignedSession) error) ([]byte, error) {
	document, err := ParseSignedDocument(raw)
	if err != nil {
		return nil, &APIError{Code: "signed_document_invalid"}
	}
	// The provider operates on the whole signed file; we cannot trim records
	// without invalidating its signature. Do not reauthorize unrelated plans.
	for _, account := range document.Accounts {
		identity, err := TokenIdentity(account.Credentials.AccessToken)
		if err != nil || identity.PlanType != EligiblePlan || account.Platform != "openai" || account.Type != "oauth" {
			return nil, &APIError{Code: "signed_document_ineligible_plan"}
		}
	}

	if state.Stage == "verify_submitting" || state.Stage == "task_submitting" {
		return nil, &APIError{Code: "submission_uncertain"}
	}
	persist := func(stage string) error { state.Stage = stage; return save(*state) }
	if state.JobID == "" {
		if err := persist("verify_submitting"); err != nil {
			return nil, err
		}
		r, err := c.call(ctx, http.MethodPost, "/verify/start?workers=1", raw, "")
		if err != nil {
			return nil, err
		}
		if r.Job.JobID == "" || r.Job.TaskToken == "" {
			return nil, &APIError{Code: "verify_response_invalid"}
		}
		state.JobID = r.Job.JobID
		state.TaskToken = r.Job.TaskToken
		if err := persist("verifying"); err != nil {
			return nil, err
		}
	}
	if state.PreflightID == "" {
		for {
			r, err := c.call(ctx, http.MethodGet, "/verify/"+url.PathEscape(state.JobID)+"?summary=1", nil, state.TaskToken)
			if err != nil {
				return nil, err
			}
			if r.Job.Status == "failed" {
				*state = SignedSession{}
				if err := save(*state); err != nil {
					return nil, err
				}
				return nil, &APIError{Code: "verification_failed"}
			}
			if r.Job.Status == "completed" {
				if r.Job.PreflightID == "" {
					return nil, &APIError{Code: "missing_preflight"}
				}
				state.PreflightID = r.Job.PreflightID
				if err := persist("verified"); err != nil {
					return nil, err
				}
				break
			}
			if r.Job.Status != "running" && r.Job.Status != "queued" {
				return nil, &APIError{Code: "verify_status_invalid"}
			}
			if err := c.pause(ctx, 0); err != nil {
				return nil, err
			}
		}
	}
	if state.TaskID == "" {
		if err := persist("task_submitting"); err != nil {
			return nil, err
		}
		r, err := c.call(ctx, http.MethodPost, "/tasks?workers=1&auto_start=1&preflight_id="+url.QueryEscape(state.PreflightID), raw, state.TaskToken)
		if err != nil {
			return nil, err
		}
		if r.Task.TaskID == "" {
			return nil, &APIError{Code: "task_response_invalid"}
		}
		state.TaskID = r.Task.TaskID
		if r.Task.TaskToken != "" {
			state.TaskToken = r.Task.TaskToken
		}
		if err := persist("recovering"); err != nil {
			return nil, err
		}
	}
	for {
		r, err := c.call(ctx, http.MethodGet, "/tasks/"+url.PathEscape(state.TaskID), nil, state.TaskToken)
		if err != nil {
			return nil, err
		}
		switch r.Task.Status {
		case "normal", "recovered", "partial":
			if !r.Task.AllDownloadReady {
				return nil, &APIError{Code: "download_not_ready"}
			}
			data, err := c.request(ctx, http.MethodGet, "/tasks/"+url.PathEscape(state.TaskID)+"/download?scope=all&format=json", nil, state.TaskToken)
			if err != nil {
				return nil, err
			}
			if _, err := ParseSignedDocument(data); err != nil {
				return nil, &APIError{Code: "download_invalid"}
			}
			if err := persist("downloaded"); err != nil {
				return nil, err
			}
			return data, nil
		case "failed", "stopped":
			*state = SignedSession{}
			if err := save(*state); err != nil {
				return nil, err
			}
			return nil, &APIError{Code: "signed_task_" + r.Task.Status}
		case "queued", "running", "pending", "verifying", "recovering", "ready":
		default:
			return nil, &APIError{Code: "task_status_invalid"}
		}
		if err := c.pause(ctx, 0); err != nil {
			return nil, err
		}
	}
}
func (c *SignedClient) call(ctx context.Context, method, path string, raw []byte, token string) (signedEnvelope, error) {
	var envelope signedEnvelope
	data, err := c.request(ctx, method, path, raw, token)
	if err != nil {
		return envelope, err
	}
	if json.Unmarshal(data, &envelope) != nil {
		return envelope, &APIError{Code: "signed_invalid_json"}
	}
	if !envelope.OK {
		return envelope, &APIError{Code: "signed_api_rejected"}
	}
	return envelope, nil
}
func (c *SignedClient) request(ctx context.Context, method, path string, raw []byte, token string) ([]byte, error) {
	failures := 0
	for {
		req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+"/api/revive/v1"+path, bytes.NewReader(raw))
		if err != nil {
			return nil, &APIError{Code: "signed_request_invalid"}
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "oaix-signed-recovery/1.0")
		if raw != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if token != "" {
			req.Header.Set("X-Revive-Task-Token", token)
		}
		resp, err := c.HTTPClient.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if method == http.MethodGet && failures < 3 {
				failures++
				if err := c.pause(ctx, time.Duration(failures)*time.Second); err != nil {
					return nil, err
				}
				continue
			}
			return nil, &APIError{Code: "signed_transport_error"}
		}
		if method == http.MethodGet && (resp.StatusCode == 429 || resp.StatusCode >= 500) && failures < 3 {
			retry, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
			resp.Body.Close()
			failures++
			wait := time.Duration(retry) * time.Second
			if wait < time.Second {
				wait = time.Duration(failures) * time.Second
			}
			if wait > time.Minute {
				wait = time.Minute
			}
			if err := c.pause(ctx, wait); err != nil {
				return nil, err
			}
			continue
		}
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, MaxSignedDocumentBytes+1))
		resp.Body.Close()
		if readErr != nil {
			return nil, &APIError{Code: "signed_response_read_failed"}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, &APIError{Code: fmt.Sprintf("signed_http_%d", resp.StatusCode)}
		}
		if len(data) > MaxSignedDocumentBytes {
			return nil, &APIError{Code: "signed_response_too_large"}
		}
		return data, nil
	}
}
func (c *SignedClient) pause(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		d = c.PollEvery
	}
	if d <= 0 {
		d = time.Second
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
