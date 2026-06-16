package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/logs"
	"github.com/yym68686/oaix/internal/protocol/openai"
	"github.com/yym68686/oaix/internal/protocol/sse"
	"github.com/yym68686/oaix/internal/store"
	"github.com/yym68686/oaix/internal/tokens"
	"github.com/yym68686/oaix/internal/transport"
)

type Pipeline struct {
	cfg            config.Config
	logger         *slog.Logger
	tokens         *tokens.Manager
	transport      *transport.Client
	logs           *logs.Writer
	store          tokenStateStore
	commitFailures atomic.Int64
}

type tokenStateStore interface {
	MarkTokenSuccess(ctx context.Context, tokenID int64) error
	MarkTokenError(ctx context.Context, tokenID int64, message string, deactivate bool, cooldownUntil *time.Time) error
}

type RequestIntent struct {
	Endpoint       string
	Model          string
	Stream         bool
	Compact        bool
	Method         string
	UpstreamSuffix string
}

type Attempt struct {
	Index       int
	RequestID   string
	Intent      RequestIntent
	Claim       *tokens.Claim
	Body        []byte
	StartedAt   time.Time
	RetryCause  Outcome
	UpstreamURL string
	Method      string
}

type Outcome string

const (
	OutcomeSuccess             Outcome = "success"
	OutcomeClientCanceled      Outcome = "client_canceled"
	OutcomeUpstream429Cooldown Outcome = "upstream_429_cooldown"
	OutcomeUpstream401Invalid  Outcome = "upstream_401_invalid"
	OutcomeUpstream403Invalid  Outcome = "upstream_403_invalid"
	OutcomeUpstream5xx         Outcome = "upstream_5xx"
	OutcomeTransportError      Outcome = "transport_error"
	OutcomeNoToken             Outcome = "no_token"
)

func New(cfg config.Config, logger *slog.Logger, tokenManager *tokens.Manager, client *transport.Client, writer *logs.Writer, stateStore tokenStateStore) *Pipeline {
	return &Pipeline{
		cfg:       cfg,
		logger:    logger,
		tokens:    tokenManager,
		transport: client,
		logs:      writer,
		store:     stateStore,
	}
}

func (p *Pipeline) TransportStats() transport.Stats {
	return p.transport.Stats()
}

func (p *Pipeline) StateCommitFailures() int64 {
	return p.commitFailures.Load()
}

func (p *Pipeline) Proxy(w http.ResponseWriter, r *http.Request, intent RequestIntent) {
	started := time.Now().UTC()
	requestID := requestID(r)
	w.Header().Set("X-Request-ID", requestID)
	bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, p.cfg.Upstream.NonStreamMaxResponseBytes))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	_ = r.Body.Close()
	intent = normalizeIntent(intent, bodyBytes)
	timing := map[string]any{"request_body_bytes": len(bodyBytes)}
	p.logs.Submit(r.Context(), store.RequestLog{
		RequestID:    requestID,
		Endpoint:     intent.Endpoint,
		Model:        nullable(intent.Model),
		ModelName:    nullable(intent.Model),
		IsStream:     intent.Stream,
		StartedAt:    started,
		ClientIP:     nullable(clientIP(r)),
		UserAgent:    nullable(r.UserAgent()),
		TimingSpans:  timing,
		AttemptCount: 0,
	}, false)

	var excluded = make(map[int64]struct{})
	var lastErr error
	var finalStatus = http.StatusBadGateway
	var selectedTokenID *int64
	var accountID *string
	for attempt := 1; attempt <= p.cfg.Upstream.MaxRetries; attempt++ {
		selectStarted := time.Now()
		claim, err := p.tokens.Claim(r.Context(), tokens.Intent{
			Endpoint:        intent.Endpoint,
			Model:           intent.Model,
			ExcludeTokenIDs: excluded,
		})
		timing["token_select_ms"] = int(time.Since(selectStarted).Milliseconds())
		if err != nil {
			finalStatus = http.StatusServiceUnavailable
			lastErr = err
			break
		}
		selectedTokenID = ptrInt64(claim.TokenID())
		accountID = claim.AccountID()
		attemptStarted := time.Now()
		attemptSpec := Attempt{
			Index:      attempt,
			RequestID:  requestID,
			Intent:     intent,
			Claim:      claim,
			Body:       bodyBytes,
			StartedAt:  attemptStarted.UTC(),
			RetryCause: classify(finalStatus, lastErr),
		}
		status, retry, committed, err := p.doAttempt(w, r, attemptSpec)
		timing["upstream_attempt_ms"] = int(time.Since(attemptStarted).Milliseconds())
		timing["upstream_attempt_count"] = attempt
		claim.Release()
		finalStatus = status
		if committed {
			success := err == nil && status >= 200 && status < 400
			var msg *string
			if err != nil {
				text := err.Error()
				msg = &text
			}
			p.finalLog(r.Context(), requestID, intent, started, finalStatus, success, attempt, selectedTokenID, accountID, msg, timing)
			if success {
				p.commitSuccess(claim.TokenID())
			}
			return
		}
		if err == nil && status >= 200 && status < 400 {
			p.commitSuccess(claim.TokenID())
			p.finalLog(r.Context(), requestID, intent, started, finalStatus, true, attempt, selectedTokenID, accountID, nil, timing)
			return
		}
		lastErr = err
		action := classify(status, err)
		if action == OutcomeUpstream401Invalid || action == OutcomeUpstream403Invalid {
			p.commitTokenError(claim.TokenID(), fmt.Sprintf("terminal upstream status %d", status), true, nil)
		} else if action == OutcomeUpstream429Cooldown {
			cooldown := time.Now().UTC().Add(p.cfg.TokenPool.DefaultCooldown)
			p.commitTokenError(claim.TokenID(), "upstream 429 cooldown", false, &cooldown)
		} else if action == OutcomeUpstream5xx || action == OutcomeTransportError {
			cooldown := time.Now().UTC().Add(5 * time.Second)
			p.commitTokenError(claim.TokenID(), fmt.Sprintf("retryable upstream failure: %v", err), false, &cooldown)
		}
		excluded[claim.TokenID()] = struct{}{}
		if !retry || attempt == p.cfg.Upstream.MaxRetries {
			break
		}
	}
	if errors.Is(lastErr, tokens.ErrNoToken) {
		writeErrorResponse(w, intent.Stream, http.StatusServiceUnavailable, "no available token")
		p.finalLog(r.Context(), requestID, intent, started, http.StatusServiceUnavailable, false, len(excluded), selectedTokenID, accountID, stringPtr("no available token"), timing)
		return
	}
	message := "upstream request failed"
	if lastErr != nil {
		message = lastErr.Error()
	}
	writeErrorResponse(w, intent.Stream, finalStatus, message)
	p.finalLog(r.Context(), requestID, intent, started, finalStatus, false, len(excluded), selectedTokenID, accountID, &message, timing)
}

func (p *Pipeline) doAttempt(w http.ResponseWriter, r *http.Request, attempt Attempt) (int, bool, bool, error) {
	upstreamURL, err := p.upstreamURL(attempt.Intent)
	if err != nil {
		return http.StatusBadGateway, false, false, err
	}
	attempt.UpstreamURL = upstreamURL
	method := attempt.Intent.Method
	if method == "" {
		method = http.MethodPost
	}
	attempt.Method = method
	req, err := http.NewRequestWithContext(r.Context(), method, upstreamURL, bytes.NewReader(attempt.Body))
	if err != nil {
		return http.StatusBadGateway, false, false, err
	}
	copyProxyHeaders(req.Header, r.Header)
	req.Header.Set("X-Request-ID", attempt.RequestID)
	req.Header.Set("Authorization", "Bearer "+attempt.Claim.AccessToken())
	req.Header.Set("Content-Type", contentType(r.Header.Get("Content-Type")))
	resp, err := p.transport.Do(r.Context(), req)
	if err != nil {
		return http.StatusBadGateway, true, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return resp.StatusCode, true, false, fmt.Errorf("upstream returned %d", resp.StatusCode)
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024))
		return resp.StatusCode, true, false, fmt.Errorf("upstream returned %d", resp.StatusCode)
	}
	copyResponseHeaders(w.Header(), resp.Header)
	w.Header().Set("X-OAIX-Token-ID", fmt.Sprint(attempt.Claim.TokenID()))
	w.WriteHeader(resp.StatusCode)
	if isSSE(resp.Header.Get("Content-Type")) || attempt.Intent.Stream {
		if flusher, ok := w.(http.Flusher); ok {
			_, _ = w.Write(sse.Keepalive())
			flusher.Flush()
		}
		_, copyErr := copyAndFlush(w, resp.Body)
		if copyErr != nil {
			return resp.StatusCode, false, true, copyErr
		}
		return resp.StatusCode, false, true, nil
	}
	_, copyErr := io.Copy(w, resp.Body)
	if copyErr != nil {
		return resp.StatusCode, false, true, copyErr
	}
	return resp.StatusCode, false, true, nil
}

func (p *Pipeline) upstreamURL(intent RequestIntent) (string, error) {
	if intent.Endpoint == "/v1/chat/completions" && strings.TrimSpace(p.cfg.Upstream.ChatCompletionsURL) != "" {
		return p.cfg.Upstream.ChatCompletionsURL, nil
	}
	base := strings.TrimSpace(p.cfg.Upstream.ResponsesURL)
	if base == "" {
		return "", errors.New("CODEX_BASE_URL is empty")
	}
	if intent.Compact && !strings.HasSuffix(base, "/compact") {
		return strings.TrimRight(base, "/") + "/compact", nil
	}
	if intent.UpstreamSuffix != "" {
		return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(intent.UpstreamSuffix, "/"), nil
	}
	if intent.Endpoint == "/v1/chat/completions" {
		parsed, err := url.Parse(base)
		if err != nil {
			return "", err
		}
		parsed.Path = strings.TrimRight(strings.TrimSuffix(parsed.Path, "/responses"), "/") + "/chat/completions"
		return parsed.String(), nil
	}
	return base, nil
}

func classify(status int, err error) Outcome {
	if err != nil && errors.Is(err, context.Canceled) {
		return OutcomeClientCanceled
	}
	switch status {
	case http.StatusTooManyRequests:
		return OutcomeUpstream429Cooldown
	case http.StatusUnauthorized:
		return OutcomeUpstream401Invalid
	case http.StatusForbidden:
		return OutcomeUpstream403Invalid
	}
	if status >= 500 {
		return OutcomeUpstream5xx
	}
	if err != nil {
		return OutcomeTransportError
	}
	return OutcomeSuccess
}

func (p *Pipeline) commitSuccess(tokenID int64) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := p.withCommitRetry(ctx, func(ctx context.Context) error {
			return p.store.MarkTokenSuccess(ctx, tokenID)
		}); err != nil {
			p.commitFailures.Add(1)
			if p.logger != nil {
				p.logger.Warn("mark token success failed", "token_id", tokenID, "error", err)
			}
		}
	}()
}

func (p *Pipeline) commitTokenError(tokenID int64, message string, deactivate bool, cooldownUntil *time.Time) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := p.withCommitRetry(ctx, func(ctx context.Context) error {
			return p.store.MarkTokenError(ctx, tokenID, message, deactivate, cooldownUntil)
		}); err != nil {
			p.commitFailures.Add(1)
			if p.logger != nil {
				p.logger.Error("mark token error failed", "token_id", tokenID, "error", err)
			}
		}
	}()
}

func (p *Pipeline) withCommitRetry(ctx context.Context, fn func(context.Context) error) error {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if err := fn(ctx); err != nil {
			lastErr = err
			timer := time.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}
		return nil
	}
	return lastErr
}

func (p *Pipeline) finalLog(ctx context.Context, requestID string, intent RequestIntent, started time.Time, status int, success bool, attempts int, tokenID *int64, accountID *string, errMsg *string, timing map[string]any) {
	finished := time.Now().UTC()
	duration := int(finished.Sub(started).Milliseconds())
	if timing == nil {
		timing = map[string]any{}
	}
	timing["total_ms"] = duration
	p.logs.Submit(ctx, store.RequestLog{
		RequestID:    requestID,
		Endpoint:     intent.Endpoint,
		Model:        nullable(intent.Model),
		ModelName:    nullable(intent.Model),
		IsStream:     intent.Stream,
		StatusCode:   ptrInt(status),
		Success:      &success,
		AttemptCount: attempts,
		TokenID:      tokenID,
		AccountID:    accountID,
		StartedAt:    started,
		FinishedAt:   &finished,
		DurationMs:   &duration,
		TimingSpans:  timing,
		ErrorMessage: errMsg,
	}, true)
}

func copyProxyHeaders(dst, src http.Header) {
	for key, values := range src {
		lower := strings.ToLower(key)
		if lower == "host" || lower == "authorization" || strings.HasPrefix(lower, "x-oaix-") {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func copyResponseHeaders(dst, src http.Header) {
	for key, values := range src {
		lower := strings.ToLower(key)
		if lower == "content-length" || lower == "connection" {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func copyAndFlush(w http.ResponseWriter, reader io.Reader) (int64, error) {
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32*1024)
	var written int64
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			m, writeErr := w.Write(buf[:n])
			written += int64(m)
			if flusher != nil {
				flusher.Flush()
			}
			if writeErr != nil {
				return written, writeErr
			}
			if m != n {
				return written, io.ErrShortWrite
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return written, nil
			}
			return written, readErr
		}
	}
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	if status <= 0 {
		status = http.StatusBadGateway
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(openai.NewError(message, "gateway_error", "oaix_gateway_error"))
}

func writeErrorResponse(w http.ResponseWriter, stream bool, status int, message string) {
	if stream {
		writeSSEError(w, status, message)
		return
	}
	writeJSONError(w, status, message)
}

func writeSSEError(w http.ResponseWriter, status int, message string) {
	if status <= 0 {
		status = http.StatusBadGateway
	}
	payload, _ := json.Marshal(map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    "gateway_error",
			"code":    "oaix_gateway_error",
			"status":  status,
		},
	})
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(status)
	_, _ = w.Write(sse.Encode("error", payload))
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

func requestID(r *http.Request) string {
	for _, key := range []string{"X-Request-ID", "X-OAIX-Request-ID", "OpenAI-Request-ID"} {
		if value := strings.TrimSpace(r.Header.Get(key)); value != "" {
			return value
		}
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%s:%s", time.Now().UnixNano(), r.RemoteAddr, r.URL.Path)))
	return "oaix-" + hex.EncodeToString(sum[:8])
}

func normalizeIntent(intent RequestIntent, body []byte) RequestIntent {
	switch intent.Endpoint {
	case "/v1/responses":
		if req, err := openai.DecodeResponsesRequest(body); err == nil {
			if intent.Model == "" {
				intent.Model = req.Model
			}
			if !intent.Stream {
				intent.Stream = req.Stream
			}
		}
	case "/v1/chat/completions":
		if req, err := openai.DecodeChatCompletionRequest(body); err == nil {
			if intent.Model == "" {
				intent.Model = req.Model
			}
			if !intent.Stream {
				intent.Stream = req.Stream
			}
		}
	case "/v1/images/generations":
		if req, err := openai.DecodeImageGenerationRequest(body); err == nil && intent.Model == "" {
			intent.Model = req.Model
		}
	default:
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err == nil {
			if intent.Model == "" {
				if model, ok := payload["model"].(string); ok {
					intent.Model = model
				}
			}
			if !intent.Stream {
				stream, _ := payload["stream"].(bool)
				intent.Stream = stream
			}
		}
	}
	return intent
}

func clientIP(r *http.Request) string {
	for _, key := range []string{"X-Forwarded-For", "X-Real-IP"} {
		if value := strings.TrimSpace(r.Header.Get(key)); value != "" {
			if first, _, ok := strings.Cut(value, ","); ok {
				return strings.TrimSpace(first)
			}
			return value
		}
	}
	return r.RemoteAddr
}

func contentType(value string) string {
	if strings.TrimSpace(value) == "" {
		return "application/json"
	}
	return value
}

func isSSE(contentType string) bool {
	return strings.Contains(strings.ToLower(contentType), "text/event-stream")
}

func nullable(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func ptrInt(value int) *int {
	return &value
}

func ptrInt64(value int64) *int64 {
	return &value
}

func stringPtr(value string) *string {
	return &value
}
