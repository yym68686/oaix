package proxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

const (
	gatewayRoutingAttemptHeader     = "X-OAIX-Routing-Attempt-ID"
	gatewayIdempotencyStatusHeader  = "X-OAIX-Idempotency-Status"
	maxGatewayRoutingAttemptIDBytes = 160
)

type gatewayIdempotencyStore interface {
	BeginGatewayIdempotency(context.Context, store.GatewayIdempotencyBegin) (store.GatewayIdempotencyBeginResult, error)
	RenewGatewayIdempotencyLease(context.Context, int64, string, string, time.Duration, time.Duration) (bool, error)
	CompleteGatewayIdempotency(context.Context, store.GatewayIdempotencyCompletion) (bool, error)
	FailGatewayIdempotency(context.Context, int64, string, string, string, time.Duration) (bool, error)
}

type gatewayIdempotencyExecution struct {
	pipeline      *Pipeline
	store         gatewayIdempotencyStore
	ownerUserID   int64
	keyHash       string
	requestHash   string
	requestID     string
	leaseToken    string
	generation    int64
	writer        *idempotencyCaptureWriter
	request       *http.Request
	cancelRequest context.CancelFunc
	stopHeartbeat func()
	finishOnce    sync.Once
}

func (p *Pipeline) beginGatewayIdempotency(
	w http.ResponseWriter,
	r *http.Request,
	intent RequestIntent,
	body []byte,
	promptCache *PromptCacheContext,
	requestID string,
) (*gatewayIdempotencyExecution, bool) {
	if p == nil || !p.cfg.Idempotency.Enabled {
		return nil, false
	}
	routingAttemptID := strings.TrimSpace(r.Header.Get(gatewayRoutingAttemptHeader))
	if routingAttemptID == "" {
		return nil, false
	}
	if err := validateGatewayRoutingAttemptID(routingAttemptID); err != nil {
		w.Header().Set(gatewayIdempotencyStatusHeader, "invalid")
		w.Header().Set("Cache-Control", "no-store")
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return nil, true
	}
	coordinator, ok := p.store.(gatewayIdempotencyStore)
	if !ok || coordinator == nil {
		p.logIdempotencyError("gateway idempotency store unavailable", requestID, hashText(routingAttemptID), nil)
		writeGatewayIdempotencyUnavailable(w, "idempotency coordinator unavailable")
		return nil, true
	}
	if intent.OwnerUserID <= 0 {
		p.logIdempotencyError("gateway idempotency owner unavailable", requestID, hashText(routingAttemptID), nil)
		writeGatewayIdempotencyUnavailable(w, "idempotency owner scope unavailable")
		return nil, true
	}

	keyHash := hashText(routingAttemptID)
	requestHash, err := gatewayIdempotencyRequestHash(intent, body, r.Header, promptCache)
	if err != nil {
		p.logIdempotencyError("gateway idempotency request hashing failed", requestID, keyHash, err)
		writeGatewayIdempotencyUnavailable(w, "failed to prepare idempotent request")
		return nil, true
	}
	leaseToken, err := randomHex(32)
	if err != nil {
		p.logIdempotencyError("gateway idempotency lease token generation failed", requestID, keyHash, err)
		writeGatewayIdempotencyUnavailable(w, "failed to prepare idempotent request")
		return nil, true
	}

	waitStarted := time.Now()
	pollDelay := 25 * time.Millisecond
	for {
		beginCtx, cancel := p.idempotencyStoreContext(r.Context())
		result, beginErr := coordinator.BeginGatewayIdempotency(beginCtx, store.GatewayIdempotencyBegin{
			OwnerUserID:   intent.OwnerUserID,
			KeyHash:       keyHash,
			RequestHash:   requestHash,
			RequestID:     boundedRequestID(requestID),
			LeaseToken:    leaseToken,
			LeaseDuration: p.cfg.Idempotency.LeaseDuration,
			RecordTTL:     p.cfg.Idempotency.RecordTTL,
		})
		cancel()
		if beginErr != nil {
			if r.Context().Err() != nil {
				return nil, true
			}
			p.logIdempotencyError("gateway idempotency begin failed", requestID, keyHash, beginErr)
			writeGatewayIdempotencyUnavailable(w, "idempotency coordinator unavailable")
			return nil, true
		}
		switch result.Action {
		case store.GatewayIdempotencyExecute:
			w.Header().Set(gatewayIdempotencyStatusHeader, "executed")
			capture := newIdempotencyCaptureWriter(w, p.cfg.Idempotency.MaxResponseBytes)
			executionCtx, cancelExecution := context.WithTimeout(context.WithoutCancel(r.Context()), p.cfg.Idempotency.ExecutionTimeout)
			executionRequest := r.Clone(executionCtx)
			execution := &gatewayIdempotencyExecution{
				pipeline:      p,
				store:         coordinator,
				ownerUserID:   intent.OwnerUserID,
				keyHash:       keyHash,
				requestHash:   requestHash,
				requestID:     requestID,
				leaseToken:    leaseToken,
				generation:    result.Record.Generation,
				writer:        capture,
				request:       executionRequest,
				cancelRequest: cancelExecution,
			}
			execution.stopHeartbeat = execution.startHeartbeat()
			if p.logger != nil {
				p.logger.Info(
					"gateway idempotency execution claimed",
					"request_id", requestID,
					"routing_attempt_id_hash", keyHash,
					"generation", execution.generation,
				)
			}
			return execution, false
		case store.GatewayIdempotencyReplay:
			committed, err := replayGatewayIdempotencyResponse(w, result.Record)
			if err != nil {
				p.logIdempotencyError("gateway idempotency replay failed", requestID, keyHash, err)
				if !committed {
					writeGatewayIdempotencyUnavailable(w, "stored idempotency response is invalid")
				}
				return nil, true
			}
			if p.logger != nil {
				p.logger.Info(
					"gateway idempotency response replayed",
					"request_id", requestID,
					"original_request_id", result.Record.RequestID,
					"routing_attempt_id_hash", keyHash,
					"generation", result.Record.Generation,
				)
			}
			return nil, true
		case store.GatewayIdempotencyConflict:
			w.Header().Set(gatewayIdempotencyStatusHeader, "conflict")
			w.Header().Set("Cache-Control", "no-store")
			writeJSONError(w, http.StatusConflict, "routing attempt ID was already used with a different request")
			if p.logger != nil {
				p.logger.Warn(
					"gateway idempotency request hash conflict",
					"request_id", requestID,
					"original_request_id", result.Record.RequestID,
					"routing_attempt_id_hash", keyHash,
				)
			}
			return nil, true
		case store.GatewayIdempotencyWait:
			if time.Since(waitStarted) >= p.cfg.Idempotency.WaitTimeout {
				w.Header().Set(gatewayIdempotencyStatusHeader, "in-progress")
				w.Header().Set("Retry-After", "1")
				w.Header().Set("Cache-Control", "no-store")
				writeJSONError(w, http.StatusServiceUnavailable, "matching idempotent request is still in progress")
				return nil, true
			}
			waitFor := pollDelay
			if remaining := p.cfg.Idempotency.WaitTimeout - time.Since(waitStarted); waitFor > remaining {
				waitFor = remaining
			}
			timer := time.NewTimer(waitFor)
			select {
			case <-r.Context().Done():
				if !timer.Stop() {
					<-timer.C
				}
				return nil, true
			case <-timer.C:
			}
			if pollDelay < 500*time.Millisecond {
				pollDelay *= 2
				if pollDelay > 500*time.Millisecond {
					pollDelay = 500 * time.Millisecond
				}
			}
		default:
			p.logIdempotencyError("gateway idempotency returned unknown action", requestID, keyHash, fmt.Errorf("action %q", result.Action))
			writeGatewayIdempotencyUnavailable(w, "idempotency coordinator returned an invalid state")
			return nil, true
		}
	}
}

func (e *gatewayIdempotencyExecution) finish() {
	if e == nil {
		return
	}
	e.finishOnce.Do(func() {
		defer e.cancelRequest()
		defer e.stopHeartbeat()
		status, headers, body, wroteHeader, truncated, deliveryErr := e.writer.snapshot()
		if deliveryErr != nil && e.pipeline.logger != nil {
			e.pipeline.logger.Warn(
				"gateway idempotent execution outlived downstream delivery",
				"request_id", e.requestID,
				"routing_attempt_id_hash", e.keyHash,
				"generation", e.generation,
				"error", deliveryErr,
			)
		}
		if !wroteHeader {
			e.persistFailure("response_not_started")
			return
		}
		if truncated {
			e.persistFailure("response_too_large_to_replay")
			return
		}
		if status >= http.StatusInternalServerError {
			e.persistFailure("retryable_server_response")
			return
		}
		headerJSON, err := json.Marshal(replayableResponseHeaders(headers))
		if err != nil {
			e.pipeline.logIdempotencyError("gateway idempotency response header encoding failed", e.requestID, e.keyHash, err)
			e.persistFailure("response_header_encoding_failed")
			return
		}
		var lastErr error
		for attempt := 0; attempt < 3; attempt++ {
			ctx, cancel := e.pipeline.idempotencyBackgroundStoreContext()
			completed, err := e.store.CompleteGatewayIdempotency(ctx, store.GatewayIdempotencyCompletion{
				OwnerUserID:     e.ownerUserID,
				KeyHash:         e.keyHash,
				LeaseToken:      e.leaseToken,
				StatusCode:      status,
				ResponseHeaders: headerJSON,
				ResponseBody:    body,
				RecordTTL:       e.pipeline.cfg.Idempotency.RecordTTL,
			})
			cancel()
			if err == nil && completed {
				return
			}
			if err == nil {
				err = errors.New("idempotency lease ownership was lost before completion")
			}
			lastErr = err
			if attempt < 2 {
				time.Sleep(time.Duration(attempt+1) * 50 * time.Millisecond)
			}
		}
		e.pipeline.logIdempotencyError("gateway idempotency completion persistence failed", e.requestID, e.keyHash, lastErr)
	})
}

func (e *gatewayIdempotencyExecution) failAfterPanic() {
	if e == nil {
		return
	}
	e.finishOnce.Do(func() {
		defer e.cancelRequest()
		defer e.stopHeartbeat()
		e.persistFailure("execution_panic")
	})
}

func (e *gatewayIdempotencyExecution) persistFailure(reason string) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		ctx, cancel := e.pipeline.idempotencyBackgroundStoreContext()
		failed, err := e.store.FailGatewayIdempotency(
			ctx,
			e.ownerUserID,
			e.keyHash,
			e.leaseToken,
			reason,
			e.pipeline.cfg.Idempotency.RecordTTL,
		)
		cancel()
		if err == nil && failed {
			return
		}
		if err == nil {
			err = errors.New("idempotency lease ownership was lost before failure persistence")
		}
		lastErr = err
		if attempt < 2 {
			time.Sleep(time.Duration(attempt+1) * 50 * time.Millisecond)
		}
	}
	e.pipeline.logIdempotencyError("gateway idempotency failure persistence failed", e.requestID, e.keyHash, lastErr)
}

func (e *gatewayIdempotencyExecution) startHeartbeat() func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(e.pipeline.cfg.Idempotency.HeartbeatInterval)
		defer ticker.Stop()
		lastSuccessfulRenewal := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				renewCtx, renewCancel := e.pipeline.idempotencyBackgroundStoreContext()
				owned, err := e.store.RenewGatewayIdempotencyLease(
					renewCtx,
					e.ownerUserID,
					e.keyHash,
					e.leaseToken,
					e.pipeline.cfg.Idempotency.LeaseDuration,
					e.pipeline.cfg.Idempotency.RecordTTL,
				)
				renewCancel()
				if err == nil && owned {
					lastSuccessfulRenewal = time.Now()
					continue
				}
				if err == nil && !owned {
					e.pipeline.logIdempotencyError("gateway idempotency lease ownership lost", e.requestID, e.keyHash, nil)
					e.cancelRequest()
					return
				}
				e.pipeline.logIdempotencyError("gateway idempotency lease renewal failed", e.requestID, e.keyHash, err)
				if time.Since(lastSuccessfulRenewal) >= e.pipeline.cfg.Idempotency.LeaseDuration {
					e.cancelRequest()
					return
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func replayGatewayIdempotencyResponse(w http.ResponseWriter, record store.GatewayIdempotencyRecord) (bool, error) {
	if record.StatusCode == nil || *record.StatusCode < 100 {
		return false, errors.New("stored response has no valid status code")
	}
	headers := make(http.Header)
	if len(record.ResponseHeaders) > 0 {
		if err := json.Unmarshal(record.ResponseHeaders, &headers); err != nil {
			return false, fmt.Errorf("decode stored response headers: %w", err)
		}
	}
	for key, values := range replayableResponseHeaders(headers) {
		w.Header().Del(key)
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.Header().Set(gatewayIdempotencyStatusHeader, "replayed")
	w.Header().Set("Content-Length", strconv.Itoa(len(record.ResponseBody)))
	w.WriteHeader(*record.StatusCode)
	if len(record.ResponseBody) > 0 {
		if _, err := w.Write(record.ResponseBody); err != nil {
			return true, err
		}
	}
	if strings.Contains(strings.ToLower(w.Header().Get("Content-Type")), "text/event-stream") {
		if err := http.NewResponseController(w).Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return true, err
		}
	}
	return true, nil
}

func gatewayIdempotencyRequestHash(intent RequestIntent, body []byte, headers http.Header, promptCache *PromptCacheContext) (string, error) {
	method := strings.ToUpper(strings.TrimSpace(intent.Method))
	if method == "" {
		method = http.MethodPost
	}
	sessionID := ""
	promptAffinityKey := ""
	if promptCache != nil {
		sessionID = promptCache.SessionID
		promptAffinityKey = promptCache.AffinityKey
	}
	metadata, err := json.Marshal(struct {
		Method              string `json:"method"`
		Endpoint            string `json:"endpoint"`
		Model               string `json:"model"`
		OwnerUserID         int64  `json:"owner_user_id"`
		APIKeyID            *int64 `json:"api_key_id"`
		SelectionMode       string `json:"selection_mode"`
		CallerOwnerUserID   int64  `json:"caller_owner_user_id"`
		ExcludeOwnerUserID  int64  `json:"exclude_owner_user_id"`
		TargetTokenID       int64  `json:"target_token_id"`
		ServiceTier         string `json:"service_tier"`
		RequireFast         bool   `json:"require_fast"`
		Stream              bool   `json:"stream"`
		Compact             bool   `json:"compact"`
		UpstreamSuffix      string `json:"upstream_suffix"`
		UpstreamEndpoint    string `json:"upstream_endpoint"`
		UpstreamContentType string `json:"upstream_content_type"`
		UpstreamAccept      string `json:"upstream_accept"`
		ResponseModelAlias  string `json:"response_model_alias"`
		ImageResponseFormat string `json:"image_response_format"`
		ImageStreamPrefix   string `json:"image_stream_prefix"`
		RequestContentType  string `json:"request_content_type"`
		RequestAccept       string `json:"request_accept"`
		CredentialHash      string `json:"credential_hash"`
		SessionID           string `json:"session_id"`
		PromptAffinityKey   string `json:"prompt_affinity_key"`
	}{
		Method:              method,
		Endpoint:            intent.Endpoint,
		Model:               intent.Model,
		OwnerUserID:         intent.OwnerUserID,
		APIKeyID:            intent.APIKeyID,
		SelectionMode:       intent.SelectionMode,
		CallerOwnerUserID:   intent.CallerOwnerUserID,
		ExcludeOwnerUserID:  intent.ExcludeOwnerUserID,
		TargetTokenID:       intent.TargetTokenID,
		ServiceTier:         intent.ServiceTier,
		RequireFast:         intent.RequireFast,
		Stream:              intent.Stream,
		Compact:             intent.Compact,
		UpstreamSuffix:      intent.UpstreamSuffix,
		UpstreamEndpoint:    intent.UpstreamEndpoint,
		UpstreamContentType: intent.UpstreamContentType,
		UpstreamAccept:      intent.UpstreamAccept,
		ResponseModelAlias:  intent.ResponseModelAlias,
		ImageResponseFormat: intent.ImageResponseFormat,
		ImageStreamPrefix:   intent.ImageStreamPrefix,
		RequestContentType:  contentType(headers.Get("Content-Type")),
		RequestAccept:       strings.TrimSpace(headers.Get("Accept")),
		CredentialHash:      gatewayCredentialHash(headers),
		SessionID:           sessionID,
		PromptAffinityKey:   promptAffinityKey,
	})
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, _ = hash.Write(metadata)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(body)
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func gatewayCredentialHash(headers http.Header) string {
	for _, key := range []string{"Authorization", "X-API-Key"} {
		if value := strings.TrimSpace(headers.Get(key)); value != "" {
			return hashText(strings.ToLower(key) + ":" + value)
		}
	}
	return ""
}

func validateGatewayRoutingAttemptID(value string) error {
	if len(value) == 0 || len(value) > maxGatewayRoutingAttemptIDBytes {
		return fmt.Errorf("%s must contain 1 to %d characters", gatewayRoutingAttemptHeader, maxGatewayRoutingAttemptIDBytes)
	}
	for _, character := range []byte(value) {
		if character < 0x21 || character > 0x7e {
			return fmt.Errorf("%s must contain only visible ASCII characters", gatewayRoutingAttemptHeader)
		}
	}
	return nil
}

func hashText(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func randomHex(byteCount int) (string, error) {
	value := make([]byte, byteCount)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func boundedRequestID(value string) string {
	value = strings.TrimSpace(value)
	if value != "" && len(value) <= 128 {
		valid := true
		for _, character := range value {
			if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune("-_.:", character) {
				continue
			}
			valid = false
			break
		}
		if valid {
			return value
		}
	}
	return "request-sha256-" + hashText(value)
}

func writeGatewayIdempotencyUnavailable(w http.ResponseWriter, message string) {
	w.Header().Set(gatewayIdempotencyStatusHeader, "unavailable")
	w.Header().Set("Retry-After", "1")
	w.Header().Set("Cache-Control", "no-store")
	writeJSONError(w, http.StatusServiceUnavailable, message)
}

func (p *Pipeline) idempotencyStoreContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := p.cfg.Database.AcquireTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return context.WithTimeout(parent, timeout)
}

func (p *Pipeline) idempotencyBackgroundStoreContext() (context.Context, context.CancelFunc) {
	return p.idempotencyStoreContext(context.Background())
}

func (p *Pipeline) logIdempotencyError(message, requestID, keyHash string, err error) {
	if p == nil || p.logger == nil {
		return
	}
	attributes := []any{
		"request_id", requestID,
		"routing_attempt_id_hash", keyHash,
	}
	if err != nil {
		attributes = append(attributes, "error", err)
	}
	p.logger.Error(message, attributes...)
}

func replayableResponseHeaders(headers http.Header) http.Header {
	result := make(http.Header)
	for key, values := range headers {
		switch strings.ToLower(key) {
		case "connection", "content-length", "date", "keep-alive", "proxy-authenticate", "proxy-authorization", "set-cookie", "set-cookie2", "te", "trailer", "transfer-encoding", "upgrade":
			continue
		}
		for _, value := range values {
			result.Add(key, value)
		}
	}
	return result
}

func observeGatewayIdempotencyDelivery(timing map[string]any, execution *gatewayIdempotencyExecution) {
	if timing == nil || execution == nil || execution.writer == nil {
		return
	}
	status, wroteHeader, truncated, deliveryErr := execution.writer.deliverySnapshot()
	replayable := wroteHeader && !truncated && status < http.StatusInternalServerError
	timing["idempotency_response_replayable"] = replayable
	if truncated {
		timing["idempotency_response_limit_exceeded"] = true
	}
	if deliveryErr != nil {
		timing["idempotency_downstream_delivery"] = "detached_after_error"
		timing["idempotency_downstream_delivery_error"] = truncateDetail(deliveryErr.Error(), 512)
	}
}

type idempotencyCaptureWriter struct {
	destination      http.ResponseWriter
	maxResponseBytes int64
	status           int
	wroteHeader      bool
	committedHeader  http.Header
	body             bytes.Buffer
	truncated        bool
	deliveryErr      error
	destinationLost  bool
}

func newIdempotencyCaptureWriter(destination http.ResponseWriter, maxResponseBytes int64) *idempotencyCaptureWriter {
	return &idempotencyCaptureWriter{destination: destination, maxResponseBytes: maxResponseBytes}
}

func (w *idempotencyCaptureWriter) Header() http.Header {
	return w.destination.Header()
}

func (w *idempotencyCaptureWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = status
	w.committedHeader = w.destination.Header().Clone()
	w.destination.WriteHeader(status)
}

func (w *idempotencyCaptureWriter) Write(value []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	w.capture(value)
	if w.destinationLost {
		return len(value), nil
	}
	written, err := w.destination.Write(value)
	if err == nil && written != len(value) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.deliveryErr = err
		w.destinationLost = true
		return len(value), nil
	}
	return written, nil
}

func (w *idempotencyCaptureWriter) Flush() {
	_ = w.FlushError()
}

func (w *idempotencyCaptureWriter) FlushError() error {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if w.destinationLost {
		return nil
	}
	if err := http.NewResponseController(w.destination).Flush(); err != nil {
		w.deliveryErr = err
		w.destinationLost = true
	}
	return nil
}

func (w *idempotencyCaptureWriter) capture(value []byte) {
	if w.truncated {
		return
	}
	remaining := w.maxResponseBytes - int64(w.body.Len())
	if remaining <= 0 {
		w.truncated = len(value) > 0
		return
	}
	if int64(len(value)) <= remaining {
		_, _ = w.body.Write(value)
		return
	}
	_, _ = w.body.Write(value[:remaining])
	w.truncated = true
}

func (w *idempotencyCaptureWriter) snapshot() (int, http.Header, []byte, bool, bool, error) {
	status := w.status
	if status == 0 && w.wroteHeader {
		status = http.StatusOK
	}
	headers := w.committedHeader
	if headers == nil {
		headers = w.Header().Clone()
	}
	return status, headers, bytes.Clone(w.body.Bytes()), w.wroteHeader, w.truncated, w.deliveryErr
}

func (w *idempotencyCaptureWriter) deliverySnapshot() (int, bool, bool, error) {
	return w.status, w.wroteHeader, w.truncated, w.deliveryErr
}

var _ http.ResponseWriter = (*idempotencyCaptureWriter)(nil)
var _ http.Flusher = (*idempotencyCaptureWriter)(nil)
