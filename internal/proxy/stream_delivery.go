package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

const maxStreamDeliveryErrorLength = 512

type bufferedStreamEvent struct {
	raw                   []byte
	eventType             string
	eventOrdinal          int
	payloadSequenceNumber *int64
}

type streamDeliveryOperationError struct {
	reason string
	err    error
}

func (e streamDeliveryOperationError) Error() string { return e.err.Error() }
func (e streamDeliveryOperationError) Unwrap() error { return e.err }

func observeUpstreamStreamEvent(trace *store.StreamDeliveryTrace, eventType string, eventOrdinal int, payloadSequenceNumber *int64, data []byte) {
	if trace == nil {
		return
	}
	trace.Upstream.ParserEventCount++
	trace.Upstream.LastEventOrdinal = eventOrdinal
	trace.Upstream.LastPayloadSequenceNumber = cloneInt64(payloadSequenceNumber)
	var first **store.StreamDeliveryCompletedUpstream
	switch eventType {
	case "response.completed":
		trace.Upstream.ResponseCompletedCount++
		first = &trace.Upstream.FirstResponseCompleted
	case "response.failed":
		trace.Upstream.ResponseFailedCount++
		first = &trace.Upstream.FirstResponseFailed
	default:
		return
	}
	if *first != nil {
		return
	}
	*first = &store.StreamDeliveryCompletedUpstream{
		EventOrdinal:          eventOrdinal,
		PayloadSequenceNumber: cloneInt64(payloadSequenceNumber),
		ReceivedAt:            time.Now().UTC(),
		DataBytes:             len(data),
		DataSHA256:            sha256Bytes(data),
	}
}

func writeDownstreamStreamEvent(w http.ResponseWriter, trace *store.StreamDeliveryTrace, event bufferedStreamEvent, flush bool) error {
	if trace != nil {
		trace.Downstream.EventWriteAttemptCount++
		trace.Downstream.BytesWriteAttempted += int64(len(event.raw))
	}
	isTerminal := event.eventType == "response.completed" || event.eventType == "response.failed"
	var terminal *store.StreamDeliveryCompletedDownstream
	if isTerminal && trace != nil {
		var first **store.StreamDeliveryCompletedDownstream
		if event.eventType == "response.completed" {
			first = &trace.Downstream.FirstResponseCompleted
		} else {
			first = &trace.Downstream.FirstResponseFailed
		}
		if *first == nil {
			terminal = &store.StreamDeliveryCompletedDownstream{
				EventOrdinal:          event.eventOrdinal,
				PayloadSequenceNumber: cloneInt64(event.payloadSequenceNumber),
				WireBytes:             len(event.raw),
				WireSHA256:            sha256Bytes(event.raw),
				WriteAttemptedAt:      time.Now().UTC(),
				FlushResult:           "not_attempted",
			}
			*first = terminal
		}
	}
	n, writeErr := w.Write(event.raw)
	if trace != nil {
		trace.Downstream.BytesWritten += int64(n)
	}
	if terminal != nil {
		terminal.WrittenBytes = n
		terminal.WriteCompletedAt = time.Now().UTC()
	}
	if writeErr == nil && n != len(event.raw) {
		writeErr = io.ErrShortWrite
	}
	if writeErr != nil {
		result := "failed"
		if n > 0 && n < len(event.raw) {
			result = "partial"
		}
		if terminal != nil {
			terminal.WriteResult = result
			terminal.WriteError = streamDeliveryError(writeErr)
		}
		reason := "downstream_write_error"
		if isTerminal {
			reason = "downstream_terminal_write_error"
		}
		return streamDeliveryOperationError{reason: reason, err: writeErr}
	}
	if trace != nil {
		trace.Downstream.EventWriteSuccessCount++
	}
	if terminal != nil {
		terminal.WriteResult = "succeeded"
	}
	if !flush {
		return nil
	}
	flushAttemptedAt := time.Now().UTC()
	if terminal != nil {
		terminal.FlushAttemptedAt = &flushAttemptedAt
	}
	flushErr := http.NewResponseController(w).Flush()
	flushCompletedAt := time.Now().UTC()
	if terminal != nil {
		terminal.FlushCompletedAt = &flushCompletedAt
	}
	if flushErr == nil {
		if terminal != nil {
			terminal.FlushResult = "succeeded"
		}
		return nil
	}
	if errors.Is(flushErr, http.ErrNotSupported) {
		if terminal != nil {
			terminal.FlushResult = "not_supported"
			terminal.FlushError = streamDeliveryError(flushErr)
		}
		reason := "downstream_flush_not_supported"
		if isTerminal {
			reason = "downstream_terminal_flush_not_supported"
		}
		return streamDeliveryOperationError{reason: reason, err: flushErr}
	}
	if terminal != nil {
		terminal.FlushResult = "failed"
		terminal.FlushError = streamDeliveryError(flushErr)
	}
	reason := "downstream_flush_error"
	if isTerminal {
		reason = "downstream_terminal_flush_error"
	}
	return streamDeliveryOperationError{reason: reason, err: flushErr}
}

func finishStreamDeliveryTrace(trace *store.StreamDeliveryTrace, parseErr error) {
	if trace == nil {
		return
	}
	now := time.Now().UTC()
	trace.End.At = now
	if parseErr == nil {
		trace.Upstream.EOFAt = &now
		trace.Downstream.FinalBodyProducedAt = &now
		if !hasUpstreamStreamTerminal(trace) {
			trace.End.Reason = "upstream_eof_before_terminal"
		} else {
			trace.End.Reason = "upstream_eof_after_terminal"
		}
		return
	}
	trace.End.Error = streamDeliveryError(parseErr)
	var operationErr streamDeliveryOperationError
	if errors.As(parseErr, &operationErr) {
		trace.End.Reason = operationErr.reason
		return
	}
	if errors.Is(parseErr, context.Canceled) {
		if !hasUpstreamStreamTerminal(trace) {
			trace.End.Reason = "request_context_canceled_before_terminal"
		} else {
			trace.End.Reason = "request_context_canceled_after_terminal"
		}
		return
	}
	if !hasUpstreamStreamTerminal(trace) {
		trace.End.Reason = "upstream_stream_error_before_terminal"
	} else {
		trace.End.Reason = "upstream_stream_error_after_terminal"
	}
}

func finishBufferedResponsesFailureTrace(trace *store.StreamDeliveryTrace, sourceEventType string) {
	if trace == nil {
		return
	}
	now := time.Now().UTC()
	trace.End.At = now
	if strings.EqualFold(strings.TrimSpace(sourceEventType), "error") {
		trace.End.Reason = "upstream_provider_error_normalized_failure_buffered"
	} else {
		trace.End.Reason = "upstream_response_failed_terminal_buffered"
	}
	trace.End.Error = ""
}

func finishDeliveredResponsesFailureTrace(trace *store.StreamDeliveryTrace, deliveryErr error, asHTTPError bool, sourceEventType string) {
	if trace == nil {
		return
	}
	if deliveryErr != nil {
		finishStreamDeliveryTrace(trace, deliveryErr)
		if asHTTPError {
			trace.End.Reason = "downstream_http_error_write_error"
		}
		return
	}
	now := time.Now().UTC()
	trace.Downstream.FinalBodyProducedAt = &now
	trace.End.At = now
	if strings.EqualFold(strings.TrimSpace(sourceEventType), "error") && asHTTPError {
		trace.End.Reason = "upstream_provider_error_normalized_http_error_delivered"
	} else if strings.EqualFold(strings.TrimSpace(sourceEventType), "error") {
		trace.End.Reason = "upstream_provider_error_normalized_failure_delivered"
	} else if asHTTPError {
		trace.End.Reason = "upstream_response_failed_http_error_delivered"
	} else {
		trace.End.Reason = "upstream_response_failed_terminal_delivered"
	}
	trace.End.Error = ""
}

func hasUpstreamStreamTerminal(trace *store.StreamDeliveryTrace) bool {
	return trace != nil && (trace.Upstream.FirstResponseCompleted != nil || trace.Upstream.FirstResponseFailed != nil)
}

func payloadSequenceNumber(payload map[string]any) *int64 {
	if payload == nil {
		return nil
	}
	value, ok := payload["sequence_number"]
	if !ok {
		return nil
	}
	var result int64
	switch typed := value.(type) {
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) || math.Trunc(typed) != typed || typed < -9223372036854775808 || typed >= 9223372036854775808 {
			return nil
		}
		result = int64(typed)
	case int:
		result = int64(typed)
	case int64:
		result = typed
	default:
		return nil
	}
	return &result
}

func sha256Bytes(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func streamDeliveryError(err error) string {
	if err == nil {
		return ""
	}
	value := strings.TrimSpace(err.Error())
	if len(value) > maxStreamDeliveryErrorLength {
		value = value[:maxStreamDeliveryErrorLength]
	}
	return value
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func streamDeliveryState(trace *store.StreamDeliveryTrace) *string {
	if trace == nil {
		return nil
	}
	state := trace.State()
	if state == "" {
		return nil
	}
	return &state
}
