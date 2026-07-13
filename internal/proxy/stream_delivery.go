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
	if eventType != "response.completed" {
		return
	}
	trace.Upstream.ResponseCompletedCount++
	if trace.Upstream.FirstResponseCompleted != nil {
		return
	}
	trace.Upstream.FirstResponseCompleted = &store.StreamDeliveryCompletedUpstream{
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
	isCompleted := event.eventType == "response.completed"
	var completed *store.StreamDeliveryCompletedDownstream
	if isCompleted && trace != nil && trace.Downstream.FirstResponseCompleted == nil {
		completed = &store.StreamDeliveryCompletedDownstream{
			EventOrdinal:          event.eventOrdinal,
			PayloadSequenceNumber: cloneInt64(event.payloadSequenceNumber),
			WireBytes:             len(event.raw),
			WireSHA256:            sha256Bytes(event.raw),
			WriteAttemptedAt:      time.Now().UTC(),
			FlushResult:           "not_attempted",
		}
		trace.Downstream.FirstResponseCompleted = completed
	}
	n, writeErr := w.Write(event.raw)
	if trace != nil {
		trace.Downstream.BytesWritten += int64(n)
	}
	if completed != nil {
		completed.WrittenBytes = n
		completed.WriteCompletedAt = time.Now().UTC()
	}
	if writeErr == nil && n != len(event.raw) {
		writeErr = io.ErrShortWrite
	}
	if writeErr != nil {
		result := "failed"
		if n > 0 && n < len(event.raw) {
			result = "partial"
		}
		if completed != nil {
			completed.WriteResult = result
			completed.WriteError = streamDeliveryError(writeErr)
		}
		reason := "downstream_write_error"
		if isCompleted {
			reason = "downstream_terminal_write_error"
		}
		return streamDeliveryOperationError{reason: reason, err: writeErr}
	}
	if trace != nil {
		trace.Downstream.EventWriteSuccessCount++
	}
	if completed != nil {
		completed.WriteResult = "succeeded"
	}
	if !flush {
		return nil
	}
	flushAttemptedAt := time.Now().UTC()
	if completed != nil {
		completed.FlushAttemptedAt = &flushAttemptedAt
	}
	flushErr := http.NewResponseController(w).Flush()
	flushCompletedAt := time.Now().UTC()
	if completed != nil {
		completed.FlushCompletedAt = &flushCompletedAt
	}
	if flushErr == nil {
		if completed != nil {
			completed.FlushResult = "succeeded"
		}
		return nil
	}
	if errors.Is(flushErr, http.ErrNotSupported) {
		if completed != nil {
			completed.FlushResult = "not_supported"
			completed.FlushError = streamDeliveryError(flushErr)
		}
		reason := "downstream_flush_not_supported"
		if isCompleted {
			reason = "downstream_terminal_flush_not_supported"
		}
		return streamDeliveryOperationError{reason: reason, err: flushErr}
	}
	if completed != nil {
		completed.FlushResult = "failed"
		completed.FlushError = streamDeliveryError(flushErr)
	}
	reason := "downstream_flush_error"
	if isCompleted {
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
		if trace.Upstream.FirstResponseCompleted == nil {
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
		if trace.Upstream.FirstResponseCompleted == nil {
			trace.End.Reason = "request_context_canceled_before_terminal"
		} else {
			trace.End.Reason = "request_context_canceled_after_terminal"
		}
		return
	}
	if trace.Upstream.FirstResponseCompleted == nil {
		trace.End.Reason = "upstream_stream_error_before_terminal"
	} else {
		trace.End.Reason = "upstream_stream_error_after_terminal"
	}
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
