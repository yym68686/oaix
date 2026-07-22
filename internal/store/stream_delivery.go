package store

import "time"

const StreamDeliveryTraceSchemaVersion = 3

// StreamDeliveryTrace records only what the OAIX process observed locally.
// Successful writes and flush calls do not assert that the downstream
// application received, parsed, or persisted the event.
type StreamDeliveryTrace struct {
	SchemaVersion int                           `json:"schema_version"`
	Semantics     string                        `json:"semantics"`
	Upstream      StreamDeliveryUpstreamTrace   `json:"upstream"`
	Downstream    StreamDeliveryDownstreamTrace `json:"downstream"`
	End           StreamDeliveryEndTrace        `json:"end"`
}

type StreamDeliveryUpstreamTrace struct {
	ParserEventCount          int                              `json:"parser_event_count"`
	LastEventOrdinal          int                              `json:"last_event_ordinal"`
	LastPayloadSequenceNumber *int64                           `json:"last_payload_sequence_number,omitempty"`
	ResponseCompletedCount    int                              `json:"response_completed_count"`
	FirstResponseCompleted    *StreamDeliveryCompletedUpstream `json:"first_response_completed,omitempty"`
	ResponseFailedCount       int                              `json:"response_failed_count"`
	FirstResponseFailed       *StreamDeliveryCompletedUpstream `json:"first_response_failed,omitempty"`
	EOFAt                     *time.Time                       `json:"eof_at,omitempty"`
}

type StreamDeliveryCompletedUpstream struct {
	EventOrdinal          int       `json:"event_ordinal"`
	PayloadSequenceNumber *int64    `json:"payload_sequence_number,omitempty"`
	ReceivedAt            time.Time `json:"received_at"`
	DataBytes             int       `json:"data_bytes"`
	DataSHA256            string    `json:"data_sha256"`
}

type StreamDeliveryDownstreamTrace struct {
	ConnectionID           string                             `json:"connection_id,omitempty"`
	EventWriteAttemptCount int                                `json:"event_write_attempt_count"`
	EventWriteSuccessCount int                                `json:"event_write_success_count"`
	BytesWriteAttempted    int64                              `json:"bytes_write_attempted"`
	BytesWritten           int64                              `json:"bytes_written"`
	FirstResponseCompleted *StreamDeliveryCompletedDownstream `json:"first_response_completed,omitempty"`
	FirstResponseFailed    *StreamDeliveryCompletedDownstream `json:"first_response_failed,omitempty"`
	FinalBodyProducedAt    *time.Time                         `json:"final_body_produced_at,omitempty"`
}

type StreamDeliveryCompletedDownstream struct {
	EventOrdinal           int        `json:"event_ordinal"`
	PayloadSequenceNumber  *int64     `json:"payload_sequence_number,omitempty"`
	WireBytes              int        `json:"wire_bytes"`
	WireSHA256             string     `json:"wire_sha256"`
	WriteAttemptedAt       time.Time  `json:"write_attempted_at"`
	WrittenBytes           int        `json:"written_bytes"`
	WriteResult            string     `json:"write_result"`
	WriteError             string     `json:"write_error,omitempty"`
	WriteCompletedAt       time.Time  `json:"write_completed_at"`
	FlushAttemptedAt       *time.Time `json:"flush_attempted_at,omitempty"`
	FlushResult            string     `json:"flush_result"`
	FlushError             string     `json:"flush_error,omitempty"`
	FlushCompletedAt       *time.Time `json:"flush_completed_at,omitempty"`
	FlushMarkerContract    string     `json:"flush_marker_contract,omitempty"`
	MarkerWriteAttemptedAt *time.Time `json:"marker_write_attempted_at,omitempty"`
	MarkerWriteCompletedAt *time.Time `json:"marker_write_completed_at,omitempty"`
	MarkerWriteResult      string     `json:"marker_write_result,omitempty"`
	MarkerWriteError       string     `json:"marker_write_error,omitempty"`
	MarkerFlushAttemptedAt *time.Time `json:"marker_flush_attempted_at,omitempty"`
	MarkerFlushCompletedAt *time.Time `json:"marker_flush_completed_at,omitempty"`
	MarkerFlushResult      string     `json:"marker_flush_result,omitempty"`
	MarkerFlushError       string     `json:"marker_flush_error,omitempty"`
}

type StreamDeliveryEndTrace struct {
	At     time.Time `json:"at"`
	Reason string    `json:"reason"`
	Error  string    `json:"error,omitempty"`
}

func NewStreamDeliveryTrace(connectionID string) *StreamDeliveryTrace {
	return &StreamDeliveryTrace{
		SchemaVersion: StreamDeliveryTraceSchemaVersion,
		Semantics:     "oaix_local_write_flush_only",
		Downstream: StreamDeliveryDownstreamTrace{
			ConnectionID: connectionID,
		},
	}
}

func streamDeliveryTraceBytes(trace *StreamDeliveryTrace) []byte {
	if trace == nil {
		return nil
	}
	return jsonBytes(trace)
}

func (t *StreamDeliveryTrace) State() string {
	if t == nil {
		return ""
	}
	switch t.End.Reason {
	case "upstream_response_failed_http_error_delivered", "upstream_provider_error_normalized_http_error_delivered":
		return "http_error_delivered"
	case "upstream_provider_error_normalized_failure_buffered":
		return "terminal_received"
	case "downstream_http_error_write_error":
		return "http_error_write_failed"
	}
	terminal := t.Downstream.FirstResponseCompleted
	if terminal == nil {
		terminal = t.Downstream.FirstResponseFailed
	}
	if terminal != nil {
		switch terminal.WriteResult {
		case "partial":
			return "terminal_write_partial"
		case "failed":
			return "terminal_write_failed"
		case "succeeded":
			switch terminal.FlushResult {
			case "succeeded":
				return "terminal_flushed"
			case "not_supported":
				return "terminal_flush_not_supported"
			case "failed":
				return "terminal_flush_failed"
			default:
				return "terminal_written"
			}
		}
	}
	if t.Upstream.FirstResponseCompleted != nil || t.Upstream.FirstResponseFailed != nil {
		return "terminal_received"
	}
	if t.End.Error != "" {
		return "stream_error"
	}
	return "terminal_missing"
}
