package store

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestStreamDeliveryTraceJSONRoundTrip(t *testing.T) {
	receivedAt := time.Date(2026, time.July, 13, 10, 11, 12, 123456789, time.UTC)
	writeCompletedAt := receivedAt.Add(time.Millisecond)
	flushAttemptedAt := writeCompletedAt.Add(time.Millisecond)
	flushCompletedAt := flushAttemptedAt.Add(time.Millisecond)
	eofAt := flushCompletedAt.Add(time.Millisecond)
	finalBodyProducedAt := eofAt.Add(time.Millisecond)
	endAt := finalBodyProducedAt.Add(time.Millisecond)
	sequenceNumber := int64(27)

	want := NewStreamDeliveryTrace("oaixc-test-1")
	want.Upstream = StreamDeliveryUpstreamTrace{
		ParserEventCount:          2,
		LastEventOrdinal:          2,
		LastPayloadSequenceNumber: &sequenceNumber,
		ResponseCompletedCount:    1,
		FirstResponseCompleted: &StreamDeliveryCompletedUpstream{
			EventOrdinal:          2,
			PayloadSequenceNumber: &sequenceNumber,
			ReceivedAt:            receivedAt,
			DataBytes:             42,
			DataSHA256:            "upstream-sha256",
		},
		EOFAt: &eofAt,
	}
	want.Downstream.EventWriteAttemptCount = 2
	want.Downstream.EventWriteSuccessCount = 2
	want.Downstream.BytesWriteAttempted = 84
	want.Downstream.BytesWritten = 84
	want.Downstream.FirstResponseCompleted = &StreamDeliveryCompletedDownstream{
		EventOrdinal:          2,
		PayloadSequenceNumber: &sequenceNumber,
		WireBytes:             44,
		WireSHA256:            "wire-sha256",
		WriteAttemptedAt:      receivedAt,
		WrittenBytes:          44,
		WriteResult:           "succeeded",
		WriteCompletedAt:      writeCompletedAt,
		FlushAttemptedAt:      &flushAttemptedAt,
		FlushResult:           "succeeded",
		FlushCompletedAt:      &flushCompletedAt,
	}
	want.Downstream.FinalBodyProducedAt = &finalBodyProducedAt
	want.End = StreamDeliveryEndTrace{At: endAt, Reason: "upstream_eof_after_terminal"}

	encoded := streamDeliveryTraceBytes(want)
	if len(encoded) == 0 {
		t.Fatal("stream delivery trace encoded to an empty payload")
	}
	var got StreamDeliveryTrace
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("unmarshal stream delivery trace: %v", err)
	}
	if !reflect.DeepEqual(got, *want) {
		t.Fatalf("stream delivery trace round-trip mismatch:\n got: %#v\nwant: %#v", got, *want)
	}
	if got.SchemaVersion != StreamDeliveryTraceSchemaVersion {
		t.Fatalf("unexpected trace schema version: got %d want %d", got.SchemaVersion, StreamDeliveryTraceSchemaVersion)
	}
}

func TestStreamDeliveryTraceBytesNil(t *testing.T) {
	if encoded := streamDeliveryTraceBytes(nil); encoded != nil {
		t.Fatalf("nil trace must encode as nil, got %q", encoded)
	}
}

func TestStreamDeliveryTraceState(t *testing.T) {
	tests := []struct {
		name  string
		trace *StreamDeliveryTrace
		want  string
	}{
		{name: "nil", trace: nil, want: ""},
		{name: "terminal missing", trace: NewStreamDeliveryTrace(""), want: "terminal_missing"},
		{
			name:  "stream error",
			trace: &StreamDeliveryTrace{End: StreamDeliveryEndTrace{Error: "upstream reset"}},
			want:  "stream_error",
		},
		{
			name: "terminal received",
			trace: &StreamDeliveryTrace{Upstream: StreamDeliveryUpstreamTrace{
				FirstResponseCompleted: &StreamDeliveryCompletedUpstream{},
			}},
			want: "terminal_received",
		},
		{
			name:  "terminal write partial",
			trace: traceWithCompletedDelivery("partial", ""),
			want:  "terminal_write_partial",
		},
		{
			name:  "terminal write failed",
			trace: traceWithCompletedDelivery("failed", ""),
			want:  "terminal_write_failed",
		},
		{
			name:  "terminal written",
			trace: traceWithCompletedDelivery("succeeded", ""),
			want:  "terminal_written",
		},
		{
			name:  "terminal flushed",
			trace: traceWithCompletedDelivery("succeeded", "succeeded"),
			want:  "terminal_flushed",
		},
		{
			name:  "terminal flush not supported",
			trace: traceWithCompletedDelivery("succeeded", "not_supported"),
			want:  "terminal_flush_not_supported",
		},
		{
			name:  "terminal flush failed",
			trace: traceWithCompletedDelivery("succeeded", "failed"),
			want:  "terminal_flush_failed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.trace.State(); got != test.want {
				t.Fatalf("unexpected state: got %q want %q", got, test.want)
			}
		})
	}
}

func traceWithCompletedDelivery(writeResult, flushResult string) *StreamDeliveryTrace {
	return &StreamDeliveryTrace{Downstream: StreamDeliveryDownstreamTrace{
		FirstResponseCompleted: &StreamDeliveryCompletedDownstream{
			WriteResult: writeResult,
			FlushResult: flushResult,
		},
	}}
}
