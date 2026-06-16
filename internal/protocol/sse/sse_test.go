package sse

import (
	"context"
	"strings"
	"testing"
)

func TestParserHandlesMultilineDataAndComments(t *testing.T) {
	input := ": keepalive\n" +
		"event: response.output_text.delta\n" +
		"id: evt_1\n" +
		"data: hello\n" +
		"data: world\n\n"
	var events []Event
	err := NewParser(1024).Parse(context.Background(), strings.NewReader(input), func(event Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Event != "response.output_text.delta" {
		t.Fatalf("unexpected event name: %q", events[0].Event)
	}
	if string(events[0].Data) != "hello\nworld" {
		t.Fatalf("unexpected data: %q", events[0].Data)
	}
	if events[0].ID != "evt_1" {
		t.Fatalf("unexpected id: %q", events[0].ID)
	}
}

func TestParserEmitsFinalEventWithoutTrailingBlankLine(t *testing.T) {
	input := "data: {\"type\":\"done\"}"
	var events []Event
	err := NewParser(1024).Parse(context.Background(), strings.NewReader(input), func(event Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if string(events[0].Data) != "{\"type\":\"done\"}" {
		t.Fatalf("unexpected data: %q", events[0].Data)
	}
}

func TestParserRejectsOversizedEvent(t *testing.T) {
	input := "data: " + strings.Repeat("x", 32) + "\n\n"
	err := NewParser(16).Parse(context.Background(), strings.NewReader(input), func(Event) error {
		t.Fatal("oversized event should not be emitted")
		return nil
	})
	if err == nil {
		t.Fatal("expected oversized event error")
	}
}

func TestWriter(t *testing.T) {
	var out strings.Builder
	writer := NewWriter(&out)
	if err := writer.WriteKeepalive(); err != nil {
		t.Fatalf("WriteKeepalive returned error: %v", err)
	}
	if err := writer.WriteEvent("done", []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("WriteEvent returned error: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, ": keepalive") || !strings.Contains(got, "event: done") {
		t.Fatalf("unexpected writer output: %q", got)
	}
}
