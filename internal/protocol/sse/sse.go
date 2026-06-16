package sse

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type Event struct {
	Event string
	ID    string
	Retry string
	Data  []byte
	Raw   []byte
}

type Parser struct {
	maxEventBytes int
}

type Writer struct {
	w       io.Writer
	flusher http.Flusher
}

func NewParser(maxEventBytes int) *Parser {
	if maxEventBytes <= 0 {
		maxEventBytes = 8 * 1024 * 1024
	}
	return &Parser{maxEventBytes: maxEventBytes}
}

func (p *Parser) Parse(ctx context.Context, reader io.Reader, emit func(Event) error) error {
	br := bufio.NewReader(reader)
	var event Event
	var data bytes.Buffer
	var raw bytes.Buffer
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			if raw.Len()+len(line) > p.maxEventBytes {
				return fmt.Errorf("sse event exceeds %d bytes", p.maxEventBytes)
			}
			raw.Write(line)
			trimmed := strings.TrimRight(string(line), "\r\n")
			if trimmed == "" {
				if data.Len() > 0 || event.Event != "" || event.ID != "" || event.Retry != "" {
					event.Data = bytes.TrimSuffix(data.Bytes(), []byte{'\n'})
					event.Raw = append([]byte(nil), raw.Bytes()...)
					if err := emit(event); err != nil {
						return err
					}
				}
				event = Event{}
				data.Reset()
				raw.Reset()
				continue
			}
			if strings.HasPrefix(trimmed, ":") {
				continue
			}
			field, value, ok := strings.Cut(trimmed, ":")
			if ok && strings.HasPrefix(value, " ") {
				value = value[1:]
			}
			switch field {
			case "event":
				event.Event = value
			case "id":
				event.ID = value
			case "retry":
				event.Retry = value
			case "data":
				data.WriteString(value)
				data.WriteByte('\n')
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				if data.Len() > 0 || event.Event != "" || event.ID != "" || event.Retry != "" {
					event.Data = bytes.TrimSuffix(data.Bytes(), []byte{'\n'})
					event.Raw = append([]byte(nil), raw.Bytes()...)
					return emit(event)
				}
				return nil
			}
			return err
		}
	}
}

func Encode(event string, data []byte) []byte {
	var buf bytes.Buffer
	if event != "" {
		buf.WriteString("event: ")
		buf.WriteString(event)
		buf.WriteByte('\n')
	}
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		buf.WriteString("data: ")
		buf.Write(line)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')
	return buf.Bytes()
}

func Keepalive() []byte {
	return []byte(": keepalive\n\n")
}

func NewWriter(w io.Writer) *Writer {
	writer := &Writer{w: w}
	if flusher, ok := w.(http.Flusher); ok {
		writer.flusher = flusher
	}
	return writer
}

func (w *Writer) WriteEvent(event string, data []byte) error {
	if _, err := w.w.Write(Encode(event, data)); err != nil {
		return err
	}
	if w.flusher != nil {
		w.flusher.Flush()
	}
	return nil
}

func (w *Writer) WriteKeepalive() error {
	if _, err := w.w.Write(Keepalive()); err != nil {
		return err
	}
	if w.flusher != nil {
		w.flusher.Flush()
	}
	return nil
}
