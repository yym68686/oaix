package egress

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const MaxTraceEvents = 96
const MaxTraceBytes = 16 * 1024

type RouteSnapshot struct {
	Kind            string `json:"kind"`
	ChannelID       int64  `json:"channel_id,omitempty"`
	ChannelRevision string `json:"channel_revision,omitempty"`
	BindingRevision string `json:"binding_revision,omitempty"`
	Protocol        string `json:"protocol,omitempty"`
	Endpoint        string `json:"endpoint,omitempty"`
}

type Event struct {
	Phase        string `json:"phase"`
	Step         string `json:"step"`
	AtUS         int64  `json:"at_us"`
	Endpoint     string `json:"endpoint,omitempty"`
	Error        string `json:"error,omitempty"`
	Status       int    `json:"status,omitempty"`
	ConnectionID string `json:"connection_id,omitempty"`
	RequestID    string `json:"upstream_request_id,omitempty"`
}

type TraceRecord struct {
	TransportError         string        `json:"transport_error,omitempty"`
	ProxySelectionObserved bool          `json:"proxy_selection_observed"`
	ProxyHandshakeCoverage string        `json:"proxy_handshake_coverage,omitempty"`
	DownstreamHeaderCount  int           `json:"downstream_request_id_header_count"`
	SchemaVersion          int           `json:"schema_version"`
	AttemptID              string        `json:"attempt_id"`
	RequestID              string        `json:"request_id"`
	AttemptIndex           int           `json:"attempt_index"`
	TraceID                string        `json:"trace_id"`
	SpanID                 string        `json:"span_id"`
	ParentSpanID           string        `json:"parent_span_id,omitempty"`
	TokenID                int64         `json:"token_id"`
	StartedAt              time.Time     `json:"started_at"`
	DurationUS             int64         `json:"duration_us"`
	Route                  RouteSnapshot `json:"route"`
	ConnectionID           string        `json:"connection_id,omitempty"`
	Reused                 bool          `json:"reused"`
	WasIdle                bool          `json:"was_idle"`
	IdleMS                 int64         `json:"idle_ms,omitempty"`
	LocalEndpoint          string        `json:"local_endpoint,omitempty"`
	RemoteEndpoint         string        `json:"remote_endpoint,omitempty"`
	TLSProtocol            string        `json:"tls_protocol,omitempty"`
	HTTPProtocol           string        `json:"http_protocol,omitempty"`
	UpstreamStatus         int           `json:"upstream_status,omitempty"`
	ContentType            string        `json:"content_type,omitempty"`
	ContentLength          int64         `json:"content_length"`
	TransferEncoding       string        `json:"transfer_encoding,omitempty"`
	ContentEncoding        string        `json:"content_encoding,omitempty"`
	Uncompressed           bool          `json:"uncompressed"`
	UpstreamRequestID      string        `json:"upstream_request_id,omitempty"`
	CFRay                  string        `json:"cf_ray,omitempty"`
	HeaderValueCount       int           `json:"upstream_request_id_header_count,omitempty"`
	BodyBytes              int64         `json:"body_bytes"`
	BodyReadCalls          int64         `json:"body_read_calls"`
	FirstBodyUS            *int64        `json:"first_body_us,omitempty"`
	LastBodyUS             *int64        `json:"last_body_us,omitempty"`
	BodyReadError          string        `json:"body_read_error,omitempty"`
	BodyEOF                bool          `json:"body_eof"`
	BodyCloseCalled        bool          `json:"body_close_called"`
	ParserEvents           int64         `json:"parser_events"`
	LastEventType          string        `json:"last_event_type,omitempty"`
	LastSequence           *int64        `json:"last_sequence,omitempty"`
	CompletedEvents        int64         `json:"completed_events"`
	FailedEvents           int64         `json:"failed_events"`
	DoneSeen               bool          `json:"done_seen"`
	LocalStatus            int           `json:"local_status"`
	Committed              bool          `json:"committed"`
	DownstreamStarted      bool          `json:"downstream_started"`
	Retry                  bool          `json:"retry"`
	ErrorClass             string        `json:"error_class,omitempty"`
	ContextError           string        `json:"context_error,omitempty"`
	SampleReason           string        `json:"sample_reason"`
	Events                 []Event       `json:"events"`
	DroppedEvents          int           `json:"dropped_events"`
	Truncated              bool          `json:"truncated"`
}

type traceKey struct{}
type Trace struct {
	mu                   sync.Mutex
	start                time.Time
	data                 TraceRecord
	finished             bool
	late                 *atomic.Uint64
	successSamplePercent int
}

func TraceFrom(ctx context.Context) *Trace {
	if ctx == nil {
		return nil
	}
	t, _ := ctx.Value(traceKey{}).(*Trace)
	return t
}

func (t *Trace) update(fn func(*TraceRecord)) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished {
		if t.late != nil {
			t.late.Add(1)
		}
		return
	}
	fn(&t.data)
}

func (t *Trace) Event(phase, step, endpoint string, err error, status int) {
	if t == nil {
		return
	}
	t.update(func(d *TraceRecord) {
		if phase == "round_trip" && err != nil {
			d.TransportError = ErrorClass(err)
		}
		if len(d.Events) >= MaxTraceEvents {
			d.DroppedEvents++
			d.Truncated = true
			return
		}
		d.Events = append(d.Events, Event{Phase: phase, Step: step, AtUS: time.Since(t.start).Microseconds(), Endpoint: safeEndpoint(endpoint), Error: ErrorClass(err), Status: status, ConnectionID: d.ConnectionID, RequestID: func() string {
			if phase == "headers" {
				return d.UpstreamRequestID
			}
			return ""
		}()})
	})
}

// ErrorClass never serializes an error message, URL, certificate name or token.
func ErrorClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof"
	case errors.Is(err, io.EOF):
		return "eof"
	case errors.Is(err, syscall.ECONNRESET):
		return "connection_reset"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection_refused"
	case errors.Is(err, syscall.EPIPE):
		return "broken_pipe"
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return "dns"
	}
	var cert x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	if errors.As(err, &cert) || errors.As(err, &hostname) || errors.As(err, &invalid) {
		return "tls_certificate"
	}
	var record tls.RecordHeaderError
	if errors.As(err, &record) {
		return "tls_record"
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	return "unknown"
}

func safeID(s string) string {
	if len(s) > 128 {
		return ""
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-_ .:", c)) {
			return ""
		}
	}
	return s
}

func safeEndpoint(s string) string {
	if s == "" {
		return ""
	}
	host, port, err := net.SplitHostPort(s)
	if err != nil || len(host) > 253 || len(port) > 5 {
		return ""
	}
	for _, c := range host {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-.:", c)) {
			return ""
		}
	}
	for _, c := range port {
		if c < '0' || c > '9' {
			return ""
		}
	}
	return net.JoinHostPort(host, port)
}

func knownValue(value string, allowed ...string) string {
	for _, v := range allowed {
		if value == v {
			return v
		}
	}
	if value == "" {
		return ""
	}
	return "other"
}

func (t *Trace) Response(resp *http.Response) {
	if t == nil || resp == nil {
		return
	}
	t.update(func(d *TraceRecord) {
		d.UpstreamStatus = resp.StatusCode
		d.HTTPProtocol = knownValue(resp.Proto, "HTTP/1.0", "HTTP/1.1", "HTTP/2.0", "HTTP/3.0")
		ct, _, _ := strings.Cut(resp.Header.Get("Content-Type"), ";")
		d.ContentType = knownValue(strings.TrimSpace(ct), "text/event-stream", "application/json", "text/plain")
		d.ContentLength = resp.ContentLength
		d.ContentEncoding = knownValue(resp.Header.Get("Content-Encoding"), "gzip", "br", "zstd", "identity")
		d.TransferEncoding = knownValue(strings.Join(resp.TransferEncoding, ","), "chunked", "identity")
		d.Uncompressed = resp.Uncompressed
		d.UpstreamRequestID = safeID(resp.Header.Get("X-Oai-Request-ID"))
		d.HeaderValueCount = len(resp.Header.Values("X-Oai-Request-ID"))
		d.CFRay = safeID(resp.Header.Get("CF-Ray"))
	})
	t.Event("headers", "done", "", nil, resp.StatusCode)
}

// ObserveSSE records protocol metadata only; it never keeps event data/text.
func ObserveSSE(ctx context.Context, eventType string, sequence *int64) {
	t := TraceFrom(ctx)
	t.update(func(d *TraceRecord) {
		d.ParserEvents++
		d.LastEventType = knownValue(eventType, "response.created", "response.queued", "response.in_progress", "response.completed", "response.failed", "response.incomplete", "response.output_item.added", "response.output_item.done", "response.content_part.added", "response.content_part.done", "response.output_text.delta", "response.output_text.done", "response.reasoning_summary_text.delta", "response.reasoning_summary_text.done", "response.function_call_arguments.delta", "response.function_call_arguments.done", "keepalive", "error", "[DONE]")
		if sequence != nil {
			copy := *sequence
			d.LastSequence = &copy
		}
		if eventType == "response.completed" {
			d.CompletedEvents++
		}
		if eventType == "response.failed" {
			d.FailedEvents++
		}
		if eventType == "[DONE]" {
			d.DoneSeen = true
		}
	})
}

type tracedBody struct {
	io.ReadCloser
	trace *Trace
}

func (b *tracedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.trace.update(func(d *TraceRecord) {
		d.BodyReadCalls++
		if n > 0 {
			us := time.Since(b.trace.start).Microseconds()
			d.BodyBytes += int64(n)
			if d.FirstBodyUS == nil {
				copy := us
				d.FirstBodyUS = &copy
			}
			d.LastBodyUS = &us
		}
		if err != nil {
			d.BodyEOF = errors.Is(err, io.EOF)
			if !d.BodyEOF {
				d.BodyReadError = ErrorClass(err)
			}
		}
	})
	if err != nil {
		readErr := err
		if errors.Is(err, io.EOF) {
			readErr = nil
		}
		b.trace.Event("body_read", "done", "", readErr, 0)
	}
	return n, err
}
func (b *tracedBody) Close() error {
	b.trace.update(func(d *TraceRecord) { d.BodyCloseCalled = true })
	err := b.ReadCloser.Close()
	b.trace.Event("body_close", "done", "", err, 0)
	return err
}

var bootID = randomHex(8)
var connectionSequence atomic.Uint64

type observedConn struct {
	net.Conn
	id string
}

func wrapConn(conn net.Conn) net.Conn {
	return &observedConn{Conn: conn, id: fmt.Sprintf("egress-%s-%x", bootID, connectionSequence.Add(1))}
}
func connectionID(conn net.Conn) string {
	for i := 0; conn != nil && i < 4; i++ {
		if c, ok := conn.(*observedConn); ok {
			return c.id
		}
		if c, ok := conn.(interface{ NetConn() net.Conn }); ok {
			conn = c.NetConn()
			continue
		}
		break
	}
	return "" // Explicitly unknown for a transport we do not own.
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%0*x", n*2, time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func validHex(s string, n int) bool {
	if len(s) != n || s == strings.Repeat("0", n) {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func TraceIdentity(headers http.Header) (trace, parent string) {
	parts := strings.Split(headers.Get("Traceparent"), "-")
	if len(parts) == 4 && parts[0] == "00" && len(parts[3]) == 2 && validHex(parts[1], 32) && validHex(parts[2], 16) {
		if _, err := hex.DecodeString(parts[3]); err == nil {
			return strings.ToLower(parts[1]), strings.ToLower(parts[2])
		}
	}
	if v := headers.Get("X-Fugue-Trace-ID"); validHex(v, 32) {
		return strings.ToLower(v), ""
	}
	return "", ""
}

func (t *Trace) withHooks(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, traceKey{}, t)
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GetConn: func(address string) { t.Event("connection", "start", address, nil, 0) },
		GotConn: func(info httptrace.GotConnInfo) {
			t.update(func(d *TraceRecord) {
				d.ConnectionID = connectionID(info.Conn)
				d.Reused = info.Reused
				d.WasIdle = info.WasIdle
				d.IdleMS = info.IdleTime.Milliseconds()
				d.LocalEndpoint = safeEndpoint(info.Conn.LocalAddr().String())
				d.RemoteEndpoint = safeEndpoint(info.Conn.RemoteAddr().String())
			})
			t.Event("connection", "done", "", nil, 0)
		},
		DNSStart:          func(info httptrace.DNSStartInfo) { t.Event("dns", "start", "", nil, 0) },
		DNSDone:           func(info httptrace.DNSDoneInfo) { t.Event("dns", "done", "", info.Err, 0) },
		ConnectStart:      func(network, address string) { t.Event(networkPhase(network), "start", address, nil, 0) },
		ConnectDone:       func(network, address string, err error) { t.Event(networkPhase(network), "done", address, err, 0) },
		TLSHandshakeStart: func() { t.Event("tls", "start", "", nil, 0) },
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			t.update(func(d *TraceRecord) { d.TLSProtocol = knownValue(state.NegotiatedProtocol, "h2", "http/1.1") })
			t.Event("tls", "done", "", err, 0)
		},
		WroteRequest:         func(info httptrace.WroteRequestInfo) { t.Event("request_write", "done", "", info.Err, 0) },
		GotFirstResponseByte: func() { t.Event("response_first_byte", "done", "", nil, 0) },
	})
}

func (t *Trace) ObserveDownstreamHeaders(headers http.Header) {
	t.update(func(d *TraceRecord) { d.DownstreamHeaderCount = len(headers.Values("X-Oai-Request-ID")) })
}

// GotConn occurs after TLS. Preserve the socket identity for earlier failures.
func observeDialConn(ctx context.Context, conn net.Conn) net.Conn {
	wrapped := wrapConn(conn)
	trace := TraceFrom(ctx)
	trace.update(func(d *TraceRecord) {
		d.ConnectionID = connectionID(wrapped)
		d.LocalEndpoint = safeEndpoint(conn.LocalAddr().String())
		d.RemoteEndpoint = safeEndpoint(conn.RemoteAddr().String())
	})
	trace.Event("socket_open", "done", "", nil, 0)
	return wrapped
}

func networkPhase(network string) string {
	if strings.HasPrefix(network, "tcp") {
		return "tcp"
	}
	if strings.HasPrefix(network, "udp") {
		return "udp"
	}
	return "network_connect"
}
