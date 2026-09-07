package egress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// OTLP JSON uses the existing platform endpoint, never an account proxy.
type OTLPExporter struct {
	endpoint string
	client   *http.Client
	resource map[string]string
}

func NewOTLPExporter(endpoint string, resource map[string]string) *OTLPExporter {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/v1/traces"
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	tr.MaxIdleConns = 2
	tr.MaxIdleConnsPerHost = 2
	return &OTLPExporter{endpoint: u.String(), client: &http.Client{Transport: tr, Timeout: 900 * time.Millisecond, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, resource: resource}
}

func (e *OTLPExporter) Close() {
	if e != nil {
		e.client.CloseIdleConnections()
	}
}

func otlpAttrs(fields map[string]string) []map[string]any {
	attrs := make([]map[string]any, 0, len(fields))
	for key, value := range fields {
		if value != "" {
			attrs = append(attrs, map[string]any{"key": key, "value": map[string]string{"stringValue": value}})
		}
	}
	return attrs
}

func (e *OTLPExporter) Export(ctx context.Context, d TraceRecord) error {
	if e == nil {
		return errors.New("telemetry endpoint unavailable")
	}
	span := func(id, parent, name string, start, end int64, fields map[string]string) map[string]any {
		fields["request_id"] = d.RequestID
		fields["attempt_id"] = d.AttemptID
		fields["service"] = "oaix"
		fields["stage"] = name
		return map[string]any{"traceId": d.TraceID, "spanId": id, "parentSpanId": parent, "name": name, "kind": 3, "startTimeUnixNano": strconv.FormatInt(d.StartedAt.Add(time.Duration(start)*time.Microsecond).UnixNano(), 10), "endTimeUnixNano": strconv.FormatInt(d.StartedAt.Add(time.Duration(end)*time.Microsecond).UnixNano(), 10), "attributes": otlpAttrs(fields)}
	}
	root := span(d.SpanID, d.ParentSpanID, "egress_attempt", 0, d.DurationUS, map[string]string{"status_code": strconv.Itoa(d.LocalStatus), "upstream_status_code": strconv.Itoa(d.UpstreamStatus), "error_type": d.ErrorClass, "body_read_error": d.BodyReadError, "transport_error": d.TransportError, "proxy_channel_id": strconv.FormatInt(d.Route.ChannelID, 10), "connection_id": d.ConnectionID})
	if d.ErrorClass != "" {
		root["status"] = map[string]any{"code": 2, "message": d.ErrorClass}
	}
	spans := []map[string]any{root}
	starts := map[string][]int64{}
	ambiguous := map[string]bool{}
	for i, event := range d.Events {
		key := event.Phase + "|" + event.Endpoint
		if event.Step == "start" {
			if len(starts[key]) > 0 {
				ambiguous[key] = true
			}
			starts[key] = append(starts[key], event.AtUS)
			continue
		}
		if len(starts[key]) == 0 && event.Phase == "connection" {
			for k := range starts {
				if strings.HasPrefix(k, "connection|") {
					key = k
					break
				}
			}
		}
		start, ok := event.AtUS, false
		if pending := starts[key]; len(pending) > 0 {
			// Hooks do not identify parallel DNS dials to the same endpoint.
			// Preserve their events without inventing which start matches a done.
			if !ambiguous[key] {
				start = pending[0]
				ok = true
			}
			starts[key] = pending[1:]
			if len(starts[key]) == 0 {
				delete(starts, key)
				delete(ambiguous, key)
			}
		}
		hash := sha256.Sum256([]byte(d.SpanID + ":" + strconv.Itoa(i)))
		spans = append(spans, span(hex.EncodeToString(hash[:8]), d.SpanID, event.Phase, start, event.AtUS, map[string]string{"error_type": event.Error, "status_code": strconv.Itoa(event.Status), "paired_start": strconv.FormatBool(ok)}))
	}
	data, err := json.Marshal(map[string]any{"resourceSpans": []any{map[string]any{"resource": map[string]any{"attributes": otlpAttrs(e.resource)}, "scopeSpans": []any{map[string]any{"scope": map[string]string{"name": "oaix.egress", "version": "2"}, "spans": spans}}}}})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(data))
	if err != nil {
		return errors.New("invalid telemetry endpoint")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		return errors.New("telemetry export failed")
	}
	defer resp.Body.Close()
	reply, readErr := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if readErr != nil || len(reply) > 4096 {
		return errors.New("telemetry acknowledgement incomplete")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.New("telemetry export rejected")
	}
	if len(bytes.TrimSpace(reply)) > 0 {
		var ack struct {
			PartialSuccess struct {
				RejectedSpans json.RawMessage `json:"rejectedSpans"`
			} `json:"partialSuccess"`
		}
		if err := json.Unmarshal(reply, &ack); err != nil {
			return errors.New("invalid telemetry acknowledgement")
		}
		rejected := strings.Trim(string(ack.PartialSuccess.RejectedSpans), `"`)
		if rejected != "" && rejected != "0" {
			return errors.New("telemetry spans partially rejected")
		}
	}
	return nil
}
