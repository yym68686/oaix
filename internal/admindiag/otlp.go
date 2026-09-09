package admindiag

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

type Exporter struct {
	endpoint string
	client   *http.Client
	resource map[string]string
}

func NewExporter(endpoint string, resource map[string]string) *Exporter {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/v1/traces"
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	tr.MaxIdleConns = 1
	tr.MaxIdleConnsPerHost = 1
	return &Exporter{endpoint: u.String(), resource: resource, client: &http.Client{Transport: tr, Timeout: 900 * time.Millisecond, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}
func (e *Exporter) Close() {
	if e != nil {
		e.client.CloseIdleConnections()
	}
}
func attributes(values map[string]string) []map[string]any {
	out := []map[string]any{}
	for k, v := range values {
		if v != "" {
			out = append(out, map[string]any{"key": k, "value": map[string]string{"stringValue": v}})
		}
	}
	return out
}
func (e *Exporter) Export(ctx context.Context, d Record, _ []byte) error {
	if e == nil {
		return errors.New("telemetry endpoint unavailable")
	}
	if len(d.ID) != 32 || len(d.TraceID) != 32 {
		return errors.New("invalid trace identity")
	}
	rootID := d.ID[:16]
	span := func(id, parent, name string, start, end int64, fields map[string]string) map[string]any {
		fields["request_id"] = d.RequestID
		fields["observation_id"] = d.ID
		fields["stage"] = name
		fields["service"] = "oaix"
		fields["route"] = d.Route
		s := map[string]any{"traceId": d.TraceID, "spanId": id, "parentSpanId": parent, "name": name, "kind": 1, "startTimeUnixNano": strconv.FormatInt(d.StartedAt.Add(time.Duration(start)*time.Microsecond).UnixNano(), 10), "endTimeUnixNano": strconv.FormatInt(d.StartedAt.Add(time.Duration(end)*time.Microsecond).UnixNano(), 10), "attributes": attributes(fields)}
		if fields["error_type"] != "" {
			s["status"] = map[string]any{"code": 2, "message": fields["error_type"]}
		}
		return s
	}
	failure := ""
	if d.Status >= 400 {
		failure = "http_error"
	}
	spans := []map[string]any{span(rootID, d.ParentSpanID, "admin_request", 0, d.DurationUS, map[string]string{"status_code": strconv.Itoa(d.Status), "error_type": failure, "revision": d.Revision, "instance": d.Instance, "sample_reason": d.Reason})}
	for _, v := range d.Events {
		end := d.DurationUS
		complete := v.EndUS != nil
		if complete {
			end = *v.EndUS
		}
		h := sha256.Sum256([]byte(d.ID + ":" + v.ID))
		s := span(hex.EncodeToString(h[:8]), rootID, v.Stage+"."+v.Kind, v.StartUS, end, map[string]string{"query_execution_id": v.ID, "fingerprint": v.Fingerprint, "pid": strconv.FormatUint(uint64(v.PID), 10), "connection": v.Connection, "error_type": v.Error, "context_error": v.ContextError, "completed": strconv.FormatBool(complete)})
		events := []map[string]any{}
		for _, w := range d.Samples {
			if w.QueryID != v.ID {
				continue
			}
			events = append(events, map[string]any{"name": "postgres_wait_sample", "timeUnixNano": strconv.FormatInt(d.StartedAt.Add(time.Duration(w.AtUS)*time.Microsecond).UnixNano(), 10), "attributes": attributes(map[string]string{"state": w.State, "wait_type": w.WaitType, "wait": w.Wait, "backend_start": w.BackendStart.Format(time.RFC3339Nano), "pg_query_id": w.PGQueryID})})
		}
		if len(events) > 0 {
			s["events"] = events
		}
		spans = append(spans, s)
	}
	raw, err := json.Marshal(map[string]any{"resourceSpans": []any{map[string]any{"resource": map[string]any{"attributes": attributes(e.resource)}, "scopeSpans": []any{map[string]any{"scope": map[string]string{"name": "oaix.admin", "version": "1"}, "spans": spans}}}}})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", e.endpoint, bytes.NewReader(raw))
	if err != nil {
		return errors.New("invalid telemetry endpoint")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		return errors.New("telemetry export failed")
	}
	defer resp.Body.Close()
	reply, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if err != nil || len(reply) > 4096 {
		return errors.New("telemetry acknowledgement incomplete")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.New("telemetry rejected")
	}
	if len(bytes.TrimSpace(reply)) > 0 {
		var ack struct {
			PartialSuccess struct {
				RejectedSpans json.RawMessage `json:"rejectedSpans"`
			} `json:"partialSuccess"`
		}
		if json.Unmarshal(reply, &ack) != nil {
			return errors.New("invalid telemetry acknowledgement")
		}
		n := strings.Trim(string(ack.PartialSuccess.RejectedSpans), `"`)
		if n != "" && n != "0" {
			return errors.New("telemetry partially rejected")
		}
	}
	return nil
}
