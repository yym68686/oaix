package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/yym68686/oaix/internal/admindiag"
	"github.com/yym68686/oaix/internal/observability"
)

func (a *App) StartAdminObservability(parent context.Context) func(context.Context) {
	if a.store == nil {
		return func(context.Context) {}
	}
	ctx, cancel := context.WithCancel(parent)
	exporter := admindiag.NewExporter(firstNonEmpty(os.Getenv("FUGUE_OBSERVABILITY_ENDPOINT"), os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")), map[string]string{"service.name": "oaix", "service": "oaix", "tenant_id": os.Getenv("FUGUE_OBSERVABILITY_TENANT_ID"), "project_id": os.Getenv("FUGUE_OBSERVABILITY_PROJECT_ID"), "app_id": os.Getenv("FUGUE_OBSERVABILITY_APP_ID"), "runtime_id": os.Getenv("FUGUE_OBSERVABILITY_RUNTIME_ID")})
	reload := func() {
		c, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		p, err := a.store.GetAdminPolicy(c)
		if err != nil {
			a.adminRecorder.PolicyFailed()
			return
		}
		_ = a.adminRecorder.SetPolicy(p)
	}
	worker, control, sampler := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(worker)
		a.adminRecorder.Run(ctx, func(c context.Context, d admindiag.Record, raw []byte) error {
			err := a.store.SaveAdminObservation(c, d, raw)
			if a.logger != nil {
				a.logger.Info("admin_request_observation", "event_type", "admin_request_observation", "request_id", d.RequestID, "trace_id", d.TraceID, "observation_id", d.ID, "route", d.Route, "status", d.Status, "duration_ms", d.DurationUS/1000, "stored", err == nil, "wait_samples", len(d.Samples))
			}
			return err
		}, exporter.Export)
	}()
	go func() {
		defer close(control)
		reload()
		ticker := time.NewTicker(15 * time.Second)
		cleanup := time.NewTicker(time.Minute)
		defer ticker.Stop()
		defer cleanup.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				reload()
			case <-cleanup.C:
				c, stop := context.WithTimeout(ctx, time.Second)
				err := a.store.CleanupAdminObservations(c)
				stop()
				if err != nil {
					a.adminRecorder.CleanupFailed()
				}
			}
		}
	}()
	go func() {
		defer close(sampler)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.store.SampleAdminQueries(ctx)
			}
		}
	}()
	return func(shutdown context.Context) {
		cancel()
		c, stop := context.WithTimeout(shutdown, 3*time.Second)
		defer stop()
		for _, done := range []chan struct{}{worker, control, sampler} {
			select {
			case <-done:
			case <-c.Done():
			}
		}
		exporter.Close()
	}
}
func (a *App) getAdminObservability(w http.ResponseWriter, r *http.Request) {
	c, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	p, err := a.store.GetAdminPolicy(c)
	if err != nil {
		writeError(w, 503, errors.New("cannot read administrator observation policy"))
		return
	}
	writeJSON(w, 200, map[string]any{"policy": p, "runtime_policy": a.adminRecorder.Policy(), "stats": a.adminRecorder.Stats(), "retention_hours": 24, "max_record_bytes": admindiag.MaxBytes, "query_first_row_observed": false, "execution_plan_observed": false, "node_resources_observed": false})
}
func (a *App) updateAdminObservability(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var p admindiag.Policy
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&p) != nil {
		writeError(w, 400, errors.New("invalid administrator observation policy"))
		return
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		writeError(w, 400, errors.New("only one policy object is allowed"))
		return
	}
	if err := p.Validate(); err != nil {
		writeError(w, 400, err)
		return
	}
	c, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	p, err := a.store.UpdateAdminPolicy(c, p)
	if err != nil {
		writeError(w, 503, errors.New("cannot save administrator observation policy"))
		return
	}
	_ = a.adminRecorder.SetPolicy(p)
	writeJSON(w, 200, map[string]any{"policy": p, "stats": a.adminRecorder.Stats()})
}
func (a *App) getAdminObservations(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("request_id")
	if id == "" || observability.NormalizeRequestID(id) != id {
		writeError(w, 400, errors.New("valid request_id is required"))
		return
	}
	c, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	items, err := a.store.ListAdminObservations(c, id)
	if err != nil {
		writeError(w, 503, errors.New("cannot read administrator observations"))
		return
	}
	writeJSON(w, 200, map[string]any{"items": items, "retention_hours": 24})
}
func (a *App) writeAdminMetrics(w http.ResponseWriter) {
	s := a.adminRecorder.Stats()
	for name, value := range map[string]uint64{"started": s.Started, "enqueued": s.Enqueued, "dropped": s.Dropped, "sampled_out": s.SampledOut, "stored": s.Stored, "store_errors": s.StoreErrors, "exported": s.Exported, "export_errors": s.ExportErrors, "policy_errors": s.PolicyErrors, "cleanup_errors": s.CleanupErrors} {
		fmt.Fprintf(w, "# TYPE oaix_admin_observation_%s_total counter\noaix_admin_observation_%s_total %d\n", name, name, value)
	}
	fmt.Fprintf(w, "# TYPE oaix_admin_observation_active gauge\noaix_admin_observation_active %d\n# TYPE oaix_admin_observation_queue_depth gauge\noaix_admin_observation_queue_depth %d\n", s.Active, s.QueueDepth)
}
func observeAdminRoute(method, path string) bool {
	if method != "GET" {
		return false
	}
	switch path {
	case "/api/admin/users", "/api/admin/pool-summary/by-user", "/api/admin/analytics/users":
		return true
	}
	return false
}
