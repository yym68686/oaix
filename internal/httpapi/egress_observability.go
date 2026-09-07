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

	"github.com/yym68686/oaix/internal/egress"
	"github.com/yym68686/oaix/internal/observability"
)

func (a *App) newEgressRecorder() *egress.Recorder {
	exporter := egress.NewOTLPExporter(firstNonEmpty(os.Getenv("FUGUE_OBSERVABILITY_ENDPOINT"), os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")), map[string]string{
		"service.name": "oaix", "service": "oaix", "tenant_id": os.Getenv("FUGUE_OBSERVABILITY_TENANT_ID"), "project_id": os.Getenv("FUGUE_OBSERVABILITY_PROJECT_ID"), "app_id": os.Getenv("FUGUE_OBSERVABILITY_APP_ID"), "runtime_id": os.Getenv("FUGUE_OBSERVABILITY_RUNTIME_ID"),
	})
	a.egressExporter = exporter
	return egress.NewRecorder(a.logger, func(ctx context.Context, record egress.TraceRecord, data []byte) error {
		if a.store == nil {
			return errors.New("diagnostic store unavailable")
		}
		return a.store.SaveEgressObservation(ctx, record, data)
	}, exporter.Export)
}

// Observability is independently stoppable and never owns serving configuration.
// A policy reload failure keeps the last valid observation policy, with a counter.
func (a *App) StartEgressObservability(parent context.Context) func(context.Context) {
	ctx, cancel := context.WithCancel(parent)
	reload := func() {
		readCtx, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		p, err := a.store.GetEgressPolicy(readCtx)
		if err != nil {
			a.egressRecorder.PolicyLoadFailed()
			return
		}
		_ = a.egressRecorder.SetPolicy(p)
	}
	reload()
	workerDone, controlDone := make(chan struct{}), make(chan struct{})
	go func() { defer close(workerDone); a.egressRecorder.Run(ctx) }()
	go func() {
		defer close(controlDone)
		reloadTicker, cleanupTicker := time.NewTicker(15*time.Second), time.NewTicker(time.Minute)
		defer reloadTicker.Stop()
		defer cleanupTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-reloadTicker.C:
				reload()
			case <-cleanupTicker.C:
				cleanCtx, stop := context.WithTimeout(ctx, time.Second)
				if err := a.store.CleanupEgressObservations(cleanCtx); err != nil {
					a.egressRecorder.CleanupFailed()
				}
				stop()
			}
		}
	}()
	return func(shutdown context.Context) {
		cancel()
		bounded, stop := context.WithTimeout(shutdown, 3*time.Second)
		defer stop()
		for _, done := range []chan struct{}{workerDone, controlDone} {
			select {
			case <-done:
			case <-bounded.Done():
			}
		}
		a.egressExporter.Close()
	}
}

func (a *App) getEgressObservability(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	p, err := a.store.GetEgressPolicy(ctx)
	if err != nil {
		writeError(w, 503, errors.New("cannot read observation policy"))
		return
	}
	writeJSON(w, 200, map[string]any{"policy": p, "runtime_policy": a.egressRecorder.Policy(), "stats": a.egressRecorder.Stats(), "retention_hours": 24, "max_trace_bytes": egress.MaxTraceBytes, "max_events": egress.MaxTraceEvents, "max_records_per_minute": 600, "otlp_configured": a.egressExporter != nil})
}

func (a *App) updateEgressObservability(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var p egress.Policy
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&p); err != nil {
		writeError(w, 400, errors.New("invalid observation policy"))
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, 400, errors.New("only one policy object is allowed"))
		return
	}
	if err := p.Validate(); err != nil {
		writeError(w, 400, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	p, err := a.store.UpdateEgressPolicy(ctx, p)
	if err != nil {
		writeError(w, 503, errors.New("cannot save observation policy"))
		return
	}
	_ = a.egressRecorder.SetPolicy(p)
	_ = a.store.WriteAuditLog(ctx, "egress_observability_update", "admin", "setting", egress.PolicyKey, p)
	writeJSON(w, 200, map[string]any{"policy": p, "stats": a.egressRecorder.Stats()})
}

func (a *App) getEgressObservations(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("request_id")
	if id == "" || observability.NormalizeRequestID(id) != id {
		writeError(w, 400, errors.New("valid request_id is required"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	items, err := a.store.ListEgressObservations(ctx, id)
	if err != nil {
		writeError(w, 503, errors.New("cannot read egress observations"))
		return
	}
	writeJSON(w, 200, map[string]any{"items": items, "retention_hours": 24})
}

func (a *App) writeEgressMetrics(w http.ResponseWriter) {
	s := a.egressRecorder.Stats()
	for name, value := range map[string]uint64{"started": s.Started, "sampled_out": s.SampledOut, "enqueued": s.Enqueued, "dropped": s.Dropped, "stored": s.Stored, "store_errors": s.StoreErrors, "exported": s.Exported, "export_errors": s.ExportErrors, "late_events": s.LateEvents, "policy_errors": s.PolicyErrors, "cleanup_errors": s.CleanupErrors, "rate_dropped": s.RateDropped} {
		_, _ = fmt.Fprintf(w, "# TYPE oaix_egress_observation_%s_total counter\noaix_egress_observation_%s_total %d\n", name, name, value)
	}
	_, _ = fmt.Fprintf(w, "# TYPE oaix_egress_observation_queue_depth gauge\noaix_egress_observation_queue_depth %d\n", s.QueueDepth)
}
