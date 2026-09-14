package runtime

import (
	"context"
	"log/slog"
	"time"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/importer"
	"github.com/yym68686/oaix/internal/store"
	"github.com/yym68686/oaix/internal/tokens"
)

// RunImportWorker is independent of billing aggregation, retention and remote
// synchronization. Its concurrency/claim ordering still come from the existing
// import worker and SKIP LOCKED database queue, including across replicas.
func RunImportWorker(ctx context.Context, cfg config.Config, logger *slog.Logger, db *store.Store, tokenManager *tokens.Manager) {
	wake := make(chan struct{}, 1)
	listenerCtx, stopListener := context.WithCancel(ctx)
	listenerDone := make(chan struct{})
	go func() {
		defer close(listenerDone)
		for listenerCtx.Err() == nil {
			err := db.ListenForImportJobs(listenerCtx, wake)
			if listenerCtx.Err() != nil {
				return
			}
			if logger != nil {
				logger.Warn("import listener reconnecting", "error", err)
			}
			if !waitImportRetry(listenerCtx) {
				return
			}
		}
	}()
	defer func() { stopListener(); <-listenerDone }()
	worker := newImportWorker(cfg)
	runImportLoop(ctx, wake, time.Minute, func() {
		runStep(ctx, logger, "stale import job resume", 5*time.Second, func(stepCtx context.Context) error {
			resumed, err := db.ResumeStaleImportJobs(stepCtx, 5*time.Minute)
			if resumed > 0 && logger != nil {
				logger.Info("stale import jobs resumed", "count", resumed)
			}
			return err
		})
	}, func() (int, error) {
		stepCtx, cancel := context.WithTimeout(ctx, maxDuration(30*time.Second, cfg.RequestLog.AggregationWindow))
		defer cancel()
		count, err := processImportBatch(stepCtx, cfg, logger, db, tokenManager, worker)
		if err != nil && logger != nil && ctx.Err() == nil {
			logger.Warn("import item processing failed", "error", err)
		}
		return count, err
	})
}

// Notifications may coalesce. Always drain all committed work before sleeping;
// the periodic sweep also covers a missing notification and stale claims.
func runImportLoop(ctx context.Context, wake <-chan struct{}, sweepInterval time.Duration, sweep func(), process func() (int, error)) {
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	sweep()
	for ctx.Err() == nil {
		count, err := process()
		if err != nil {
			if !waitImportRetry(ctx) {
				return
			}
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep()
			continue
		default:
		}
		if count > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-ticker.C:
			sweep()
		}
	}
}

func waitImportRetry(ctx context.Context) bool {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func processImportBatch(ctx context.Context, cfg config.Config, logger *slog.Logger, db *store.Store, tokenManager *tokens.Manager, worker *importer.Worker) (int, error) {
	started := time.Now()
	claimed, err := db.ClaimImportItems(ctx, cfg.Import.StagingBatchSize)
	claimDuration := time.Since(started)
	if err != nil || len(claimed) == 0 {
		return 0, err
	}
	stage := time.Now()
	updates := worker.ValidateBatch(ctx, claimed)
	validationDuration := time.Since(stage)
	stage = time.Now()
	if err := db.UpdateImportItems(ctx, updates); err != nil {
		return len(claimed), err
	}
	validationWriteDuration := time.Since(stage)
	stage = time.Now()
	published := publishValidatedAccessTokens(ctx, db, claimed, updates, logger)
	publishDuration := time.Since(stage)
	worker.RecordPublished(published)
	stage = time.Now()
	if published > 0 && tokenManager != nil {
		if err := tokenManager.Refresh(ctx); err != nil && logger != nil {
			logger.Warn("token snapshot refresh after import failed", "error", err)
		}
	}
	if logger != nil {
		jobIDs := make([]int64, 0)
		seen := make(map[int64]bool)
		var queueMax time.Duration
		for _, item := range claimed {
			if !seen[item.JobID] {
				seen[item.JobID] = true
				jobIDs = append(jobIDs, item.JobID)
			}
			if item.ValidationStarted != nil {
				queueMax = max(queueMax, item.ValidationStarted.Sub(item.CreatedAt))
			}
		}
		logger.Info("import batch processed", "job_ids", jobIDs,
			"claimed", len(claimed), "published", published, "metrics", worker.Stats(),
			"queue_max_ms", float64(queueMax.Microseconds())/1000,
			"claim_ms", float64(claimDuration.Microseconds())/1000,
			"validation_ms", float64(validationDuration.Microseconds())/1000,
			"validation_write_ms", float64(validationWriteDuration.Microseconds())/1000,
			"publish_ms", float64(publishDuration.Microseconds())/1000,
			"snapshot_ms", float64(time.Since(stage).Microseconds())/1000,
			"duration_ms", float64(time.Since(started).Microseconds())/1000)
	}
	return len(claimed), nil
}
