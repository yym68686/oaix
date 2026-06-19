package runtime

import (
	"context"
	"log/slog"
	"time"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/importer"
	"github.com/yym68686/oaix/internal/oauth"
	"github.com/yym68686/oaix/internal/store"
	"github.com/yym68686/oaix/internal/tokens"
)

func startEmbeddedWorker(ctx context.Context, cfg config.Config, logger *slog.Logger, db *store.Store, tokenManager *tokens.Manager) func(context.Context) {
	if !cfg.Worker.Embedded {
		if logger != nil {
			logger.Info("embedded worker disabled")
		}
		return func(context.Context) {}
	}
	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	importWorker := newImportWorker(cfg)
	go func() {
		defer close(done)
		runMaintenanceOnce(workerCtx, cfg, logger, db, tokenManager, importWorker, true)
		aggregationInterval := cfg.RequestLog.AggregationWindow
		if aggregationInterval <= 0 {
			aggregationInterval = time.Minute
		}
		ticker := time.NewTicker(aggregationInterval)
		defer ticker.Stop()
		cleanupInterval := cfg.RequestLog.CleanupInterval
		if cleanupInterval <= 0 {
			cleanupInterval = time.Hour
		}
		cleanupTicker := time.NewTicker(cleanupInterval)
		defer cleanupTicker.Stop()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
				runMaintenanceOnce(workerCtx, cfg, logger, db, tokenManager, importWorker, false)
			case <-cleanupTicker.C:
				runRequestLogCleanup(workerCtx, cfg, logger, db)
			}
		}
	}()
	if logger != nil {
		logger.Info("embedded worker started")
	}
	return func(shutdownCtx context.Context) {
		cancel()
		select {
		case <-done:
		case <-shutdownCtx.Done():
		}
	}
}

func newImportWorker(cfg config.Config) *importer.Worker {
	oauthClient := oauth.NewHTTPClient(cfg.Upstream.OAuthTokenURL)
	oauthClient.ClientID = cfg.Upstream.OAuthClientID
	oauthClient.Scope = cfg.Upstream.OAuthScope
	return &importer.Worker{
		MaxConcurrency: cfg.Import.MaxConcurrency,
		Validator: importer.TokenPayloadValidator{
			Refresh: importer.OAuthRefreshValidator{Client: oauthClient},
		},
	}
}

func runMaintenanceOnce(ctx context.Context, cfg config.Config, logger *slog.Logger, db *store.Store, tokenManager *tokens.Manager, importWorker *importer.Worker, includeCleanup bool) {
	runStep(ctx, logger, "request log outbox drain", 30*time.Second, func(stepCtx context.Context) error {
		drained, err := db.DrainRequestLogOutbox(stepCtx, cfg.RequestLog.OutboxDrainBatch)
		if err == nil && drained > 0 && logger != nil {
			logger.Info("request log outbox drained", "count", drained)
		}
		return err
	})
	runStep(ctx, logger, "stale import job resume", 30*time.Second, func(stepCtx context.Context) error {
		resumed, err := db.ResumeStaleImportJobs(stepCtx, 5*time.Minute)
		if err == nil && resumed > 0 && logger != nil {
			logger.Info("stale import jobs resumed", "count", resumed)
		}
		return err
	})
	runStep(ctx, logger, "import item processing", maxDuration(30*time.Second, cfg.RequestLog.AggregationWindow), func(stepCtx context.Context) error {
		claimed, err := db.ClaimImportItems(stepCtx, cfg.Import.StagingBatchSize)
		if err != nil || len(claimed) == 0 {
			return err
		}
		updates := importWorker.ValidateBatch(stepCtx, claimed)
		if err := db.UpdateImportItems(stepCtx, updates); err != nil {
			return err
		}
		published := publishValidatedAccessTokens(stepCtx, db, claimed, updates, logger)
		importWorker.RecordPublished(published)
		if published > 0 && tokenManager != nil {
			if err := tokenManager.Refresh(stepCtx); err != nil && logger != nil {
				logger.Warn("token snapshot refresh after import failed", "error", err)
			}
		}
		if logger != nil {
			logger.Info("import batch processed", "claimed", len(claimed), "published", published, "metrics", importWorker.Stats())
		}
		return nil
	})
	runStep(ctx, logger, "request token cost reconciliation", maxDuration(30*time.Second, cfg.RequestLog.AggregationWindow), func(stepCtx context.Context) error {
		reconciled, err := db.ReconcileRecordedTokenCosts(stepCtx)
		if err == nil && reconciled && logger != nil {
			logger.Info("request token costs reconciled")
		}
		return err
	})
	runStep(ctx, logger, "request log hourly aggregation", maxDuration(30*time.Second, cfg.RequestLog.AggregationWindow), func(stepCtx context.Context) error {
		aggregated, err := db.AggregateRequestHourlyStats(stepCtx)
		if err == nil && aggregated > 0 && logger != nil {
			logger.Info("request log hourly stats aggregated", "count", aggregated)
		}
		return err
	})
	if includeCleanup {
		runRequestLogCleanup(ctx, cfg, logger, db)
	}
}

func runRequestLogCleanup(ctx context.Context, cfg config.Config, logger *slog.Logger, db *store.Store) {
	runStep(ctx, logger, "request log retention cleanup", 30*time.Second, func(stepCtx context.Context) error {
		deleted, err := db.DeleteOldRequestLogs(stepCtx, cfg.RequestLog.RetentionDays)
		if err == nil && deleted > 0 && logger != nil {
			logger.Info("request log retention cleanup deleted rows", "count", deleted)
		}
		return err
	})
}

func runStep(parent context.Context, logger *slog.Logger, name string, timeout time.Duration, fn func(context.Context) error) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	if err := fn(ctx); err != nil && logger != nil && parent.Err() == nil {
		logger.Warn(name+" failed", "error", err)
	}
}

func publishValidatedAccessTokens(ctx context.Context, db *store.Store, items []store.ImportItem, updates []store.ImportItemUpdate, logger *slog.Logger) int {
	jobByItemID := make(map[int64]int64, len(items))
	for _, item := range items {
		jobByItemID[item.ID] = item.JobID
	}
	payloadsByJob := make(map[int64][]map[string]any)
	updatesByJob := make(map[int64][]store.ImportItemUpdate)
	for _, update := range updates {
		if update.Status != string(importer.ItemValidated) {
			continue
		}
		if len(update.ValidatedPayload) == 0 {
			continue
		}
		jobID := jobByItemID[update.ID]
		payloadsByJob[jobID] = append(payloadsByJob[jobID], update.ValidatedPayload)
		updatesByJob[jobID] = append(updatesByJob[jobID], update)
	}
	published := 0
	for jobID, payloads := range payloadsByJob {
		result, err := db.PublishImportPayloads(ctx, jobID, payloads, "embedded_import_worker")
		if err != nil {
			if logger != nil {
				logger.Warn("import token publish failed", "job_id", jobID, "error", err)
			}
			continue
		}
		publishedUpdates := make([]store.ImportItemUpdate, 0, len(updatesByJob[jobID]))
		for index, update := range updatesByJob[jobID] {
			if index < len(result.Items) {
				itemResult := result.Items[index]
				update.TokenID = itemResult.TokenID
				if itemResult.Action != "" {
					update.Action = itemResult.Action
				}
				if itemResult.Status == "failed" {
					update.Status = string(importer.ItemFailed)
					update.ErrorMessage = itemResult.ErrorMessage
				} else {
					update.Status = string(importer.ItemPublished)
					update.Published = true
				}
			} else {
				update.Status = string(importer.ItemPublished)
				update.Published = true
			}
			publishedUpdates = append(publishedUpdates, update)
		}
		if err := db.UpdateImportItems(ctx, publishedUpdates); err != nil {
			if logger != nil {
				logger.Warn("import item publish status update failed", "job_id", jobID, "error", err)
			}
		}
		published += result.Created + result.Updated
	}
	return published
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
