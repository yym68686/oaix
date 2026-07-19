package runtime

import (
	"context"
	"log/slog"
	"time"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/importer"
	"github.com/yym68686/oaix/internal/oauth"
	"github.com/yym68686/oaix/internal/store"
	"github.com/yym68686/oaix/internal/sub2api"
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
	sub2apiSyncer := sub2api.NewSyncer(db, nil, logger, cfg.Upstream.OAuthClientID)
	go func() {
		defer close(done)
		runMaintenanceOnce(workerCtx, cfg, logger, db, tokenManager, importWorker, sub2apiSyncer, true)
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
				runMaintenanceOnce(workerCtx, cfg, logger, db, tokenManager, importWorker, sub2apiSyncer, false)
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

func runMaintenanceOnce(ctx context.Context, cfg config.Config, logger *slog.Logger, db *store.Store, tokenManager *tokens.Manager, importWorker *importer.Worker, sub2apiSyncer *sub2api.Syncer, includeCleanup bool) {
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
	runStep(ctx, logger, "GPT-5.6 request cost repricing", maxDuration(2*time.Minute, cfg.RequestLog.AggregationWindow), func(stepCtx context.Context) error {
		return runGPT56RequestCostRepricing(stepCtx, logger, db)
	})
	runStep(ctx, logger, "Fast request cost repricing", maxDuration(2*time.Minute, cfg.RequestLog.AggregationWindow), func(stepCtx context.Context) error {
		return runFastRequestCostRepricing(stepCtx, logger, db)
	})
	runStep(ctx, logger, "sub2api sync", maxDuration(30*time.Second, cfg.RequestLog.AggregationWindow), func(stepCtx context.Context) error {
		if sub2apiSyncer == nil {
			return nil
		}
		runs, err := sub2apiSyncer.RunDueTargets(stepCtx)
		if err == nil && len(runs) > 0 && logger != nil {
			logger.Info("sub2api sync processed", "count", len(runs))
		}
		return err
	})
	runStep(ctx, logger, "sub2api usage sync", maxDuration(45*time.Second, cfg.RequestLog.AggregationWindow), func(stepCtx context.Context) error {
		if sub2apiSyncer == nil {
			return nil
		}
		synced, err := sub2apiSyncer.SyncDueUsage(stepCtx)
		if synced > 0 && logger != nil {
			logger.Info("sub2api usage snapshots synced", "count", synced)
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
	if cfg.Idempotency.Enabled {
		runStep(ctx, logger, "gateway idempotency retention cleanup", 10*time.Second, func(stepCtx context.Context) error {
			deleted, err := db.DeleteExpiredGatewayIdempotencyRecords(stepCtx, 1000)
			if err == nil && deleted > 0 && logger != nil {
				logger.Info("gateway idempotency retention cleanup deleted rows", "count", deleted)
			}
			return err
		})
	}
	if includeCleanup {
		runRequestLogCleanup(ctx, cfg, logger, db)
	}
}

func runGPT56RequestCostRepricing(ctx context.Context, logger *slog.Logger, db *store.Store) error {
	const cycleWriteBudget = 45 * time.Second
	deadline := time.Now().Add(cycleWriteBudget)
	var total store.GPT56CostRepriceResult
	batchCount := 0
	for {
		result, err := db.RepriceGPT56RequestCosts(ctx)
		if err != nil {
			return err
		}
		batchCount++
		total.ScannedLogs += result.ScannedLogs
		total.UpdatedLogs += result.UpdatedLogs
		total.RebuiltTokenCosts += result.RebuiltTokenCosts
		total.RebuiltHourlyCosts += result.RebuiltHourlyCosts
		total.Phase = result.Phase
		total.Completed = result.Completed

		progressed := result.ScannedLogs > 0 || result.RebuiltTokenCosts > 0 || result.RebuiltHourlyCosts > 0
		if result.Completed || !progressed || time.Now().After(deadline) {
			break
		}
	}
	if logger != nil && (total.ScannedLogs > 0 || total.RebuiltTokenCosts > 0 || total.RebuiltHourlyCosts > 0) {
		logger.Info(
			"GPT-5.6 request costs repriced",
			"phase", total.Phase,
			"batches", batchCount,
			"scanned_logs", total.ScannedLogs,
			"updated_logs", total.UpdatedLogs,
			"rebuilt_token_costs", total.RebuiltTokenCosts,
			"rebuilt_hourly_costs", total.RebuiltHourlyCosts,
			"completed", total.Completed,
		)
	}
	return nil
}

type fastCostRepricer interface {
	RepriceFastRequestCosts(context.Context) (store.FastCostRepriceResult, error)
}

func runFastRequestCostRepricing(ctx context.Context, logger *slog.Logger, db fastCostRepricer) error {
	// Keep the one-time repair deliberately below the duty cycle of normal
	// maintenance and request-log writes. Each database call also carries its
	// own five-second statement timeout, so a cold page cannot monopolize the
	// small production database.
	const cycleWriteBudget = 5 * time.Second
	deadline := time.Now().Add(cycleWriteBudget)
	var total store.FastCostRepriceResult
	batchCount := 0
	for {
		result, err := db.RepriceFastRequestCosts(ctx)
		if err != nil {
			return err
		}
		batchCount++
		total.ScannedLogs += result.ScannedLogs
		total.UpdatedLogs += result.UpdatedLogs
		total.UpdatedTokenCosts += result.UpdatedTokenCosts
		total.UpdatedHourlyCosts += result.UpdatedHourlyCosts
		total.Phase = result.Phase
		total.Completed = result.Completed

		progressed := result.ScannedLogs > 0
		if result.Completed || result.Phase == "busy" || result.Phase == "waiting_gpt56" || !progressed || time.Now().After(deadline) {
			break
		}
	}
	if logger != nil && (total.ScannedLogs > 0 || total.UpdatedTokenCosts > 0 || total.UpdatedHourlyCosts > 0) {
		logger.Info(
			"Fast request costs repriced",
			"phase", total.Phase,
			"batches", batchCount,
			"scanned_logs", total.ScannedLogs,
			"updated_logs", total.UpdatedLogs,
			"updated_token_costs", total.UpdatedTokenCosts,
			"updated_hourly_costs", total.UpdatedHourlyCosts,
			"completed", total.Completed,
		)
	}
	return nil
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
	publishedJobIDs := make([]int64, 0, len(payloadsByJob))
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
				update.MatchedExistingTokenID = itemResult.MatchedExistingTokenID
				update.Reactivated = itemResult.Reactivated
				update.PreviousIsActive = itemResult.PreviousIsActive
				update.NextIsActive = itemResult.NextIsActive
				update.PreviousDisabledAt = itemResult.PreviousDisabledAt
				update.NextDisabledAt = itemResult.NextDisabledAt
				update.RefreshErrorCode = itemResult.RefreshErrorCode
				update.RefreshErrorMessageExcerpt = itemResult.RefreshErrorMessage
				update.PublishAttempted = true
				if itemResult.Action != "" {
					update.Action = itemResult.Action
				}
				if itemResult.Status == "failed" {
					update.Status = string(importer.ItemFailed)
					update.ErrorMessage = itemResult.ErrorMessage
					if update.PublishSkippedReason == "" {
						update.PublishSkippedReason = "publish_failed"
					}
				} else {
					update.Status = string(importer.ItemPublished)
					update.Published = true
				}
			} else {
				update.Status = string(importer.ItemPublished)
				update.Published = true
				update.PublishAttempted = true
			}
			publishedUpdates = append(publishedUpdates, update)
		}
		if err := db.UpdateImportItems(ctx, publishedUpdates); err != nil {
			if logger != nil {
				logger.Warn("import item publish status update failed", "job_id", jobID, "error", err)
			}
		}
		if result.Created+result.Updated > 0 {
			published += result.Created + result.Updated
			publishedJobIDs = append(publishedJobIDs, jobID)
		}
	}
	if published > 0 {
		if err := db.MarkSub2APITargetsDueForImportJobs(ctx, publishedJobIDs); err != nil && logger != nil {
			logger.Warn("sub2api sync due mark failed", "error", err)
		}
	}
	return published
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
