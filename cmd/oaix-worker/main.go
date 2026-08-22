package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/importer"
	"github.com/yym68686/oaix/internal/oauth"
	"github.com/yym68686/oaix/internal/observability"
	"github.com/yym68686/oaix/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config load failed", "error", err)
		os.Exit(1)
	}
	logger := observability.NewLogger(cfg.Observability.LogLevel)
	db, err := store.ConnectObserved(ctx, cfg.Database, logger)
	if err != nil {
		logger.Error("database connect failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	ticker := time.NewTicker(cfg.RequestLog.AggregationWindow)
	defer ticker.Stop()
	cleanupInterval := cfg.RequestLog.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = time.Hour
	}
	cleanupTicker := time.NewTicker(cleanupInterval)
	defer cleanupTicker.Stop()
	importWorker := newImportWorker(cfg)
	go ensureRequestAttemptRetentionIndex(ctx, db, logger)
	logger.Info("oaix worker started")
	for {
		select {
		case <-ctx.Done():
			logger.Info("oaix worker stopped")
			return
		case <-ticker.C:
			drained, err := db.DrainRequestLogOutbox(ctx, cfg.RequestLog.OutboxDrainBatch)
			if err != nil {
				logger.Warn("request log outbox drain failed", "error", err)
				continue
			}
			if drained > 0 {
				logger.Info("request log outbox drained", "count", drained)
			}
			resumed, err := db.ResumeStaleImportJobs(ctx, 5*time.Minute)
			if err != nil {
				logger.Warn("stale import job resume failed", "error", err)
			} else if resumed > 0 {
				logger.Info("stale import jobs resumed", "count", resumed)
			}
			claimed, err := db.ClaimImportItems(ctx, cfg.Import.StagingBatchSize)
			if err != nil {
				logger.Warn("import item claim failed", "error", err)
			} else if len(claimed) > 0 {
				updates := importWorker.ValidateBatch(ctx, claimed)
				if err := db.UpdateImportItems(ctx, updates); err != nil {
					logger.Warn("import item update failed", "error", err)
				} else {
					published := publishValidatedAccessTokens(ctx, db, claimed, updates, logger)
					importWorker.RecordPublished(published)
					logger.Info("import batch processed", "claimed", len(claimed), "published", published, "metrics", importWorker.Stats())
				}
			}
			aggregated, err := db.AggregateRequestHourlyStats(ctx)
			if err != nil {
				logger.Warn("request log hourly aggregation failed", "error", err)
			} else if aggregated > 0 {
				logger.Info("request log hourly stats aggregated", "count", aggregated)
			}
			deleted, err := db.DeleteOldRequestLogs(ctx, cfg.RequestLog.RetentionDays)
			if err != nil {
				logger.Warn("request log retention cleanup failed", "error", err)
			} else if deleted > 0 {
				logger.Info("request log retention cleanup deleted rows", "count", deleted)
			}
		case <-cleanupTicker.C:
			cacheRetention, err := db.DeleteExpiredAffinityRows(ctx, cfg.PromptCache.RetentionBatchSize, cfg.PromptCache.RetentionMaxBatches)
			if err != nil {
				logger.Warn("prompt cache retention cleanup failed", "error", err)
			} else if cacheRetention.PromptAffinityLanes > 0 || cacheRetention.ResponseOwnerBindings > 0 {
				logger.Info("prompt cache retention cleanup deleted rows",
					"prompt_affinity_lanes", cacheRetention.PromptAffinityLanes,
					"response_owner_bindings", cacheRetention.ResponseOwnerBindings,
				)
			}
			attemptsDeleted, err := db.DeleteOldRequestAttempts(ctx, cfg.RequestLog.AttemptRetentionDays, cfg.PromptCache.RetentionBatchSize, cfg.PromptCache.RetentionMaxBatches)
			if err != nil {
				logger.Warn("request attempt retention cleanup failed", "error", err)
			} else if attemptsDeleted > 0 {
				logger.Info("request attempt retention cleanup deleted rows", "count", attemptsDeleted)
			}
		}
	}
}

func ensureRequestAttemptRetentionIndex(ctx context.Context, db *store.Store, logger *slog.Logger) {
	for {
		stepCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
		err := db.EnsureRequestAttemptRetentionIndex(stepCtx)
		cancel()
		if err == nil {
			logger.Info("request attempt retention index ready")
			return
		}
		if ctx.Err() == nil {
			logger.Warn("request attempt retention index ensure failed", "error", err)
		}
		timer := time.NewTimer(time.Hour)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
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
		result, err := db.PublishImportPayloads(ctx, jobID, payloads, "import_worker")
		if err != nil {
			logger.Warn("import token publish failed", "job_id", jobID, "error", err)
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
			logger.Warn("import item publish status update failed", "job_id", jobID, "error", err)
		}
		published += result.Created + result.Updated
	}
	return published
}
