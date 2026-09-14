package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/observability"
	"github.com/yym68686/oaix/internal/runtime"
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
	importCtx, stopImports := context.WithCancel(ctx)
	importDone := make(chan struct{})
	go func() {
		defer close(importDone)
		runtime.RunImportWorker(importCtx, cfg, logger, db, nil)
	}()
	defer func() { stopImports(); <-importDone }()
	go ensureRequestAttemptRetentionIndex(ctx, db, logger)
	go runtime.RunRequestCostIndexWorker(ctx, logger, db)
	go runtime.RunPerformanceRollupWorker(ctx, logger, db)
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
