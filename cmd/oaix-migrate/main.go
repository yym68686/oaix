package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/observability"
	"github.com/yym68686/oaix/internal/store"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config load failed", "error", err)
		os.Exit(1)
	}
	logger := observability.NewLogger(cfg.Observability.LogLevel)
	db, err := store.Connect(ctx, cfg.Database)
	if err != nil {
		logger.Error("database connect failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	if len(os.Args) > 1 && os.Args[1] == "down" {
		if err := db.MigrateDown(ctx); err != nil {
			logger.Error("down migration failed", "error", err)
			os.Exit(1)
		}
		logger.Info("down migration completed")
		return
	}
	if err := db.Migrate(ctx); err != nil {
		logger.Error("migration failed", "error", err)
		os.Exit(1)
	}
	logger.Info("migration completed", "schema_version", store.SchemaVersion)
}
