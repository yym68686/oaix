package runtime

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/httpapi"
	"github.com/yym68686/oaix/internal/importer"
	"github.com/yym68686/oaix/internal/logs"
	"github.com/yym68686/oaix/internal/observability"
	"github.com/yym68686/oaix/internal/proxy"
	"github.com/yym68686/oaix/internal/store"
	"github.com/yym68686/oaix/internal/tokens"
	"github.com/yym68686/oaix/internal/transport"
)

func RunGateway(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	logger := observability.NewLogger(cfg.Observability.LogLevel)
	logger.Info("starting oaix gateway", "config", cfg.SanitizedSummary())
	db, err := store.Connect(ctx, cfg.Database)
	if err != nil {
		return err
	}
	defer db.Close()
	if cfg.Database.AutoMigrate {
		logger.Info("running startup database migration")
		if err := db.Migrate(ctx); err != nil {
			return err
		}
	}
	if cfg.Database.RuntimeMustMatch {
		if _, err := db.CheckSchema(ctx); err != nil {
			return err
		}
	}
	if cfg.TokenPool.AccessTokenFile != "" {
		accessTokens, err := importer.ReadAccessTokenFile(cfg.TokenPool.AccessTokenFile)
		if err != nil {
			return err
		}
		result, err := db.UpsertAccessTokens(ctx, accessTokens, cfg.TokenPool.AccessTokenFile)
		if err != nil {
			return err
		}
		logger.Info("access token file imported", "created", result.Created, "updated", result.Updated, "skipped", result.Skipped, "failed", result.Failed)
	}
	tokenManager := tokens.NewManager(db, logger, cfg.TokenPool.SnapshotMaxAge, cfg.TokenPool.RefreshInterval, cfg.TokenPool.ActiveStreamCap)
	tokenManager.Start(ctx)
	logWriter := logs.NewWriter(db, logger, cfg.RequestLog)
	logWriter.Start(ctx)
	upstream := transport.New(cfg.Upstream)
	defer upstream.CloseIdleConnections()
	pipeline := proxy.New(cfg, logger, tokenManager, upstream, logWriter, db)
	app := httpapi.NewApp(cfg, logger, db, tokenManager, logWriter, pipeline)
	server := &http.Server{
		Addr:         cfg.Address(),
		Handler:      app.Handler(),
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}
	errCh := make(chan error, 1)
	go func() {
		logger.Info("oaix gateway listening", "addr", cfg.Address())
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			return err
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("gateway shutdown failed", "error", err)
	}
	_ = logWriter.Stop(context.Background())
	tokenManager.Stop()
	logger.Info("oaix gateway stopped")
	return nil
}

func LogAndExit(logger *slog.Logger, message string, err error) {
	if logger != nil {
		logger.Error(message, "error", err)
	}
}
