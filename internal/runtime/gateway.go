package runtime

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/yym68686/oaix/internal/affinity"
	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/httpapi"
	"github.com/yym68686/oaix/internal/importer"
	"github.com/yym68686/oaix/internal/logs"
	"github.com/yym68686/oaix/internal/oauth"
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
	db, err := store.ConnectObserved(ctx, cfg.Database, logger)
	if err != nil {
		return err
	}
	defer db.Close()
	if cfg.Database.AutoMigrate {
		logger.Info("running startup database migration")
		if err := db.MigrateForStartup(ctx); err != nil {
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
	activeStreamCap := cfg.TokenPool.ActiveStreamCap
	if settings, err := db.GetTokenSelectionSettings(ctx, cfg.TokenPool.ActiveStreamCap); err != nil {
		logger.Warn("token selection settings load failed; using configured active stream cap", "error", err, "active_stream_cap", activeStreamCap)
	} else {
		activeStreamCap = settings.ActiveStreamCap
	}
	ordinary429Cooldown := cfg.TokenPool.DefaultCooldown
	if settings, err := db.GetOrdinary429CooldownSettings(ctx, cfg.TokenPool.DefaultCooldown); err != nil {
		logger.Warn("ordinary 429 cooldown settings load failed; using configured default", "error", err, "cooldown", cfg.TokenPool.DefaultCooldown)
	} else {
		ordinary429Cooldown = time.Duration(settings.CooldownSeconds) * time.Second
	}
	gpt6AstraLongContextPricing := false
	if settings, err := db.GetGPT6AstraLongContextPricingSettings(ctx); err != nil {
		logger.Warn("GPT-6 Astra long-context pricing settings load failed; using disabled default", "error", err)
	} else {
		gpt6AstraLongContextPricing = settings.Enabled
	}
	tokenManager := tokens.NewManager(db, logger, cfg.TokenPool.SnapshotMaxAge, cfg.TokenPool.RefreshInterval, activeStreamCap)
	tokenManager.Start(ctx)
	logWriter := logs.NewWriter(db, logger, cfg.RequestLog)
	logWriter.Start(ctx)
	stopEmbeddedWorker := startEmbeddedWorker(ctx, cfg, logger, db, tokenManager)
	upstream := transport.New(cfg.Upstream)
	defer upstream.CloseIdleConnections()
	affinityStore := affinity.NewPostgresStore(db.Pool())
	pipeline := proxy.New(cfg, logger, tokenManager, upstream, logWriter, db, affinityStore)
	pipeline.SetOrdinary429Cooldown(ordinary429Cooldown)
	pipeline.SetGPT6AstraLongContextPricing(gpt6AstraLongContextPricing)
	oauthClient := oauth.NewHTTPClient(cfg.Upstream.OAuthTokenURL)
	oauthClient.ClientID = cfg.Upstream.OAuthClientID
	oauthClient.Scope = cfg.Upstream.OAuthScope
	pipeline.SetOAuthClient(oauthClient)
	app := httpapi.NewApp(cfg, logger, db, tokenManager, logWriter, pipeline)
	app.SetProbeRequestDoer(upstream)
	pipeline.SetTokenModelCapabilityLossHandler(app.HandleTokenModelCapabilityLoss)
	app.StartQuotaRecovery(ctx)
	connectionIDs := observability.NewConnectionIDGenerator()
	server := &http.Server{
		Addr:         cfg.Address(),
		Handler:      app.Handler(),
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
		ConnContext:  connectionIDs.ConnContext,
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
	stopEmbeddedWorker(shutdownCtx)
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
