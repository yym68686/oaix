package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Server        ServerConfig
	Database      DatabaseConfig
	Auth          AuthConfig
	Upstream      UpstreamConfig
	TokenPool     TokenPoolConfig
	PromptCache   PromptCacheConfig
	RequestLog    RequestLogConfig
	Import        ImportConfig
	Worker        WorkerConfig
	Observability ObservabilityConfig
}

type ServerConfig struct {
	Host            string
	Port            int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
}

type DatabaseConfig struct {
	URL              string
	MaxConns         int32
	MinConns         int32
	ConnectTimeout   time.Duration
	AcquireTimeout   time.Duration
	RuntimeMustMatch bool
	AutoMigrate      bool
}

type AuthConfig struct {
	ServiceAPIKeys []string
}

type UpstreamConfig struct {
	ResponsesURL              string
	ChatCompletionsURL        string
	OAuthTokenURL             string
	OAuthClientID             string
	OAuthScope                string
	MaxRetries                int
	ShardCount                int
	MaxIdleConns              int
	MaxIdleConnsPerHost       int
	MaxConnsPerHost           int
	IdleConnTimeout           time.Duration
	ResponseHeaderTimeout     time.Duration
	TLSHandshakeTimeout       time.Duration
	DisableCompression        bool
	ForceAttemptHTTP2         bool
	NonStreamMaxResponseBytes int64
}

type TokenPoolConfig struct {
	SnapshotMaxAge       time.Duration
	RefreshInterval      time.Duration
	ActiveStreamCap      int64
	DefaultCooldown      time.Duration
	CompactErrorCooldown time.Duration
	AccessTokenFile      string
}

type PromptCacheConfig struct {
	AffinityEnabled       bool
	AutoKeyEnabled        bool
	MaxLanesPerKey        int
	PrimaryWait           time.Duration
	LaneWait              time.Duration
	PreviousOwnerWait     time.Duration
	PreviousStrict        bool
	GlobalFallbackEnabled bool
	SessionPreferHeader   bool
	LaneTTL               time.Duration
	ResponseTTL           time.Duration
}

type RequestLogConfig struct {
	Concurrency       int
	BatchSize         int
	QueueMaxSize      int
	FlushInterval     time.Duration
	RetentionDays     int
	CleanupInterval   time.Duration
	FinalSyncOnFull   bool
	OutboxDrainBatch  int
	AggregationWindow time.Duration
}

type ImportConfig struct {
	MaxConcurrency     int
	StagingBatchSize   int
	PublishBatchSize   int
	PublishInterval    time.Duration
	DefaultQueuePolicy string
}

type WorkerConfig struct {
	Embedded bool
}

type ObservabilityConfig struct {
	LogLevel    string
	ServiceName string
}

func Load() (Config, error) {
	cfg := Config{
		Server: ServerConfig{
			Host:            envString("HOST", "0.0.0.0"),
			Port:            envInt("PORT", 8000),
			ReadTimeout:     envDurationSeconds("SERVER_READ_TIMEOUT_SECONDS", 30*time.Second),
			WriteTimeout:    envDurationSeconds("SERVER_WRITE_TIMEOUT_SECONDS", 0),
			IdleTimeout:     envDurationSeconds("SERVER_IDLE_TIMEOUT_SECONDS", 120*time.Second),
			ShutdownTimeout: envDurationSeconds("SERVER_SHUTDOWN_TIMEOUT_SECONDS", 15*time.Second),
		},
		Database: DatabaseConfig{
			URL:              normalizeDatabaseURL(envString("DATABASE_URL", "postgresql://oaix:oaix_password@127.0.0.1:5432/oaix_gateway")),
			MaxConns:         int32(envInt("DATABASE_POOL_SIZE", 32)),
			MinConns:         int32(envInt("DATABASE_MIN_POOL_SIZE", 2)),
			ConnectTimeout:   envDurationSeconds("DATABASE_CONNECT_TIMEOUT_SECONDS", 5*time.Second),
			AcquireTimeout:   envDurationSeconds("DATABASE_POOL_TIMEOUT_SECONDS", 5*time.Second),
			RuntimeMustMatch: envBool("OAIX_RUNTIME_SCHEMA_MUST_MATCH", true),
			AutoMigrate:      envBool("OAIX_AUTO_MIGRATE_ON_STARTUP", false),
		},
		Auth: AuthConfig{
			ServiceAPIKeys: envCSV("SERVICE_API_KEYS"),
		},
		Upstream: UpstreamConfig{
			ResponsesURL:              envString("CODEX_BASE_URL", "https://chatgpt.com/backend-api/codex/responses"),
			ChatCompletionsURL:        envString("CHAT_COMPLETIONS_BASE_URL", ""),
			OAuthTokenURL:             envString("CODEX_OAUTH_TOKEN_URL", "https://auth.openai.com/oauth/token"),
			OAuthClientID:             envString("CODEX_OAUTH_CLIENT_ID", "app_EMoamEEZ73f0CkXaXp7hrann"),
			OAuthScope:                envString("CODEX_OAUTH_SCOPE", "openid profile email"),
			MaxRetries:                envInt("MAX_REQUEST_ACCOUNT_RETRIES", 100),
			ShardCount:                envInt("UPSTREAM_HTTP_SHARD_COUNT", 4),
			MaxIdleConns:              envInt("UPSTREAM_HTTP_MAX_IDLE_CONNECTIONS", 512),
			MaxIdleConnsPerHost:       envInt("UPSTREAM_HTTP_MAX_IDLE_CONNECTIONS_PER_HOST", 512),
			MaxConnsPerHost:           envInt("UPSTREAM_HTTP_MAX_CONNECTIONS_PER_HOST", 2048),
			IdleConnTimeout:           envDurationSeconds("UPSTREAM_HTTP_IDLE_TIMEOUT_SECONDS", 90*time.Second),
			ResponseHeaderTimeout:     envDurationSeconds("UPSTREAM_HTTP_RESPONSE_HEADER_TIMEOUT_SECONDS", 90*time.Second),
			TLSHandshakeTimeout:       envDurationSeconds("UPSTREAM_HTTP_TLS_HANDSHAKE_TIMEOUT_SECONDS", 10*time.Second),
			DisableCompression:        envBool("UPSTREAM_HTTP_DISABLE_COMPRESSION", false),
			ForceAttemptHTTP2:         envBool("UPSTREAM_HTTP_FORCE_HTTP2", false),
			NonStreamMaxResponseBytes: int64(envInt("UPSTREAM_NON_STREAM_MAX_RESPONSE_BYTES", 64*1024*1024)),
		},
		TokenPool: TokenPoolConfig{
			SnapshotMaxAge:       envDurationSeconds("TOKEN_POOL_SNAPSHOT_MAX_AGE_SECONDS", 10*time.Second),
			RefreshInterval:      envDurationSeconds("TOKEN_POOL_REFRESH_INTERVAL_SECONDS", 2*time.Second),
			ActiveStreamCap:      int64(envInt("TOKEN_ACTIVE_STREAM_CAP", envInt("TOKEN_SELECTION_ACTIVE_STREAM_CAP", 4))),
			DefaultCooldown:      envDurationSeconds("DEFAULT_USAGE_LIMIT_COOLDOWN_SECONDS", 300*time.Second),
			CompactErrorCooldown: envDurationSeconds("COMPACT_SERVER_ERROR_COOLDOWN_SECONDS", 60*time.Second),
			AccessTokenFile:      envString("OAIX_ACCESS_TOKEN_FILE", envString("ACCESS_TOKEN_FILE", "")),
		},
		PromptCache: PromptCacheConfig{
			AffinityEnabled:       envBool("PROMPT_CACHE_AFFINITY_ENABLED", true),
			AutoKeyEnabled:        envBool("PROMPT_CACHE_AUTO_KEY_ENABLED", true),
			MaxLanesPerKey:        envInt("PROMPT_CACHE_MAX_LANES_PER_KEY", 3),
			PrimaryWait:           envDurationMilliseconds("PROMPT_CACHE_PRIMARY_WAIT_MS", 500*time.Millisecond),
			LaneWait:              envDurationMilliseconds("PROMPT_CACHE_LANE_WAIT_MS", 100*time.Millisecond),
			PreviousOwnerWait:     envDurationMilliseconds("PROMPT_CACHE_PREVIOUS_OWNER_WAIT_MS", 800*time.Millisecond),
			PreviousStrict:        promptCachePreviousStrictDefault(),
			GlobalFallbackEnabled: envBool("PROMPT_CACHE_GLOBAL_FALLBACK_ENABLED", true),
			SessionPreferHeader:   promptCacheSessionPreferHeader(),
			LaneTTL:               envDurationSeconds("PROMPT_CACHE_LANE_TTL_SECONDS", time.Hour),
			ResponseTTL:           envDurationSeconds("PROMPT_CACHE_RESPONSE_TTL_SECONDS", 24*time.Hour),
		},
		RequestLog: RequestLogConfig{
			Concurrency:       envInt("REQUEST_LOG_WRITE_CONCURRENCY", 1),
			BatchSize:         envInt("REQUEST_LOG_WRITE_BATCH_SIZE", 250),
			QueueMaxSize:      envInt("REQUEST_LOG_WRITE_QUEUE_MAX_SIZE", 20000),
			FlushInterval:     envDurationSeconds("REQUEST_LOG_WRITE_FLUSH_INTERVAL_SECONDS", time.Second),
			RetentionDays:     envInt("REQUEST_LOG_RETENTION_DAYS", 30),
			CleanupInterval:   envDurationSeconds("REQUEST_LOG_CLEANUP_INTERVAL_SECONDS", time.Hour),
			FinalSyncOnFull:   envBool("REQUEST_LOG_FINAL_SYNC_ON_FULL", true),
			OutboxDrainBatch:  envInt("REQUEST_LOG_OUTBOX_DRAIN_BATCH_SIZE", 500),
			AggregationWindow: envDurationSeconds("REQUEST_LOG_AGGREGATION_INTERVAL_SECONDS", 60*time.Second),
		},
		Import: ImportConfig{
			MaxConcurrency:     envInt("IMPORT_JOB_MAX_CONCURRENCY", 16),
			StagingBatchSize:   envInt("IMPORT_STAGING_INSERT_BATCH_SIZE", 1000),
			PublishBatchSize:   envInt("IMPORT_PUBLISH_BATCH_SIZE", 100),
			PublishInterval:    envDurationSeconds("IMPORT_PUBLISH_FLUSH_INTERVAL_SECONDS", 500*time.Millisecond),
			DefaultQueuePolicy: envString("DEFAULT_TOKEN_IMPORT_QUEUE_POSITION", "front"),
		},
		Worker: WorkerConfig{
			Embedded: envBool("OAIX_EMBEDDED_WORKER_ENABLED", true),
		},
		Observability: ObservabilityConfig{
			LogLevel:    envString("LOG_LEVEL", "INFO"),
			ServiceName: envString("FUGUE_OBSERVABILITY_SERVICE_NAME", "oaix"),
		},
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	var errs []error
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		errs = append(errs, fmt.Errorf("PORT must be between 1 and 65535"))
	}
	if strings.TrimSpace(c.Database.URL) == "" {
		errs = append(errs, fmt.Errorf("DATABASE_URL is required"))
	}
	if _, err := url.Parse(c.Database.URL); err != nil {
		errs = append(errs, fmt.Errorf("DATABASE_URL is invalid: %w", err))
	}
	if c.TokenPool.ActiveStreamCap <= 0 {
		errs = append(errs, fmt.Errorf("TOKEN_ACTIVE_STREAM_CAP must be positive"))
	}
	if c.PromptCache.MaxLanesPerKey <= 0 {
		errs = append(errs, fmt.Errorf("PROMPT_CACHE_MAX_LANES_PER_KEY must be positive"))
	}
	if c.RequestLog.BatchSize <= 0 {
		errs = append(errs, fmt.Errorf("REQUEST_LOG_WRITE_BATCH_SIZE must be positive"))
	}
	if c.RequestLog.QueueMaxSize <= 0 {
		errs = append(errs, fmt.Errorf("REQUEST_LOG_WRITE_QUEUE_MAX_SIZE must be positive"))
	}
	if c.Upstream.MaxRetries <= 0 {
		errs = append(errs, fmt.Errorf("MAX_REQUEST_ACCOUNT_RETRIES must be positive"))
	}
	if c.Upstream.ShardCount <= 0 {
		errs = append(errs, fmt.Errorf("UPSTREAM_HTTP_SHARD_COUNT must be positive"))
	}
	return errors.Join(errs...)
}

func (c Config) Address() string {
	return fmt.Sprintf("%s:%d", c.Server.Host, c.Server.Port)
}

func (c Config) SanitizedSummary() map[string]any {
	return map[string]any{
		"server": map[string]any{
			"host": c.Server.Host,
			"port": c.Server.Port,
		},
		"database": map[string]any{
			"max_conns":    c.Database.MaxConns,
			"min_conns":    c.Database.MinConns,
			"auto_migrate": c.Database.AutoMigrate,
		},
		"auth": map[string]any{
			"service_key_protected": len(c.Auth.ServiceAPIKeys) > 0,
		},
		"upstream": map[string]any{
			"responses_url":           c.Upstream.ResponsesURL,
			"chat_completions_url":    c.Upstream.ChatCompletionsURL,
			"max_retries":             c.Upstream.MaxRetries,
			"shard_count":             c.Upstream.ShardCount,
			"max_conns_per_host":      c.Upstream.MaxConnsPerHost,
			"max_idle_conns_per_host": c.Upstream.MaxIdleConnsPerHost,
		},
		"token_pool": map[string]any{
			"snapshot_max_age":  c.TokenPool.SnapshotMaxAge.String(),
			"refresh_interval":  c.TokenPool.RefreshInterval.String(),
			"active_stream_cap": c.TokenPool.ActiveStreamCap,
			"access_token_file": c.TokenPool.AccessTokenFile != "",
		},
		"prompt_cache": map[string]any{
			"affinity_enabled":        c.PromptCache.AffinityEnabled,
			"auto_key_enabled":        c.PromptCache.AutoKeyEnabled,
			"max_lanes_per_key":       c.PromptCache.MaxLanesPerKey,
			"global_fallback_enabled": c.PromptCache.GlobalFallbackEnabled,
			"session_prefer_header":   c.PromptCache.SessionPreferHeader,
		},
		"request_log": map[string]any{
			"concurrency":  c.RequestLog.Concurrency,
			"batch_size":   c.RequestLog.BatchSize,
			"queue_size":   c.RequestLog.QueueMaxSize,
			"retention":    c.RequestLog.RetentionDays,
			"final_sync":   c.RequestLog.FinalSyncOnFull,
			"outbox_batch": c.RequestLog.OutboxDrainBatch,
		},
		"worker": map[string]any{
			"embedded": c.Worker.Embedded,
		},
	}
}

func normalizeDatabaseURL(raw string) string {
	value := strings.TrimSpace(raw)
	value = strings.TrimPrefix(value, "postgresql+asyncpg://")
	if value != strings.TrimSpace(raw) {
		value = "postgresql://" + value
	}
	value = strings.TrimPrefix(value, "postgres+asyncpg://")
	if value != strings.TrimSpace(raw) && !strings.HasPrefix(value, "postgresql://") {
		value = "postgres://" + value
	}
	return value
}

func envString(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return fallback
}

func envCSV(key string) []string {
	raw := envString(key, "")
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value != "" {
			values = append(values, value)
		}
	}
	return values
}

func envInt(key string, fallback int) int {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return fallback
	}
	return value
}

func envBool(key string, fallback bool) bool {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func envDurationSeconds(key string, fallback time.Duration) time.Duration {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return fallback
	}
	return time.Duration(value * float64(time.Second))
}

func envDurationMilliseconds(key string, fallback time.Duration) time.Duration {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value < 0 {
		return fallback
	}
	return time.Duration(value) * time.Millisecond
}

func promptCacheSessionPreferHeader() bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv("PROMPT_CACHE_SESSION_ID_MODE")))
	switch value {
	case "header", "prefer_header", "client", "client_header":
		return true
	default:
		return false
	}
}

func promptCachePreviousStrictDefault() bool {
	if _, ok := os.LookupEnv("PROMPT_CACHE_PREVIOUS_STRICT_ENABLED"); ok {
		return envBool("PROMPT_CACHE_PREVIOUS_STRICT_ENABLED", true)
	}
	return !envBool("PROMPT_CACHE_PREVIOUS_REPLAY_FALLBACK_ENABLED", false)
}
