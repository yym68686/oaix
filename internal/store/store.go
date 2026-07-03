package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yym68686/oaix/internal/config"
)

const SchemaVersion = 14

type Workload string

const (
	WorkloadHotPath   Workload = "hot_path"
	WorkloadAdmin     Workload = "admin"
	WorkloadWorker    Workload = "worker"
	WorkloadAnalytics Workload = "analytics"
)

type Store struct {
	pool          *pgxpool.Pool
	workloadPools map[Workload]*pgxpool.Pool
}

type ResourceScope struct {
	OwnerUserID *int64
	AllowAll    bool
}

func AllResources() ResourceScope {
	return ResourceScope{AllowAll: true}
}

func OwnerResources(ownerUserID int64) ResourceScope {
	return ResourceScope{OwnerUserID: &ownerUserID}
}

func (s ResourceScope) ownerFilter(column string, args *[]any) string {
	if s.AllowAll {
		return "true"
	}
	if s.OwnerUserID == nil || *s.OwnerUserID <= 0 {
		return "false"
	}
	*args = append(*args, *s.OwnerUserID)
	return fmt.Sprintf("%s = $%d", column, len(*args))
}

type Health struct {
	OK            bool      `json:"ok"`
	SchemaVersion int       `json:"schema_version"`
	CheckedAt     time.Time `json:"checked_at"`
	Error         string    `json:"error,omitempty"`
}

type PoolStats struct {
	AcquireCount         int64 `json:"acquire_count"`
	AcquireDurationMs    int64 `json:"acquire_duration_ms"`
	AcquiredConns        int32 `json:"acquired_conns"`
	ConstructingConns    int32 `json:"constructing_conns"`
	EmptyAcquireCount    int64 `json:"empty_acquire_count"`
	IdleConns            int32 `json:"idle_conns"`
	MaxConns             int32 `json:"max_conns"`
	TotalConns           int32 `json:"total_conns"`
	CanceledAcquireCount int64 `json:"canceled_acquire_count"`
}

func Connect(ctx context.Context, cfg config.DatabaseConfig) (*Store, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, err
	}
	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	poolCfg.ConnConfig.RuntimeParams["application_name"] = "oaix-go"
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{
		pool: pool,
		workloadPools: map[Workload]*pgxpool.Pool{
			WorkloadHotPath:   pool,
			WorkloadAdmin:     pool,
			WorkloadWorker:    pool,
			WorkloadAnalytics: pool,
		},
	}, nil
}

func (s *Store) Close() {
	if s == nil {
		return
	}
	seen := map[*pgxpool.Pool]struct{}{}
	for _, pool := range s.workloadPools {
		if pool == nil {
			continue
		}
		if _, ok := seen[pool]; ok {
			continue
		}
		seen[pool] = struct{}{}
		pool.Close()
	}
	if s.pool != nil {
		if _, ok := seen[s.pool]; !ok {
			s.pool.Close()
		}
	}
}

func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

func (s *Store) PoolFor(workload Workload) *pgxpool.Pool {
	if s == nil {
		return nil
	}
	if pool := s.workloadPools[workload]; pool != nil {
		return pool
	}
	return s.pool
}

func (s *Store) PoolStats() PoolStats {
	stats := s.pool.Stat()
	return PoolStats{
		AcquireCount:         stats.AcquireCount(),
		AcquireDurationMs:    stats.AcquireDuration().Milliseconds(),
		AcquiredConns:        stats.AcquiredConns(),
		ConstructingConns:    stats.ConstructingConns(),
		EmptyAcquireCount:    stats.EmptyAcquireCount(),
		IdleConns:            stats.IdleConns(),
		MaxConns:             stats.MaxConns(),
		TotalConns:           stats.TotalConns(),
		CanceledAcquireCount: stats.CanceledAcquireCount(),
	}
}

func (s *Store) Ping(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return errors.New("store is not connected")
	}
	return s.pool.Ping(ctx)
}

func (s *Store) WithTx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := fn(ctx, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) CheckSchema(ctx context.Context) (Health, error) {
	health := Health{CheckedAt: time.Now().UTC()}
	var version int
	err := s.pool.QueryRow(ctx, `select version from schema_migrations where name = 'oaix_go'`).Scan(&version)
	if err != nil {
		health.Error = err.Error()
		return health, err
	}
	health.OK = version >= SchemaVersion
	health.SchemaVersion = version
	if !health.OK {
		return health, fmt.Errorf("schema version %d is older than required %d", version, SchemaVersion)
	}
	return health, nil
}

func NormalizePostgresURL(raw string) string {
	value := strings.TrimSpace(raw)
	value = strings.TrimPrefix(value, "postgresql+asyncpg://")
	if value != strings.TrimSpace(raw) {
		return "postgresql://" + value
	}
	return value
}

func RedactedURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	if parsed.User != nil {
		username := parsed.User.Username()
		parsed.User = url.UserPassword(username, "xxxxx")
	}
	host, port, err := net.SplitHostPort(parsed.Host)
	if err == nil {
		parsed.Host = net.JoinHostPort(host, port)
	}
	return parsed.String()
}

func hashString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func jsonBytes(value any) []byte {
	if value == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return data
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
