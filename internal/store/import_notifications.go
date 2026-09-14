package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// ListenForImportJobs uses a dedicated connection: a listener must never pin a
// connection from the request pool. Notifications carry no credentials and only
// wake the worker; committed job/item rows remain the durable source of work.
// The caller reconnects on error. Waking after LISTEN covers jobs committed
// during a disconnect or before this worker started.
func (s *Store) ListenForImportJobs(ctx context.Context, wake chan<- struct{}) error {
	cfg := s.pool.Config().ConnConfig.Copy()
	cfg.RuntimeParams["application_name"] = "oaix-import-listener"
	cfg.Tracer = nil
	connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	conn, err := pgx.ConnectConfig(connectCtx, cfg)
	cancel()
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()
	listenCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	_, err = conn.Exec(listenCtx, "listen oaix_import_jobs")
	cancel()
	if err != nil {
		return err
	}
	notify := func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	notify()
	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return err
		}
		notify()
	}
}
