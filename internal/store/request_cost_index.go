package store

import "context"

const requestCostRepairIndexName = "ix_gateway_request_logs_late_cost"
const requestCostRepairCoveringIndexName = "ix_gateway_request_logs_late_cost_covering"

// EnsureRequestCostRepairIndex runs outside startup readiness: the concurrent
// build still reads the full log heap even though the resulting index is small.
func (s *Store) EnsureRequestCostRepairIndex(ctx context.Context) error {
	if err := s.ensureConcurrentIndex(ctx, requestCostRepairIndexName,
		"create index concurrently if not exists "+requestCostRepairIndexName+
			" on gateway_request_logs (token_id) where analytics_recorded_at < finished_at and estimated_cost_usd is not null"); err != nil {
		return err
	}
	return s.ensureConcurrentIndex(ctx, requestCostRepairCoveringIndexName,
		"create index concurrently if not exists "+requestCostRepairCoveringIndexName+
			" on gateway_request_logs (token_id) include (estimated_cost_usd) where analytics_recorded_at < finished_at and estimated_cost_usd is not null")
}

func (s *Store) requestCostRepairIndexReady(ctx context.Context) (bool, error) {
	var ready bool
	err := s.pool.QueryRow(ctx, `
		select exists (
			select 1 from pg_class c
			join pg_namespace n on n.oid = c.relnamespace
			join pg_index i on i.indexrelid = c.oid
			where n.nspname = current_schema() and c.relname = $1
			  and i.indisready and i.indisvalid
		)
	`, requestCostRepairIndexName).Scan(&ready)
	return ready, err
}
