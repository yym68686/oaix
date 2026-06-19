package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type RequestLogListOptions struct {
	Limit        int
	Offset       int
	IncludeTotal bool
	RequestID    string
	OwnerUserID  int64
	APIKeyID     int64
	Model        string
	Endpoint     string
	StatusCode   *int
	Success      *bool
	TokenID      int64
	AccountID    string
	Stream       *bool
	From         *time.Time
	To           *time.Time
	QueryError   string
}

type RequestUsageSummary struct {
	Hours             int     `json:"hours"`
	Total             int     `json:"total"`
	Success           int     `json:"success"`
	Failure           int     `json:"failure"`
	InputTokens       int     `json:"input_tokens"`
	CachedInputTokens int     `json:"cached_input_tokens"`
	TotalTokens       int     `json:"total_tokens"`
	EstimatedCostUSD  float64 `json:"estimated_cost_usd"`
	CacheHitRatio     float64 `json:"cache_hit_ratio"`
	AverageTTFTMS     float64 `json:"average_ttft_ms"`
	AverageDurationMS float64 `json:"average_duration_ms"`
}

type OwnerUsageSummary struct {
	OwnerUserID       int64   `json:"owner_user_id"`
	Hours             int     `json:"hours"`
	RequestCount      int     `json:"request_count"`
	SuccessCount      int     `json:"success_count"`
	FailureCount      int     `json:"failure_count"`
	StreamingCount    int     `json:"streaming_count"`
	InputTokens       int     `json:"input_tokens"`
	CachedInputTokens int     `json:"cached_input_tokens"`
	TotalTokens       int     `json:"total_tokens"`
	EstimatedCostUSD  float64 `json:"estimated_cost_usd"`
	SuccessRate       float64 `json:"success_rate"`
	CacheHitRatio     float64 `json:"cache_hit_ratio"`
}

type CostAggregate struct {
	Key              string  `json:"key"`
	TokenID          *int64  `json:"token_id,omitempty"`
	AccountID        *string `json:"account_id,omitempty"`
	RequestCount     int     `json:"request_count"`
	EstimatedCostUSD float64 `json:"estimated_cost_usd"`
	SuccessCount     int     `json:"success_count"`
	FailureCount     int     `json:"failure_count"`
}

type CacheAnalytics struct {
	Hours              int            `json:"hours"`
	TotalRequests      int            `json:"total_requests"`
	RequestsWithUsage  int            `json:"requests_with_usage"`
	InputTokens        int            `json:"input_tokens"`
	CachedInputTokens  int            `json:"cached_input_tokens"`
	CacheHitRatio      float64        `json:"cache_hit_ratio"`
	PromptCacheSources map[string]int `json:"prompt_cache_sources"`
	AffinityResults    map[string]int `json:"affinity_results"`
}

type ErrorAnalyticsItem struct {
	ErrorKey  string `json:"error_key"`
	ModelName string `json:"model_name"`
	Endpoint  string `json:"endpoint"`
	TokenID   *int64 `json:"token_id,omitempty"`
	Count     int    `json:"count"`
}

type LatencyAnalytics struct {
	Hours       int            `json:"hours"`
	TTFTMs      map[string]int `json:"ttft_ms"`
	DurationMs  map[string]int `json:"duration_ms"`
	SampleCount int            `json:"sample_count"`
}

type OutboxStats struct {
	Depth       int        `json:"depth"`
	Retryable   int        `json:"retryable"`
	MaxAttempts int        `json:"max_attempts"`
	OldestAt    *time.Time `json:"oldest_at,omitempty"`
}

func (s *Store) ListRequestLogsFiltered(ctx context.Context, opts RequestLogListOptions) ([]RequestLog, int, error) {
	return s.ListRequestLogsFilteredScoped(ctx, AllResources(), opts)
}

func (s *Store) ListRequestLogsFilteredScoped(ctx context.Context, scope ResourceScope, opts RequestLogListOptions) ([]RequestLog, int, error) {
	if opts.Limit <= 0 || opts.Limit > 1000 {
		opts.Limit = 100
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}
	where, args := requestLogWhereScoped(opts, scope)
	var total int
	queryLimit := opts.Limit
	if opts.IncludeTotal {
		if err := s.pool.QueryRow(ctx, "select count(*) from gateway_request_logs where "+where, args...).Scan(&total); err != nil {
			return nil, 0, err
		}
	} else {
		queryLimit++
	}
	args = append(args, queryLimit, opts.Offset)
	rows, err := s.pool.Query(ctx, `
		select request_id, endpoint, model, model_name, is_stream, status_code, success,
		       owner_user_id, api_key_id, token_owner_user_id,
		       attempt_count, token_id, account_id, client_ip, user_agent, started_at, finished_at,
		       first_token_at, ttft_ms, duration_ms, input_tokens, cached_input_tokens, output_tokens,
		       total_tokens, estimated_cost_usd, request_payload_hash, upstream_payload_hash,
		       prompt_template_hash, prompt_dynamic_hash, prompt_cache_source, prompt_cache_key_hash,
		       prompt_cache_retention_requested, prompt_cache_retention_sent, session_id_hash,
		       session_id_source, previous_response_id_hash, upstream_response_id, cache_hit_ratio,
		       cache_affinity_result, cache_affinity_lane_index, prompt_cache_trace, error_message
		from gateway_request_logs
		where `+where+`
		order by started_at desc, request_id desc
		limit $`+fmt.Sprint(len(args)-1)+` offset $`+fmt.Sprint(len(args))+`
	`, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items, err := scanRequestLogs(rows)
	if err != nil {
		return nil, 0, err
	}
	if !opts.IncludeTotal {
		if len(items) > opts.Limit {
			items = items[:opts.Limit]
			total = opts.Offset + len(items) + 1
		} else {
			total = opts.Offset + len(items)
		}
	}
	return items, total, err
}

func (s *Store) GetRequestLog(ctx context.Context, requestID string) (RequestLog, error) {
	return s.GetRequestLogScoped(ctx, AllResources(), requestID)
}

func (s *Store) GetRequestLogScoped(ctx context.Context, scope ResourceScope, requestID string) (RequestLog, error) {
	items, _, err := s.ListRequestLogsFilteredScoped(ctx, scope, RequestLogListOptions{Limit: 1, RequestID: requestID})
	if err != nil {
		return RequestLog{}, err
	}
	if len(items) == 0 {
		return RequestLog{}, pgxErrNoRows()
	}
	return items[0], nil
}

func requestLogWhere(opts RequestLogListOptions) (string, []any) {
	return requestLogWhereScoped(opts, AllResources())
}

func requestLogWhereScoped(opts RequestLogListOptions, scope ResourceScope) (string, []any) {
	filters := []string{"true"}
	args := []any{}
	arg := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}
	filters = append(filters, scope.ownerFilter("owner_user_id", &args))
	if opts.RequestID != "" {
		filters = append(filters, fmt.Sprintf("request_id = %s", arg(opts.RequestID)))
	}
	if opts.OwnerUserID > 0 {
		filters = append(filters, fmt.Sprintf("owner_user_id = %s", arg(opts.OwnerUserID)))
	}
	if opts.APIKeyID > 0 {
		filters = append(filters, fmt.Sprintf("api_key_id = %s", arg(opts.APIKeyID)))
	}
	if opts.Model != "" {
		filters = append(filters, fmt.Sprintf("coalesce(model_name, model, '') = %s", arg(opts.Model)))
	}
	if opts.Endpoint != "" {
		filters = append(filters, fmt.Sprintf("endpoint = %s", arg(opts.Endpoint)))
	}
	if opts.StatusCode != nil {
		filters = append(filters, fmt.Sprintf("status_code = %s", arg(*opts.StatusCode)))
	}
	if opts.Success != nil {
		filters = append(filters, fmt.Sprintf("success = %s", arg(*opts.Success)))
	}
	if opts.TokenID > 0 {
		filters = append(filters, fmt.Sprintf("token_id = %s", arg(opts.TokenID)))
	}
	if opts.AccountID != "" {
		filters = append(filters, fmt.Sprintf("account_id = %s", arg(opts.AccountID)))
	}
	if opts.Stream != nil {
		filters = append(filters, fmt.Sprintf("is_stream = %s", arg(*opts.Stream)))
	}
	if opts.From != nil {
		filters = append(filters, fmt.Sprintf("started_at >= %s", arg(*opts.From)))
	}
	if opts.To != nil {
		filters = append(filters, fmt.Sprintf("started_at <= %s", arg(*opts.To)))
	}
	if opts.QueryError != "" {
		filters = append(filters, fmt.Sprintf("coalesce(error_message, '') ilike %s", arg("%"+opts.QueryError+"%")))
	}
	return strings.Join(filters, " and "), args
}

func scanRequestLogs(rows pgxRows) ([]RequestLog, error) {
	var items []RequestLog
	for rows.Next() {
		var item RequestLog
		var promptTrace []byte
		if err := rows.Scan(
			&item.RequestID, &item.Endpoint, &item.Model, &item.ModelName, &item.IsStream, &item.StatusCode,
			&item.Success, &item.OwnerUserID, &item.APIKeyID, &item.TokenOwnerUserID, &item.AttemptCount,
			&item.TokenID, &item.AccountID, &item.ClientIP, &item.UserAgent,
			&item.StartedAt, &item.FinishedAt, &item.FirstTokenAt, &item.TTFTMs, &item.DurationMs,
			&item.InputTokens, &item.CachedInputTokens, &item.OutputTokens, &item.TotalTokens, &item.EstimatedCostUSD,
			&item.RequestPayloadHash, &item.UpstreamPayloadHash, &item.PromptTemplateHash, &item.PromptDynamicHash,
			&item.PromptCacheSource, &item.PromptCacheKeyHash, &item.PromptCacheRetentionRequested,
			&item.PromptCacheRetentionSent, &item.SessionIDHash, &item.SessionIDSource, &item.PreviousResponseIDHash,
			&item.UpstreamResponseID, &item.CacheHitRatio, &item.CacheAffinityResult, &item.CacheAffinityLaneIndex,
			&promptTrace, &item.ErrorMessage,
		); err != nil {
			return nil, err
		}
		if len(promptTrace) > 0 {
			_ = json.Unmarshal(promptTrace, &item.PromptCacheTrace)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) RequestUsageSummaryScoped(ctx context.Context, scope ResourceScope, hours int) (RequestUsageSummary, error) {
	if hours <= 0 {
		hours = 24
	}
	from := time.Now().Add(-time.Duration(hours) * time.Hour)
	where, args := requestLogWhereScoped(RequestLogListOptions{From: &from}, scope)
	var out RequestUsageSummary
	out.Hours = hours
	err := s.pool.QueryRow(ctx, `
		select
			count(*)::int,
			count(*) filter (where success = true)::int,
			count(*) filter (where success = false)::int,
			coalesce(sum(input_tokens), 0)::int,
			coalesce(sum(cached_input_tokens), 0)::int,
			coalesce(sum(total_tokens), 0)::int,
			coalesce(sum(estimated_cost_usd), 0)::float8,
			coalesce(avg(ttft_ms) filter (where ttft_ms is not null), 0)::float8,
			coalesce(avg(duration_ms) filter (where duration_ms is not null), 0)::float8
		from gateway_request_logs
		where `+where, args...).Scan(
		&out.Total, &out.Success, &out.Failure, &out.InputTokens, &out.CachedInputTokens,
		&out.TotalTokens, &out.EstimatedCostUSD, &out.AverageTTFTMS, &out.AverageDurationMS,
	)
	if err != nil {
		return RequestUsageSummary{}, err
	}
	if out.InputTokens > 0 {
		out.CacheHitRatio = float64(out.CachedInputTokens) / float64(out.InputTokens)
	}
	return out, nil
}

func (s *Store) RequestUsageByOwner(ctx context.Context, ownerIDs []int64, hours int) (map[int64]OwnerUsageSummary, error) {
	if hours <= 0 {
		hours = 24
	}
	out := make(map[int64]OwnerUsageSummary, len(ownerIDs))
	if len(ownerIDs) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, `
		select
			owner_user_id,
			coalesce(sum(request_count), 0)::int,
			coalesce(sum(success_count), 0)::int,
			coalesce(sum(failure_count), 0)::int,
			coalesce(sum(streaming_count), 0)::int,
			coalesce(sum(input_tokens), 0)::int,
			coalesce(sum(cached_input_tokens), 0)::int,
			coalesce(sum(total_tokens), 0)::int,
			coalesce(sum(estimated_cost_usd), 0)::float8
		from gateway_request_hourly_stats
		where owner_user_id = any($1)
		  and bucket_start >= date_trunc('hour', now() - make_interval(hours => $2))
		group by owner_user_id
	`, ownerIDs, hours)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		item := OwnerUsageSummary{Hours: hours}
		if err := rows.Scan(
			&item.OwnerUserID, &item.RequestCount, &item.SuccessCount, &item.FailureCount,
			&item.StreamingCount, &item.InputTokens, &item.CachedInputTokens, &item.TotalTokens,
			&item.EstimatedCostUSD,
		); err != nil {
			return nil, err
		}
		if item.RequestCount > 0 {
			item.SuccessRate = float64(item.SuccessCount) / float64(item.RequestCount)
		}
		if item.InputTokens > 0 {
			item.CacheHitRatio = float64(item.CachedInputTokens) / float64(item.InputTokens)
		}
		out[item.OwnerUserID] = item
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, ownerID := range ownerIDs {
		if _, ok := out[ownerID]; !ok {
			out[ownerID] = OwnerUsageSummary{OwnerUserID: ownerID, Hours: hours}
		}
	}
	return out, nil
}

type pgxRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func (s *Store) CostAggregatesByToken(ctx context.Context, opts RequestLogListOptions, limit int, offset int) ([]CostAggregate, int, error) {
	return s.CostAggregatesByTokenScoped(ctx, AllResources(), opts, limit, offset)
}

func (s *Store) CostAggregatesByTokenScoped(ctx context.Context, scope ResourceScope, opts RequestLogListOptions, limit int, offset int) ([]CostAggregate, int, error) {
	opts.Limit = limit
	opts.Offset = offset
	return s.costAggregatesScoped(ctx, scope, opts, "token")
}

func (s *Store) CostAggregatesByAccount(ctx context.Context, opts RequestLogListOptions, limit int, offset int) ([]CostAggregate, int, error) {
	return s.CostAggregatesByAccountScoped(ctx, AllResources(), opts, limit, offset)
}

func (s *Store) CostAggregatesByAccountScoped(ctx context.Context, scope ResourceScope, opts RequestLogListOptions, limit int, offset int) ([]CostAggregate, int, error) {
	opts.Limit = limit
	opts.Offset = offset
	return s.costAggregatesScoped(ctx, scope, opts, "account")
}

func (s *Store) costAggregatesScoped(ctx context.Context, scope ResourceScope, opts RequestLogListOptions, dimension string) ([]CostAggregate, int, error) {
	if opts.Limit <= 0 || opts.Limit > 500 {
		opts.Limit = 100
	}
	where, args := requestLogWhereScoped(opts, scope)
	keyExpr := "token_id::text"
	nullClause := "token_id is not null"
	if dimension == "account" {
		keyExpr = "account_id"
		nullClause = "account_id is not null and account_id <> ''"
	}
	var total int
	if err := s.pool.QueryRow(ctx, `select count(*) from (select `+keyExpr+` from gateway_request_logs where `+where+` and `+nullClause+` group by 1) grouped`, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	args = append(args, opts.Limit, opts.Offset)
	rows, err := s.pool.Query(ctx, `
		select `+keyExpr+` as key,
		       count(*)::int,
		       coalesce(sum(estimated_cost_usd), 0)::float8,
		       count(*) filter (where success = true)::int,
		       count(*) filter (where success = false)::int
		from gateway_request_logs
		where `+where+` and `+nullClause+`
		group by 1
		order by coalesce(sum(estimated_cost_usd), 0) desc
		limit $`+fmt.Sprint(len(args)-1)+` offset $`+fmt.Sprint(len(args))+`
	`, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := []CostAggregate{}
	for rows.Next() {
		var item CostAggregate
		if err := rows.Scan(&item.Key, &item.RequestCount, &item.EstimatedCostUSD, &item.SuccessCount, &item.FailureCount); err != nil {
			return nil, 0, err
		}
		if dimension == "token" {
			var tokenID int64
			_, _ = fmt.Sscan(item.Key, &tokenID)
			item.TokenID = &tokenID
		} else {
			item.AccountID = &item.Key
		}
		item.EstimatedCostUSD = roundCostUSD(item.EstimatedCostUSD)
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func (s *Store) CacheAnalytics(ctx context.Context, hours int) (CacheAnalytics, error) {
	return s.CacheAnalyticsScoped(ctx, AllResources(), hours)
}

func (s *Store) CacheAnalyticsScoped(ctx context.Context, scope ResourceScope, hours int) (CacheAnalytics, error) {
	if hours <= 0 {
		hours = 24
	}
	from := time.Now().Add(-time.Duration(hours) * time.Hour)
	where, args := requestLogWhereScoped(RequestLogListOptions{From: &from}, scope)
	rows, err := s.pool.Query(ctx, `
		select
			count(*)::int,
			count(*) filter (where input_tokens is not null)::int,
			coalesce(sum(input_tokens), 0)::int,
			coalesce(sum(cached_input_tokens), 0)::int
		from gateway_request_logs
		where `+where, args...)
	if err != nil {
		return CacheAnalytics{}, err
	}
	defer rows.Close()
	out := CacheAnalytics{Hours: hours, PromptCacheSources: map[string]int{}, AffinityResults: map[string]int{}}
	if rows.Next() {
		if err := rows.Scan(&out.TotalRequests, &out.RequestsWithUsage, &out.InputTokens, &out.CachedInputTokens); err != nil {
			return CacheAnalytics{}, err
		}
	}
	rows.Close()
	if out.InputTokens > 0 {
		out.CacheHitRatio = float64(out.CachedInputTokens) / float64(out.InputTokens)
	}
	if err := s.fillStringCountMapScoped(ctx, scope, `prompt_cache_source`, hours, out.PromptCacheSources); err != nil {
		return CacheAnalytics{}, err
	}
	if err := s.fillStringCountMapScoped(ctx, scope, `cache_affinity_result`, hours, out.AffinityResults); err != nil {
		return CacheAnalytics{}, err
	}
	return out, nil
}

func (s *Store) fillStringCountMap(ctx context.Context, column string, hours int, out map[string]int) error {
	return s.fillStringCountMapScoped(ctx, AllResources(), column, hours, out)
}

func (s *Store) fillStringCountMapScoped(ctx context.Context, scope ResourceScope, column string, hours int, out map[string]int) error {
	if column != "prompt_cache_source" && column != "cache_affinity_result" {
		return fmt.Errorf("invalid analytics column")
	}
	if hours <= 0 {
		hours = 24
	}
	from := time.Now().Add(-time.Duration(hours) * time.Hour)
	where, args := requestLogWhereScoped(RequestLogListOptions{From: &from}, scope)
	rows, err := s.pool.Query(ctx, `
		select coalesce(`+column+`, 'unknown'), count(*)::int
		from gateway_request_logs
		where `+where+`
		group by 1
	`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var count int
		if err := rows.Scan(&key, &count); err != nil {
			return err
		}
		out[key] = count
	}
	return rows.Err()
}

func (s *Store) ErrorAnalytics(ctx context.Context, hours int, limit int) ([]ErrorAnalyticsItem, error) {
	return s.ErrorAnalyticsScoped(ctx, AllResources(), hours, limit)
}

func (s *Store) ErrorAnalyticsScoped(ctx context.Context, scope ResourceScope, hours int, limit int) ([]ErrorAnalyticsItem, error) {
	if hours <= 0 {
		hours = 24
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	from := time.Now().Add(-time.Duration(hours) * time.Hour)
	where, args := requestLogWhereScoped(RequestLogListOptions{From: &from, Success: ptrBool(false)}, scope)
	args = append(args, limit)
	rows, err := s.pool.Query(ctx, `
		select left(coalesce(error_message, 'unknown'), 160) as error_key,
		       coalesce(model_name, model, '') as model_name,
		       endpoint,
		       token_id,
		       count(*)::int
		from gateway_request_logs
		where `+where+`
		group by 1,2,3,4
		order by count(*) desc
		limit $`+fmt.Sprint(len(args))+`
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ErrorAnalyticsItem{}
	for rows.Next() {
		var item ErrorAnalyticsItem
		if err := rows.Scan(&item.ErrorKey, &item.ModelName, &item.Endpoint, &item.TokenID, &item.Count); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) LatencyAnalytics(ctx context.Context, hours int) (LatencyAnalytics, error) {
	return s.LatencyAnalyticsScoped(ctx, AllResources(), hours)
}

func (s *Store) LatencyAnalyticsScoped(ctx context.Context, scope ResourceScope, hours int) (LatencyAnalytics, error) {
	if hours <= 0 {
		hours = 24
	}
	from := time.Now().Add(-time.Duration(hours) * time.Hour)
	where, args := requestLogWhereScoped(RequestLogListOptions{From: &from}, scope)
	rows, err := s.pool.Query(ctx, `
		select ttft_ms, duration_ms
		from gateway_request_logs
		where `+where+`
		  and (ttft_ms is not null or duration_ms is not null)
		order by started_at desc
		limit 10000
	`, args...)
	if err != nil {
		return LatencyAnalytics{}, err
	}
	defer rows.Close()
	var ttfts []int
	var durations []int
	for rows.Next() {
		var ttft *int
		var duration *int
		if err := rows.Scan(&ttft, &duration); err != nil {
			return LatencyAnalytics{}, err
		}
		if ttft != nil {
			ttfts = append(ttfts, *ttft)
		}
		if duration != nil {
			durations = append(durations, *duration)
		}
	}
	return LatencyAnalytics{
		Hours:       hours,
		TTFTMs:      percentiles(ttfts),
		DurationMs:  percentiles(durations),
		SampleCount: maxInt(len(ttfts), len(durations)),
	}, rows.Err()
}

func percentiles(values []int) map[string]int {
	out := map[string]int{"p50": 0, "p90": 0, "p95": 0, "p99": 0}
	if len(values) == 0 {
		return out
	}
	sort.Ints(values)
	for key, pct := range map[string]float64{"p50": 0.50, "p90": 0.90, "p95": 0.95, "p99": 0.99} {
		index := int(float64(len(values)-1) * pct)
		out[key] = values[index]
	}
	return out
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func ptrBool(value bool) *bool {
	return &value
}

func (s *Store) RequestLogOutboxStats(ctx context.Context) (OutboxStats, error) {
	var stats OutboxStats
	err := s.pool.QueryRow(ctx, `
		select count(*)::int,
		       count(*) filter (where next_attempt_at <= now())::int,
		       coalesce(max(attempt_count), 0)::int,
		       min(created_at)
		from request_log_outbox
	`).Scan(&stats.Depth, &stats.Retryable, &stats.MaxAttempts, &stats.OldestAt)
	return stats, err
}

func pgxErrNoRows() error {
	return pgx.ErrNoRows
}
