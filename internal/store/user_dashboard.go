package store

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

type UserDashboardRange string

const (
	UserDashboardToday UserDashboardRange = "today"
	UserDashboardWeek  UserDashboardRange = "week"
	UserDashboardMonth UserDashboardRange = "month"
	UserDashboardYear  UserDashboardRange = "year"
)

type UserDashboardOptions struct {
	Now      time.Time
	Location *time.Location
	Range    UserDashboardRange
}

type UserDashboardPeriod struct {
	RequestCount      int64   `json:"request_count"`
	TotalTokens       int64   `json:"total_tokens"`
	EstimatedCostUSD  float64 `json:"estimated_cost_usd"`
	InputTokens       int64   `json:"input_tokens"`
	CachedInputTokens int64   `json:"cached_input_tokens"`
	CacheHitRatio     float64 `json:"cache_hit_ratio"`
}

type UserDashboardTrendPoint struct {
	BucketStart       time.Time `json:"bucket_start"`
	RequestCount      int64     `json:"request_count"`
	TotalTokens       int64     `json:"total_tokens"`
	InputTokens       int64     `json:"input_tokens"`
	CachedInputTokens int64     `json:"cached_input_tokens"`
	CacheHitRatio     *float64  `json:"cache_hit_ratio"`
	EstimatedCostUSD  float64   `json:"estimated_cost_usd"`
}

type UserDashboardModel struct {
	ModelName        string  `json:"model_name"`
	RequestCount     int64   `json:"request_count"`
	TotalTokens      int64   `json:"total_tokens"`
	EstimatedCostUSD float64 `json:"estimated_cost_usd"`
}

type UserDashboard struct {
	GeneratedAt time.Time                      `json:"generated_at"`
	Timezone    string                         `json:"timezone"`
	Range       UserDashboardRange             `json:"range"`
	Bucket      string                         `json:"bucket"`
	Periods     map[string]UserDashboardPeriod `json:"periods"`
	Trend       []UserDashboardTrendPoint      `json:"trend"`
	Models      []UserDashboardModel           `json:"models"`
}

type userDashboardWindow struct {
	periodStarts map[string]time.Time
	selectedFrom time.Time
	bucket       string
}

func (s *Store) UserDashboardScoped(ctx context.Context, scope ResourceScope, opts UserDashboardOptions) (UserDashboard, error) {
	now := opts.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	location := opts.Location
	if location == nil {
		location = time.UTC
	}
	selectedRange := opts.Range
	if !validUserDashboardRange(selectedRange) {
		selectedRange = UserDashboardMonth
	}
	window := buildUserDashboardWindow(now, location, selectedRange)
	periods, err := s.userDashboardPeriods(ctx, scope, window.periodStarts, now)
	if err != nil {
		return UserDashboard{}, err
	}
	trend, err := s.userDashboardTrend(ctx, scope, window.selectedFrom, now, location, window.bucket)
	if err != nil {
		return UserDashboard{}, err
	}
	models, err := s.userDashboardModels(ctx, scope, window.selectedFrom, now)
	if err != nil {
		return UserDashboard{}, err
	}
	return UserDashboard{
		GeneratedAt: now,
		Timezone:    location.String(),
		Range:       selectedRange,
		Bucket:      window.bucket,
		Periods:     periods,
		Trend:       fillUserDashboardTrend(trend, window.selectedFrom, now, location, window.bucket),
		Models:      models,
	}, nil
}

func validUserDashboardRange(value UserDashboardRange) bool {
	switch value {
	case UserDashboardToday, UserDashboardWeek, UserDashboardMonth, UserDashboardYear:
		return true
	default:
		return false
	}
}

func buildUserDashboardWindow(now time.Time, location *time.Location, selectedRange UserDashboardRange) userDashboardWindow {
	localNow := now.In(location)
	today := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), 0, 0, 0, 0, location)
	starts := map[string]time.Time{
		string(UserDashboardToday): today,
		string(UserDashboardWeek):  today.AddDate(0, 0, -6),
		string(UserDashboardMonth): today.AddDate(0, 0, -29),
		string(UserDashboardYear):  time.Date(localNow.Year(), time.January, 1, 0, 0, 0, 0, location),
	}
	bucket := "day"
	if selectedRange == UserDashboardToday {
		bucket = "hour"
	} else if selectedRange == UserDashboardYear {
		bucket = "month"
	}
	return userDashboardWindow{periodStarts: starts, selectedFrom: starts[string(selectedRange)], bucket: bucket}
}

func (s *Store) userDashboardPeriods(ctx context.Context, scope ResourceScope, starts map[string]time.Time, now time.Time) (map[string]UserDashboardPeriod, error) {
	args := []any{}
	ownerFilter := scope.ownerFilter("stats.owner_user_id", &args)
	addArg := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}
	todayArg := addArg(starts[string(UserDashboardToday)])
	weekArg := addArg(starts[string(UserDashboardWeek)])
	monthArg := addArg(starts[string(UserDashboardMonth)])
	yearArg := addArg(starts[string(UserDashboardYear)])
	nowArg := addArg(now)
	earliest := starts[string(UserDashboardMonth)]
	if starts[string(UserDashboardYear)].Before(earliest) {
		earliest = starts[string(UserDashboardYear)]
	}
	earliestArg := addArg(earliest)
	rows, err := s.pool.Query(ctx, `
		with periods(name, starts_at) as (
			values
				('today', `+todayArg+`::timestamptz),
				('week', `+weekArg+`::timestamptz),
				('month', `+monthArg+`::timestamptz),
				('year', `+yearArg+`::timestamptz)
		)
		select periods.name,
		       coalesce(sum(stats.request_count), 0)::bigint,
		       coalesce(sum(stats.total_tokens), 0)::bigint,
		       coalesce(sum(stats.estimated_cost_usd), 0)::float8,
		       coalesce(sum(stats.input_tokens), 0)::bigint,
		       coalesce(sum(stats.cached_input_tokens), 0)::bigint
		from periods
		left join gateway_request_hourly_stats stats
		  on `+ownerFilter+`
		 and stats.bucket_start >= periods.starts_at
		 and stats.bucket_start >= `+earliestArg+`
		 and stats.bucket_start <= `+nowArg+`
		group by periods.name
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	periods := map[string]UserDashboardPeriod{}
	for rows.Next() {
		var name string
		var item UserDashboardPeriod
		if err := rows.Scan(&name, &item.RequestCount, &item.TotalTokens, &item.EstimatedCostUSD, &item.InputTokens, &item.CachedInputTokens); err != nil {
			return nil, err
		}
		if item.InputTokens > 0 {
			item.CacheHitRatio = float64(item.CachedInputTokens) / float64(item.InputTokens)
		}
		periods[name] = item
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, name := range []string{"today", "week", "month", "year"} {
		if _, ok := periods[name]; !ok {
			periods[name] = UserDashboardPeriod{}
		}
	}
	return periods, nil
}

func (s *Store) userDashboardTrend(ctx context.Context, scope ResourceScope, from time.Time, now time.Time, location *time.Location, bucket string) ([]UserDashboardTrendPoint, error) {
	args := []any{}
	ownerFilter := scope.ownerFilter("owner_user_id", &args)
	addArg := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}
	fromArg := addArg(from)
	nowArg := addArg(now)
	bucketExpression := "date_trunc('hour', bucket_start)"
	if bucket == "day" || bucket == "month" {
		timezoneArg := addArg(location.String())
		bucketExpression = "date_trunc('" + bucket + "', bucket_start at time zone " + timezoneArg + ") at time zone " + timezoneArg
	}
	rows, err := s.pool.Query(ctx, `
		select `+bucketExpression+` as trend_bucket,
		       coalesce(sum(request_count), 0)::bigint,
		       coalesce(sum(total_tokens), 0)::bigint,
		       coalesce(sum(input_tokens), 0)::bigint,
		       coalesce(sum(cached_input_tokens), 0)::bigint,
		       coalesce(sum(estimated_cost_usd), 0)::float8
		from gateway_request_hourly_stats
		where `+ownerFilter+`
		  and bucket_start >= `+fromArg+`
		  and bucket_start <= `+nowArg+`
		group by trend_bucket
		order by trend_bucket
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []UserDashboardTrendPoint{}
	for rows.Next() {
		var item UserDashboardTrendPoint
		if err := rows.Scan(&item.BucketStart, &item.RequestCount, &item.TotalTokens, &item.InputTokens, &item.CachedInputTokens, &item.EstimatedCostUSD); err != nil {
			return nil, err
		}
		if item.InputTokens > 0 {
			ratio := float64(item.CachedInputTokens) / float64(item.InputTokens)
			item.CacheHitRatio = &ratio
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) userDashboardModels(ctx context.Context, scope ResourceScope, from time.Time, now time.Time) ([]UserDashboardModel, error) {
	args := []any{}
	ownerFilter := scope.ownerFilter("owner_user_id", &args)
	addArg := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}
	fromArg := addArg(from)
	nowArg := addArg(now)
	rows, err := s.pool.Query(ctx, `
		select coalesce(nullif(trim(model_name), ''), 'unknown') as model_name,
		       coalesce(sum(request_count), 0)::bigint,
		       coalesce(sum(total_tokens), 0)::bigint,
		       coalesce(sum(estimated_cost_usd), 0)::float8
		from gateway_request_hourly_stats
		where `+ownerFilter+`
		  and bucket_start >= `+fromArg+`
		  and bucket_start <= `+nowArg+`
		group by coalesce(nullif(trim(model_name), ''), 'unknown')
		order by coalesce(sum(estimated_cost_usd), 0) desc,
		         coalesce(sum(total_tokens), 0) desc,
		         model_name
		limit 20
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []UserDashboardModel{}
	for rows.Next() {
		var item UserDashboardModel
		if err := rows.Scan(&item.ModelName, &item.RequestCount, &item.TotalTokens, &item.EstimatedCostUSD); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func fillUserDashboardTrend(items []UserDashboardTrendPoint, from time.Time, now time.Time, location *time.Location, bucket string) []UserDashboardTrendPoint {
	byBucket := make(map[int64]UserDashboardTrendPoint, len(items))
	for _, item := range items {
		byBucket[item.BucketStart.Unix()] = item
	}
	cursor := dashboardBucketStart(from, location, bucket)
	last := dashboardBucketStart(now, location, bucket)
	filled := make([]UserDashboardTrendPoint, 0, len(items)+1)
	for !cursor.After(last) {
		key := cursor.Unix()
		if item, ok := byBucket[key]; ok {
			filled = append(filled, item)
		} else {
			filled = append(filled, UserDashboardTrendPoint{BucketStart: cursor})
		}
		cursor = nextDashboardBucket(cursor, location, bucket)
	}
	sort.SliceStable(filled, func(i, j int) bool { return filled[i].BucketStart.Before(filled[j].BucketStart) })
	return filled
}

func dashboardBucketStart(value time.Time, location *time.Location, bucket string) time.Time {
	local := value.In(location)
	switch strings.ToLower(bucket) {
	case "hour":
		return local.Truncate(time.Hour)
	case "month":
		return time.Date(local.Year(), local.Month(), 1, 0, 0, 0, 0, location)
	default:
		return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location)
	}
}

func nextDashboardBucket(value time.Time, location *time.Location, bucket string) time.Time {
	local := value.In(location)
	switch strings.ToLower(bucket) {
	case "hour":
		return local.Add(time.Hour)
	case "month":
		return local.AddDate(0, 1, 0)
	default:
		return local.AddDate(0, 0, 1)
	}
}
