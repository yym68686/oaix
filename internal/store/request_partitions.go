package store

import (
	"context"
	"fmt"
	"time"
)

type RequestLogPartition struct {
	Date       time.Time `json:"date"`
	TableName  string    `json:"table_name"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
}

func RequestLogPartitionName(day time.Time) string {
	return fmt.Sprintf("gateway_request_logs_%s", day.UTC().Format("20060102"))
}

func (s *Store) EnsureRequestLogDailyPartition(ctx context.Context, day time.Time) (RequestLogPartition, error) {
	start := time.Date(day.UTC().Year(), day.UTC().Month(), day.UTC().Day(), 0, 0, 0, 0, time.UTC)
	finish := start.Add(24 * time.Hour)
	name := RequestLogPartitionName(start)
	partition := RequestLogPartition{
		Date:       start,
		TableName:  name,
		StartedAt:  start,
		FinishedAt: finish,
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return RequestLogPartition{}, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, fmt.Sprintf(`create table if not exists %s (like gateway_request_logs including all)`, pgIdentifier(name))); err != nil {
		return RequestLogPartition{}, err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`create index if not exists %s on %s (started_at desc)`, pgIdentifier(name+"_started_at_idx"), pgIdentifier(name))); err != nil {
		return RequestLogPartition{}, err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`create index if not exists %s on %s (request_id)`, pgIdentifier(name+"_request_id_idx"), pgIdentifier(name))); err != nil {
		return RequestLogPartition{}, err
	}
	if _, err := tx.Exec(ctx, `
		insert into gateway_request_log_partitions(partition_date, table_name, started_at, finished_at)
		values ($1, $2, $3, $4)
		on conflict (partition_date) do update
		set table_name = excluded.table_name,
		    started_at = excluded.started_at,
		    finished_at = excluded.finished_at
	`, start, name, start, finish); err != nil {
		return RequestLogPartition{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RequestLogPartition{}, err
	}
	return partition, nil
}

func pgIdentifier(value string) string {
	escaped := make([]rune, 0, len(value)+2)
	escaped = append(escaped, '"')
	for _, r := range value {
		if r == '"' {
			escaped = append(escaped, '"')
		}
		escaped = append(escaped, r)
	}
	escaped = append(escaped, '"')
	return string(escaped)
}
