package store

import (
	"context"
	"encoding/json"
	"time"
)

type Setting struct {
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value"`
	UpdatedAt time.Time       `json:"updated_at"`
}

func (s *Store) ListSettings(ctx context.Context) ([]Setting, error) {
	rows, err := s.pool.Query(ctx, `select key, coalesce(value::jsonb, '{}'::jsonb), updated_at from gateway_settings order by key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Setting, 0)
	for rows.Next() {
		var item Setting
		if err := rows.Scan(&item.Key, &item.Value, &item.UpdatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) UpsertSetting(ctx context.Context, key string, value json.RawMessage) (Setting, error) {
	row := s.pool.QueryRow(ctx, `
		insert into gateway_settings(key, value, updated_at)
		values ($1, $2, now())
		on conflict (key) do update set value = excluded.value, updated_at = now()
		returning key, coalesce(value::jsonb, '{}'::jsonb), updated_at
	`, key, value)
	var item Setting
	err := row.Scan(&item.Key, &item.Value, &item.UpdatedAt)
	return item, err
}

func (s *Store) WriteAuditLog(ctx context.Context, action, actor, targetType, targetID string, payload any) error {
	_, err := s.pool.Exec(ctx, `
		insert into admin_audit_logs(action, actor, target_type, target_id, payload)
		values ($1, $2, $3, $4, $5)
	`, action, nullableString(actor), nullableString(targetType), nullableString(targetID), jsonBytes(payload))
	return err
}
