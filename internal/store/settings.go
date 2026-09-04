package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	TokenSelectionSettingKey        = "token_selection"
	TokenConcurrencySettingKey      = "token_concurrency"
	Ordinary429CooldownSettingKey   = "ordinary_429_cooldown"
	DefaultTokenSelectionStrategy   = "least_recently_used"
	TokenSelectionStrategyFillFirst = "fill_first"
	MinTokenActiveStreamCap         = int64(1)
	MaxTokenActiveStreamCap         = int64(50)
	MinOrdinary429CooldownSeconds   = int64(1)
	MaxOrdinary429CooldownSeconds   = int64(24 * 60 * 60)
	DefaultOrdinary429Cooldown      = 5 * time.Minute
	defaultTokenActiveStreamCap     = int64(10)
)

var defaultTokenPlanOrder = []string{"free", "plus", "team", "pro"}

type Setting struct {
	Key         string          `json:"key"`
	OwnerUserID *int64          `json:"owner_user_id,omitempty"`
	Scope       string          `json:"scope,omitempty"`
	Value       json.RawMessage `json:"value"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

type TokenSelectionSettings struct {
	Strategy         string     `json:"strategy"`
	TokenOrder       []int64    `json:"token_order"`
	PlanOrderEnabled bool       `json:"plan_order_enabled"`
	PlanOrder        []string   `json:"plan_order"`
	ActiveStreamCap  int64      `json:"active_stream_cap"`
	UpdatedAt        *time.Time `json:"updated_at,omitempty"`
}

type Ordinary429CooldownSettings struct {
	CooldownSeconds int64      `json:"cooldown_seconds"`
	UpdatedAt       *time.Time `json:"updated_at,omitempty"`
}

// TokenConcurrencySettings contains only the user-owned per-plan overrides.
// An absent plan is intentionally different from a value of zero: absent means
// inherit the administrator's global active stream cap, while every stored
// value is validated to the same range as the admin setting.
type TokenConcurrencySettings struct {
	PlanConcurrency map[string]int64 `json:"plan_concurrency"`
	UpdatedAt       *time.Time       `json:"updated_at,omitempty"`
}

func (s TokenConcurrencySettings) ActiveStreamCapForPlan(planType *string) (int64, bool) {
	plan := CanonicalTokenPlan("")
	if planType != nil {
		plan = CanonicalTokenPlan(*planType)
	}
	cap, ok := s.PlanConcurrency[plan]
	return cap, ok
}

func CanonicalTokenPlan(value string) string {
	if normalized := normalizePlanType(value); normalized != "" {
		return normalized
	}
	return "unknown"
}

type tokenConcurrencyPayload struct {
	PlanConcurrency map[string]int64 `json:"plan_concurrency"`
}

type ordinary429CooldownPayload struct {
	CooldownSeconds int64 `json:"cooldown_seconds"`
}

func (s *Store) GetOrdinary429CooldownSettings(ctx context.Context, fallback time.Duration) (Ordinary429CooldownSettings, error) {
	setting, err := s.GetSetting(ctx, Ordinary429CooldownSettingKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return buildOrdinary429CooldownSettings(nil, nil, fallback), nil
	}
	if err != nil {
		return Ordinary429CooldownSettings{}, err
	}
	updatedAt := setting.UpdatedAt.UTC()
	return buildOrdinary429CooldownSettings(setting.Value, &updatedAt, fallback), nil
}

func (s *Store) UpdateOrdinary429CooldownSettings(ctx context.Context, cooldownSeconds int64) (Ordinary429CooldownSettings, error) {
	resolved, err := ParseOrdinary429CooldownSeconds(cooldownSeconds)
	if err != nil {
		return Ordinary429CooldownSettings{}, err
	}
	payload, err := json.Marshal(ordinary429CooldownPayload{CooldownSeconds: resolved})
	if err != nil {
		return Ordinary429CooldownSettings{}, err
	}
	setting, err := s.UpsertSetting(ctx, Ordinary429CooldownSettingKey, payload)
	if err != nil {
		return Ordinary429CooldownSettings{}, err
	}
	updatedAt := setting.UpdatedAt.UTC()
	return Ordinary429CooldownSettings{CooldownSeconds: resolved, UpdatedAt: &updatedAt}, nil
}

func (s *Store) DeleteOrdinary429CooldownSettings(ctx context.Context) error {
	return s.deleteGlobalSetting(ctx, Ordinary429CooldownSettingKey)
}

func ParseOrdinary429CooldownSeconds(value int64) (int64, error) {
	if value < MinOrdinary429CooldownSeconds || value > MaxOrdinary429CooldownSeconds {
		return 0, fmt.Errorf(
			"unsupported ordinary 429 cooldown; expected an integer from %d to %d seconds",
			MinOrdinary429CooldownSeconds,
			MaxOrdinary429CooldownSeconds,
		)
	}
	return value, nil
}

func buildOrdinary429CooldownSettings(raw json.RawMessage, updatedAt *time.Time, fallback time.Duration) Ordinary429CooldownSettings {
	fallbackSeconds := normalizedOrdinary429CooldownFallback(fallback)
	settings := Ordinary429CooldownSettings{CooldownSeconds: fallbackSeconds}
	if len(bytes.TrimSpace(raw)) > 0 {
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err == nil && payload != nil {
			if parsed, ok := parseInt64Value(payload["cooldown_seconds"]); ok {
				if resolved, err := ParseOrdinary429CooldownSeconds(parsed); err == nil {
					settings.CooldownSeconds = resolved
				}
			}
		}
	}
	if updatedAt != nil {
		value := updatedAt.UTC()
		settings.UpdatedAt = &value
	}
	return settings
}

func normalizedOrdinary429CooldownFallback(fallback time.Duration) int64 {
	seconds := int64(fallback / time.Second)
	if resolved, err := ParseOrdinary429CooldownSeconds(seconds); err == nil {
		return resolved
	}
	return int64(DefaultOrdinary429Cooldown / time.Second)
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
		item.Scope = "global"
		if err := rows.Scan(&item.Key, &item.Value, &item.UpdatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) GetSetting(ctx context.Context, key string) (Setting, error) {
	item := Setting{Scope: "global"}
	err := s.pool.QueryRow(ctx, `
		select key, coalesce(value::jsonb, '{}'::jsonb), updated_at
		from gateway_settings
		where key = $1
	`, key).Scan(&item.Key, &item.Value, &item.UpdatedAt)
	return item, err
}

func (s *Store) UpsertSetting(ctx context.Context, key string, value json.RawMessage) (Setting, error) {
	row := s.pool.QueryRow(ctx, `
		insert into gateway_settings(key, value, updated_at)
		values ($1, $2, now())
		on conflict (key) do update set value = excluded.value, updated_at = now()
		returning key, coalesce(value::jsonb, '{}'::jsonb), updated_at
	`, key, value)
	var item Setting
	item.Scope = "global"
	err := row.Scan(&item.Key, &item.Value, &item.UpdatedAt)
	return item, err
}

func (s *Store) ListUserSettings(ctx context.Context, ownerUserID int64) ([]Setting, error) {
	rows, err := s.pool.Query(ctx, `
		select key, coalesce(value::jsonb, '{}'::jsonb), updated_at
		from user_settings
		where owner_user_id = $1
		order by key
	`, ownerUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Setting, 0)
	for rows.Next() {
		var item Setting
		item.OwnerUserID = &ownerUserID
		item.Scope = "user"
		if err := rows.Scan(&item.Key, &item.Value, &item.UpdatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) GetUserSetting(ctx context.Context, ownerUserID int64, key string) (Setting, error) {
	item := Setting{OwnerUserID: &ownerUserID, Scope: "user"}
	err := s.pool.QueryRow(ctx, `
		select key, coalesce(value::jsonb, '{}'::jsonb), updated_at
		from user_settings
		where owner_user_id = $1 and key = $2
	`, ownerUserID, key).Scan(&item.Key, &item.Value, &item.UpdatedAt)
	return item, err
}

func (s *Store) GetUserTokenConcurrency(ctx context.Context, ownerUserID int64) (TokenConcurrencySettings, error) {
	if ownerUserID <= 0 {
		return TokenConcurrencySettings{PlanConcurrency: map[string]int64{}}, nil
	}
	setting, err := s.GetUserSetting(ctx, ownerUserID, TokenConcurrencySettingKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return TokenConcurrencySettings{PlanConcurrency: map[string]int64{}}, nil
	}
	if err != nil {
		return TokenConcurrencySettings{}, err
	}
	settings := buildUserTokenConcurrencySettings(setting.Value)
	updatedAt := setting.UpdatedAt.UTC()
	settings.UpdatedAt = &updatedAt
	return settings, nil
}

func (s *Store) UserTokenConcurrencyByOwner(ctx context.Context, ownerUserIDs []int64) (map[int64]TokenConcurrencySettings, error) {
	unique := make([]int64, 0, len(ownerUserIDs))
	seen := make(map[int64]struct{}, len(ownerUserIDs))
	for _, ownerUserID := range ownerUserIDs {
		if ownerUserID <= 0 {
			continue
		}
		if _, exists := seen[ownerUserID]; exists {
			continue
		}
		seen[ownerUserID] = struct{}{}
		unique = append(unique, ownerUserID)
	}
	result := make(map[int64]TokenConcurrencySettings, len(unique))
	if len(unique) == 0 {
		return result, nil
	}
	rows, err := s.pool.Query(ctx, `
		select owner_user_id, coalesce(value::jsonb, '{}'::jsonb), updated_at
		from user_settings
		where owner_user_id = any($1::bigint[])
		  and key = $2
	`, unique, TokenConcurrencySettingKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ownerUserID int64
		var raw json.RawMessage
		var updatedAt time.Time
		if err := rows.Scan(&ownerUserID, &raw, &updatedAt); err != nil {
			return nil, err
		}
		settings := buildUserTokenConcurrencySettings(raw)
		value := updatedAt.UTC()
		settings.UpdatedAt = &value
		result[ownerUserID] = settings
	}
	return result, rows.Err()
}

func (s *Store) UpsertUserTokenConcurrency(ctx context.Context, ownerUserID int64, planConcurrency map[string]int64) (TokenConcurrencySettings, error) {
	if ownerUserID <= 0 {
		return TokenConcurrencySettings{}, errors.New("owner user id is required")
	}
	normalized, err := NormalizeTokenPlanConcurrency(planConcurrency)
	if err != nil {
		return TokenConcurrencySettings{}, err
	}
	payload, err := json.Marshal(map[string]any{"plan_concurrency": normalized})
	if err != nil {
		return TokenConcurrencySettings{}, err
	}
	setting, err := s.UpsertUserSetting(ctx, ownerUserID, TokenConcurrencySettingKey, payload)
	if err != nil {
		return TokenConcurrencySettings{}, err
	}
	settings := buildUserTokenConcurrencySettings(setting.Value)
	updatedAt := setting.UpdatedAt.UTC()
	settings.UpdatedAt = &updatedAt
	return settings, nil
}

func (s *Store) DeleteUserTokenConcurrency(ctx context.Context, ownerUserID int64) error {
	if ownerUserID <= 0 {
		return errors.New("owner user id is required")
	}
	return s.DeleteUserSetting(ctx, ownerUserID, TokenConcurrencySettingKey)
}

func buildUserTokenConcurrencySettings(raw json.RawMessage) TokenConcurrencySettings {
	settings := TokenConcurrencySettings{PlanConcurrency: map[string]int64{}}
	if len(bytes.TrimSpace(raw)) == 0 {
		return settings
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil || payload == nil {
		return settings
	}
	value, ok := payload["plan_concurrency"].(map[string]any)
	if !ok {
		return settings
	}
	for plan, rawCap := range value {
		parsed, ok := parseInt64Value(rawCap)
		if !ok {
			continue
		}
		canonical := normalizePlanType(plan)
		if canonical == "" {
			continue
		}
		if cap, err := ParseTokenActiveStreamCap(parsed); err == nil {
			settings.PlanConcurrency[canonical] = cap
		}
	}
	return settings
}

func NormalizeTokenPlanConcurrency(values map[string]int64) (map[string]int64, error) {
	normalized := make(map[string]int64, len(values))
	for plan, rawCap := range values {
		canonical := normalizePlanType(plan)
		if canonical == "" || canonical == "all" {
			return nil, fmt.Errorf("invalid token concurrency plan %q", plan)
		}
		if len(canonical) > 64 {
			return nil, fmt.Errorf("token concurrency plan %q is too long", plan)
		}
		cap, err := ParseTokenActiveStreamCap(rawCap)
		if err != nil {
			return nil, fmt.Errorf("token concurrency plan %q: %w", canonical, err)
		}
		if _, exists := normalized[canonical]; exists {
			return nil, fmt.Errorf("duplicate token concurrency plan %q", canonical)
		}
		normalized[canonical] = cap
	}
	return normalized, nil
}

func ParseTokenConcurrencyPayload(raw []byte) (map[string]int64, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var payload tokenConcurrencyPayload
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("invalid token concurrency setting: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("invalid token concurrency setting: multiple JSON values")
	}
	if payload.PlanConcurrency == nil {
		payload.PlanConcurrency = map[string]int64{}
	}
	return NormalizeTokenPlanConcurrency(payload.PlanConcurrency)
}

func (s *Store) UpsertUserSetting(ctx context.Context, ownerUserID int64, key string, value json.RawMessage) (Setting, error) {
	item := Setting{OwnerUserID: &ownerUserID, Scope: "user"}
	err := s.pool.QueryRow(ctx, `
		insert into user_settings(owner_user_id, key, value, created_at, updated_at)
		values ($1, $2, $3, now(), now())
		on conflict (owner_user_id, key) do update set value = excluded.value, updated_at = now()
		returning key, coalesce(value::jsonb, '{}'::jsonb), updated_at
	`, ownerUserID, key, value).Scan(&item.Key, &item.Value, &item.UpdatedAt)
	return item, err
}

func (s *Store) DeleteUserSetting(ctx context.Context, ownerUserID int64, key string) error {
	tag, err := s.pool.Exec(ctx, `delete from user_settings where owner_user_id = $1 and key = $2`, ownerUserID, key)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (s *Store) GetTokenSelectionSettings(ctx context.Context, fallbackCap int64) (TokenSelectionSettings, error) {
	var raw json.RawMessage
	var updatedAt time.Time
	err := s.pool.QueryRow(ctx, `
		select coalesce(value::jsonb, '{}'::jsonb), updated_at
		from gateway_settings
		where key = $1
	`, TokenSelectionSettingKey).Scan(&raw, &updatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return buildTokenSelectionSettings(nil, nil, fallbackCap), nil
		}
		return TokenSelectionSettings{}, err
	}
	return buildTokenSelectionSettings(raw, &updatedAt, fallbackCap), nil
}

func (s *Store) UpdateTokenActiveStreamCapSettings(ctx context.Context, activeStreamCap int64, fallbackCap int64) (TokenSelectionSettings, error) {
	resolvedCap, err := ParseTokenActiveStreamCap(activeStreamCap)
	if err != nil {
		return TokenSelectionSettings{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TokenSelectionSettings{}, err
	}
	defer tx.Rollback(ctx)

	var raw json.RawMessage
	err = tx.QueryRow(ctx, `
		select coalesce(value::jsonb, '{}'::jsonb)
		from gateway_settings
		where key = $1
		for update
	`, TokenSelectionSettingKey).Scan(&raw)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return TokenSelectionSettings{}, err
	}

	payload := normalizeTokenSelectionPayload(raw, fallbackCap)
	payload["active_stream_cap"] = resolvedCap

	var saved json.RawMessage
	var updatedAt time.Time
	err = tx.QueryRow(ctx, `
		insert into gateway_settings(key, value, updated_at)
		values ($1, $2, now())
		on conflict (key) do update set value = excluded.value, updated_at = now()
		returning coalesce(value::jsonb, '{}'::jsonb), updated_at
	`, TokenSelectionSettingKey, jsonBytes(payload)).Scan(&saved, &updatedAt)
	if err != nil {
		return TokenSelectionSettings{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return TokenSelectionSettings{}, err
	}
	return buildTokenSelectionSettings(saved, &updatedAt, fallbackCap), nil
}

func ParseTokenActiveStreamCap(value int64) (int64, error) {
	if value < MinTokenActiveStreamCap || value > MaxTokenActiveStreamCap {
		return 0, fmt.Errorf("unsupported token concurrency cap; expected an integer from %d to %d", MinTokenActiveStreamCap, MaxTokenActiveStreamCap)
	}
	return value, nil
}

func NormalizeTokenActiveStreamCap(value int64) int64 {
	if value < MinTokenActiveStreamCap || value > MaxTokenActiveStreamCap {
		return defaultTokenActiveStreamCap
	}
	return value
}

func buildTokenSelectionSettings(raw json.RawMessage, updatedAt *time.Time, fallbackCap int64) TokenSelectionSettings {
	payload := tokenSelectionPayload(raw)
	capFallback := NormalizeTokenActiveStreamCap(fallbackCap)
	settings := TokenSelectionSettings{
		Strategy:         normalizeTokenSelectionStrategy(payload["strategy"]),
		TokenOrder:       normalizeTokenOrder(payload["token_order"]),
		PlanOrderEnabled: normalizeTokenPlanOrderEnabled(payload["plan_order_enabled"]),
		PlanOrder:        normalizeTokenPlanOrder(payload["plan_order"]),
		ActiveStreamCap:  normalizeTokenActiveStreamCapValue(payload["active_stream_cap"], capFallback),
	}
	if updatedAt != nil {
		value := updatedAt.UTC()
		settings.UpdatedAt = &value
	}
	return settings
}

func normalizeTokenSelectionPayload(raw json.RawMessage, fallbackCap int64) map[string]any {
	payload := tokenSelectionPayload(raw)
	settings := buildTokenSelectionSettings(raw, nil, fallbackCap)
	payload["strategy"] = settings.Strategy
	payload["token_order"] = settings.TokenOrder
	payload["plan_order_enabled"] = settings.PlanOrderEnabled
	payload["plan_order"] = settings.PlanOrder
	payload["active_stream_cap"] = settings.ActiveStreamCap
	return payload
}

func tokenSelectionPayload(raw json.RawMessage) map[string]any {
	if len(bytes.TrimSpace(raw)) == 0 {
		return map[string]any{}
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil || payload == nil {
		return map[string]any{}
	}
	return payload
}

func normalizeTokenSelectionStrategy(value any) string {
	raw := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(fmt.Sprint(value)), "-", "_"))
	switch raw {
	case "fill_first", "fillfirst":
		return TokenSelectionStrategyFillFirst
	case "oldest_unused", "least_recently_used", "lru":
		return DefaultTokenSelectionStrategy
	default:
		return DefaultTokenSelectionStrategy
	}
}

func normalizeTokenActiveStreamCapValue(value any, fallback int64) int64 {
	if parsed, ok := parseInt64Value(value); ok {
		if cap, err := ParseTokenActiveStreamCap(parsed); err == nil {
			return cap
		}
	}
	return NormalizeTokenActiveStreamCap(fallback)
}

func normalizeTokenOrder(value any) []int64 {
	items, ok := value.([]any)
	if !ok {
		return []int64{}
	}
	out := make([]int64, 0, len(items))
	seen := map[int64]struct{}{}
	for _, item := range items {
		parsed, ok := parseInt64Value(item)
		if !ok || parsed <= 0 {
			continue
		}
		if _, exists := seen[parsed]; exists {
			continue
		}
		seen[parsed] = struct{}{}
		out = append(out, parsed)
	}
	return out
}

func normalizeTokenPlanOrderEnabled(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		normalized := strings.ToLower(strings.TrimSpace(typed))
		switch normalized {
		case "1", "true", "yes", "on", "enabled", "custom":
			return true
		case "0", "false", "no", "off", "disabled", "default":
			return false
		}
	}
	return value != nil && fmt.Sprint(value) != "" && fmt.Sprint(value) != "0"
}

func normalizeTokenPlanOrder(value any) []string {
	items, ok := value.([]any)
	if !ok {
		items = nil
	}
	allowed := map[string]struct{}{}
	for _, plan := range defaultTokenPlanOrder {
		allowed[plan] = struct{}{}
	}
	out := make([]string, 0, len(defaultTokenPlanOrder))
	seen := map[string]struct{}{}
	for _, item := range items {
		plan := normalizePlanType(item)
		if _, ok := allowed[plan]; !ok {
			continue
		}
		if _, exists := seen[plan]; exists {
			continue
		}
		seen[plan] = struct{}{}
		out = append(out, plan)
	}
	for _, plan := range defaultTokenPlanOrder {
		if _, exists := seen[plan]; exists {
			continue
		}
		out = append(out, plan)
	}
	return out
}

func normalizePlanType(value any) string {
	raw := strings.ToLower(strings.TrimSpace(fmt.Sprint(value)))
	raw = strings.TrimPrefix(raw, "chatgpt_")
	return raw
}

func parseInt64Value(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int8:
		return int64(typed), true
	case int16:
		return int64(typed), true
	case int32:
		return int64(typed), true
	case int64:
		return typed, true
	case uint:
		return int64(typed), true
	case uint8:
		return int64(typed), true
	case uint16:
		return int64(typed), true
	case uint32:
		return int64(typed), true
	case uint64:
		if typed > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(typed), true
	case float64:
		parsed := int64(typed)
		if float64(parsed) != typed {
			return 0, false
		}
		return parsed, true
	case json.Number:
		parsed, err := typed.Int64()
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func (s *Store) WriteAuditLog(ctx context.Context, action, actor, targetType, targetID string, payload any) error {
	_, err := s.pool.Exec(ctx, `
		insert into admin_audit_logs(action, actor, target_type, target_id, payload)
		values ($1, $2, $3, $4, $5)
	`, action, nullableString(actor), nullableString(targetType), nullableString(targetID), jsonBytes(payload))
	return err
}
