package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yym68686/oaix/internal/modelaccess"
)

const TokenModelAccessSettingKey = "token_model_access"

// TokenModelAccessSettings stores explicit per-plan allow-list overrides. A
// present plan with an empty list intentionally denies every model for that
// plan; an absent plan inherits the administrator/default policy.
type TokenModelAccessSettings struct {
	PlanModels map[string][]string `json:"plan_models"`
	UpdatedAt  *time.Time          `json:"updated_at,omitempty"`
}

type tokenModelAccessPayload struct {
	PlanModels map[string][]string `json:"plan_models"`
}

func (s *Store) GetTokenModelAccess(ctx context.Context) (TokenModelAccessSettings, error) {
	setting, err := s.GetSetting(ctx, TokenModelAccessSettingKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return TokenModelAccessSettings{PlanModels: map[string][]string{}}, nil
	}
	if err != nil {
		return TokenModelAccessSettings{}, err
	}
	settings := BuildTokenModelAccessSettings(setting.Value)
	updatedAt := setting.UpdatedAt.UTC()
	settings.UpdatedAt = &updatedAt
	return settings, nil
}

func (s *Store) GetUserTokenModelAccess(ctx context.Context, ownerUserID int64) (TokenModelAccessSettings, error) {
	if ownerUserID <= 0 {
		return TokenModelAccessSettings{PlanModels: map[string][]string{}}, nil
	}
	setting, err := s.GetUserSetting(ctx, ownerUserID, TokenModelAccessSettingKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return TokenModelAccessSettings{PlanModels: map[string][]string{}}, nil
	}
	if err != nil {
		return TokenModelAccessSettings{}, err
	}
	settings := BuildTokenModelAccessSettings(setting.Value)
	updatedAt := setting.UpdatedAt.UTC()
	settings.UpdatedAt = &updatedAt
	return settings, nil
}

func (s *Store) UpsertTokenModelAccess(ctx context.Context, planModels map[string][]string) (TokenModelAccessSettings, error) {
	normalized, err := NormalizeTokenModelAccess(planModels)
	if err != nil {
		return TokenModelAccessSettings{}, err
	}
	payload, err := json.Marshal(tokenModelAccessPayload{PlanModels: normalized})
	if err != nil {
		return TokenModelAccessSettings{}, err
	}
	setting, err := s.UpsertSetting(ctx, TokenModelAccessSettingKey, payload)
	if err != nil {
		return TokenModelAccessSettings{}, err
	}
	settings := BuildTokenModelAccessSettings(setting.Value)
	updatedAt := setting.UpdatedAt.UTC()
	settings.UpdatedAt = &updatedAt
	return settings, nil
}

func (s *Store) UpsertUserTokenModelAccess(ctx context.Context, ownerUserID int64, planModels map[string][]string) (TokenModelAccessSettings, error) {
	if ownerUserID <= 0 {
		return TokenModelAccessSettings{}, errors.New("owner user id is required")
	}
	normalized, err := NormalizeTokenModelAccess(planModels)
	if err != nil {
		return TokenModelAccessSettings{}, err
	}
	payload, err := json.Marshal(tokenModelAccessPayload{PlanModels: normalized})
	if err != nil {
		return TokenModelAccessSettings{}, err
	}
	setting, err := s.UpsertUserSetting(ctx, ownerUserID, TokenModelAccessSettingKey, payload)
	if err != nil {
		return TokenModelAccessSettings{}, err
	}
	settings := BuildTokenModelAccessSettings(setting.Value)
	updatedAt := setting.UpdatedAt.UTC()
	settings.UpdatedAt = &updatedAt
	return settings, nil
}

func (s *Store) DeleteTokenModelAccess(ctx context.Context) error {
	return s.deleteGlobalSetting(ctx, TokenModelAccessSettingKey)
}

func (s *Store) DeleteUserTokenModelAccess(ctx context.Context, ownerUserID int64) error {
	if ownerUserID <= 0 {
		return errors.New("owner user id is required")
	}
	return s.DeleteUserSetting(ctx, ownerUserID, TokenModelAccessSettingKey)
}

func (s *Store) deleteGlobalSetting(ctx context.Context, key string) error {
	tag, err := s.pool.Exec(ctx, `delete from gateway_settings where key = $1`, key)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func BuildTokenModelAccessSettings(raw json.RawMessage) TokenModelAccessSettings {
	settings := TokenModelAccessSettings{PlanModels: map[string][]string{}}
	if len(bytes.TrimSpace(raw)) == 0 {
		return settings
	}
	var payload tokenModelAccessPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return settings
	}
	for plan, models := range payload.PlanModels {
		canonical := normalizePlanType(plan)
		if canonical == "" || canonical == "all" {
			continue
		}
		normalized, err := modelaccess.NormalizeModels(models)
		if err != nil {
			continue
		}
		settings.PlanModels[canonical] = normalized
	}
	return settings
}

func NormalizeTokenModelAccess(values map[string][]string) (map[string][]string, error) {
	normalized := make(map[string][]string, len(values))
	for plan, models := range values {
		canonical := normalizePlanType(plan)
		if canonical == "" || canonical == "all" {
			return nil, fmt.Errorf("invalid token model access plan %q", plan)
		}
		if len(canonical) > 64 {
			return nil, fmt.Errorf("token model access plan %q is too long", plan)
		}
		if _, exists := normalized[canonical]; exists {
			return nil, fmt.Errorf("duplicate token model access plan %q", canonical)
		}
		models, err := modelaccess.NormalizeModels(models)
		if err != nil {
			return nil, fmt.Errorf("token model access plan %q: %w", canonical, err)
		}
		normalized[canonical] = models
	}
	return normalized, nil
}

func ParseTokenModelAccessPayload(raw []byte) (map[string][]string, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var payload tokenModelAccessPayload
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("invalid token model access setting: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("invalid token model access setting: multiple JSON values")
	}
	if payload.PlanModels == nil {
		payload.PlanModels = map[string][]string{}
	}
	return NormalizeTokenModelAccess(payload.PlanModels)
}
