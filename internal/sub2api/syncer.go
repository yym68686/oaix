package sub2api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

type Syncer struct {
	Store         *store.Store
	Client        *Client
	Logger        *slog.Logger
	OAuthClientID string
}

type SyncMode string

const (
	SyncModeCheck SyncMode = "check"
	SyncModeSync  SyncMode = "sync"
)

func NewSyncer(st *store.Store, client *Client, logger *slog.Logger, oauthClientID string) *Syncer {
	if client == nil {
		client = NewClient(nil)
	}
	return &Syncer{Store: st, Client: client, Logger: logger, OAuthClientID: oauthClientID}
}

func (s *Syncer) ListGroups(ctx context.Context, target store.Sub2APISyncTarget) ([]Group, error) {
	return s.Client.ListGroups(ctx, target.BaseURL, target.AdminKey, "openai")
}

func (s *Syncer) ProbeGroups(ctx context.Context, baseURL string, adminKey string) ([]Group, error) {
	return s.Client.ListGroups(ctx, baseURL, adminKey, "openai")
}

func (s *Syncer) RunDueTargets(ctx context.Context) ([]store.Sub2APISyncRun, error) {
	targets, err := s.Store.ClaimDueSub2APISyncTargets(ctx, 10)
	if err != nil || len(targets) == 0 {
		return nil, err
	}
	runs := make([]store.Sub2APISyncRun, 0, len(targets))
	for _, target := range targets {
		runCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
		run, runErr := s.RunTarget(runCtx, target, "schedule", SyncModeSync)
		cancel()
		if runErr != nil && s.Logger != nil {
			s.Logger.Warn("sub2api sync target failed", "target_id", target.ID, "error", runErr)
		}
		runs = append(runs, run)
	}
	return runs, nil
}

func (s *Syncer) CheckTarget(ctx context.Context, targetID int64, trigger string) (store.Sub2APISyncRun, error) {
	target, err := s.Store.GetSub2APISyncTarget(ctx, targetID)
	if err != nil {
		return store.Sub2APISyncRun{}, err
	}
	return s.RunTarget(ctx, target, trigger, SyncModeCheck)
}

func (s *Syncer) SyncTarget(ctx context.Context, targetID int64, trigger string) (store.Sub2APISyncRun, error) {
	target, err := s.Store.GetSub2APISyncTarget(ctx, targetID)
	if err != nil {
		return store.Sub2APISyncRun{}, err
	}
	return s.RunTarget(ctx, target, trigger, SyncModeSync)
}

func (s *Syncer) RunTarget(ctx context.Context, target store.Sub2APISyncTarget, trigger string, mode SyncMode) (store.Sub2APISyncRun, error) {
	run, err := s.Store.StartSub2APISyncRun(ctx, target.ID, trigger)
	if err != nil {
		return run, err
	}
	result := store.Sub2APISyncRunResult{
		Status:    store.Sub2APISyncStatusCompleted,
		Threshold: target.MinAvailable,
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			result.Status = store.Sub2APISyncStatusFailed
			result.ErrorMessage = fmt.Sprintf("panic: %v", recovered)
			if finished, finishErr := s.Store.FinishSub2APISyncRun(context.Background(), run.ID, result); finishErr == nil {
				run = finished
			}
		}
	}()

	if len(target.TargetGroupIDs) == 0 {
		result.Status = store.Sub2APISyncStatusFailed
		result.ErrorMessage = "target_group_ids is empty"
		return s.finish(ctx, run, result)
	}
	remoteCount, err := s.Client.CountActiveOAuthAccounts(ctx, target.BaseURL, target.AdminKey, target.TargetGroupIDs)
	if err != nil {
		result.Status = store.Sub2APISyncStatusFailed
		result.ErrorMessage = err.Error()
		return s.finish(ctx, run, result)
	}
	result.RemoteCount = remoteCount
	if target.MinAvailable > remoteCount {
		result.NeededCount = target.MinAvailable - remoteCount
	}
	if mode == SyncModeCheck {
		result.Status = store.Sub2APISyncStatusCompleted
		result.Details = map[string]any{"mode": "check", "group_ids": target.TargetGroupIDs}
		return s.finish(ctx, run, result)
	}

	limit := target.TopUpBatchSize
	if limit <= 0 {
		limit = 10
	}
	if !target.AutoSyncNew {
		if result.NeededCount <= 0 {
			result.Status = store.Sub2APISyncStatusSkipped
			result.Details = map[string]any{"reason": "remote_count_at_or_above_threshold", "group_ids": target.TargetGroupIDs}
			return s.finish(ctx, run, result)
		}
		if result.NeededCount < limit {
			limit = result.NeededCount
		}
	}

	candidates, err := s.Store.ListSub2APITokenCandidates(ctx, target, store.Sub2APITokenCandidateOptions{Limit: limit})
	if err != nil {
		result.Status = store.Sub2APISyncStatusFailed
		result.ErrorMessage = err.Error()
		return s.finish(ctx, run, result)
	}
	result.SelectedCount = len(candidates)
	if len(candidates) == 0 {
		result.Status = store.Sub2APISyncStatusSkipped
		result.Details = map[string]any{"reason": "no_unsynced_oaix_candidates", "group_ids": target.TargetGroupIDs}
		return s.finish(ctx, run, result)
	}

	requests := make([]CreateAccountRequest, 0, len(candidates))
	for _, candidate := range candidates {
		requests = append(requests, s.createAccountRequest(target, candidate))
	}
	batch, err := s.Client.CreateAccounts(ctx, target.BaseURL, target.AdminKey, requests)
	if err != nil {
		result.Status = store.Sub2APISyncStatusFailed
		result.FailedCount = len(candidates)
		result.ErrorMessage = err.Error()
		result.Details = map[string]any{"selected_token_ids": tokenCandidateIDs(candidates)}
		return s.finish(ctx, run, result)
	}
	for index, candidate := range candidates {
		var item BatchAccountResult
		if index < len(batch.Results) {
			item = batch.Results[index]
		}
		if item.Success {
			result.SyncedCount++
			_ = s.Store.RecordSub2APISyncMapping(ctx, target.ID, candidate.ID, item.ID, run.ID, "synced", "")
			continue
		}
		result.FailedCount++
		message := strings.TrimSpace(item.Error)
		if message == "" {
			message = "sub2api batch result did not mark this account as success"
		}
		_ = s.Store.RecordSub2APISyncMapping(ctx, target.ID, candidate.ID, item.ID, run.ID, "failed", message)
	}
	if result.FailedCount > 0 && result.SyncedCount == 0 {
		result.Status = store.Sub2APISyncStatusFailed
		result.ErrorMessage = "all selected accounts failed to sync"
	} else {
		result.Status = store.Sub2APISyncStatusCompleted
	}
	result.Details = map[string]any{
		"group_ids":           target.TargetGroupIDs,
		"selected_token_ids":  tokenCandidateIDs(candidates),
		"sub2api_batch_total": map[string]any{"success": batch.Success, "failed": batch.Failed},
	}
	return s.finish(ctx, run, result)
}

func (s *Syncer) finish(ctx context.Context, run store.Sub2APISyncRun, result store.Sub2APISyncRunResult) (store.Sub2APISyncRun, error) {
	finished, err := s.Store.FinishSub2APISyncRun(ctx, run.ID, result)
	if err != nil {
		return run, err
	}
	return finished, nil
}

func (s *Syncer) createAccountRequest(target store.Sub2APISyncTarget, candidate store.Sub2APITokenCandidate) CreateAccountRequest {
	credentials := map[string]any{
		"access_token":  candidate.AccessToken,
		"refresh_token": candidate.RefreshToken,
		"client_id":     s.OAuthClientID,
	}
	if candidate.IDToken != "" {
		credentials["id_token"] = candidate.IDToken
	}
	if candidate.Email != "" {
		credentials["email"] = candidate.Email
	}
	if candidate.AccountID != "" {
		credentials["chatgpt_account_id"] = candidate.AccountID
	}
	if candidate.PlanType != "" {
		credentials["plan_type"] = candidate.PlanType
	}
	for _, key := range []string{"chatgpt_user_id", "organization_id", "org_id"} {
		if value := stringFromRawPayload(candidate.RawPayload, key); value != "" {
			if key == "org_id" {
				credentials["organization_id"] = value
			} else {
				credentials[key] = value
			}
		}
	}

	extra := map[string]any{
		"oaix_token_id":      candidate.ID,
		"oaix_owner_user_id": candidate.OwnerUserID,
		"oaix_source":        "oaix_sub2api_sync",
		"oaix_synced_at":     time.Now().UTC().Format(time.RFC3339),
	}
	if candidate.AccountID != "" {
		extra["oaix_account_id"] = candidate.AccountID
	}
	if candidate.PlanType != "" {
		extra["oaix_plan_type"] = candidate.PlanType
	}
	if target.ID > 0 {
		extra["oaix_sub2api_target_id"] = target.ID
	}

	name := candidate.Email
	if name == "" {
		name = candidate.AccountID
	}
	if name == "" {
		name = fmt.Sprintf("oaix token %d", candidate.ID)
	}
	name = fmt.Sprintf("%s - oaix #%d", name, candidate.ID)
	notes := fmt.Sprintf("Synced from oaix target %d, owner %d, token %d", target.ID, candidate.OwnerUserID, candidate.ID)
	autoPause := true
	confirmMixedChannelRisk := true
	return CreateAccountRequest{
		Name:                    name,
		Notes:                   &notes,
		Platform:                "openai",
		Type:                    "oauth",
		Credentials:             credentials,
		Extra:                   extra,
		Concurrency:             1,
		Priority:                50,
		GroupIDs:                target.TargetGroupIDs,
		AutoPauseOnExpired:      &autoPause,
		ConfirmMixedChannelRisk: &confirmMixedChannelRisk,
	}
}

func tokenCandidateIDs(items []store.Sub2APITokenCandidate) []int64 {
	out := make([]int64, 0, len(items))
	for _, item := range items {
		out = append(out, item.ID)
	}
	return out
}

func stringFromRawPayload(raw json.RawMessage, key string) string {
	if len(raw) == 0 {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ""
	}
	value := strings.TrimSpace(fmt.Sprint(payload[key]))
	if value == "" || value == "<nil>" {
		return ""
	}
	return value
}
