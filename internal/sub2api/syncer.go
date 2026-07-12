package sub2api

import (
	"context"
	"encoding/json"
	"errors"
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

type mappedAccountReconcileResult struct {
	CheckedTokenIDs        []int64 `json:"checked_token_ids,omitempty"`
	StaleTokenIDs          []int64 `json:"stale_token_ids,omitempty"`
	StaleRemoteAccountIDs  []int64 `json:"stale_remote_account_ids,omitempty"`
	RestoredTokenIDs       []int64 `json:"restored_token_ids,omitempty"`
	RestoredRemoteAccounts []int64 `json:"restored_remote_account_ids,omitempty"`
}

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

func (s *Syncer) ListProxies(ctx context.Context, target store.Sub2APISyncTarget) ([]Proxy, error) {
	return s.Client.ListProxies(ctx, target.BaseURL, target.AdminKey)
}

func (s *Syncer) ProbeProxies(ctx context.Context, baseURL string, adminKey string) ([]Proxy, error) {
	return s.Client.ListProxies(ctx, baseURL, adminKey)
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

func (s *Syncer) RestoreTokenAvailability(ctx context.Context, token store.Token) (int, error) {
	if s == nil || s.Store == nil || s.Client == nil || !tokenAvailableForSub2API(token, time.Now().UTC()) {
		return 0, nil
	}
	mappings, err := s.Store.ListSub2APIAvailabilityMappingsForToken(ctx, token.ID)
	if err != nil {
		return 0, err
	}
	if len(mappings) == 0 {
		return 0, nil
	}
	candidate, err := s.Store.GetSub2APITokenCandidate(ctx, token.ID)
	if err != nil {
		return 0, err
	}
	restored := 0
	errs := make([]error, 0)
	for _, mapping := range mappings {
		if mapping.RemoteAccountID <= 0 {
			continue
		}
		if err := s.syncMappedAccount(ctx, mapping.BaseURL, mapping.AdminKey, mapping.TargetID, mapping.RemoteAccountID, candidate); err != nil {
			errs = append(errs, fmt.Errorf("target %d remote account %d: %w", mapping.TargetID, mapping.RemoteAccountID, err))
			if s.Logger != nil {
				s.Logger.Warn("sub2api account availability restore failed", "target_id", mapping.TargetID, "remote_account_id", mapping.RemoteAccountID, "oaix_token_id", token.ID, "error", err)
			}
			continue
		}
		restored++
	}
	return restored, errors.Join(errs...)
}

func tokenAvailableForSub2API(token store.Token, now time.Time) bool {
	if !token.IsActive || token.DisabledAt != nil {
		return false
	}
	if token.CooldownUntil != nil && token.CooldownUntil.After(now) {
		return false
	}
	return token.ID > 0
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
	reconcile := mappedAccountReconcileResult{}
	if shouldReconcileMappedAccounts(trigger, result.NeededCount) {
		reconciled, reconcileErr := s.reconcileMappedAccounts(ctx, target, run.ID)
		if reconcileErr != nil {
			result.Status = store.Sub2APISyncStatusFailed
			result.ErrorMessage = reconcileErr.Error()
			return s.finish(ctx, run, result)
		}
		reconcile = reconciled
		if len(reconcile.RestoredRemoteAccounts) > 0 {
			remoteCount, err = s.Client.CountActiveOAuthAccounts(ctx, target.BaseURL, target.AdminKey, target.TargetGroupIDs)
			if err != nil {
				result.Status = store.Sub2APISyncStatusFailed
				result.ErrorMessage = err.Error()
				return s.finish(ctx, run, result)
			}
			result.RemoteCount = remoteCount
			result.NeededCount = 0
			if target.MinAvailable > remoteCount {
				result.NeededCount = target.MinAvailable - remoteCount
			}
		}
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
		result.Details = syncDetails(map[string]any{"reason": "no_unsynced_oaix_candidates", "group_ids": target.TargetGroupIDs}, reconcile)
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
		result.Details = syncDetails(map[string]any{"selected_token_ids": tokenCandidateIDs(candidates)}, reconcile)
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
	result.Details = syncDetails(result.Details.(map[string]any), reconcile)
	return s.finish(ctx, run, result)
}

func (s *Syncer) reconcileMappedAccounts(ctx context.Context, target store.Sub2APISyncTarget, runID int64) (mappedAccountReconcileResult, error) {
	result := mappedAccountReconcileResult{}
	if s == nil || s.Store == nil || s.Client == nil {
		return result, nil
	}
	mappings, err := s.Store.ListSub2APISyncedTokenMappings(ctx, target, 500)
	if err != nil {
		return result, err
	}
	now := time.Now().UTC()
	for _, mapping := range mappings {
		if mapping.RemoteAccountID <= 0 || mapping.TokenID <= 0 {
			continue
		}
		result.CheckedTokenIDs = append(result.CheckedTokenIDs, mapping.TokenID)
		account, err := s.Client.GetAccount(ctx, target.BaseURL, target.AdminKey, mapping.RemoteAccountID)
		if errors.Is(err, ErrAccountNotFound) {
			message := fmt.Sprintf("remote sub2api account %d not found; mapping marked stale for recreation", mapping.RemoteAccountID)
			if markErr := s.Store.MarkSub2APISyncMappingStale(ctx, target.ID, mapping.TokenID, runID, message); markErr != nil {
				return result, markErr
			}
			result.StaleTokenIDs = append(result.StaleTokenIDs, mapping.TokenID)
			result.StaleRemoteAccountIDs = append(result.StaleRemoteAccountIDs, mapping.RemoteAccountID)
			continue
		}
		if err != nil {
			return result, err
		}
		if !tokenAvailableForSub2API(mapping.Token(), now) || remoteAccountAvailable(account) {
			continue
		}
		candidate, err := s.Store.GetSub2APITokenCandidate(ctx, mapping.TokenID)
		if err != nil {
			return result, err
		}
		if err := s.syncMappedAccount(ctx, target.BaseURL, target.AdminKey, target.ID, mapping.RemoteAccountID, candidate); err != nil {
			return result, err
		}
		result.RestoredTokenIDs = append(result.RestoredTokenIDs, mapping.TokenID)
		result.RestoredRemoteAccounts = append(result.RestoredRemoteAccounts, mapping.RemoteAccountID)
	}
	return result, nil
}

func shouldReconcileMappedAccounts(trigger string, neededCount int) bool {
	return neededCount > 0 || strings.EqualFold(strings.TrimSpace(trigger), "manual")
}

func (s *Syncer) syncMappedAccount(ctx context.Context, baseURL string, adminKey string, targetID int64, remoteAccountID int64, candidate store.Sub2APITokenCandidate) error {
	if err := s.Client.ApplyOAuthCredentials(ctx, baseURL, adminKey, remoteAccountID, s.oauthCredentials(candidate), sub2APISyncMetadata(targetID, candidate)); err != nil {
		return fmt.Errorf("update oauth credentials: %w", err)
	}
	if err := s.Client.RestoreAccountAvailability(ctx, baseURL, adminKey, remoteAccountID); err != nil {
		return fmt.Errorf("restore availability: %w", err)
	}
	return nil
}

func remoteAccountAvailable(account Account) bool {
	if strings.ToLower(strings.TrimSpace(account.Status)) != "active" {
		return false
	}
	if account.Schedulable != nil && !*account.Schedulable {
		return false
	}
	return account.ID > 0
}

func syncDetails(base map[string]any, reconcile mappedAccountReconcileResult) map[string]any {
	if base == nil {
		base = map[string]any{}
	}
	if len(reconcile.CheckedTokenIDs) > 0 {
		base["mapped_check_count"] = len(reconcile.CheckedTokenIDs)
	}
	if len(reconcile.StaleTokenIDs) > 0 {
		base["stale_mapping_token_ids"] = reconcile.StaleTokenIDs
		base["stale_mapping_remote_account_ids"] = reconcile.StaleRemoteAccountIDs
	}
	if len(reconcile.RestoredTokenIDs) > 0 {
		base["restored_token_ids"] = reconcile.RestoredTokenIDs
		base["restored_remote_account_ids"] = reconcile.RestoredRemoteAccounts
	}
	return base
}

func (s *Syncer) finish(ctx context.Context, run store.Sub2APISyncRun, result store.Sub2APISyncRunResult) (store.Sub2APISyncRun, error) {
	finished, err := s.Store.FinishSub2APISyncRun(ctx, run.ID, result)
	if err != nil {
		return run, err
	}
	return finished, nil
}

func (s *Syncer) createAccountRequest(target store.Sub2APISyncTarget, candidate store.Sub2APITokenCandidate) CreateAccountRequest {
	credentials := s.oauthCredentials(candidate)
	extra := sub2APISyncMetadata(target.ID, candidate)

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
	accountConcurrency := target.AccountConcurrency
	if accountConcurrency <= 0 {
		accountConcurrency = store.Sub2APIDefaultAccountConcurrency
	} else if accountConcurrency > store.Sub2APIMaxAccountConcurrency {
		accountConcurrency = store.Sub2APIMaxAccountConcurrency
	}
	accountPriority := target.AccountPriority
	if accountPriority < 0 {
		accountPriority = store.Sub2APIDefaultAccountPriority
	}
	var proxyID *int64
	if target.ProxyID > 0 {
		id := target.ProxyID
		proxyID = &id
	}
	return CreateAccountRequest{
		Name:                    name,
		Notes:                   &notes,
		Platform:                "openai",
		Type:                    "oauth",
		Credentials:             credentials,
		Extra:                   extra,
		ProxyID:                 proxyID,
		Concurrency:             accountConcurrency,
		Priority:                accountPriority,
		GroupIDs:                target.TargetGroupIDs,
		AutoPauseOnExpired:      &autoPause,
		ConfirmMixedChannelRisk: &confirmMixedChannelRisk,
	}
}

func (s *Syncer) oauthCredentials(candidate store.Sub2APITokenCandidate) map[string]any {
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
	return credentials
}

func sub2APISyncMetadata(targetID int64, candidate store.Sub2APITokenCandidate) map[string]any {
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
	if targetID > 0 {
		extra["oaix_sub2api_target_id"] = targetID
	}
	return extra
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
