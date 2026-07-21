package sub2api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/yym68686/oaix/internal/agentidentity"
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
	SyncModeCheck                 SyncMode = "check"
	SyncModeSync                  SyncMode = "sync"
	sub2APIUsageTodayBatchLimit            = 500
	sub2APIUsageBaselineLimit              = 32
	sub2APIUsageFinalizationLimit          = 8
	sub2APIUsageQueryConcurrency           = 4
)

type mappedAccountReconcileResult struct {
	CheckedTokenIDs        []int64 `json:"checked_token_ids,omitempty"`
	StaleTokenIDs          []int64 `json:"stale_token_ids,omitempty"`
	StaleRemoteAccountIDs  []int64 `json:"stale_remote_account_ids,omitempty"`
	RestoredTokenIDs       []int64 `json:"restored_token_ids,omitempty"`
	RestoredRemoteAccounts []int64 `json:"restored_remote_account_ids,omitempty"`
}

type agentIdentityCompatibility struct {
	CandidateCount int
	TargetVersion  string
	Supported      bool
	CheckFailed    bool
}

func (c agentIdentityCompatibility) blocked() bool {
	return c.CandidateCount > 0 && !c.Supported
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

// SyncDueUsage refreshes remote usage snapshots outside request and admin-page paths.
func (s *Syncer) SyncDueUsage(ctx context.Context) (int, error) {
	if s == nil || s.Store == nil || s.Client == nil {
		return 0, nil
	}
	targets, err := s.Store.ListSub2APISyncTargets(ctx)
	if err != nil {
		return 0, err
	}
	total := 0
	var syncErrors []error
	type serverClock struct {
		timezoneName string
		location     *time.Location
	}
	clocks := make(map[string]serverClock)
	for _, target := range targets {
		if !target.Enabled {
			continue
		}
		clock, ok := clocks[target.BaseURL]
		if !ok {
			timezoneName, location, clockErr := s.Client.GetServerTimezone(ctx, target.BaseURL)
			if clockErr != nil {
				syncErrors = append(syncErrors, fmt.Errorf("target %d load server timezone: %w", target.ID, clockErr))
				continue
			}
			clock = serverClock{timezoneName: timezoneName, location: location}
			clocks[target.BaseURL] = clock
		}
		today := startOfUsageDay(time.Now(), clock.location)
		yesterday := today.AddDate(0, 0, -1)

		todaySynced, todayErr := s.syncTargetTodayUsage(ctx, target, today)
		total += todaySynced
		if todayErr != nil {
			syncErrors = append(syncErrors, todayErr)
		}

		baselineSynced, baselineErr := s.syncTargetUsageBaselines(ctx, target, yesterday, clock.timezoneName, clock.location)
		total += baselineSynced
		if baselineErr != nil {
			syncErrors = append(syncErrors, baselineErr)
		}

		finalized, finalizeErr := s.finalizeTargetDailyUsage(ctx, target, today, clock.timezoneName)
		total += finalized
		if finalizeErr != nil {
			syncErrors = append(syncErrors, finalizeErr)
		}
	}
	return total, errors.Join(syncErrors...)
}

func startOfUsageDay(now time.Time, location *time.Location) time.Time {
	local := now.In(location)
	return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location)
}

func (s *Syncer) syncTargetTodayUsage(ctx context.Context, target store.Sub2APISyncTarget, usageDate time.Time) (int, error) {
	mappings, err := s.Store.ListDueSub2APITodayUsageSyncMappings(ctx, target, usageDate, sub2APIUsageTodayBatchLimit)
	if err != nil || len(mappings) == 0 {
		return 0, err
	}
	if err := store.ValidateDistinctSub2APIRemoteAccounts(mappings); err != nil {
		_ = s.Store.MarkSub2APIDailyUsageSyncError(ctx, target.ID, usageDate, mappings, err.Error())
		return 0, fmt.Errorf("target %d validate today usage mappings: %w", target.ID, err)
	}
	accountIDs := make([]int64, 0, len(mappings))
	for _, mapping := range mappings {
		accountIDs = append(accountIDs, mapping.RemoteAccountID)
	}
	batch, err := s.Client.GetAccountTodayUsageBatch(ctx, target.BaseURL, target.AdminKey, accountIDs)
	if err != nil {
		_ = s.Store.MarkSub2APIDailyUsageSyncError(ctx, target.ID, usageDate, mappings, err.Error())
		return 0, fmt.Errorf("target %d batch today usage: %w", target.ID, err)
	}
	inputs := make([]store.Sub2APIDailyUsageSnapshotInput, 0, len(mappings))
	for _, mapping := range mappings {
		usage := batch.Stats[mapping.RemoteAccountID]
		if usage == nil {
			err := fmt.Errorf("today usage missing for remote account %d", mapping.RemoteAccountID)
			_ = s.Store.MarkSub2APIDailyUsageSyncError(ctx, target.ID, usageDate, []store.Sub2APIUsageSyncMapping{mapping}, err.Error())
			return 0, fmt.Errorf("target %d: %w", target.ID, err)
		}
		inputs = append(inputs, dailyUsageSnapshotInput(mapping, usage, batch.ComputedAt, usageDate, false))
	}
	if err := s.Store.SaveSub2APIDailyUsageSnapshots(ctx, target.ID, inputs); err != nil {
		return 0, fmt.Errorf("target %d save today usage: %w", target.ID, err)
	}
	return len(inputs), nil
}

func (s *Syncer) syncTargetUsageBaselines(ctx context.Context, target store.Sub2APISyncTarget, throughDate time.Time, timezoneName string, location *time.Location) (int, error) {
	mappings, err := s.Store.ListSub2APIUsageBaselineMappings(ctx, target, sub2APIUsageBaselineLimit)
	if err != nil || len(mappings) == 0 {
		return 0, err
	}
	if err := store.ValidateDistinctSub2APIRemoteAccounts(mappings); err != nil {
		_ = s.Store.MarkSub2APIUsageSyncError(ctx, target.ID, mappings, err.Error())
		return 0, fmt.Errorf("target %d validate usage baselines: %w", target.ID, err)
	}
	inputs := make([]store.Sub2APIUsageSnapshotInput, 0, len(mappings))
	var queryErrors []error
	var mu sync.Mutex
	semaphore := make(chan struct{}, sub2APIUsageQueryConcurrency)
	var workers sync.WaitGroup
	for _, mapping := range mappings {
		mapping := mapping
		workers.Add(1)
		go func() {
			defer workers.Done()
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				mu.Lock()
				queryErrors = append(queryErrors, ctx.Err())
				mu.Unlock()
				return
			}
			startDate := startOfUsageDay(mapping.MappingCreatedAt, location).Format("2006-01-02")
			usage, queryErr := s.Client.GetAccountUsageRange(ctx, target.BaseURL, target.AdminKey, mapping.RemoteAccountID, startDate, throughDate.Format("2006-01-02"), timezoneName)
			if queryErr != nil {
				_ = s.Store.MarkSub2APIUsageSyncError(ctx, target.ID, []store.Sub2APIUsageSyncMapping{mapping}, queryErr.Error())
				mu.Lock()
				queryErrors = append(queryErrors, fmt.Errorf("target %d account %d baseline usage: %w", target.ID, mapping.RemoteAccountID, queryErr))
				mu.Unlock()
				return
			}
			input := usageSnapshotInput(mapping, &usage, usage.ComputedAt, throughDate)
			mu.Lock()
			inputs = append(inputs, input)
			mu.Unlock()
		}()
	}
	workers.Wait()
	if len(inputs) > 0 {
		if err := s.Store.SaveSub2APIUsageSnapshots(ctx, target.ID, inputs); err != nil {
			queryErrors = append(queryErrors, fmt.Errorf("target %d save usage baselines: %w", target.ID, err))
			return 0, errors.Join(queryErrors...)
		}
	}
	return len(inputs), errors.Join(queryErrors...)
}

func (s *Syncer) finalizeTargetDailyUsage(ctx context.Context, target store.Sub2APISyncTarget, beforeDate time.Time, timezoneName string) (int, error) {
	items, err := s.Store.ListPendingSub2APIDailyUsageFinalizations(ctx, target, beforeDate, sub2APIUsageFinalizationLimit)
	if err != nil || len(items) == 0 {
		return 0, err
	}
	inputs := make([]store.Sub2APIDailyUsageSnapshotInput, 0, len(items))
	var queryErrors []error
	var mu sync.Mutex
	semaphore := make(chan struct{}, sub2APIUsageQueryConcurrency)
	var workers sync.WaitGroup
	for _, item := range items {
		item := item
		workers.Add(1)
		go func() {
			defer workers.Done()
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				mu.Lock()
				queryErrors = append(queryErrors, ctx.Err())
				mu.Unlock()
				return
			}
			date := item.UsageDate.Format("2006-01-02")
			usage, queryErr := s.Client.GetAccountUsageRange(ctx, target.BaseURL, target.AdminKey, item.RemoteAccountID, date, date, timezoneName)
			if queryErr != nil {
				_ = s.Store.MarkSub2APIDailyUsageSyncError(ctx, target.ID, item.UsageDate, []store.Sub2APIUsageSyncMapping{item.Sub2APIUsageSyncMapping}, queryErr.Error())
				mu.Lock()
				queryErrors = append(queryErrors, fmt.Errorf("target %d account %d finalize %s usage: %w", target.ID, item.RemoteAccountID, date, queryErr))
				mu.Unlock()
				return
			}
			input := dailyUsageSnapshotInput(item.Sub2APIUsageSyncMapping, &usage, usage.ComputedAt, item.UsageDate, true)
			mu.Lock()
			inputs = append(inputs, input)
			mu.Unlock()
		}()
	}
	workers.Wait()
	if len(inputs) > 0 {
		if err := s.Store.SaveSub2APIDailyUsageSnapshots(ctx, target.ID, inputs); err != nil {
			queryErrors = append(queryErrors, fmt.Errorf("target %d save finalized daily usage: %w", target.ID, err))
			return 0, errors.Join(queryErrors...)
		}
	}
	return len(inputs), errors.Join(queryErrors...)
}

func usageSnapshotInput(mapping store.Sub2APIUsageSyncMapping, usage *AccountUsageTotal, batchComputedAt time.Time, throughDate time.Time) store.Sub2APIUsageSnapshotInput {
	computedAt := usage.ComputedAt
	if computedAt.IsZero() {
		computedAt = batchComputedAt
	}
	if computedAt.IsZero() {
		computedAt = time.Now().UTC()
	}
	computedAt = computedAt.UTC()
	return store.Sub2APIUsageSnapshotInput{
		TokenID:          mapping.TokenID,
		RemoteAccountID:  mapping.RemoteAccountID,
		AccountCostUSD:   usage.AccountCost,
		StandardCostUSD:  usage.StandardCost,
		UserCostUSD:      usage.UserCost,
		TotalRequests:    usage.TotalRequests,
		TotalTokens:      usage.TotalTokens,
		SourceComputedAt: &computedAt,
		ThroughDate:      &throughDate,
	}
}

func dailyUsageSnapshotInput(mapping store.Sub2APIUsageSyncMapping, usage *AccountUsageTotal, batchComputedAt time.Time, usageDate time.Time, finalized bool) store.Sub2APIDailyUsageSnapshotInput {
	computedAt := usage.ComputedAt
	if computedAt.IsZero() {
		computedAt = batchComputedAt
	}
	if computedAt.IsZero() {
		computedAt = time.Now().UTC()
	}
	computedAt = computedAt.UTC()
	return store.Sub2APIDailyUsageSnapshotInput{
		TokenID:          mapping.TokenID,
		RemoteAccountID:  mapping.RemoteAccountID,
		UsageDate:        usageDate,
		AccountCostUSD:   usage.AccountCost,
		StandardCostUSD:  usage.StandardCost,
		UserCostUSD:      usage.UserCost,
		TotalRequests:    usage.TotalRequests,
		TotalTokens:      usage.TotalTokens,
		SourceComputedAt: &computedAt,
		Finalized:        finalized,
	}
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
		compatibility, compatibilityErr := s.agentIdentityCompatibility(ctx, target)
		if compatibilityErr != nil {
			result.Status = store.Sub2APISyncStatusFailed
			result.ErrorMessage = compatibilityErr.Error()
			return s.finish(ctx, run, result)
		}
		result.Status = store.Sub2APISyncStatusCompleted
		result.Details = agentIdentityCompatibilityDetails(map[string]any{"mode": "check", "group_ids": target.TargetGroupIDs}, compatibility)
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

	compatibility, err := s.agentIdentityCompatibility(ctx, target)
	if err != nil {
		result.Status = store.Sub2APISyncStatusFailed
		result.ErrorMessage = err.Error()
		return s.finish(ctx, run, result)
	}
	candidates, err := s.Store.ListSub2APITokenCandidates(ctx, target, store.Sub2APITokenCandidateOptions{
		Limit:                limit,
		IncludeAgentIdentity: compatibility.Supported,
	})
	if err != nil {
		result.Status = store.Sub2APISyncStatusFailed
		result.ErrorMessage = err.Error()
		return s.finish(ctx, run, result)
	}
	result.SelectedCount = len(candidates)
	if len(candidates) == 0 {
		result.Status = store.Sub2APISyncStatusSkipped
		reason := "no_unsynced_oaix_candidates"
		if compatibility.blocked() {
			reason = "sub2api_agent_identity_version_unsupported"
			if compatibility.CheckFailed || compatibility.TargetVersion == "" {
				reason = "sub2api_agent_identity_version_unknown"
			}
		}
		details := syncDetails(map[string]any{"reason": reason, "group_ids": target.TargetGroupIDs}, reconcile)
		result.Details = agentIdentityCompatibilityDetails(details, compatibility)
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
		details := syncDetails(map[string]any{"selected_token_ids": tokenCandidateIDs(candidates)}, reconcile)
		result.Details = agentIdentityCompatibilityDetails(details, compatibility)
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
	result.Details = agentIdentityCompatibilityDetails(syncDetails(result.Details.(map[string]any), reconcile), compatibility)
	return s.finish(ctx, run, result)
}

func (s *Syncer) agentIdentityCompatibility(ctx context.Context, target store.Sub2APISyncTarget) (agentIdentityCompatibility, error) {
	compatibility := agentIdentityCompatibility{}
	count, err := s.Store.CountSub2APIAgentIdentityCandidates(ctx, target)
	if err != nil {
		return compatibility, fmt.Errorf("count sub2api agent identity candidates: %w", err)
	}
	compatibility.CandidateCount = count
	if count == 0 {
		return compatibility, nil
	}
	metadata, err := s.Client.GetServerMetadata(ctx, target.BaseURL)
	if err != nil {
		compatibility.CheckFailed = true
		return compatibility, nil
	}
	compatibility.TargetVersion = metadata.Version
	compatibility.Supported = SupportsAgentIdentity(metadata.Version)
	return compatibility, nil
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
	if candidate.AgentIdentity != nil {
		metadata, err := s.Client.GetServerMetadata(ctx, baseURL)
		if err != nil {
			return fmt.Errorf("verify agent identity target version: %w", err)
		}
		if !SupportsAgentIdentity(metadata.Version) {
			return fmt.Errorf("sub2api %s does not support agent identity credentials; version %s or newer is required", displayTargetVersion(metadata.Version), MinimumAgentIdentityTargetVersion)
		}
	}
	if err := s.Client.ApplyOAuthCredentials(ctx, baseURL, adminKey, remoteAccountID, s.accountCredentials(candidate), sub2APISyncMetadata(targetID, candidate)); err != nil {
		return fmt.Errorf("update account credentials: %w", err)
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

func agentIdentityCompatibilityDetails(base map[string]any, compatibility agentIdentityCompatibility) map[string]any {
	if base == nil {
		base = map[string]any{}
	}
	if compatibility.CandidateCount <= 0 {
		return base
	}
	base["agent_identity_candidate_count"] = compatibility.CandidateCount
	if compatibility.TargetVersion != "" {
		base["target_version"] = compatibility.TargetVersion
	}
	if compatibility.blocked() {
		base["blocked_agent_identity_count"] = compatibility.CandidateCount
		base["required_target_version"] = MinimumAgentIdentityTargetVersion
		base["target_version_check_failed"] = compatibility.CheckFailed
	}
	return base
}

func displayTargetVersion(version string) string {
	version = strings.TrimSpace(version)
	if version == "" {
		return "version unknown"
	}
	return version
}

func (s *Syncer) finish(ctx context.Context, run store.Sub2APISyncRun, result store.Sub2APISyncRunResult) (store.Sub2APISyncRun, error) {
	finished, err := s.Store.FinishSub2APISyncRun(ctx, run.ID, result)
	if err != nil {
		return run, err
	}
	return finished, nil
}

func (s *Syncer) createAccountRequest(target store.Sub2APISyncTarget, candidate store.Sub2APITokenCandidate) CreateAccountRequest {
	credentials := s.accountCredentials(candidate)
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

func (s *Syncer) accountCredentials(candidate store.Sub2APITokenCandidate) map[string]any {
	if candidate.AgentIdentity != nil {
		return agentIdentityCredentials(*candidate.AgentIdentity)
	}
	return s.oauthCredentials(candidate)
}

func agentIdentityCredentials(credentials agentidentity.Credentials) map[string]any {
	payload := map[string]any{
		"auth_mode":                  agentidentity.AuthMode,
		"agent_runtime_id":           credentials.RuntimeID,
		"agent_private_key":          credentials.PrivateKey,
		"chatgpt_account_id":         credentials.AccountID,
		"chatgpt_user_id":            credentials.UserID,
		"chatgpt_account_is_fedramp": credentials.FedRAMP,
	}
	if credentials.TaskID != "" {
		payload["task_id"] = credentials.TaskID
	}
	if credentials.Email != "" {
		payload["email"] = credentials.Email
	}
	if credentials.PlanType != "" {
		payload["plan_type"] = credentials.PlanType
	}
	if credentials.WorkspaceID != "" {
		payload["workspace_id"] = credentials.WorkspaceID
	}
	return payload
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
