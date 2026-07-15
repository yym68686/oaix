package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/store"
)

type quotaRecoveryStore interface {
	ListQuotaRecoveryCandidates(context.Context) ([]store.QuotaRecoveryCandidate, error)
	TryQuotaRecoveryCheckLease(context.Context, int64) (*store.QuotaRecoveryCheckLease, bool, error)
	BeginQuotaRecoveryProbe(context.Context, store.QuotaRecoveryCandidate, time.Duration, time.Duration) (*store.QuotaRecoveryClaim, *time.Time, error)
	GetToken(context.Context, int64) (*store.Token, error)
	CompleteQuotaRecovery(context.Context, store.QuotaRecoveryClaim, store.QuotaRecoveryCredentialFence, map[string]any) (bool, error)
	ApplyQuotaRecoveryUsageLimit(context.Context, store.QuotaRecoveryClaim, store.QuotaRecoveryCredentialFence, *time.Time, time.Duration, string, map[string]any) (bool, *time.Time, error)
	RecordQuotaRecoveryResult(context.Context, store.QuotaRecoveryClaim, store.QuotaRecoveryResult) error
}

const quotaRecoveryMinimumLeadTime = 3 * time.Minute

type quotaRecoveryCheckErrorReason string

const (
	quotaRecoveryCheckErrorCheckLease                 quotaRecoveryCheckErrorReason = "check_lease"
	quotaRecoveryCheckErrorTokenLoad                  quotaRecoveryCheckErrorReason = "token_load"
	quotaRecoveryCheckErrorContextCanceled            quotaRecoveryCheckErrorReason = "context_canceled"
	quotaRecoveryCheckErrorDeadlineExceeded           quotaRecoveryCheckErrorReason = "deadline_exceeded"
	quotaRecoveryCheckErrorCredentialRefreshConflict  quotaRecoveryCheckErrorReason = "credential_refresh_conflict"
	quotaRecoveryCheckErrorCredentialRefreshRejected  quotaRecoveryCheckErrorReason = "credential_refresh_rejected"
	quotaRecoveryCheckErrorCredentialRefreshOther     quotaRecoveryCheckErrorReason = "credential_refresh_other"
	quotaRecoveryCheckErrorTransport                  quotaRecoveryCheckErrorReason = "transport"
	quotaRecoveryCheckErrorAccessTokenInactive        quotaRecoveryCheckErrorReason = "access_token_inactive"
	quotaRecoveryCheckErrorAuthenticationInvalidated  quotaRecoveryCheckErrorReason = "authentication_token_invalidated"
	quotaRecoveryCheckErrorOwnerInactive              quotaRecoveryCheckErrorReason = "owner_inactive"
	quotaRecoveryCheckErrorAuthenticationTokenExpired quotaRecoveryCheckErrorReason = "authentication_token_expired"
	quotaRecoveryCheckErrorHTTPUnauthorized           quotaRecoveryCheckErrorReason = "http_unauthorized"
	quotaRecoveryCheckErrorHTTPForbidden              quotaRecoveryCheckErrorReason = "http_forbidden"
	quotaRecoveryCheckErrorHTTPNotFound               quotaRecoveryCheckErrorReason = "http_not_found"
	quotaRecoveryCheckErrorHTTPRateLimited            quotaRecoveryCheckErrorReason = "http_rate_limited"
	quotaRecoveryCheckErrorHTTPOtherClient            quotaRecoveryCheckErrorReason = "http_other_4xx"
	quotaRecoveryCheckErrorHTTPServer                 quotaRecoveryCheckErrorReason = "http_5xx"
	quotaRecoveryCheckErrorInvalidJSON                quotaRecoveryCheckErrorReason = "invalid_json"
	quotaRecoveryCheckErrorInvalidPayload             quotaRecoveryCheckErrorReason = "invalid_payload"
	quotaRecoveryCheckErrorFreshSnapshotUnknown       quotaRecoveryCheckErrorReason = "fresh_snapshot_unknown"
	quotaRecoveryCheckErrorOther                      quotaRecoveryCheckErrorReason = "other"
)

var quotaRecoveryCheckErrorReasons = [...]quotaRecoveryCheckErrorReason{
	quotaRecoveryCheckErrorCheckLease,
	quotaRecoveryCheckErrorTokenLoad,
	quotaRecoveryCheckErrorContextCanceled,
	quotaRecoveryCheckErrorDeadlineExceeded,
	quotaRecoveryCheckErrorCredentialRefreshConflict,
	quotaRecoveryCheckErrorCredentialRefreshRejected,
	quotaRecoveryCheckErrorCredentialRefreshOther,
	quotaRecoveryCheckErrorTransport,
	quotaRecoveryCheckErrorAccessTokenInactive,
	quotaRecoveryCheckErrorAuthenticationInvalidated,
	quotaRecoveryCheckErrorOwnerInactive,
	quotaRecoveryCheckErrorAuthenticationTokenExpired,
	quotaRecoveryCheckErrorHTTPUnauthorized,
	quotaRecoveryCheckErrorHTTPForbidden,
	quotaRecoveryCheckErrorHTTPNotFound,
	quotaRecoveryCheckErrorHTTPRateLimited,
	quotaRecoveryCheckErrorHTTPOtherClient,
	quotaRecoveryCheckErrorHTTPServer,
	quotaRecoveryCheckErrorInvalidJSON,
	quotaRecoveryCheckErrorInvalidPayload,
	quotaRecoveryCheckErrorFreshSnapshotUnknown,
	quotaRecoveryCheckErrorOther,
}

type quotaRecoveryStats struct {
	Scans                        int64            `json:"scans"`
	Candidates                   int64            `json:"candidates"`
	QuotaChecks                  int64            `json:"quota_checks"`
	QuotaCheckErrors             int64            `json:"quota_check_errors"`
	QuotaCheckErrorsByReason     map[string]int64 `json:"quota_check_errors_by_reason"`
	FreshQuotaErrorSkips         int64            `json:"fresh_quota_error_skips"`
	FreshQuotaErrorSkipsByReason map[string]int64 `json:"fresh_quota_error_skips_by_reason"`
	CapacityPositive             int64            `json:"capacity_positive"`
	ProbesStarted                int64            `json:"probes_started"`
	ProbeCompleted               int64            `json:"probe_completed"`
	Reactivated                  int64            `json:"reactivated"`
	UsageLimited                 int64            `json:"usage_limited"`
	Inconclusive                 int64            `json:"inconclusive"`
	StateConflicts               int64            `json:"state_conflicts"`
	PersistenceErrors            int64            `json:"persistence_errors"`
	LastScanUnix                 int64            `json:"last_scan_unix"`
}

type quotaRecoveryWorker struct {
	app                     *App
	cfg                     config.QuotaRecoveryConfig
	store                   quotaRecoveryStore
	quota                   *adminQuotaService
	logger                  *slog.Logger
	startOnce               sync.Once
	scheduleMu              sync.Mutex
	nextCheck               map[int64]time.Time
	nextProbe               map[int64]time.Time
	errorMu                 sync.Mutex
	checkErrorsByReason     map[quotaRecoveryCheckErrorReason]int64
	freshErrorSkipsByReason map[quotaRecoveryCheckErrorReason]int64

	scans                atomic.Int64
	candidates           atomic.Int64
	quotaChecks          atomic.Int64
	quotaCheckErrors     atomic.Int64
	freshQuotaErrorSkips atomic.Int64
	capacityPositive     atomic.Int64
	probesStarted        atomic.Int64
	probeCompleted       atomic.Int64
	reactivated          atomic.Int64
	usageLimited         atomic.Int64
	inconclusive         atomic.Int64
	stateConflicts       atomic.Int64
	persistenceErrors    atomic.Int64
	lastScanUnix         atomic.Int64
}

func newQuotaRecoveryWorker(app *App) *quotaRecoveryWorker {
	if app == nil || app.store == nil || app.quota == nil {
		return nil
	}
	recoveryCfg := app.cfg.QuotaRecovery
	recoveryCfg.Concurrency = quotaRecoveryEffectiveConcurrency(recoveryCfg.Concurrency, app.cfg.Database.MaxConns)
	if recoveryCfg.Enabled && recoveryCfg.Concurrency == 0 {
		recoveryCfg.Enabled = false
		if app.logger != nil {
			app.logger.Warn("automatic quota recovery disabled because the database pool is too small",
				"database_max_conns", app.cfg.Database.MaxConns,
			)
		}
	}
	return &quotaRecoveryWorker{
		app:                     app,
		cfg:                     recoveryCfg,
		store:                   app.store,
		quota:                   app.quota,
		logger:                  app.logger,
		nextCheck:               make(map[int64]time.Time),
		nextProbe:               make(map[int64]time.Time),
		checkErrorsByReason:     make(map[quotaRecoveryCheckErrorReason]int64),
		freshErrorSkipsByReason: make(map[quotaRecoveryCheckErrorReason]int64),
	}
}

func quotaRecoveryEffectiveConcurrency(configured int, maxDatabaseConns int32) int {
	if configured <= 0 || maxDatabaseConns < 4 {
		return 0
	}
	// A stale-quota check briefly holds one session-lock connection while its
	// normal store operations can need another. Keep two connections outside
	// that worst-case pair budget for request traffic and health checks.
	limit := (int(maxDatabaseConns) - 2) / 2
	if configured < limit {
		return configured
	}
	return limit
}

func (a *App) StartQuotaRecovery(ctx context.Context) {
	if a == nil || a.recovery == nil || !a.cfg.QuotaRecovery.Enabled {
		return
	}
	a.recovery.Start(ctx)
}

func (w *quotaRecoveryWorker) Start(ctx context.Context) {
	if w == nil || w.store == nil || w.quota == nil || !w.cfg.Enabled {
		return
	}
	w.startOnce.Do(func() {
		go w.run(ctx)
	})
}

func (w *quotaRecoveryWorker) run(ctx context.Context) {
	if w.logger != nil {
		w.logger.Info("automatic quota recovery worker started",
			"startup_delay", w.cfg.StartupDelay,
			"scan_interval", w.cfg.ScanInterval,
			"recheck_interval", w.cfg.RecheckInterval,
			"probe_retry_interval", w.cfg.ProbeRetryInterval,
			"batch_size", w.cfg.BatchSize,
			"concurrency", w.cfg.Concurrency,
			"model", store.QuotaRecoveryModel,
		)
	}
	startup := time.NewTimer(w.cfg.StartupDelay)
	defer startup.Stop()
	select {
	case <-ctx.Done():
		return
	case <-startup.C:
	}
	w.scan(ctx)
	ticker := time.NewTicker(w.cfg.ScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.scan(ctx)
		}
	}
}

func (w *quotaRecoveryWorker) scan(parent context.Context) {
	if parent.Err() != nil {
		return
	}
	w.scans.Add(1)
	w.lastScanUnix.Store(time.Now().UTC().Unix())
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	candidates, err := w.store.ListQuotaRecoveryCandidates(ctx)
	if err != nil {
		if w.logger != nil && parent.Err() == nil {
			w.logger.Warn("automatic quota recovery candidate scan failed", "error", err)
		}
		return
	}
	w.candidates.Store(int64(len(candidates)))
	w.pruneNextChecks(candidates)
	if len(candidates) == 0 {
		return
	}
	now := time.Now().UTC()
	selected := make([]store.QuotaRecoveryCandidate, 0, w.cfg.BatchSize)
	selectedIDs := make(map[int64]struct{}, w.cfg.BatchSize)
	// A fresh positive UI snapshot already gives us the signal the user cares
	// about. Scan the whole candidate set for those first so stale/zero entries
	// near the front cannot consume the batch and delay a real recovery.
	for _, candidate := range candidates {
		if len(selected) >= w.cfg.BatchSize {
			break
		}
		if next, ok := w.nextProbe[candidate.TokenID]; ok && next.After(now) {
			continue
		}
		if !candidate.CooldownUntil.After(now.Add(quotaRecoveryMinimumLeadTime)) {
			continue
		}
		if snapshot, fresh := w.snapshotForCandidate(candidate, now); fresh && quotaSnapshotHasCapacity(snapshot) {
			selected = append(selected, candidate)
			selectedIDs[candidate.TokenID] = struct{}{}
		}
	}
	// Spend any remaining batch capacity on stale/missing snapshots. A fresh
	// non-positive snapshot is authoritative until it ages out and must not
	// displace a later fresh-positive candidate.
	for _, candidate := range candidates {
		if len(selected) >= w.cfg.BatchSize {
			break
		}
		if _, ok := selectedIDs[candidate.TokenID]; ok {
			continue
		}
		if next, ok := w.nextProbe[candidate.TokenID]; ok && next.After(now) {
			continue
		}
		if !candidate.CooldownUntil.After(now.Add(quotaRecoveryMinimumLeadTime)) {
			continue
		}
		if snapshot, fresh := w.snapshotForCandidate(candidate, now); fresh {
			if snapshot != nil && snapshot.Error != nil {
				reason := snapshot.recoveryErrorReason
				if reason == "" {
					reason = quotaRecoveryCheckErrorFreshSnapshotUnknown
				}
				w.recordFreshQuotaErrorSkip(reason)
			}
			continue
		}
		if next, ok := w.nextCheck[candidate.TokenID]; ok && next.After(now) {
			continue
		}
		w.nextCheck[candidate.TokenID] = now.Add(w.cfg.RecheckInterval)
		selected = append(selected, candidate)
		selectedIDs[candidate.TokenID] = struct{}{}
	}
	if len(selected) == 0 {
		return
	}
	sem := make(chan struct{}, w.cfg.Concurrency)
	var wg sync.WaitGroup
	for _, candidate := range selected {
		candidate := candidate
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			w.processCandidate(ctx, candidate)
		}()
	}
	wg.Wait()
}

func (w *quotaRecoveryWorker) processCandidate(ctx context.Context, candidate store.QuotaRecoveryCandidate) {
	now := time.Now().UTC()
	snapshot, fresh := w.snapshotForCandidate(candidate, now)
	var checkLease *store.QuotaRecoveryCheckLease
	releaseCheckLease := func() {
		if checkLease == nil {
			return
		}
		checkLease.Release()
		checkLease = nil
	}
	defer releaseCheckLease()
	if !fresh {
		lease, acquired, err := w.store.TryQuotaRecoveryCheckLease(ctx, candidate.TokenID)
		if err != nil {
			w.recordQuotaCheckError(quotaRecoveryCheckErrorCheckLease)
			if w.logger != nil && ctx.Err() == nil {
				w.logger.Warn("automatic quota recovery check lease failed", "token_id", candidate.TokenID, "error", err)
			}
			return
		}
		if !acquired || lease == nil {
			return
		}
		checkLease = lease
		token, err := w.store.GetToken(ctx, candidate.TokenID)
		if err != nil || token == nil {
			w.recordQuotaCheckError(quotaRecoveryCheckErrorTokenLoad)
			return
		}
		w.quotaChecks.Add(1)
		var failureReason quotaRecoveryCheckErrorReason
		snapshot, failureReason = w.quota.fetchSnapshotWithoutHistory(ctx, *token)
		if snapshot == nil {
			w.recordQuotaCheckError(failureReason)
			return
		}
		if snapshot.Error != nil {
			w.recordQuotaCheckError(failureReason)
			return
		}
	}
	if !quotaSnapshotHasCapacity(snapshot) {
		return
	}
	w.capacityPositive.Add(1)
	claim, nextEligibleAt, err := w.store.BeginQuotaRecoveryProbe(ctx, candidate, w.cfg.ProbeRetryInterval, quotaRecoveryMinimumLeadTime)
	// The durable probe claim now prevents duplicate model probes. Release the
	// session advisory lock before the potentially long upstream model request.
	releaseCheckLease()
	if err != nil {
		w.persistenceErrors.Add(1)
		if w.logger != nil && ctx.Err() == nil {
			w.logger.Warn("automatic quota recovery probe claim failed", "token_id", candidate.TokenID, "error", err)
		}
		return
	}
	if claim == nil {
		w.stateConflicts.Add(1)
		if nextEligibleAt != nil {
			w.scheduleDurableClaimRetry(candidate.TokenID, *nextEligibleAt)
		}
		return
	}
	w.deferProbe(candidate.TokenID)
	w.probesStarted.Add(1)
	token, err := w.store.GetToken(ctx, claim.TokenID)
	if err != nil || token == nil {
		w.recordInconclusive(ctx, *claim, 0, "token credentials became unavailable before probe", snapshot, "credentials_unavailable")
		return
	}
	attempt, probedToken := w.app.executeTokenProbeWithAuth(ctx, *token, store.QuotaRecoveryModel)
	fence := quotaRecoveryCredentialFence(probedToken)
	metadata := quotaRecoveryProbeMetadata(snapshot, attempt)
	switch attempt.Outcome {
	case tokenProbeCompleted:
		w.probeCompleted.Add(1)
		applied, err := w.store.CompleteQuotaRecovery(ctx, *claim, fence, metadata)
		if err != nil {
			w.persistenceErrors.Add(1)
			if w.logger != nil && ctx.Err() == nil {
				w.logger.Warn("automatic quota recovery commit failed", "token_id", claim.TokenID, "error", err)
			}
			return
		}
		if !applied {
			w.stateConflicts.Add(1)
			w.recordInconclusive(ctx, *claim, http.StatusConflict, "token state changed while recovery probe was running", snapshot, "state_conflict")
			return
		}
		w.reactivated.Add(1)
		w.refreshRecoveredToken(ctx, *claim)
		if w.logger != nil {
			w.logger.Info("automatic quota recovery reactivated token",
				"token_id", claim.TokenID,
				"owner_user_id", claim.OwnerUserID,
				"previous_cooldown_until", claim.CooldownUntil,
				"model", store.QuotaRecoveryModel,
			)
		}
	case tokenProbeUsageLimited:
		applied, appliedUntil, err := w.store.ApplyQuotaRecoveryUsageLimit(ctx, *claim, fence, attempt.UsageLimit.ResetAt, w.app.cfg.TokenPool.DefaultCooldown, attempt.Detail, metadata)
		if err != nil {
			w.persistenceErrors.Add(1)
			if w.logger != nil && ctx.Err() == nil {
				w.logger.Warn("automatic quota recovery cooldown update failed", "token_id", claim.TokenID, "error", err)
			}
			return
		}
		if !applied {
			w.stateConflicts.Add(1)
			return
		}
		w.usageLimited.Add(1)
		if w.logger != nil {
			w.logger.Info("automatic quota recovery probe confirmed usage limit",
				"token_id", claim.TokenID,
				"owner_user_id", claim.OwnerUserID,
				"cooldown_until", appliedUntil,
				"explicit_reset", attempt.UsageLimit.ExplicitReset,
			)
		}
	default:
		w.recordInconclusive(ctx, *claim, attempt.StatusCode, attempt.Detail, snapshot, string(attempt.Outcome))
	}
}

func (w *quotaRecoveryWorker) recordInconclusive(ctx context.Context, claim store.QuotaRecoveryClaim, statusCode int, reason string, snapshot *codexQuotaSnapshot, outcome string) {
	w.inconclusive.Add(1)
	metadata := quotaRecoveryProbeMetadata(snapshot, tokenProbeAttempt{Outcome: tokenProbeOutcome(outcome), StatusCode: statusCode})
	if err := w.store.RecordQuotaRecoveryResult(ctx, claim, store.QuotaRecoveryResult{
		Outcome:    outcome,
		Reason:     shortenError(reason, 500),
		StatusCode: statusCode,
		Metadata:   metadata,
	}); err != nil {
		w.persistenceErrors.Add(1)
		if w.logger != nil && ctx.Err() == nil {
			w.logger.Warn("automatic quota recovery result persistence failed", "token_id", claim.TokenID, "error", err)
		}
	}
}

func (w *quotaRecoveryWorker) refreshRecoveredToken(parent context.Context, claim store.QuotaRecoveryClaim) {
	if w.app == nil || w.app.tokens == nil {
		return
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	if err := w.app.tokens.Refresh(ctx); err != nil {
		if w.logger != nil {
			w.logger.Warn("token snapshot refresh after automatic quota recovery failed", "token_id", claim.TokenID, "error", err)
		}
		return
	}
	if claim.OwnerUserID > 0 {
		if err := w.app.tokens.RefreshOwner(ctx, claim.OwnerUserID); err != nil && w.logger != nil {
			w.logger.Warn("owner token snapshot refresh after automatic quota recovery failed", "token_id", claim.TokenID, "owner_user_id", claim.OwnerUserID, "error", err)
		}
	}
}

func (w *quotaRecoveryWorker) pruneNextChecks(candidates []store.QuotaRecoveryCandidate) {
	if len(w.nextCheck) == 0 && len(w.nextProbe) == 0 {
		return
	}
	active := make(map[int64]struct{}, len(candidates))
	for _, candidate := range candidates {
		active[candidate.TokenID] = struct{}{}
	}
	for tokenID := range w.nextCheck {
		if _, ok := active[tokenID]; !ok {
			delete(w.nextCheck, tokenID)
		}
	}
	for tokenID := range w.nextProbe {
		if _, ok := active[tokenID]; !ok {
			delete(w.nextProbe, tokenID)
		}
	}
}

func (w *quotaRecoveryWorker) deferProbe(tokenID int64) {
	w.scheduleMu.Lock()
	w.nextProbe[tokenID] = time.Now().UTC().Add(w.cfg.ProbeRetryInterval)
	w.scheduleMu.Unlock()
}

func (w *quotaRecoveryWorker) scheduleDurableClaimRetry(tokenID int64, nextEligibleAt time.Time) {
	nextEligibleAt = nextEligibleAt.UTC()
	w.scheduleMu.Lock()
	w.nextProbe[tokenID] = nextEligibleAt
	if current, ok := w.nextCheck[tokenID]; !ok || current.After(nextEligibleAt) {
		w.nextCheck[tokenID] = nextEligibleAt
	}
	w.scheduleMu.Unlock()
}

func (w *quotaRecoveryWorker) Stats() quotaRecoveryStats {
	if w == nil {
		return quotaRecoveryStats{}
	}
	quotaCheckErrors, quotaCheckErrorsByReason, freshQuotaErrorSkips, freshQuotaErrorSkipsByReason := w.quotaErrorStats()
	return quotaRecoveryStats{
		Scans:                        w.scans.Load(),
		Candidates:                   w.candidates.Load(),
		QuotaChecks:                  w.quotaChecks.Load(),
		QuotaCheckErrors:             quotaCheckErrors,
		QuotaCheckErrorsByReason:     quotaCheckErrorsByReason,
		FreshQuotaErrorSkips:         freshQuotaErrorSkips,
		FreshQuotaErrorSkipsByReason: freshQuotaErrorSkipsByReason,
		CapacityPositive:             w.capacityPositive.Load(),
		ProbesStarted:                w.probesStarted.Load(),
		ProbeCompleted:               w.probeCompleted.Load(),
		Reactivated:                  w.reactivated.Load(),
		UsageLimited:                 w.usageLimited.Load(),
		Inconclusive:                 w.inconclusive.Load(),
		StateConflicts:               w.stateConflicts.Load(),
		PersistenceErrors:            w.persistenceErrors.Load(),
		LastScanUnix:                 w.lastScanUnix.Load(),
	}
}

func (w *quotaRecoveryWorker) recordQuotaCheckError(reason quotaRecoveryCheckErrorReason) {
	if w == nil {
		return
	}
	reason = normalizeQuotaRecoveryCheckErrorReason(reason)
	w.errorMu.Lock()
	if w.checkErrorsByReason == nil {
		w.checkErrorsByReason = make(map[quotaRecoveryCheckErrorReason]int64)
	}
	w.checkErrorsByReason[reason]++
	w.quotaCheckErrors.Add(1)
	w.errorMu.Unlock()
}

func (w *quotaRecoveryWorker) recordFreshQuotaErrorSkip(reason quotaRecoveryCheckErrorReason) {
	if w == nil {
		return
	}
	reason = normalizeQuotaRecoveryCheckErrorReason(reason)
	w.errorMu.Lock()
	if w.freshErrorSkipsByReason == nil {
		w.freshErrorSkipsByReason = make(map[quotaRecoveryCheckErrorReason]int64)
	}
	w.freshErrorSkipsByReason[reason]++
	w.freshQuotaErrorSkips.Add(1)
	w.errorMu.Unlock()
}

func (w *quotaRecoveryWorker) quotaErrorStats() (int64, map[string]int64, int64, map[string]int64) {
	w.errorMu.Lock()
	defer w.errorMu.Unlock()
	checkCounts := make(map[string]int64, len(quotaRecoveryCheckErrorReasons))
	skipCounts := make(map[string]int64, len(quotaRecoveryCheckErrorReasons))
	for _, reason := range quotaRecoveryCheckErrorReasons {
		checkCounts[string(reason)] = w.checkErrorsByReason[reason]
		skipCounts[string(reason)] = w.freshErrorSkipsByReason[reason]
	}
	return w.quotaCheckErrors.Load(), checkCounts, w.freshQuotaErrorSkips.Load(), skipCounts
}

func normalizeQuotaRecoveryCheckErrorReason(reason quotaRecoveryCheckErrorReason) quotaRecoveryCheckErrorReason {
	for _, known := range quotaRecoveryCheckErrorReasons {
		if reason == known {
			return reason
		}
	}
	return quotaRecoveryCheckErrorOther
}

func quotaRecoveryPersistedSnapshot(candidate store.QuotaRecoveryCandidate, now time.Time, maxAge time.Duration) (*codexQuotaSnapshot, bool) {
	if len(candidate.QuotaSnapshot) == 0 || candidate.QuotaFetchedAt == nil || maxAge <= 0 || candidate.QuotaFetchedAt.Before(now.Add(-maxAge)) {
		return nil, false
	}
	var snapshot codexQuotaSnapshot
	if err := json.Unmarshal(candidate.QuotaSnapshot, &snapshot); err != nil {
		return nil, false
	}
	return &snapshot, true
}

func (w *quotaRecoveryWorker) snapshotForCandidate(candidate store.QuotaRecoveryCandidate, now time.Time) (*codexQuotaSnapshot, bool) {
	if snapshot, fresh := quotaRecoveryPersistedSnapshot(candidate, now, w.cfg.QuotaMaxAge); fresh {
		return snapshot, true
	}
	if w != nil && w.quota != nil {
		return w.quota.cached(candidate.TokenID, now, false)
	}
	return nil, false
}

func quotaSnapshotHasCapacity(snapshot *codexQuotaSnapshot) bool {
	if snapshot == nil || snapshot.Error != nil || snapshot.Disabled || len(snapshot.Windows) == 0 {
		return false
	}
	for _, window := range snapshot.Windows {
		if window.Exhausted {
			return false
		}
		remaining := window.RemainingPercent
		if remaining == nil && window.UsedPercent != nil {
			value := 100 - *window.UsedPercent
			remaining = &value
		}
		if remaining == nil || *remaining <= 0 {
			return false
		}
	}
	return true
}

func quotaRecoveryProbeMetadata(snapshot *codexQuotaSnapshot, attempt tokenProbeAttempt) map[string]any {
	metadata := map[string]any{
		"probe_model":   store.QuotaRecoveryModel,
		"probe_outcome": string(attempt.Outcome),
	}
	if attempt.StatusCode > 0 {
		metadata["status_code"] = attempt.StatusCode
	}
	if attempt.ResponseModel != "" {
		metadata["response_model"] = attempt.ResponseModel
	}
	if snapshot == nil {
		return metadata
	}
	metadata["quota_fetched_at"] = snapshot.FetchedAt.UTC().Format(time.RFC3339Nano)
	windows := make([]map[string]any, 0, len(snapshot.Windows))
	for _, window := range snapshot.Windows {
		windows = append(windows, map[string]any{
			"id":                window.ID,
			"remaining_percent": window.RemainingPercent,
			"exhausted":         window.Exhausted,
		})
	}
	metadata["quota_windows"] = windows
	return metadata
}

func quotaRecoveryCredentialFence(token store.Token) store.QuotaRecoveryCredentialFence {
	accountID := ""
	if token.AccountID != nil {
		accountID = strings.TrimSpace(*token.AccountID)
	}
	return store.QuotaRecoveryCredentialFence{
		AccessToken:  token.AccessToken,
		RefreshToken: token.RefreshToken,
		AccountID:    accountID,
	}
}
