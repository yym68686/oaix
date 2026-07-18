package tokens

import (
	"context"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/blake2b"

	"github.com/yym68686/oaix/internal/affinity"
	"github.com/yym68686/oaix/internal/store"
)

type PromptAffinityOptions struct {
	AffinityKey           string
	PreviousResponseID    string
	MaxLanesPerKey        int
	PrimaryWait           time.Duration
	LaneWait              time.Duration
	PreviousOwnerWait     time.Duration
	PreviousStrict        bool
	GlobalFallbackEnabled bool
	LaneTTL               time.Duration
	ResponseTTL           time.Duration
}

type PromptAffinityResult struct {
	Result    string
	LaneIndex *int
	LaneCount int
}

func (m *Manager) ClaimPromptAffinity(ctx context.Context, store affinity.Store, intent Intent, opts PromptAffinityOptions) (*Claim, PromptAffinityResult, error) {
	intent = m.withTokenModelCapabilityLossExclusions(intent)
	if intent.TargetTokenID > 0 {
		claim, err := m.Claim(ctx, intent)
		return claim, PromptAffinityResult{Result: claimReason(claim)}, err
	}
	if store == nil || (opts.AffinityKey == "" && opts.PreviousResponseID == "") {
		claim, err := m.Claim(ctx, intent)
		return claim, PromptAffinityResult{Result: claimReason(claim)}, err
	}
	if opts.MaxLanesPerKey <= 0 {
		opts.MaxLanesPerKey = 3
	}
	snapshot, _, err := m.snapshotForClaim(ctx, intent)
	if err != nil {
		if err == ErrNoToken {
			m.recordNoTokenDenial()
		}
		return nil, PromptAffinityResult{Result: "snapshot_unavailable"}, err
	}

	if opts.PreviousResponseID != "" {
		if binding, ok := store.GetResponseOwner(opts.PreviousResponseID); ok {
			if claim := m.claimSpecific(ctx, snapshot, intent, binding.TokenID, opts.PreviousOwnerWait, "previous_owner_hit"); claim != nil {
				result := PromptAffinityResult{Result: "previous_owner_hit"}
				lock, status := responseOwnerPriceLock(store, opts.PreviousResponseID, binding, claim, intent, opts.ResponseTTL)
				if opts.AffinityKey != "" {
					index, count, laneLock, laneStatus := bindLane(store, opts.AffinityKey, claim, opts.MaxLanesPerKey, opts.LaneTTL, true, lock)
					result.LaneIndex = index
					result.LaneCount = count
					if laneLock != nil {
						lock = laneLock
						status = laneStatus
					}
				}
				applyClaimMarketplacePriceLock(claim, lock, status)
				return claim, result, nil
			}
			if opts.PreviousStrict {
				m.recordNoTokenDenial()
				return nil, PromptAffinityResult{Result: "previous_owner_busy"}, ErrNoToken
			}
		}
	}

	if opts.AffinityKey != "" {
		lane, ok := store.Get(opts.AffinityKey)
		if ok {
			if lane.PrimaryTokenID > 0 {
				if claim := m.claimSpecific(ctx, snapshot, intent, lane.PrimaryTokenID, opts.PrimaryWait, "primary_hit"); claim != nil {
					index, count, lock, status := bindLane(store, opts.AffinityKey, claim, opts.MaxLanesPerKey, opts.LaneTTL, true, nil)
					applyClaimMarketplacePriceLock(claim, lock, status)
					return claim, PromptAffinityResult{Result: "primary_hit", LaneIndex: index, LaneCount: count}, nil
				}
			}
			for _, tokenID := range sortedLaneCandidates(opts.AffinityKey, snapshot, intent, lane.SecondaryTokenIDs) {
				if claim := m.claimSpecific(ctx, snapshot, intent, tokenID, 0, "lane_hit"); claim != nil {
					index, count, lock, status := bindLane(store, opts.AffinityKey, claim, opts.MaxLanesPerKey, opts.LaneTTL, false, nil)
					applyClaimMarketplacePriceLock(claim, lock, status)
					return claim, PromptAffinityResult{Result: "lane_hit", LaneIndex: index, LaneCount: count}, nil
				}
			}
			if laneSize(lane) < opts.MaxLanesPerKey {
				excluded := map[int64]struct{}{}
				if lane.PrimaryTokenID > 0 {
					excluded[lane.PrimaryTokenID] = struct{}{}
				}
				for _, tokenID := range lane.SecondaryTokenIDs {
					excluded[tokenID] = struct{}{}
				}
				nextIntent := intent
				nextIntent.ExcludeTokenIDs = mergeExcluded(intent.ExcludeTokenIDs, excluded)
				for _, tokenID := range sortedNewLaneCandidates(opts.AffinityKey, snapshot, nextIntent) {
					if claim := m.claimSpecific(ctx, snapshot, nextIntent, tokenID, 0, "lane_created"); claim != nil {
						index, count, lock, status := bindLane(store, opts.AffinityKey, claim, opts.MaxLanesPerKey, opts.LaneTTL, false, nil)
						applyClaimMarketplacePriceLock(claim, lock, status)
						return claim, PromptAffinityResult{Result: "lane_created", LaneIndex: index, LaneCount: count}, nil
					}
				}
			}
			if opts.LaneWait > 0 {
				timer := time.NewTimer(opts.LaneWait)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil, PromptAffinityResult{Result: "lane_wait_cancelled"}, ctx.Err()
				case <-timer.C:
				}
				if refreshed, ok := store.Get(opts.AffinityKey); ok {
					lane = refreshed
				}
				for _, tokenID := range sortedLaneCandidates(opts.AffinityKey, snapshot, intent, append([]int64{lane.PrimaryTokenID}, lane.SecondaryTokenIDs...)) {
					if claim := m.claimSpecific(ctx, snapshot, intent, tokenID, 0, "lane_hit_after_wait"); claim != nil {
						index, count, lock, status := bindLane(store, opts.AffinityKey, claim, opts.MaxLanesPerKey, opts.LaneTTL, false, nil)
						applyClaimMarketplacePriceLock(claim, lock, status)
						return claim, PromptAffinityResult{Result: "lane_hit_after_wait", LaneIndex: index, LaneCount: count}, nil
					}
				}
			}
		} else {
			for _, tokenID := range sortedNewLaneCandidates(opts.AffinityKey, snapshot, intent) {
				if claim := m.claimSpecific(ctx, snapshot, intent, tokenID, 0, "lane_created"); claim != nil {
					index, count, lock, status := bindLane(store, opts.AffinityKey, claim, opts.MaxLanesPerKey, opts.LaneTTL, true, nil)
					applyClaimMarketplacePriceLock(claim, lock, status)
					return claim, PromptAffinityResult{Result: "lane_created", LaneIndex: index, LaneCount: count}, nil
				}
			}
			if !opts.GlobalFallbackEnabled {
				m.recordNoTokenDenial()
				return nil, PromptAffinityResult{Result: "lane_create_failed"}, ErrNoToken
			}
		}
		if !opts.GlobalFallbackEnabled {
			m.recordNoTokenDenial()
			return nil, PromptAffinityResult{Result: "global_fallback_disabled"}, ErrNoToken
		}
	}

	claim, err := m.Claim(ctx, intent)
	result := PromptAffinityResult{Result: "global_fallback"}
	if err == nil && claim != nil && opts.AffinityKey != "" {
		index, count, lock, status := bindLane(store, opts.AffinityKey, claim, opts.MaxLanesPerKey, opts.LaneTTL, false, nil)
		applyClaimMarketplacePriceLock(claim, lock, status)
		result.LaneIndex = index
		result.LaneCount = count
	}
	return claim, result, err
}

func (m *Manager) ClaimStrictAffinity(ctx context.Context, store affinity.Store, intent Intent, affinityKey string, wait, ttl time.Duration) (*Claim, PromptAffinityResult, error) {
	intent = m.withTokenModelCapabilityLossExclusions(intent)
	if store == nil || strings.TrimSpace(affinityKey) == "" {
		m.recordNoTokenDenial()
		return nil, PromptAffinityResult{Result: "strict_store_unavailable"}, ErrNoToken
	}
	if lane, ok := store.Get(affinityKey); ok {
		identityHash := lane.StrictTokenIdentityHash()
		if lane.PolicyVersion != affinity.PolicyVersionStrictToken || lane.PrimaryTokenID <= 0 || identityHash == "" {
			m.recordNoTokenDenial()
			return nil, PromptAffinityResult{Result: "strict_policy_mismatch"}, ErrNoToken
		}
		snapshot, _, err := m.snapshotForClaim(ctx, intent)
		if err != nil {
			return nil, PromptAffinityResult{Result: "strict_snapshot_unavailable"}, err
		}
		if claim := m.claimSpecific(ctx, snapshot, intent, lane.PrimaryTokenID, wait, "strict_affinity_hit"); claim != nil {
			if !strictClaimIdentityMatches(claim, identityHash) {
				claim.Release()
				m.recordNoTokenDenial()
				return nil, PromptAffinityResult{Result: "strict_identity_mismatch"}, ErrNoToken
			}
			return claim, PromptAffinityResult{Result: "strict_affinity_hit"}, nil
		}
		m.recordNoTokenDenial()
		return nil, PromptAffinityResult{Result: "strict_affinity_busy"}, ErrNoToken
	}

	claim, identityHash, err := m.claimWithStrictIdentity(ctx, intent)
	if err != nil {
		return nil, PromptAffinityResult{Result: "strict_claim_failed"}, err
	}
	resolved, ok := store.BindPrimaryIfAbsent(
		affinityKey,
		claim.TokenID(),
		identityHash,
		ttl,
		affinity.PolicyVersionStrictToken,
	)
	resolvedIdentityHash := resolved.StrictTokenIdentityHash()
	if !ok || resolved.PolicyVersion != affinity.PolicyVersionStrictToken || resolved.PrimaryTokenID <= 0 || resolvedIdentityHash == "" {
		claim.Release()
		m.recordNoTokenDenial()
		return nil, PromptAffinityResult{Result: "strict_bind_failed"}, ErrNoToken
	}
	if resolved.PrimaryTokenID == claim.TokenID() {
		if !strictClaimIdentityMatches(claim, resolvedIdentityHash) {
			claim.Release()
			m.recordNoTokenDenial()
			return nil, PromptAffinityResult{Result: "strict_identity_mismatch"}, ErrNoToken
		}
		return claim, PromptAffinityResult{Result: "strict_affinity_created"}, nil
	}
	claim.Release()
	snapshot, _, err := m.snapshotForClaim(ctx, intent)
	if err != nil {
		return nil, PromptAffinityResult{Result: "strict_race_snapshot_unavailable"}, err
	}
	if winner := m.claimSpecific(ctx, snapshot, intent, resolved.PrimaryTokenID, wait, "strict_affinity_race_hit"); winner != nil {
		if !strictClaimIdentityMatches(winner, resolvedIdentityHash) {
			winner.Release()
			m.recordNoTokenDenial()
			return nil, PromptAffinityResult{Result: "strict_identity_race_mismatch"}, ErrNoToken
		}
		return winner, PromptAffinityResult{Result: "strict_affinity_race_hit"}, nil
	}
	m.recordNoTokenDenial()
	return nil, PromptAffinityResult{Result: "strict_affinity_race_busy"}, ErrNoToken
}

func (m *Manager) claimWithStrictIdentity(ctx context.Context, intent Intent) (*Claim, string, error) {
	next := intent
	next.ExcludeTokenIDs = make(map[int64]struct{}, len(intent.ExcludeTokenIDs))
	for tokenID := range intent.ExcludeTokenIDs {
		next.ExcludeTokenIDs[tokenID] = struct{}{}
	}
	for {
		claim, err := m.Claim(ctx, next)
		if err != nil {
			return nil, "", err
		}
		identityHash := strictClaimPrimaryIdentityHash(claim)
		if identityHash != "" {
			return claim, identityHash, nil
		}
		next.ExcludeTokenIDs[claim.TokenID()] = struct{}{}
		claim.Release()
	}
}

func strictClaimPrimaryIdentityHash(claim *Claim) string {
	if claim == nil || claim.Token == nil {
		return ""
	}
	if accountID := claim.AccountID(); accountID != nil && strings.TrimSpace(*accountID) != "" {
		return strictIdentityHash("account", strings.TrimSpace(*accountID))
	}
	if email := claim.Token.Token.Email; email != nil && strings.TrimSpace(*email) != "" {
		return strictIdentityHash("email", strings.ToLower(strings.TrimSpace(*email)))
	}
	return ""
}

func strictClaimIdentityMatches(claim *Claim, expected string) bool {
	if expected == "" || claim == nil || claim.Token == nil {
		return false
	}
	if accountID := claim.AccountID(); accountID != nil && strings.TrimSpace(*accountID) != "" {
		return strictIdentityHash("account", strings.TrimSpace(*accountID)) == expected
	}
	if email := claim.Token.Token.Email; email != nil && strings.TrimSpace(*email) != "" {
		return strictIdentityHash("email", strings.ToLower(strings.TrimSpace(*email))) == expected
	}
	return false
}

func strictIdentityHash(kind, value string) string {
	sum := blake2b.Sum256([]byte(kind + ":" + value))
	return hex.EncodeToString(sum[:])
}

func (m *Manager) BindPromptResponseOwner(store affinity.Store, responseID string, claim *Claim, ttl time.Duration) {
	if store == nil || responseID == "" || claim == nil || claim.TokenID() <= 0 {
		return
	}
	binding := affinity.ResponseOwnerBinding{TokenID: claim.TokenID()}
	if lock := priceLockFromClaim(claim); lock != nil {
		binding.MarketplacePriceLock = lock
	}
	store.BindResponseOwner(responseID, binding, ttl)
}

func (m *Manager) RemovePromptAffinityToken(store affinity.Store, tokenID int64) {
	if store == nil || tokenID <= 0 {
		return
	}
	store.RemoveToken(tokenID)
}

func (m *Manager) snapshotForClaim(ctx context.Context, intent Intent) (*Snapshot, *atomic.Uint64, error) {
	cursorState := &m.cursor
	snapshot := m.Snapshot()
	if !isMarketplaceSelection(intent.SelectionMode) && intent.OwnerUserID > 0 {
		ownerState := m.ownerState(intent.OwnerUserID)
		ownerState.lastUsedUnix.Store(time.Now().UTC().Unix())
		cursorState = &ownerState.cursor
		snapshot = ownerState.snapshotValue()
	}
	if m.snapshotStale(snapshot) {
		if err := m.refreshSnapshotForIntent(ctx, intent); err != nil {
			if m.logger != nil {
				m.logger.Warn("stale token snapshot refresh failed", "error", err)
			}
			if m.snapshotStale(snapshot) {
				return nil, nil, ErrSnapshotStale
			}
		}
		snapshot = m.snapshotForIntent(intent)
	}
	if len(snapshot.Ready) == 0 {
		if err := m.refreshSnapshotForIntent(ctx, intent); err != nil && m.logger != nil {
			m.logger.Warn("token snapshot refresh during claim failed", "error", err)
		}
		snapshot = m.snapshotForIntent(intent)
	}
	if len(snapshot.Ready) == 0 {
		return nil, nil, ErrNoToken
	}
	return snapshot, cursorState, nil
}

func (m *Manager) claimSpecific(ctx context.Context, snapshot *Snapshot, intent Intent, tokenID int64, wait time.Duration, reason string, candidateCount ...int) *Claim {
	if tokenID <= 0 || snapshot == nil {
		return nil
	}
	if _, excluded := intent.ExcludeTokenIDs[tokenID]; excluded {
		return nil
	}
	candidate := snapshot.ByID[tokenID]
	if candidate == nil || !tokenMatchesIntent(candidate, intent) {
		return nil
	}
	activeCap := m.ActiveStreamCap()
	deadline := time.Now().Add(wait)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		activeBefore := candidate.Active.Load()
		if activeBefore >= activeCap {
			if wait <= 0 || time.Now().After(deadline) {
				m.recordOverCapDenial()
				return nil
			}
		} else {
			next := candidate.Active.Add(1)
			if next <= activeCap {
				count := len(snapshot.Ready)
				if len(candidateCount) > 0 && candidateCount[0] > 0 {
					count = candidateCount[0]
				}
				return m.newClaim(snapshot, intent, candidate, reason, activeBefore, next, activeCap, count, time.Now().UTC())
			}
			candidate.Active.Add(-1)
			m.recordOverCapDenial()
		}
		if wait <= 0 || time.Now().After(deadline) {
			return nil
		}
		sleep := 50 * time.Millisecond
		if remaining := time.Until(deadline); remaining < sleep {
			sleep = remaining
		}
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func tokenMatchesIntent(candidate *RuntimeToken, intent Intent) bool {
	if candidate == nil {
		return false
	}
	if intent.TargetTokenID > 0 && candidate.Token.ID != intent.TargetTokenID {
		return false
	}
	if intent.RequireNonFree && candidate.Token.PlanType != nil && strings.EqualFold(strings.TrimSpace(*candidate.Token.PlanType), "free") {
		return false
	}
	if intent.RequiredPlan != "" && planKeyForToken(candidate) != normalizePlanKey(intent.RequiredPlan) {
		return false
	}
	if intent.RequireFast {
		if _, ok := intent.FastEligibleTokens[candidate.Token.ID]; !ok {
			return false
		}
	}
	if isMarketplaceSelection(intent.SelectionMode) {
		if intent.ExcludeOwnerUserID > 0 && candidate.Token.OwnerUserID == intent.ExcludeOwnerUserID {
			return false
		}
		if intent.OwnerUserID > 0 && candidate.Token.OwnerUserID == intent.OwnerUserID {
			return true
		}
		if candidate.Token.ShareEnabled && shareStatusSelectable(candidate.Token.ShareStatus) {
			return true
		}
		return false
	} else if intent.OwnerUserID > 0 && candidate.Token.OwnerUserID != intent.OwnerUserID {
		return false
	}
	return true
}

func shareStatusSelectable(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "", "active", "enabled", "approved":
		return true
	default:
		return false
	}
}

func sortedLaneCandidates(affinityKey string, snapshot *Snapshot, intent Intent, tokenIDs []int64) []int64 {
	seen := map[int64]struct{}{}
	candidates := make([]int64, 0, len(tokenIDs))
	for _, tokenID := range tokenIDs {
		if tokenID <= 0 {
			continue
		}
		if _, ok := seen[tokenID]; ok {
			continue
		}
		seen[tokenID] = struct{}{}
		if _, excluded := intent.ExcludeTokenIDs[tokenID]; excluded {
			continue
		}
		token := snapshot.ByID[tokenID]
		if token == nil || !tokenMatchesIntent(token, intent) || token.Active.Load() >= 1<<60 {
			continue
		}
		candidates = append(candidates, tokenID)
	}
	sortTokenCandidates(affinityKey, snapshot, candidates)
	return candidates
}

func sortNewLaneTokenCandidates(affinityKey string, snapshot *Snapshot, intent Intent, tokenIDs []int64) {
	if !isMarketplacePriceSelection(intent.SelectionMode) {
		sortTokenCandidates(affinityKey, snapshot, tokenIDs)
		return
	}
	sort.SliceStable(tokenIDs, func(i, j int) bool {
		left := snapshot.ByID[tokenIDs[i]]
		right := snapshot.ByID[tokenIDs[j]]
		if left == nil || right == nil {
			return right != nil
		}
		if marketplacePriceBucket(left) != marketplacePriceBucket(right) {
			return marketplacePriceBucket(left) < marketplacePriceBucket(right)
		}
		if left.Active.Load() != right.Active.Load() {
			return left.Active.Load() < right.Active.Load()
		}
		if left.RecentTTFTMs.Load() != right.RecentTTFTMs.Load() {
			return left.RecentTTFTMs.Load() < right.RecentTTFTMs.Load()
		}
		return rendezvousScore(affinityKey, tokenIDs[i]) > rendezvousScore(affinityKey, tokenIDs[j])
	})
}

func sortedNewLaneCandidates(affinityKey string, snapshot *Snapshot, intent Intent) []int64 {
	candidates := make([]int64, 0, len(snapshot.Ready))
	for _, token := range snapshot.Ready {
		if token == nil {
			continue
		}
		tokenID := token.Token.ID
		if _, excluded := intent.ExcludeTokenIDs[tokenID]; excluded {
			continue
		}
		if !tokenMatchesIntent(token, intent) || token.Active.Load() >= 1<<60 {
			continue
		}
		candidates = append(candidates, tokenID)
	}
	sortNewLaneTokenCandidates(affinityKey, snapshot, intent, candidates)
	return candidates
}

func sortTokenCandidates(affinityKey string, snapshot *Snapshot, tokenIDs []int64) {
	sort.SliceStable(tokenIDs, func(i, j int) bool {
		left := snapshot.ByID[tokenIDs[i]]
		right := snapshot.ByID[tokenIDs[j]]
		if left == nil || right == nil {
			return right != nil
		}
		if left.Active.Load() != right.Active.Load() {
			return left.Active.Load() < right.Active.Load()
		}
		if left.RecentTTFTMs.Load() != right.RecentTTFTMs.Load() {
			return left.RecentTTFTMs.Load() < right.RecentTTFTMs.Load()
		}
		return rendezvousScore(affinityKey, tokenIDs[i]) > rendezvousScore(affinityKey, tokenIDs[j])
	})
}

func rendezvousScore(affinityKey string, tokenID int64) uint64 {
	hash, err := blake2b.New(8, nil)
	if err != nil {
		return 0
	}
	_, _ = hash.Write([]byte(affinityKey))
	_, _ = hash.Write([]byte(":"))
	_, _ = hash.Write([]byte(strconv.FormatInt(tokenID, 10)))
	sum := hash.Sum(nil)
	if len(sum) < 8 {
		return 0
	}
	return uint64(sum[0])<<56 | uint64(sum[1])<<48 | uint64(sum[2])<<40 | uint64(sum[3])<<32 |
		uint64(sum[4])<<24 | uint64(sum[5])<<16 | uint64(sum[6])<<8 | uint64(sum[7])
}

func tokenMarketplacePriceBPS(token *RuntimeToken) int {
	if token == nil || token.Token.MarketplacePriceBPS == nil {
		return store.DefaultMarketplacePriceBPS
	}
	value := *token.Token.MarketplacePriceBPS
	if value < 0 {
		return 0
	}
	if value > store.MaxMarketplacePriceBPS {
		return store.MaxMarketplacePriceBPS
	}
	return value
}

func tokenMarketplacePriceBPSFromClaim(claim *Claim) int {
	if claim == nil {
		return store.DefaultMarketplacePriceBPS
	}
	return tokenMarketplacePriceBPS(claim.Token)
}

func tokenMarketplacePriceSource(claim *Claim) string {
	if claim == nil || claim.Token == nil {
		return "owner_default"
	}
	source := strings.TrimSpace(claim.Token.Token.MarketplacePriceSource)
	if source == "" {
		return "owner_default"
	}
	return source
}

func marketplacePriceBucket(token *RuntimeToken) int {
	const bucketSizeBPS = 10
	return tokenMarketplacePriceBPS(token) / bucketSizeBPS
}

func bindLane(store affinity.Store, affinityKey string, claim *Claim, maxLanes int, ttl time.Duration, preferPrimary bool, preferredLock *affinity.MarketplacePriceLock) (*int, int, *affinity.MarketplacePriceLock, string) {
	if store == nil || affinityKey == "" || claim == nil || claim.TokenID() <= 0 {
		return nil, 0, nil, ""
	}
	tokenID := claim.TokenID()
	lane, ok := store.Get(affinityKey)
	if !ok {
		lane = affinity.Lane{PrimaryTokenID: tokenID}
		lock := marketplacePriceLockForLane(affinityKey, claim, preferredLock)
		lane.SetMarketplacePriceLock(tokenID, lock)
		store.Put(affinityKey, lane, ttl)
		index := 0
		return &index, 1, &lock, "created"
	}
	lock, status := ensureLaneMarketplacePriceLock(&lane, affinityKey, claim, preferredLock)
	if lane.PrimaryTokenID == 0 || (preferPrimary && lane.PrimaryTokenID == tokenID) {
		lane.PrimaryTokenID = tokenID
	}
	if lane.PrimaryTokenID == tokenID {
		store.Put(affinityKey, lane, ttl)
		index := 0
		return &index, laneSize(lane), lock, status
	}
	for index, id := range lane.SecondaryTokenIDs {
		if id == tokenID {
			store.Put(affinityKey, lane, ttl)
			resolved := index + 1
			return &resolved, laneSize(lane), lock, status
		}
	}
	if laneSize(lane) < maxLanes {
		lane.SecondaryTokenIDs = append(lane.SecondaryTokenIDs, tokenID)
		store.Put(affinityKey, lane, ttl)
		index := len(lane.SecondaryTokenIDs)
		return &index, laneSize(lane), lock, status
	}
	store.Put(affinityKey, lane, ttl)
	return nil, laneSize(lane), lock, status
}

func ensureLaneMarketplacePriceLock(lane *affinity.Lane, affinityKey string, claim *Claim, preferredLock *affinity.MarketplacePriceLock) (*affinity.MarketplacePriceLock, string) {
	if lane == nil || claim == nil || claim.TokenID() <= 0 {
		return nil, ""
	}
	tokenID := claim.TokenID()
	if lock, ok := lane.MarketplacePriceLock(tokenID); ok {
		return lock, "hit"
	}
	lock := marketplacePriceLockForLane(affinityKey, claim, preferredLock)
	lane.SetMarketplacePriceLock(tokenID, lock)
	return &lock, "backfilled"
}

func responseOwnerPriceLock(store affinity.Store, responseID string, binding affinity.ResponseOwnerBinding, claim *Claim, intent Intent, ttl time.Duration) (*affinity.MarketplacePriceLock, string) {
	if claim == nil || !isMarketplaceSelection(intent.SelectionMode) {
		return nil, ""
	}
	if binding.MarketplacePriceLock != nil {
		return binding.MarketplacePriceLock, "response_hit"
	}
	lock := marketplacePriceLockForResponse(responseID, claim)
	if store != nil && responseID != "" {
		binding.TokenID = claim.TokenID()
		binding.MarketplacePriceLock = &lock
		store.BindResponseOwner(responseID, binding, ttl)
	}
	return &lock, "response_backfilled"
}

func marketplacePriceLockForLane(affinityKey string, claim *Claim, preferredLock *affinity.MarketplacePriceLock) affinity.MarketplacePriceLock {
	if preferredLock != nil {
		lock := *preferredLock
		if lock.LockedAt.IsZero() {
			lock.LockedAt = time.Now().UTC()
		}
		if lock.Source == "" {
			lock.Source = tokenMarketplacePriceSource(claim)
		}
		if lock.ContractKey == "" {
			lock.ContractKey = marketplaceContractKey("lane", affinityKey, claim.TokenID())
		}
		return lock
	}
	lock := marketplacePriceLockFromClaim(claim)
	lock.ContractKey = marketplaceContractKey("lane", affinityKey, claim.TokenID())
	return lock
}

func marketplacePriceLockForResponse(responseID string, claim *Claim) affinity.MarketplacePriceLock {
	lock := marketplacePriceLockFromClaim(claim)
	lock.ContractKey = marketplaceContractKey("response", responseID, claim.TokenID())
	return lock
}

func marketplacePriceLockFromClaim(claim *Claim) affinity.MarketplacePriceLock {
	return affinity.MarketplacePriceLock{
		PriceBPS: tokenMarketplacePriceBPSFromClaim(claim),
		Source:   tokenMarketplacePriceSource(claim),
		LockedAt: time.Now().UTC(),
	}
}

func applyClaimMarketplacePriceLock(claim *Claim, lock *affinity.MarketplacePriceLock, status string) {
	if claim == nil || lock == nil {
		return
	}
	claim.MarketplacePriceBPS = lock.PriceBPS
	claim.MarketplacePriceSource = lock.Source
	claim.MarketplacePriceLocked = true
	if !lock.LockedAt.IsZero() {
		lockedAt := lock.LockedAt
		claim.MarketplacePriceLockedAt = &lockedAt
	}
	claim.MarketplacePriceLockStatus = status
	claim.MarketplacePriceContractKey = lock.ContractKey
}

func priceLockFromClaim(claim *Claim) *affinity.MarketplacePriceLock {
	if claim == nil || !claim.MarketplacePriceLocked {
		return nil
	}
	lock := affinity.MarketplacePriceLock{
		PriceBPS:    claim.MarketplacePriceBPS,
		Source:      claim.MarketplacePriceSource,
		ContractKey: claim.MarketplacePriceContractKey,
	}
	if claim.MarketplacePriceLockedAt != nil {
		lock.LockedAt = *claim.MarketplacePriceLockedAt
	} else {
		lock.LockedAt = time.Now().UTC()
	}
	return &lock
}

func marketplaceContractKey(kind, key string, tokenID int64) string {
	hash, err := blake2b.New(16, nil)
	if err != nil {
		return ""
	}
	_, _ = hash.Write([]byte(kind))
	_, _ = hash.Write([]byte(":"))
	_, _ = hash.Write([]byte(key))
	_, _ = hash.Write([]byte(":"))
	_, _ = hash.Write([]byte(strconv.FormatInt(tokenID, 10)))
	return hex.EncodeToString(hash.Sum(nil))
}

func laneSize(lane affinity.Lane) int {
	size := len(lane.SecondaryTokenIDs)
	if lane.PrimaryTokenID > 0 {
		size++
	}
	return size
}

func mergeExcluded(left, right map[int64]struct{}) map[int64]struct{} {
	if len(left) == 0 {
		left = nil
	}
	if len(right) == 0 {
		return left
	}
	merged := make(map[int64]struct{}, len(left)+len(right))
	for key := range left {
		merged[key] = struct{}{}
	}
	for key := range right {
		merged[key] = struct{}{}
	}
	return merged
}

func claimReason(claim *Claim) string {
	if claim == nil || claim.Reason == "" {
		return "snapshot_round_robin"
	}
	return claim.Reason
}
