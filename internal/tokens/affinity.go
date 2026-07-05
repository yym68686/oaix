package tokens

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/blake2b"

	"github.com/yym68686/oaix/internal/affinity"
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
		if ownerID, ok := store.GetResponseOwner(opts.PreviousResponseID); ok {
			if claim := m.claimSpecific(ctx, snapshot, intent, ownerID, opts.PreviousOwnerWait, "previous_owner_hit"); claim != nil {
				result := PromptAffinityResult{Result: "previous_owner_hit"}
				if opts.AffinityKey != "" {
					index, count := bindLane(store, opts.AffinityKey, ownerID, opts.MaxLanesPerKey, opts.LaneTTL, true)
					result.LaneIndex = index
					result.LaneCount = count
				}
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
					index, count := bindLane(store, opts.AffinityKey, lane.PrimaryTokenID, opts.MaxLanesPerKey, opts.LaneTTL, true)
					return claim, PromptAffinityResult{Result: "primary_hit", LaneIndex: index, LaneCount: count}, nil
				}
			}
			for _, tokenID := range sortedLaneCandidates(opts.AffinityKey, snapshot, intent, lane.SecondaryTokenIDs) {
				if claim := m.claimSpecific(ctx, snapshot, intent, tokenID, 0, "lane_hit"); claim != nil {
					index, count := bindLane(store, opts.AffinityKey, tokenID, opts.MaxLanesPerKey, opts.LaneTTL, false)
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
						index, count := bindLane(store, opts.AffinityKey, tokenID, opts.MaxLanesPerKey, opts.LaneTTL, false)
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
						index, count := bindLane(store, opts.AffinityKey, tokenID, opts.MaxLanesPerKey, opts.LaneTTL, false)
						return claim, PromptAffinityResult{Result: "lane_hit_after_wait", LaneIndex: index, LaneCount: count}, nil
					}
				}
			}
		} else {
			for _, tokenID := range sortedNewLaneCandidates(opts.AffinityKey, snapshot, intent) {
				if claim := m.claimSpecific(ctx, snapshot, intent, tokenID, 0, "lane_created"); claim != nil {
					index, count := bindLane(store, opts.AffinityKey, tokenID, opts.MaxLanesPerKey, opts.LaneTTL, true)
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
		index, count := bindLane(store, opts.AffinityKey, claim.TokenID(), opts.MaxLanesPerKey, opts.LaneTTL, false)
		result.LaneIndex = index
		result.LaneCount = count
	}
	return claim, result, err
}

func (m *Manager) BindPromptResponseOwner(store affinity.Store, responseID string, tokenID int64, ttl time.Duration) {
	if store == nil || responseID == "" || tokenID <= 0 {
		return
	}
	store.BindResponseOwner(responseID, tokenID, ttl)
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
	if strings.EqualFold(strings.TrimSpace(intent.SelectionMode), "marketplace") {
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
	if intent.RequireNonFree && candidate.Token.PlanType != nil && *candidate.Token.PlanType == "free" {
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
	sortTokenCandidates(affinityKey, snapshot, candidates)
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

func bindLane(store affinity.Store, affinityKey string, tokenID int64, maxLanes int, ttl time.Duration, preferPrimary bool) (*int, int) {
	if store == nil || affinityKey == "" || tokenID <= 0 {
		return nil, 0
	}
	lane, ok := store.Get(affinityKey)
	if !ok {
		lane = affinity.Lane{PrimaryTokenID: tokenID}
		store.Put(affinityKey, lane, ttl)
		index := 0
		return &index, 1
	}
	if lane.PrimaryTokenID == 0 || (preferPrimary && lane.PrimaryTokenID == tokenID) {
		lane.PrimaryTokenID = tokenID
	}
	if lane.PrimaryTokenID == tokenID {
		store.Put(affinityKey, lane, ttl)
		index := 0
		return &index, laneSize(lane)
	}
	for index, id := range lane.SecondaryTokenIDs {
		if id == tokenID {
			store.Put(affinityKey, lane, ttl)
			resolved := index + 1
			return &resolved, laneSize(lane)
		}
	}
	if laneSize(lane) < maxLanes {
		lane.SecondaryTokenIDs = append(lane.SecondaryTokenIDs, tokenID)
		store.Put(affinityKey, lane, ttl)
		index := len(lane.SecondaryTokenIDs)
		return &index, laneSize(lane)
	}
	store.Put(affinityKey, lane, ttl)
	return nil, laneSize(lane)
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
