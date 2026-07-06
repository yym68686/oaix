package tokens

import "context"

type Selector interface {
	Select(ctx context.Context, snapshot *Snapshot, intent Intent, activeCap int64, cursor *uint64) (*RuntimeToken, string)
}

type RoundRobinSelector struct{}

func (RoundRobinSelector) Select(ctx context.Context, snapshot *Snapshot, intent Intent, activeCap int64, cursor *uint64) (*RuntimeToken, string) {
	if snapshot == nil || len(snapshot.Ready) == 0 {
		return nil, ""
	}
	total := uint64(len(snapshot.Ready))
	start := *cursor + 1
	*cursor = start
	for offset := uint64(0); offset < total; offset++ {
		select {
		case <-ctx.Done():
			return nil, ""
		default:
		}
		index := int((start + offset) % total)
		candidate := snapshot.Ready[index]
		if candidate == nil {
			continue
		}
		if _, excluded := intent.ExcludeTokenIDs[candidate.Token.ID]; excluded {
			continue
		}
		if !tokenMatchesIntent(candidate, intent) {
			continue
		}
		if candidate.Active.Load() >= activeCap {
			continue
		}
		return candidate, "snapshot_round_robin"
	}
	return nil, ""
}

type FillFirstSelector struct{}

func (FillFirstSelector) Select(ctx context.Context, snapshot *Snapshot, intent Intent, activeCap int64, cursor *uint64) (*RuntimeToken, string) {
	if snapshot == nil {
		return nil, ""
	}
	for _, candidate := range snapshot.Ready {
		select {
		case <-ctx.Done():
			return nil, ""
		default:
		}
		if candidate == nil {
			continue
		}
		if _, excluded := intent.ExcludeTokenIDs[candidate.Token.ID]; excluded {
			continue
		}
		if !tokenMatchesIntent(candidate, intent) || candidate.Active.Load() >= activeCap {
			continue
		}
		return candidate, "snapshot_fill_first"
	}
	return nil, ""
}

type LRUSelector struct{}

func (LRUSelector) Select(ctx context.Context, snapshot *Snapshot, intent Intent, activeCap int64, cursor *uint64) (*RuntimeToken, string) {
	return FillFirstSelector{}.Select(ctx, snapshot, intent, activeCap, cursor)
}

type QuotaAwareSelector struct{}

func (QuotaAwareSelector) Select(ctx context.Context, snapshot *Snapshot, intent Intent, activeCap int64, cursor *uint64) (*RuntimeToken, string) {
	nextIntent := intent
	nextIntent.RequireNonFree = true
	if token, _ := (FillFirstSelector{}).Select(ctx, snapshot, nextIntent, activeCap, cursor); token != nil {
		return token, "snapshot_quota_aware"
	}
	return (FillFirstSelector{}).Select(ctx, snapshot, intent, activeCap, cursor)
}

type LatencyAwareSelector struct{}

func (LatencyAwareSelector) Select(ctx context.Context, snapshot *Snapshot, intent Intent, activeCap int64, cursor *uint64) (*RuntimeToken, string) {
	if snapshot == nil {
		return nil, ""
	}
	var selected *RuntimeToken
	var selectedLatency int64
	for _, candidate := range snapshot.Ready {
		select {
		case <-ctx.Done():
			return nil, ""
		default:
		}
		if candidate == nil {
			continue
		}
		if _, excluded := intent.ExcludeTokenIDs[candidate.Token.ID]; excluded {
			continue
		}
		if !tokenMatchesIntent(candidate, intent) {
			continue
		}
		if candidate.Active.Load() >= activeCap {
			continue
		}
		latency := candidate.RecentTTFTMs.Load()
		if selected == nil || latency < selectedLatency {
			selected = candidate
			selectedLatency = latency
		}
	}
	if selected == nil {
		return nil, ""
	}
	return selected, "snapshot_latency_aware"
}

type MarketplacePriceSelector struct {
	Fallback Selector
}

func (s MarketplacePriceSelector) Select(ctx context.Context, snapshot *Snapshot, intent Intent, activeCap int64, cursor *uint64) (*RuntimeToken, string) {
	if !isMarketplacePriceSelection(intent.SelectionMode) {
		return marketplaceFallbackSelector(s.Fallback).Select(ctx, snapshot, intent, activeCap, cursor)
	}
	if snapshot == nil || len(snapshot.Ready) == 0 {
		return nil, ""
	}
	bestBucket := -1
	ready := make([]*RuntimeToken, 0, len(snapshot.Ready))
	for _, candidate := range snapshot.Ready {
		select {
		case <-ctx.Done():
			return nil, ""
		default:
		}
		if candidate == nil {
			continue
		}
		if _, excluded := intent.ExcludeTokenIDs[candidate.Token.ID]; excluded {
			continue
		}
		if !tokenMatchesIntent(candidate, intent) || candidate.Active.Load() >= activeCap {
			continue
		}
		bucket := marketplacePriceBucket(candidate)
		if bestBucket == -1 || bucket < bestBucket {
			bestBucket = bucket
			ready = ready[:0]
		}
		if bucket == bestBucket {
			ready = append(ready, candidate)
		}
	}
	if len(ready) == 0 {
		return nil, ""
	}
	filtered := &Snapshot{
		Ready:    ready,
		ByID:     snapshot.ByID,
		LoadedAt: snapshot.LoadedAt,
		Version:  snapshot.Version,
	}
	token, reason := marketplaceFallbackSelector(s.Fallback).Select(ctx, filtered, intent, activeCap, cursor)
	if token == nil {
		return nil, ""
	}
	if reason == "" {
		reason = "snapshot_selected"
	}
	return token, "marketplace_price_" + reason
}

func marketplaceFallbackSelector(selector Selector) Selector {
	if selector == nil {
		return RoundRobinSelector{}
	}
	return selector
}

type PromptAffinitySelector struct {
	PreferredTokenID int64
	Fallback         Selector
}

func (s PromptAffinitySelector) Select(ctx context.Context, snapshot *Snapshot, intent Intent, activeCap int64, cursor *uint64) (*RuntimeToken, string) {
	if snapshot != nil && s.PreferredTokenID > 0 {
		_, excluded := intent.ExcludeTokenIDs[s.PreferredTokenID]
		if candidate := snapshot.ByID[s.PreferredTokenID]; !excluded && candidate != nil && candidate.Active.Load() < activeCap {
			if !tokenMatchesIntent(candidate, intent) {
				goto fallback
			}
			return candidate, "prompt_affinity_preferred"
		}
	}
fallback:
	fallback := s.Fallback
	if fallback == nil {
		fallback = RoundRobinSelector{}
	}
	token, reason := fallback.Select(ctx, snapshot, intent, activeCap, cursor)
	if token == nil {
		return nil, ""
	}
	return token, "prompt_affinity_" + reason
}
