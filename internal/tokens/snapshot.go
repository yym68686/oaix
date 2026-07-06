package tokens

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

type Source interface {
	ListAvailableTokens(ctx context.Context) ([]store.Token, error)
	TouchTokens(ctx context.Context, tokenIDs []int64, usedAt time.Time) error
	MarkTokenSuccess(ctx context.Context, tokenID int64) error
	MarkTokenError(ctx context.Context, tokenID int64, message string, deactivate bool, cooldownUntil *time.Time) error
}

type Snapshot struct {
	Version  int64
	LoadedAt time.Time
	ByID     map[int64]*RuntimeToken
	ByScope  map[string][]*RuntimeToken
	ByModel  map[string][]*RuntimeToken
	ByPlan   map[string][]*RuntimeToken
	Cooling  map[int64]time.Time
	Ready    []*RuntimeToken
}

type RuntimeToken struct {
	Token        store.Token
	Active       atomic.Int64
	RecentTTFTMs atomic.Int64
}

type ReadyTransitionHandler func(ctx context.Context, tokens []store.Token)

type Manager struct {
	source          Source
	logger          *slog.Logger
	maxAge          time.Duration
	refreshInterval time.Duration
	activeCap       atomic.Int64
	selector        Selector
	selectorMisses  atomic.Int64
	claimSequence   atomic.Uint64
	claimsTotal     atomic.Int64
	releasesTotal   atomic.Int64
	overCapDenials  atomic.Int64
	noTokenDenials  atomic.Int64
	version         atomic.Int64
	snapshot        atomic.Value
	ownerSnapshots  sync.Map
	cursor          atomic.Uint64
	stopOnce        sync.Once
	stopCh          chan struct{}
	refreshMu       sync.Mutex
	readyMu         sync.RWMutex
	readyHandler    ReadyTransitionHandler
}

type TokenPoolManager = Manager

type scopedSource interface {
	ListAvailableTokensScoped(ctx context.Context, scope store.ResourceScope) ([]store.Token, error)
}

type ownerSnapshotState struct {
	ownerID      int64
	snapshot     atomic.Value
	cursor       atomic.Uint64
	lastUsedUnix atomic.Int64
}

type Stats struct {
	Version       int64         `json:"version"`
	LoadedAt      time.Time     `json:"loaded_at"`
	AgeMs         int64         `json:"age_ms"`
	ReadyTokens   int           `json:"ready_tokens"`
	ActiveStreams int64         `json:"active_streams"`
	ActiveCap     int64         `json:"active_stream_cap"`
	Stale         bool          `json:"stale"`
	Claims        ClaimCounters `json:"claim_counters"`
}

type ClaimCounters struct {
	ClaimsTotal         int64 `json:"claims_total"`
	ReleasesTotal       int64 `json:"releases_total"`
	SelectorMissesTotal int64 `json:"selector_misses_total"`
	DeniedOverCapTotal  int64 `json:"denied_over_cap_total"`
	DeniedNoTokenTotal  int64 `json:"denied_no_token_total"`
}

type ClaimTelemetry struct {
	ClaimID             uint64     `json:"claim_id"`
	TokenID             int64      `json:"token_id"`
	OwnerUserID         int64      `json:"owner_user_id"`
	Reason              string     `json:"reason"`
	SnapshotScope       string     `json:"snapshot_scope"`
	SnapshotVersion     int64      `json:"snapshot_version"`
	SnapshotAgeMs       int64      `json:"snapshot_age_ms"`
	SnapshotReadyTokens int        `json:"snapshot_ready_tokens"`
	ActiveCap           int64      `json:"active_cap"`
	ActiveBefore        int64      `json:"active_before"`
	ActiveAfter         int64      `json:"active_after"`
	CandidateCount      int        `json:"candidate_count"`
	SelectedAt          time.Time  `json:"selected_at"`
	ReleasedAt          *time.Time `json:"released_at,omitempty"`
	HeldMs              *int64     `json:"held_ms,omitempty"`
	ActiveAfterRelease  *int64     `json:"active_after_release,omitempty"`
}

type SnapshotDiagnostics struct {
	Scope         string          `json:"scope"`
	OwnerUserID   *int64          `json:"owner_user_id,omitempty"`
	Version       int64           `json:"version"`
	LoadedAt      time.Time       `json:"loaded_at"`
	AgeMs         int64           `json:"age_ms"`
	ReadyTokens   int             `json:"ready_tokens"`
	ActiveStreams int64           `json:"active_streams"`
	ActiveCap     int64           `json:"active_stream_cap"`
	Stale         bool            `json:"stale"`
	TopActive     []TokenActivity `json:"top_active_tokens,omitempty"`
}

type TokenActivity struct {
	SnapshotScope   string     `json:"snapshot_scope"`
	SnapshotVersion int64      `json:"snapshot_version"`
	TokenID         int64      `json:"token_id"`
	OwnerUserID     int64      `json:"owner_user_id"`
	AccountID       *string    `json:"account_id,omitempty"`
	PlanType        *string    `json:"plan_type,omitempty"`
	ActiveStreams   int64      `json:"active_streams"`
	ActiveCap       int64      `json:"active_stream_cap"`
	IsActive        bool       `json:"is_active"`
	ShareEnabled    bool       `json:"share_enabled"`
	ShareStatus     string     `json:"share_status,omitempty"`
	LastUsedAt      *time.Time `json:"last_used_at,omitempty"`
	CooldownUntil   *time.Time `json:"cooldown_until,omitempty"`
	LastError       *string    `json:"last_error,omitempty"`
}

type Intent struct {
	Endpoint           string
	Model              string
	OwnerUserID        int64
	SelectionMode      string
	ExcludeOwnerUserID int64
	TargetTokenID      int64
	PromptCacheKeyHash string
	ExcludeTokenIDs    map[int64]struct{}
	RequireNonFree     bool
}

type Claim struct {
	Token      *RuntimeToken
	Reason     string
	released   atomic.Bool
	releaseFn  func(*RuntimeToken) int64
	Telemetry  ClaimTelemetry
	selectedAt time.Time
}

func NewManager(source Source, logger *slog.Logger, maxAge, refreshInterval time.Duration, activeCap int64) *Manager {
	if maxAge <= 0 {
		maxAge = 10 * time.Second
	}
	if refreshInterval <= 0 {
		refreshInterval = 2 * time.Second
	}
	manager := &Manager{
		source:          source,
		logger:          logger,
		maxAge:          maxAge,
		refreshInterval: refreshInterval,
		stopCh:          make(chan struct{}),
	}
	manager.SetActiveStreamCap(activeCap)
	manager.snapshot.Store(emptySnapshot())
	return manager
}

func (m *Manager) ActiveStreamCap() int64 {
	if m == nil {
		return 0
	}
	value := m.activeCap.Load()
	if value <= 0 {
		return 1
	}
	return value
}

func (m *Manager) SetActiveStreamCap(value int64) int64 {
	if m == nil {
		return 0
	}
	if value <= 0 {
		value = 1
	}
	m.activeCap.Store(value)
	return value
}

func (m *Manager) Start(ctx context.Context) {
	go func() {
		_ = m.Refresh(ctx)
		ticker := time.NewTicker(m.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-m.stopCh:
				return
			case <-ticker.C:
				if err := m.Refresh(ctx); err != nil && m.logger != nil {
					m.logger.Warn("token snapshot refresh failed", "error", err)
				}
				if refreshed, err := m.RefreshActiveOwners(ctx); err != nil && m.logger != nil {
					m.logger.Warn("active owner token snapshot refresh failed", "error", err)
				} else if refreshed > 0 && m.logger != nil {
					m.logger.Debug("active owner token snapshots refreshed", "owners", refreshed)
				}
			}
		}
	}()
}

func (m *Manager) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
}

func (m *Manager) SetReadyTransitionHandler(handler ReadyTransitionHandler) {
	if m == nil {
		return
	}
	m.readyMu.Lock()
	m.readyHandler = handler
	m.readyMu.Unlock()
}

func (m *Manager) Refresh(ctx context.Context) error {
	m.refreshMu.Lock()
	rows, err := m.listAvailableTokens(ctx, store.AllResources())
	if err != nil {
		m.refreshMu.Unlock()
		return err
	}
	version := m.version.Add(1)
	current := m.Snapshot()
	next := buildSnapshot(rows, current, version)
	readyTransitions := newlyReadyTokens(current, next)
	m.snapshot.Store(next)
	m.refreshMu.Unlock()
	if m.logger != nil {
		m.logger.Info("token snapshot refreshed", "version", version, "ready_tokens", len(next.Ready))
	}
	m.notifyReadyTransitions(ctx, readyTransitions)
	return nil
}

func (m *Manager) RefreshOwner(ctx context.Context, ownerUserID int64) error {
	if ownerUserID <= 0 {
		return m.Refresh(ctx)
	}
	state := m.ownerState(ownerUserID)
	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()
	rows, err := m.listAvailableTokens(ctx, store.OwnerResources(ownerUserID))
	if err != nil {
		return err
	}
	version := m.version.Add(1)
	next := buildSnapshot(rows, state.snapshotValue(), version)
	state.snapshot.Store(next)
	if m.logger != nil {
		m.logger.Debug("owner token snapshot refreshed", "owner_user_id", ownerUserID, "version", version, "ready_tokens", len(next.Ready))
	}
	return nil
}

func (m *Manager) RefreshActiveOwners(ctx context.Context) (int, error) {
	if m == nil {
		return 0, nil
	}
	now := time.Now().UTC()
	refreshWindow := m.refreshInterval * 10
	if refreshWindow < 5*time.Minute {
		refreshWindow = 5 * time.Minute
	}
	retentionWindow := refreshWindow * 6
	refreshed := 0
	var firstErr error
	m.ownerSnapshots.Range(func(key, value any) bool {
		state, ok := value.(*ownerSnapshotState)
		if !ok || state == nil {
			m.ownerSnapshots.Delete(key)
			return true
		}
		lastUsedUnix := state.lastUsedUnix.Load()
		if lastUsedUnix <= 0 {
			return true
		}
		lastUsed := time.Unix(lastUsedUnix, 0).UTC()
		age := now.Sub(lastUsed)
		if age > retentionWindow {
			m.ownerSnapshots.Delete(key)
			return true
		}
		if age > refreshWindow {
			return true
		}
		if err := m.RefreshOwner(ctx, state.ownerID); err != nil {
			firstErr = err
			return false
		}
		refreshed++
		return true
	})
	return refreshed, firstErr
}

func (m *Manager) Snapshot() *Snapshot {
	snapshot, _ := m.snapshot.Load().(*Snapshot)
	if snapshot == nil {
		return emptySnapshot()
	}
	return snapshot
}

func (m *Manager) SnapshotForOwner(ownerUserID int64) *Snapshot {
	if ownerUserID <= 0 {
		return m.Snapshot()
	}
	state, ok := m.ownerSnapshots.Load(ownerUserID)
	if !ok {
		return emptySnapshot()
	}
	ownerState, ok := state.(*ownerSnapshotState)
	if !ok || ownerState == nil {
		return emptySnapshot()
	}
	return ownerState.snapshotValue()
}

func (m *Manager) Stats() Stats {
	snapshot := m.Snapshot()
	now := time.Now().UTC()
	age := now.Sub(snapshot.LoadedAt)
	if snapshot.LoadedAt.IsZero() {
		age = 0
	}
	return Stats{
		Version:       snapshot.Version,
		LoadedAt:      snapshot.LoadedAt,
		AgeMs:         age.Milliseconds(),
		ReadyTokens:   len(snapshot.Ready),
		ActiveStreams: m.ActiveStreams(),
		ActiveCap:     m.ActiveStreamCap(),
		Stale:         snapshot.LoadedAt.IsZero() || now.Sub(snapshot.LoadedAt) > m.maxAge,
		Claims:        m.ClaimCounters(),
	}
}

func (m *Manager) ClaimCounters() ClaimCounters {
	if m == nil {
		return ClaimCounters{}
	}
	return ClaimCounters{
		ClaimsTotal:         m.claimsTotal.Load(),
		ReleasesTotal:       m.releasesTotal.Load(),
		SelectorMissesTotal: m.selectorMisses.Load(),
		DeniedOverCapTotal:  m.overCapDenials.Load(),
		DeniedNoTokenTotal:  m.noTokenDenials.Load(),
	}
}

func (m *Manager) ActiveStreams() int64 {
	if m == nil {
		return 0
	}
	active := snapshotActiveStreams(m.Snapshot())
	m.ownerSnapshots.Range(func(_, value any) bool {
		state, ok := value.(*ownerSnapshotState)
		if !ok || state == nil {
			return true
		}
		active += snapshotActiveStreams(state.snapshotValue())
		return true
	})
	return active
}

func (m *Manager) ActiveStreamsForToken(tokenID int64, ownerUserID int64) int64 {
	if m == nil || tokenID == 0 {
		return 0
	}
	var active int64
	active += snapshotTokenActive(m.Snapshot(), tokenID)
	if ownerUserID > 0 {
		if value, ok := m.ownerSnapshots.Load(ownerUserID); ok {
			if state, ok := value.(*ownerSnapshotState); ok && state != nil {
				active += snapshotTokenActive(state.snapshotValue(), tokenID)
			}
		}
	}
	return active
}

func (m *Manager) SnapshotDiagnostics(limit int) []SnapshotDiagnostics {
	if m == nil {
		return nil
	}
	activeCap := m.ActiveStreamCap()
	now := time.Now().UTC()
	summaries := []SnapshotDiagnostics{
		snapshotDiagnostics("global", nil, m.Snapshot(), now, m.maxAge, activeCap, limit),
	}
	m.ownerSnapshots.Range(func(_, value any) bool {
		state, ok := value.(*ownerSnapshotState)
		if !ok || state == nil {
			return true
		}
		ownerID := state.ownerID
		summaries = append(summaries, snapshotDiagnostics("owner", &ownerID, state.snapshotValue(), now, m.maxAge, activeCap, limit))
		return true
	})
	sort.SliceStable(summaries[1:], func(i, j int) bool {
		left := summaries[i+1]
		right := summaries[j+1]
		if left.ActiveStreams != right.ActiveStreams {
			return left.ActiveStreams > right.ActiveStreams
		}
		leftOwner, rightOwner := int64(0), int64(0)
		if left.OwnerUserID != nil {
			leftOwner = *left.OwnerUserID
		}
		if right.OwnerUserID != nil {
			rightOwner = *right.OwnerUserID
		}
		return leftOwner < rightOwner
	})
	return summaries
}

func (m *Manager) ActiveTokenDiagnostics(limit int) []TokenActivity {
	if m == nil {
		return nil
	}
	activeCap := m.ActiveStreamCap()
	items := tokenActivities("global", m.Snapshot(), activeCap, limit)
	m.ownerSnapshots.Range(func(_, value any) bool {
		state, ok := value.(*ownerSnapshotState)
		if !ok || state == nil {
			return true
		}
		items = append(items, tokenActivities("owner", state.snapshotValue(), activeCap, limit)...)
		return true
	})
	sortTokenActivities(items)
	if limit > 0 && len(items) > limit {
		return items[:limit]
	}
	return items
}

func snapshotDiagnostics(scope string, ownerUserID *int64, snapshot *Snapshot, now time.Time, maxAge time.Duration, activeCap int64, limit int) SnapshotDiagnostics {
	age := snapshotAge(snapshot, now)
	diagnostics := SnapshotDiagnostics{
		Scope:         scope,
		OwnerUserID:   ownerUserID,
		Version:       snapshotVersion(snapshot),
		LoadedAt:      snapshotLoadedAt(snapshot),
		AgeMs:         age.Milliseconds(),
		ReadyTokens:   snapshotReadyTokens(snapshot),
		ActiveStreams: snapshotActiveStreams(snapshot),
		ActiveCap:     activeCap,
		Stale:         snapshot == nil || snapshot.LoadedAt.IsZero() || now.Sub(snapshot.LoadedAt) > maxAge,
	}
	if limit >= 0 {
		diagnostics.TopActive = tokenActivities(scope, snapshot, activeCap, limit)
	}
	return diagnostics
}

func tokenActivities(scope string, snapshot *Snapshot, activeCap int64, limit int) []TokenActivity {
	if snapshot == nil {
		return nil
	}
	items := make([]TokenActivity, 0, len(snapshot.Ready))
	for _, runtimeToken := range snapshot.Ready {
		if runtimeToken == nil {
			continue
		}
		token := runtimeToken.Token
		active := runtimeToken.Active.Load()
		if active <= 0 && limit > 0 {
			continue
		}
		items = append(items, TokenActivity{
			SnapshotScope:   scope,
			SnapshotVersion: snapshot.Version,
			TokenID:         token.ID,
			OwnerUserID:     token.OwnerUserID,
			AccountID:       token.AccountID,
			PlanType:        token.PlanType,
			ActiveStreams:   active,
			ActiveCap:       activeCap,
			IsActive:        token.IsActive,
			ShareEnabled:    token.ShareEnabled,
			ShareStatus:     token.ShareStatus,
			LastUsedAt:      token.LastUsedAt,
			CooldownUntil:   token.CooldownUntil,
			LastError:       token.LastError,
		})
	}
	sortTokenActivities(items)
	if limit > 0 && len(items) > limit {
		return items[:limit]
	}
	return items
}

func sortTokenActivities(items []TokenActivity) {
	sort.SliceStable(items, func(i, j int) bool {
		left := items[i]
		right := items[j]
		if left.ActiveStreams != right.ActiveStreams {
			return left.ActiveStreams > right.ActiveStreams
		}
		if left.SnapshotScope != right.SnapshotScope {
			return left.SnapshotScope < right.SnapshotScope
		}
		return left.TokenID < right.TokenID
	})
}

func snapshotAge(snapshot *Snapshot, now time.Time) time.Duration {
	if snapshot == nil || snapshot.LoadedAt.IsZero() {
		return 0
	}
	return now.Sub(snapshot.LoadedAt)
}

func snapshotVersion(snapshot *Snapshot) int64 {
	if snapshot == nil {
		return 0
	}
	return snapshot.Version
}

func snapshotLoadedAt(snapshot *Snapshot) time.Time {
	if snapshot == nil {
		return time.Time{}
	}
	return snapshot.LoadedAt
}

func snapshotReadyTokens(snapshot *Snapshot) int {
	if snapshot == nil {
		return 0
	}
	return len(snapshot.Ready)
}

func snapshotActiveStreams(snapshot *Snapshot) int64 {
	if snapshot == nil {
		return 0
	}
	var active int64
	for _, token := range snapshot.Ready {
		if token != nil {
			active += token.Active.Load()
		}
	}
	return active
}

func snapshotTokenActive(snapshot *Snapshot, tokenID int64) int64 {
	if snapshot == nil {
		return 0
	}
	token := snapshot.ByID[tokenID]
	if token == nil {
		return 0
	}
	return token.Active.Load()
}

func (m *Manager) Claim(ctx context.Context, intent Intent) (*Claim, error) {
	claimSnapshot, cursorState, err := m.snapshotForClaim(ctx, intent)
	if err != nil {
		if errors.Is(err, ErrNoToken) {
			m.recordNoTokenDenial()
		}
		return nil, err
	}
	if intent.TargetTokenID > 0 {
		if claim := m.claimSpecific(ctx, claimSnapshot, intent, intent.TargetTokenID, 0, "target_token", 1); claim != nil {
			return claim, nil
		}
		if snapshotHasCapBlockedToken(claimSnapshot, intent, m.ActiveStreamCap()) {
			m.recordOverCapDenial()
		}
		m.recordNoTokenDenial()
		return nil, ErrNoToken
	}
	total := uint64(len(claimSnapshot.Ready))
	selector := m.selector
	if selector == nil {
		selector = RoundRobinSelector{}
	}
	if isMarketplacePriceSelection(intent.SelectionMode) {
		selector = MarketplacePriceSelector{Fallback: selector}
	}
	activeCap := m.ActiveStreamCap()
	cursor := cursorState.Load()
	for offset := uint64(0); offset < total; offset++ {
		candidate, reason := selector.Select(ctx, claimSnapshot, intent, activeCap, &cursor)
		cursorState.Store(cursor)
		if candidate == nil {
			break
		}
		activeBefore := candidate.Active.Load()
		next := candidate.Active.Add(1)
		if next > activeCap {
			candidate.Active.Add(-1)
			m.recordOverCapDenial()
			continue
		}
		now := time.Now().UTC()
		return m.newClaim(claimSnapshot, intent, candidate, reason, activeBefore, next, activeCap, int(total), now), nil
	}
	m.selectorMisses.Add(1)
	if snapshotHasCapBlockedToken(claimSnapshot, intent, activeCap) {
		m.recordOverCapDenial()
	}
	m.recordNoTokenDenial()
	return nil, ErrNoToken
}

func (m *Manager) snapshotStale(snapshot *Snapshot) bool {
	if snapshot == nil || snapshot.LoadedAt.IsZero() {
		return true
	}
	return time.Since(snapshot.LoadedAt) > m.maxAge
}

func (c *Claim) Release() {
	if c == nil || c.Token == nil || c.releaseFn == nil {
		return
	}
	if c.released.CompareAndSwap(false, true) {
		held := int64(0)
		if !c.selectedAt.IsZero() {
			held = time.Since(c.selectedAt).Milliseconds()
		}
		releasedAt := time.Now().UTC()
		activeAfter := c.releaseFn(c.Token)
		c.Telemetry.ReleasedAt = &releasedAt
		c.Telemetry.HeldMs = &held
		c.Telemetry.ActiveAfterRelease = &activeAfter
	}
}

func (m *Manager) newClaim(snapshot *Snapshot, intent Intent, token *RuntimeToken, reason string, activeBefore, activeAfter, activeCap int64, candidateCount int, selectedAt time.Time) *Claim {
	if m != nil {
		m.claimsTotal.Add(1)
	}
	claimID := uint64(0)
	if m != nil {
		claimID = m.claimSequence.Add(1)
	}
	telemetry := ClaimTelemetry{
		ClaimID:             claimID,
		TokenID:             token.Token.ID,
		OwnerUserID:         token.Token.OwnerUserID,
		Reason:              reason,
		SnapshotScope:       snapshotScopeForIntent(intent),
		SnapshotVersion:     snapshotVersion(snapshot),
		SnapshotAgeMs:       snapshotAge(snapshot, selectedAt).Milliseconds(),
		SnapshotReadyTokens: snapshotReadyTokens(snapshot),
		ActiveCap:           activeCap,
		ActiveBefore:        activeBefore,
		ActiveAfter:         activeAfter,
		CandidateCount:      candidateCount,
		SelectedAt:          selectedAt,
	}
	return &Claim{
		Token:    token,
		Reason:   reason,
		released: atomic.Bool{},
		releaseFn: func(runtimeToken *RuntimeToken) int64 {
			if m != nil {
				m.releasesTotal.Add(1)
			}
			return runtimeToken.Active.Add(-1)
		},
		Telemetry:  telemetry,
		selectedAt: selectedAt,
	}
}

func (m *Manager) recordOverCapDenial() {
	if m == nil {
		return
	}
	m.overCapDenials.Add(1)
}

func (m *Manager) recordNoTokenDenial() {
	if m == nil {
		return
	}
	m.noTokenDenials.Add(1)
}

func snapshotHasCapBlockedToken(snapshot *Snapshot, intent Intent, activeCap int64) bool {
	if snapshot == nil {
		return false
	}
	for _, token := range snapshot.Ready {
		if token == nil {
			continue
		}
		if _, excluded := intent.ExcludeTokenIDs[token.Token.ID]; excluded {
			continue
		}
		if tokenMatchesIntent(token, intent) && token.Active.Load() >= activeCap {
			return true
		}
	}
	return false
}

func (c *Claim) AccessToken() string {
	if c == nil || c.Token == nil {
		return ""
	}
	return c.Token.Token.AccessToken
}

func (c *Claim) TokenID() int64 {
	if c == nil || c.Token == nil {
		return 0
	}
	return c.Token.Token.ID
}

func (c *Claim) AccountID() *string {
	if c == nil || c.Token == nil {
		return nil
	}
	return c.Token.Token.AccountID
}

var ErrNoToken = errors.New("no available token")
var ErrSnapshotStale = errors.New("token snapshot is stale")

func (m *Manager) SelectorMisses() int64 {
	return m.selectorMisses.Load()
}

func (m *Manager) snapshotForIntent(intent Intent) *Snapshot {
	if isMarketplaceSelection(intent.SelectionMode) {
		return m.Snapshot()
	}
	if intent.OwnerUserID > 0 {
		return m.SnapshotForOwner(intent.OwnerUserID)
	}
	return m.Snapshot()
}

func (m *Manager) refreshSnapshotForIntent(ctx context.Context, intent Intent) error {
	if isMarketplaceSelection(intent.SelectionMode) {
		return m.Refresh(ctx)
	}
	if intent.OwnerUserID > 0 {
		return m.RefreshOwner(ctx, intent.OwnerUserID)
	}
	return m.Refresh(ctx)
}

func snapshotScopeForIntent(intent Intent) string {
	if isMarketplaceSelection(intent.SelectionMode) {
		return "global"
	}
	if intent.OwnerUserID > 0 {
		return "owner"
	}
	return "global"
}

func isMarketplaceSelection(mode string) bool {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "marketplace", "marketplace-priced":
		return true
	default:
		return false
	}
}

func isMarketplacePriceSelection(mode string) bool {
	return strings.EqualFold(strings.TrimSpace(mode), "marketplace-priced")
}

func (m *Manager) ownerState(ownerUserID int64) *ownerSnapshotState {
	if ownerUserID <= 0 {
		return nil
	}
	if value, ok := m.ownerSnapshots.Load(ownerUserID); ok {
		if state, ok := value.(*ownerSnapshotState); ok && state != nil {
			return state
		}
	}
	state := &ownerSnapshotState{ownerID: ownerUserID}
	state.snapshot.Store(emptySnapshot())
	actual, _ := m.ownerSnapshots.LoadOrStore(ownerUserID, state)
	if stored, ok := actual.(*ownerSnapshotState); ok && stored != nil {
		return stored
	}
	return state
}

func (s *ownerSnapshotState) snapshotValue() *Snapshot {
	if s == nil {
		return emptySnapshot()
	}
	snapshot, _ := s.snapshot.Load().(*Snapshot)
	if snapshot == nil {
		return emptySnapshot()
	}
	return snapshot
}

func (m *Manager) listAvailableTokens(ctx context.Context, scope store.ResourceScope) ([]store.Token, error) {
	if m == nil || m.source == nil {
		return nil, ErrNoToken
	}
	if !scope.AllowAll {
		if scoped, ok := m.source.(scopedSource); ok {
			return scoped.ListAvailableTokensScoped(ctx, scope)
		}
	}
	rows, err := m.source.ListAvailableTokens(ctx)
	if err != nil || scope.AllowAll || scope.OwnerUserID == nil {
		return rows, err
	}
	filtered := make([]store.Token, 0, len(rows))
	for _, row := range rows {
		if row.OwnerUserID == *scope.OwnerUserID {
			filtered = append(filtered, row)
		}
	}
	return filtered, nil
}

func buildSnapshot(rows []store.Token, current *Snapshot, version int64) *Snapshot {
	sort.SliceStable(rows, func(i, j int) bool {
		left := rows[i].LastUsedAt
		right := rows[j].LastUsedAt
		if left == nil && right != nil {
			return true
		}
		if left != nil && right == nil {
			return false
		}
		if left != nil && right != nil && !left.Equal(*right) {
			return left.Before(*right)
		}
		return rows[i].ID < rows[j].ID
	})
	if current == nil {
		current = emptySnapshot()
	}
	nextByID := make(map[int64]*RuntimeToken, len(rows))
	nextByScope := make(map[string][]*RuntimeToken)
	nextByModel := make(map[string][]*RuntimeToken)
	nextByPlan := make(map[string][]*RuntimeToken)
	nextCooling := make(map[int64]time.Time)
	ready := make([]*RuntimeToken, 0, len(rows))
	for _, token := range rows {
		runtimeToken := current.ByID[token.ID]
		if runtimeToken == nil {
			runtimeToken = &RuntimeToken{}
		}
		runtimeToken.Token = token
		nextByID[token.ID] = runtimeToken
		nextByScope["default"] = append(nextByScope["default"], runtimeToken)
		nextByModel["*"] = append(nextByModel["*"], runtimeToken)
		if token.PlanType != nil && *token.PlanType != "" {
			nextByPlan[*token.PlanType] = append(nextByPlan[*token.PlanType], runtimeToken)
		}
		if token.CooldownUntil != nil {
			nextCooling[token.ID] = *token.CooldownUntil
		}
		ready = append(ready, runtimeToken)
	}
	return &Snapshot{
		Version:  version,
		LoadedAt: time.Now().UTC(),
		ByID:     nextByID,
		ByScope:  nextByScope,
		ByModel:  nextByModel,
		ByPlan:   nextByPlan,
		Cooling:  nextCooling,
		Ready:    ready,
	}
}

func newlyReadyTokens(current *Snapshot, next *Snapshot) []store.Token {
	if current == nil || next == nil || current.Version == 0 {
		return nil
	}
	ready := make([]store.Token, 0)
	for id, runtimeToken := range next.ByID {
		if _, ok := current.ByID[id]; ok || runtimeToken == nil {
			continue
		}
		ready = append(ready, runtimeToken.Token)
	}
	return ready
}

func (m *Manager) notifyReadyTransitions(ctx context.Context, ready []store.Token) {
	if m == nil || len(ready) == 0 {
		return
	}
	m.readyMu.RLock()
	handler := m.readyHandler
	m.readyMu.RUnlock()
	if handler != nil {
		handler(ctx, ready)
	}
}

func emptySnapshot() *Snapshot {
	return &Snapshot{
		Version:  0,
		LoadedAt: time.Time{},
		ByID:     map[int64]*RuntimeToken{},
		ByScope:  map[string][]*RuntimeToken{},
		ByModel:  map[string][]*RuntimeToken{},
		ByPlan:   map[string][]*RuntimeToken{},
		Cooling:  map[int64]time.Time{},
		Ready:    nil,
	}
}
