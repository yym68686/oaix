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

type Manager struct {
	source          Source
	logger          *slog.Logger
	maxAge          time.Duration
	refreshInterval time.Duration
	activeCap       atomic.Int64
	selector        Selector
	selectorMisses  atomic.Int64
	version         atomic.Int64
	snapshot        atomic.Value
	ownerSnapshots  sync.Map
	cursor          atomic.Uint64
	stopOnce        sync.Once
	stopCh          chan struct{}
	refreshMu       sync.Mutex
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
	Version       int64     `json:"version"`
	LoadedAt      time.Time `json:"loaded_at"`
	AgeMs         int64     `json:"age_ms"`
	ReadyTokens   int       `json:"ready_tokens"`
	ActiveStreams int64     `json:"active_streams"`
	ActiveCap     int64     `json:"active_stream_cap"`
	Stale         bool      `json:"stale"`
}

type Intent struct {
	Endpoint           string
	Model              string
	OwnerUserID        int64
	SelectionMode      string
	ExcludeOwnerUserID int64
	PromptCacheKeyHash string
	ExcludeTokenIDs    map[int64]struct{}
	RequireNonFree     bool
}

type Claim struct {
	Token      *RuntimeToken
	Reason     string
	released   atomic.Bool
	releaseFn  func(*RuntimeToken)
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

func (m *Manager) Refresh(ctx context.Context) error {
	m.refreshMu.Lock()
	defer m.refreshMu.Unlock()
	rows, err := m.listAvailableTokens(ctx, store.AllResources())
	if err != nil {
		return err
	}
	version := m.version.Add(1)
	next := buildSnapshot(rows, m.Snapshot(), version)
	m.snapshot.Store(next)
	if m.logger != nil {
		m.logger.Info("token snapshot refreshed", "version", version, "ready_tokens", len(next.Ready))
	}
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
		return nil, err
	}
	total := uint64(len(claimSnapshot.Ready))
	selector := m.selector
	if selector == nil {
		selector = RoundRobinSelector{}
	}
	activeCap := m.ActiveStreamCap()
	cursor := cursorState.Load()
	for offset := uint64(0); offset < total; offset++ {
		candidate, reason := selector.Select(ctx, claimSnapshot, intent, activeCap, &cursor)
		cursorState.Store(cursor)
		if candidate == nil {
			break
		}
		next := candidate.Active.Add(1)
		if next > activeCap {
			candidate.Active.Add(-1)
			continue
		}
		claim := &Claim{
			Token:      candidate,
			Reason:     reason,
			selectedAt: time.Now().UTC(),
			releaseFn: func(token *RuntimeToken) {
				token.Active.Add(-1)
			},
		}
		return claim, nil
	}
	m.selectorMisses.Add(1)
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
		c.releaseFn(c.Token)
	}
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

func isMarketplaceSelection(mode string) bool {
	return strings.EqualFold(strings.TrimSpace(mode), "marketplace")
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
