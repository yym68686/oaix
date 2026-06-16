package tokens

import (
	"context"
	"errors"
	"log/slog"
	"sort"
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
	activeCap       int64
	selector        Selector
	selectorMisses  atomic.Int64
	version         atomic.Int64
	snapshot        atomic.Value
	cursor          atomic.Uint64
	stopOnce        sync.Once
	stopCh          chan struct{}
	refreshMu       sync.Mutex
}

type Stats struct {
	Version       int64     `json:"version"`
	LoadedAt      time.Time `json:"loaded_at"`
	AgeMs         int64     `json:"age_ms"`
	ReadyTokens   int       `json:"ready_tokens"`
	ActiveStreams int64     `json:"active_streams"`
	Stale         bool      `json:"stale"`
}

type Intent struct {
	Endpoint           string
	Model              string
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
	if activeCap <= 0 {
		activeCap = 1
	}
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
		activeCap:       activeCap,
		stopCh:          make(chan struct{}),
	}
	manager.snapshot.Store(&Snapshot{
		Version:  0,
		LoadedAt: time.Time{},
		ByID:     map[int64]*RuntimeToken{},
		ByScope:  map[string][]*RuntimeToken{},
		ByModel:  map[string][]*RuntimeToken{},
		ByPlan:   map[string][]*RuntimeToken{},
		Cooling:  map[int64]time.Time{},
		Ready:    nil,
	})
	return manager
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
	rows, err := m.source.ListAvailableTokens(ctx)
	if err != nil {
		return err
	}
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
	current := m.Snapshot()
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
	version := m.version.Add(1)
	m.snapshot.Store(&Snapshot{
		Version:  version,
		LoadedAt: time.Now().UTC(),
		ByID:     nextByID,
		ByScope:  nextByScope,
		ByModel:  nextByModel,
		ByPlan:   nextByPlan,
		Cooling:  nextCooling,
		Ready:    ready,
	})
	if m.logger != nil {
		m.logger.Info("token snapshot refreshed", "version", version, "ready_tokens", len(ready))
	}
	return nil
}

func (m *Manager) Snapshot() *Snapshot {
	snapshot, _ := m.snapshot.Load().(*Snapshot)
	if snapshot == nil {
		return &Snapshot{
			ByID:    map[int64]*RuntimeToken{},
			ByScope: map[string][]*RuntimeToken{},
			ByModel: map[string][]*RuntimeToken{},
			ByPlan:  map[string][]*RuntimeToken{},
			Cooling: map[int64]time.Time{},
		}
	}
	return snapshot
}

func (m *Manager) Stats() Stats {
	snapshot := m.Snapshot()
	now := time.Now().UTC()
	var active int64
	for _, token := range snapshot.Ready {
		active += token.Active.Load()
	}
	age := now.Sub(snapshot.LoadedAt)
	if snapshot.LoadedAt.IsZero() {
		age = 0
	}
	return Stats{
		Version:       snapshot.Version,
		LoadedAt:      snapshot.LoadedAt,
		AgeMs:         age.Milliseconds(),
		ReadyTokens:   len(snapshot.Ready),
		ActiveStreams: active,
		Stale:         snapshot.LoadedAt.IsZero() || now.Sub(snapshot.LoadedAt) > m.maxAge,
	}
}

func (m *Manager) Claim(ctx context.Context, intent Intent) (*Claim, error) {
	snapshot := m.Snapshot()
	if m.snapshotStale(snapshot) {
		if err := m.Refresh(ctx); err != nil {
			if m.logger != nil {
				m.logger.Warn("stale token snapshot refresh failed", "error", err)
			}
			if m.snapshotStale(snapshot) {
				return nil, ErrSnapshotStale
			}
		}
		snapshot = m.Snapshot()
	}
	if len(snapshot.Ready) == 0 {
		if err := m.Refresh(ctx); err != nil && m.logger != nil {
			m.logger.Warn("token snapshot refresh during claim failed", "error", err)
		}
		snapshot = m.Snapshot()
	}
	if len(snapshot.Ready) == 0 {
		return nil, ErrNoToken
	}
	total := uint64(len(snapshot.Ready))
	selector := m.selector
	if selector == nil {
		selector = RoundRobinSelector{}
	}
	cursor := m.cursor.Load()
	for offset := uint64(0); offset < total; offset++ {
		candidate, reason := selector.Select(ctx, snapshot, intent, m.activeCap, &cursor)
		m.cursor.Store(cursor)
		if candidate == nil {
			break
		}
		next := candidate.Active.Add(1)
		if next > m.activeCap {
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
