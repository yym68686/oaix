package affinity

import (
	"sync"
	"sync/atomic"
	"time"
)

type Lane struct {
	PrimaryTokenID    int64     `json:"primary_token_id"`
	SecondaryTokenIDs []int64   `json:"secondary_token_ids"`
	UpdatedAt         time.Time `json:"updated_at"`
	ExpiresAt         time.Time `json:"expires_at"`
	PolicyVersion     int       `json:"policy_version"`
}

type Store interface {
	Get(promptKey string) (Lane, bool)
	Put(promptKey string, lane Lane, ttl time.Duration)
	RemoveToken(tokenID int64)
	BindResponseOwner(responseID string, tokenID int64, ttl time.Duration)
	GetResponseOwner(responseID string) (int64, bool)
}

type MemoryStore struct {
	mu       sync.RWMutex
	lanes    map[string]Lane
	response map[string]responseOwner
	now      func() time.Time
	stats    Metrics
}

type responseOwner struct {
	TokenID   int64
	ExpiresAt time.Time
}

type Metrics struct {
	Gets            int64 `json:"gets"`
	Hits            int64 `json:"hits"`
	Puts            int64 `json:"puts"`
	Removals        int64 `json:"removals"`
	ResponseBinds   int64 `json:"response_binds"`
	ResponseLookups int64 `json:"response_lookups"`
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		lanes:    make(map[string]Lane),
		response: make(map[string]responseOwner),
		now:      time.Now,
	}
}

func (s *MemoryStore) Get(promptKey string) (Lane, bool) {
	atomic.AddInt64(&s.stats.Gets, 1)
	s.mu.RLock()
	lane, ok := s.lanes[promptKey]
	s.mu.RUnlock()
	if !ok {
		return Lane{}, false
	}
	if !lane.ExpiresAt.IsZero() && s.now().After(lane.ExpiresAt) {
		s.mu.Lock()
		delete(s.lanes, promptKey)
		s.mu.Unlock()
		return Lane{}, false
	}
	atomic.AddInt64(&s.stats.Hits, 1)
	return lane, true
}

func (s *MemoryStore) Put(promptKey string, lane Lane, ttl time.Duration) {
	atomic.AddInt64(&s.stats.Puts, 1)
	now := s.now().UTC()
	lane.UpdatedAt = now
	if ttl > 0 {
		lane.ExpiresAt = now.Add(ttl)
	}
	if lane.PolicyVersion == 0 {
		lane.PolicyVersion = 1
	}
	s.mu.Lock()
	s.lanes[promptKey] = lane
	s.mu.Unlock()
}

func (s *MemoryStore) RemoveToken(tokenID int64) {
	atomic.AddInt64(&s.stats.Removals, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, lane := range s.lanes {
		if lane.PrimaryTokenID == tokenID {
			if len(lane.SecondaryTokenIDs) > 0 {
				lane.PrimaryTokenID = lane.SecondaryTokenIDs[0]
				lane.SecondaryTokenIDs = lane.SecondaryTokenIDs[1:]
				s.lanes[key] = lane
			} else {
				delete(s.lanes, key)
			}
			continue
		}
		filtered := lane.SecondaryTokenIDs[:0]
		for _, id := range lane.SecondaryTokenIDs {
			if id != tokenID {
				filtered = append(filtered, id)
			}
		}
		lane.SecondaryTokenIDs = filtered
		s.lanes[key] = lane
	}
	for key, owner := range s.response {
		if owner.TokenID == tokenID {
			delete(s.response, key)
		}
	}
}

func (s *MemoryStore) BindResponseOwner(responseID string, tokenID int64, ttl time.Duration) {
	atomic.AddInt64(&s.stats.ResponseBinds, 1)
	now := s.now().UTC()
	owner := responseOwner{TokenID: tokenID}
	if ttl > 0 {
		owner.ExpiresAt = now.Add(ttl)
	}
	s.mu.Lock()
	s.response[responseID] = owner
	s.mu.Unlock()
}

func (s *MemoryStore) GetResponseOwner(responseID string) (int64, bool) {
	atomic.AddInt64(&s.stats.ResponseLookups, 1)
	s.mu.RLock()
	owner, ok := s.response[responseID]
	s.mu.RUnlock()
	if !ok {
		return 0, false
	}
	if !owner.ExpiresAt.IsZero() && s.now().After(owner.ExpiresAt) {
		s.mu.Lock()
		delete(s.response, responseID)
		s.mu.Unlock()
		return 0, false
	}
	return owner.TokenID, true
}

func (s *MemoryStore) Stats() Metrics {
	return Metrics{
		Gets:            atomic.LoadInt64(&s.stats.Gets),
		Hits:            atomic.LoadInt64(&s.stats.Hits),
		Puts:            atomic.LoadInt64(&s.stats.Puts),
		Removals:        atomic.LoadInt64(&s.stats.Removals),
		ResponseBinds:   atomic.LoadInt64(&s.stats.ResponseBinds),
		ResponseLookups: atomic.LoadInt64(&s.stats.ResponseLookups),
	}
}
