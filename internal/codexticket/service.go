package codexticket

import (
	"context"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Repository interface {
	LoadCodexTicketPolicy(context.Context) (Policy, error)
	LoadCodexTickets(context.Context) ([]Ticket, error)
	SaveCodexTicket(context.Context, Ticket) error
}

type ProbeResult struct {
	State      string
	Status     int
	RetryAfter time.Duration
}
type Probe func(context.Context, Account, string, string) (ProbeResult, error)
type key struct {
	tokenID int64
	model   string
}
type entry struct {
	ticket     Ticket
	next       time.Time
	failures   int
	probing    bool
	lastLength int
	lastStatus int
}

type Service struct {
	repo                                                             Repository
	logger                                                           *slog.Logger
	accounts                                                         func() []Account
	probe                                                            Probe
	policy                                                           atomic.Pointer[Policy]
	mu                                                               sync.Mutex
	entries                                                          map[key]*entry
	started                                                          atomic.Bool
	probes, harvested, injected, missed, persistErrors, reloadErrors atomic.Int64
}

func New(repo Repository, logger *slog.Logger, accounts func() []Account, probe Probe) *Service {
	s := &Service{repo: repo, logger: logger, accounts: accounts, probe: probe, entries: make(map[key]*entry)}
	// Wait for the first successful configuration read before background work.
	p := DefaultPolicy()
	p.Enabled = false
	s.policy.Store(&p)
	return s
}

func (s *Service) Policy() Policy {
	p := *s.policy.Load()
	p.Models = append([]string(nil), p.Models...)
	return p
}

func (s *Service) ReloadPolicy(ctx context.Context) error {
	p, err := s.repo.LoadCodexTicketPolicy(ctx)
	if err == nil {
		err = p.Validate()
	}
	if err != nil {
		s.reloadErrors.Add(1)
		return err
	}
	p.Models = append([]string(nil), p.Models...)
	s.policy.Store(&p)
	return nil
}

func (s *Service) Lookup(account Account, model string) (Ticket, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.entries[key{account.TokenID, model}]
	if e == nil {
		return Ticket{}, false
	}
	return e.ticket, e.ticket.Valid(account, time.Now())
}

func (s *Service) Allowed(account Account, model string) bool {
	p := s.policy.Load()
	if !p.FailClosed || !p.Includes(model) {
		return true
	}
	_, ok := s.Lookup(account, model)
	return ok
}

func (s *Service) Apply(account Account, model string, headers http.Header) error {
	p := s.policy.Load()
	if !p.Includes(model) {
		return nil
	}
	// A client's ticket may belong to another account after gateway failover.
	headers.Del(Header)
	if t, ok := s.Lookup(account, model); ok {
		headers.Set(Header, t.State)
		s.injected.Add(1)
		return nil
	}
	s.missed.Add(1)
	if p.FailClosed {
		return ErrUnavailable
	}
	return nil
}

// Observe accepts only fresh tickets. An echoed ticket must not extend its TTL.
func (s *Service) Observe(account Account, model, state string, status int) {
	if status != http.StatusOK || !s.policy.Load().Includes(model) || !ValidState(state) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key{account.TokenID, model}
	e := s.entries[k]
	if e == nil {
		e = &entry{}
		s.entries[k] = e
	}
	if e.ticket.Identity == account.Identity && e.ticket.State == state {
		return
	}
	now := time.Now().UTC()
	e.ticket = Ticket{account.TokenID, model, account.Identity, state, now, now.Add(TTL)}
	// Persistence happens in the background loop, never on a business response.
	e.next = time.Time{}
}

func (s *Service) load(ctx context.Context) {
	rows, err := s.repo.LoadCodexTickets(ctx)
	if err != nil {
		s.reloadErrors.Add(1)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range rows {
		if !t.Valid(Account{TokenID: t.TokenID, Identity: t.Identity}, time.Now()) {
			continue
		}
		k := key{t.TokenID, t.Model}
		e := s.entries[k]
		if e == nil {
			e = &entry{}
			s.entries[k] = e
		}
		if t.CapturedAt.After(e.ticket.CapturedAt) {
			e.ticket = t
		}
	}
}

// Run owns all workers and waits for them on cancellation. No probes survive
// shutdown, and concurrency is bounded independently of account-pool size.
func (s *Service) Run(ctx context.Context) {
	if !s.started.CompareAndSwap(false, true) {
		return
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	slots := make(chan struct{}, MaxConcurrentProbes)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var lastLoad time.Time
	for {
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_ = s.ReloadPolicy(readCtx)
		cancel()
		if time.Since(lastLoad) >= 30*time.Second {
			readCtx, cancel = context.WithTimeout(ctx, 2*time.Second)
			s.load(readCtx)
			cancel()
			lastLoad = time.Now()
		}
		if ctx.Err() != nil {
			return
		}
		s.schedule(ctx, slots, &wg)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) schedule(ctx context.Context, slots chan struct{}, wg *sync.WaitGroup) {
	p := s.Policy()
	accounts := s.accounts()
	byID := make(map[int64]Account, len(accounts))
	for _, a := range accounts {
		byID[a.TokenID] = a
	}
	now := time.Now()
	type job struct {
		account Account
		model   string
		due     time.Time
	}
	var jobs []job
	s.mu.Lock()
	for k, e := range s.entries {
		if _, ok := byID[k.tokenID]; !ok && !e.probing && !now.Before(e.ticket.ExpiresAt) {
			delete(s.entries, k)
		}
	}
	if p.Enabled {
		for _, a := range accounts {
			for _, model := range a.Models {
				if !p.Includes(model) {
					continue
				}
				k := key{a.TokenID, model}
				e := s.entries[k]
				if e == nil {
					e = &entry{}
					s.entries[k] = e
				}
				if e.probing || now.Before(e.next) {
					continue
				}
				jobs = append(jobs, job{a, model, e.next})
			}
		}
	}
	s.mu.Unlock()
	// Fair across accounts even when a proxy cannot acquire a ticket.
	sort.SliceStable(jobs, func(i, j int) bool { return jobs[i].due.Before(jobs[j].due) })
	for _, j := range jobs {
		select {
		case slots <- struct{}{}:
		default:
			return
		}
		s.mu.Lock()
		e := s.entries[key{j.account.TokenID, j.model}]
		e.probing = true
		s.mu.Unlock()
		wg.Add(1)
		go func(a Account, model string) {
			defer wg.Done()
			defer func() { <-slots }()
			s.refresh(ctx, a, model)
		}(j.account, j.model)
	}
}

func (s *Service) refresh(parent context.Context, a Account, model string) {
	k := key{a.TokenID, model}
	ctx, cancel := context.WithTimeout(parent, AttemptTimeout)
	defer cancel()
	defer func() { s.mu.Lock(); s.entries[k].probing = false; s.mu.Unlock() }()
	p := s.Policy()
	if !p.Includes(model) || ctx.Err() != nil {
		return
	}
	t, valid := s.Lookup(a, model)
	var result ProbeResult
	var probeErr error
	if !valid || !time.Now().Add(RefreshBefore).Before(t.ExpiresAt) {
		s.probes.Add(1)
		result, probeErr = s.probe(ctx, a, model, p.HarvestProxyURL)
		if probeErr == nil && result.Status == http.StatusOK && ValidState(result.State) && result.State != t.State {
			s.Observe(a, model, result.State, result.Status)
			s.harvested.Add(1)
		}
	}
	if parent.Err() != nil {
		return
	}
	t, valid = s.Lookup(a, model)
	persistFailed := false
	if valid {
		persistCtx, stop := context.WithTimeout(parent, 2*time.Second)
		err := s.repo.SaveCodexTicket(persistCtx, t)
		stop()
		if err != nil {
			persistFailed = true
			s.persistErrors.Add(1)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.entries[k]
	e.lastStatus = result.Status
	e.lastLength = len(result.State)
	if valid && time.Now().Add(RefreshBefore).Before(t.ExpiresAt) {
		e.failures = 0
		e.next = t.ExpiresAt.Add(-RefreshBefore)
		if persistFailed {
			e.next = time.Now().Add(30 * time.Second)
		}
	} else {
		e.failures++
		delay := ProbeInterval * time.Duration(1<<min(e.failures-1, 6))
		if result.Status == 401 || result.Status == 403 || result.Status == 429 {
			delay = 5 * time.Minute
		}
		if result.RetryAfter > delay {
			delay = min(result.RetryAfter, time.Hour)
		}
		e.next = time.Now().Add(delay)
	}
	// A concurrent business response may have captured a newer ticket while
	// persistence was in flight. Leave it due for persistence on the next tick.
	if e.ticket.CapturedAt.After(t.CapturedAt) {
		e.next = time.Time{}
	}
	if s.logger != nil && (result.Status != 0 || probeErr != nil) {
		s.logger.Info("codex_ticket_probe", "token_id", a.TokenID, "model", model, "status", result.Status, "header_length", len(result.State), "ticket_ready", valid, "probe_failed", probeErr != nil)
	}
}

func (s *Service) Stats() map[string]any {
	accounts := make(map[int64]Account)
	for _, a := range s.accounts() {
		accounts[a.TokenID] = a
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ready, probing := 0, 0
	now := time.Now()
	for _, e := range s.entries {
		if a, ok := accounts[e.ticket.TokenID]; ok && e.ticket.Valid(a, now) {
			ready++
		}
		if e.probing {
			probing++
		}
	}
	return map[string]any{"ready_tickets": ready, "tracked_pairs": len(s.entries), "active_probes": probing,
		"probes": s.probes.Load(), "harvested": s.harvested.Load(), "injected": s.injected.Load(), "missing": s.missed.Load(),
		"persist_errors": s.persistErrors.Load(), "reload_errors": s.reloadErrors.Load()}
}

// Statuses returns bounded metadata for administrators, never ticket blobs.
func (s *Service) Statuses() []map[string]any {
	accounts := s.accounts()
	p := s.Policy()
	statuses := make([]map[string]any, 0)
	if !p.Enabled {
		return statuses
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range accounts {
		for _, model := range a.Models {
			e := s.entries[key{a.TokenID, model}]
			ready := e != nil && e.ticket.Valid(a, time.Now())
			status := map[string]any{"token_id": a.TokenID, "model": model, "ready": ready, "blocked": p.FailClosed && !ready}
			if e != nil {
				status["last_probe_status"] = e.lastStatus
				status["last_header_length"] = e.lastLength
				status["next_refresh_at"] = e.next
				if ready {
					status["expires_at"] = e.ticket.ExpiresAt
				}
			}
			statuses = append(statuses, status)
			if len(statuses) == 200 {
				return statuses
			}
		}
	}
	return statuses
}
