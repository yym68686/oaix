package codexticket

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type memoryRepository struct {
	mu               sync.Mutex
	policy           Policy
	tickets          []Ticket
	loadErr, saveErr error
	block            string
	entered          chan struct{}
}

func (r *memoryRepository) wait(ctx context.Context, stage string) error {
	if r.block != stage {
		return nil
	}
	select {
	case r.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}
func (r *memoryRepository) LoadCodexTicketPolicy(ctx context.Context) (Policy, error) {
	if err := r.wait(ctx, "policy"); err != nil {
		return Policy{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.policy, r.loadErr
}
func (r *memoryRepository) LoadCodexTickets(ctx context.Context) ([]Ticket, error) {
	if err := r.wait(ctx, "load"); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Ticket(nil), r.tickets...), nil
}
func (r *memoryRepository) SaveCodexTicket(ctx context.Context, ticket Ticket) error {
	if err := r.wait(ctx, "save"); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.saveErr != nil {
		return r.saveErr
	}
	r.tickets = append(r.tickets, ticket)
	return nil
}
func state(char string) string { return "gAAAAA" + strings.Repeat(char, TargetLength-6) }
func testService(t *testing.T) (*Service, *memoryRepository, Account) {
	t.Helper()
	a := Account{TokenID: 1, Identity: "account-1", Models: []string{"gpt-6-astra"}}
	r := &memoryRepository{policy: DefaultPolicy()}
	s := New(r, nil, func() []Account { return []Account{a} }, func(context.Context, Account, string, string) (ProbeResult, error) {
		return ProbeResult{State: state("A"), Status: 200}, nil
	})
	if err := s.ReloadPolicy(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s, r, a
}

func TestCaptureIsolationExpiryAndEcho(t *testing.T) {
	s, _, a := testService(t)
	model := "gpt-6-astra"
	for _, invalid := range []string{"", strings.Repeat("A", 292), state("A") + "x", state("A")[:291] + "\n"} {
		s.Observe(a, model, invalid, 200)
		if _, ok := s.Lookup(a, model); ok {
			t.Fatal("invalid ticket accepted")
		}
	}
	s.Observe(a, model, state("A"), 403)
	if _, ok := s.Lookup(a, model); ok {
		t.Fatal("non-200 accepted")
	}
	s.Observe(a, model, state("A"), 200)
	first, ok := s.Lookup(a, model)
	if !ok {
		t.Fatal("missing captured ticket")
	}
	s.Observe(a, model, state("A"), 200)
	second, _ := s.Lookup(a, model)
	if second.ExpiresAt != first.ExpiresAt {
		t.Fatal("echo extended TTL")
	}
	for _, other := range []Account{{TokenID: 2, Identity: a.Identity}, {TokenID: 1, Identity: "other-owner"}} {
		if _, ok := s.Lookup(other, model); ok {
			t.Fatal("ticket leaked across identity")
		}
	}
	if _, ok := s.Lookup(a, "gpt-5.6-sol"); ok {
		t.Fatal("ticket leaked across models")
	}
	h := http.Header{Header: []string{"client-other-account"}}
	if err := s.Apply(a, model, h); err != nil || h.Get(Header) != state("A") {
		t.Fatal("ticket injection failed")
	}
	s.mu.Lock()
	e := s.entries[key{a.TokenID, model}]
	e.ticket.CapturedAt = time.Now().Add(-2 * time.Hour)
	e.ticket.ExpiresAt = time.Now().Add(-time.Hour)
	s.mu.Unlock()
	if err := s.Apply(a, model, h); err != nil || h.Get(Header) != "" {
		t.Fatal("expired ticket or foreign client header forwarded")
	}
	s.Observe(a, model, state("A"), 200)
	if _, ok := s.Lookup(a, model); ok {
		t.Fatal("expired echoed ticket renewed")
	}
	s.Observe(a, model, state("B"), 200)
	if _, ok := s.Lookup(a, model); !ok {
		t.Fatal("fresh ticket rejected")
	}
	raw, _ := json.Marshal(first)
	if strings.Contains(string(raw), state("A")) || strings.Contains(string(raw), a.Identity) {
		t.Fatal("ticket secret exposed")
	}
}

func TestLivePolicyRetainsLastGoodAndGatesOnlyConfiguredModels(t *testing.T) {
	s, r, a := testService(t)
	if !s.Allowed(a, "gpt-6-astra") {
		t.Fatal("default must preserve service without tickets")
	}
	r.policy.FailClosed = true
	s.ReloadPolicy(context.Background())
	if s.Allowed(a, "gpt-6-astra") || !s.Allowed(a, "gpt-5.5") {
		t.Fatal("wrong gate")
	}
	r.loadErr = errors.New("unavailable")
	s.ReloadPolicy(context.Background())
	if !s.Policy().FailClosed {
		t.Fatal("failed read lost last good policy")
	}
	r.loadErr = nil
	r.policy.Enabled = false
	s.ReloadPolicy(context.Background())
	if !s.Allowed(a, "gpt-6-astra") {
		t.Fatal("disabled policy blocks")
	}
	h := http.Header{Header: []string{"existing"}}
	s.Apply(a, "gpt-6-astra", h)
	if h.Get(Header) != "existing" {
		t.Fatal("disabled policy changes forwarding")
	}
}

func TestRefreshPersistsAndRestoresWithoutReprobe(t *testing.T) {
	s, r, a := testService(t)
	s.entries[key{a.TokenID, a.Models[0]}] = &entry{}
	s.refresh(context.Background(), a, a.Models[0])
	if len(r.tickets) != 1 {
		t.Fatal("not persisted")
	}
	s2 := New(r, nil, s.accounts, func(context.Context, Account, string, string) (ProbeResult, error) {
		t.Error("reprobed fresh ticket")
		return ProbeResult{}, nil
	})
	s2.ReloadPolicy(context.Background())
	s2.load(context.Background())
	s2.refresh(context.Background(), a, a.Models[0])
	if _, ok := s2.Lookup(a, a.Models[0]); !ok {
		t.Fatal("restart lost valid ticket")
	}
	r.saveErr = errors.New("database down")
	s2.refresh(context.Background(), a, a.Models[0])
	if time.Until(s2.entries[key{a.TokenID, a.Models[0]}].next) > 31*time.Second {
		t.Fatal("persist failure not retried")
	}
}

func TestRefreshFailureRetainsTicketAndBacksOff(t *testing.T) {
	s, _, a := testService(t)
	model := a.Models[0]
	s.Observe(a, model, state("A"), 200)
	s.mu.Lock()
	e := s.entries[key{a.TokenID, model}]
	e.ticket.CapturedAt = time.Now().Add(-55 * time.Minute)
	e.ticket.ExpiresAt = e.ticket.CapturedAt.Add(TTL)
	old := e.ticket
	s.mu.Unlock()
	s.probe = func(context.Context, Account, string, string) (ProbeResult, error) {
		return ProbeResult{Status: 429, RetryAfter: 20 * time.Minute}, nil
	}
	s.refresh(context.Background(), a, model)
	got, ok := s.Lookup(a, model)
	if !ok || got != old {
		t.Fatal("refresh miss invalidated LKG")
	}
	if time.Until(e.next) < 19*time.Minute {
		t.Fatal("Retry-After ignored")
	}
	if e.probing {
		t.Fatal("probe not released")
	}
}

func TestBoundedConcurrentProbesAndShutdown(t *testing.T) {
	s, _, a := testService(t)
	var active, peak atomic.Int64
	s.accounts = func() []Account {
		out := make([]Account, 20)
		for i := range out {
			out[i] = a
			out[i].TokenID = int64(i + 1)
		}
		return out
	}
	entered := make(chan struct{}, 20)
	s.probe = func(ctx context.Context, _ Account, _, _ string) (ProbeResult, error) {
		n := active.Add(1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		defer active.Add(-1)
		entered <- struct{}{}
		<-ctx.Done()
		return ProbeResult{}, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	for i := 0; i < MaxConcurrentProbes; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("workers did not start")
		}
	}
	// Concurrent requests can safely observe/apply while workers run.
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Observe(a, a.Models[0], state("B"), 200)
			s.Apply(a, a.Models[0], http.Header{})
			s.Stats()
		}()
	}
	wg.Wait()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel probes")
	}
	if peak.Load() > MaxConcurrentProbes || active.Load() != 0 {
		t.Fatal("unbounded or surviving probes")
	}
}

func TestShutdownCancelsRepositoryStages(t *testing.T) {
	for _, stage := range []string{"policy", "load", "save"} {
		t.Run(stage, func(t *testing.T) {
			s, r, _ := testService(t)
			r.block = stage
			r.entered = make(chan struct{}, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() { s.Run(ctx); close(done) }()
			select {
			case <-r.entered:
			case <-time.After(time.Second):
				t.Fatal("stage not entered")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("shutdown blocked")
			}
		})
	}
}

func TestInitialEmptySnapshotDoesNotDiscardRestoredTickets(t *testing.T) {
	s, r, a := testService(t)
	now := time.Now().UTC()
	r.tickets = []Ticket{{TokenID: a.TokenID, Model: a.Models[0], Identity: a.Identity, State: state("A"), CapturedAt: now, ExpiresAt: now.Add(TTL)}}
	s.load(context.Background())
	s.accounts = func() []Account { return nil }
	var wg sync.WaitGroup
	s.schedule(context.Background(), make(chan struct{}, 2), &wg)
	if _, ok := s.Lookup(a, a.Models[0]); !ok {
		t.Fatal("initial empty token snapshot discarded restored ticket")
	}
}
