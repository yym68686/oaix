package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/agentidentity"
	"github.com/yym68686/oaix/internal/codexticket"
	"github.com/yym68686/oaix/internal/store"
	"github.com/yym68686/oaix/internal/transport"
)

type ticketRepository struct{ policy codexticket.Policy }

func (r ticketRepository) LoadCodexTicketPolicy(context.Context) (codexticket.Policy, error) {
	return r.policy, nil
}
func (r ticketRepository) LoadCodexTickets(context.Context) ([]codexticket.Ticket, error) {
	return nil, nil
}
func (r ticketRepository) SaveCodexTicket(context.Context, codexticket.Ticket) error { return nil }
func attachTickets(t *testing.T, p *Pipeline, policy codexticket.Policy) {
	t.Helper()
	p.codexTickets = codexticket.New(ticketRepository{policy}, p.logger, p.ticketAccounts, p.probeCodexTicket)
	p.ticketTransport = transport.New(p.cfg.Upstream)
	if err := p.codexTickets.ReloadPolicy(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func ticketState(char string) string { return "gAAAAA" + strings.Repeat(char, 286) }

func TestCodexTicketFailoverUsesSelectedAccount(t *testing.T) {
	var mu sync.Mutex
	var got []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expected := map[string]string{"Bearer first": ticketState("A"), "Bearer second": ticketState("B")}[r.Header.Get("Authorization")]
		if expected == "" || r.Header.Get(codexticket.Header) != expected {
			t.Error("selected account and ticket differ")
		}
		mu.Lock()
		got = append(got, r.Header.Get(codexticket.Header))
		n := len(got)
		mu.Unlock()
		if n == 1 {
			w.WriteHeader(502)
			io.WriteString(w, `{"error":{"message":"transient"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-ticket\",\"status\":\"completed\",\"output\":[]}}\n\n")
	}))
	defer upstream.Close()
	fakes := &fakeProxyStore{tokens: []store.Token{{ID: 1, AccessToken: "first", IsActive: true}, {ID: 2, AccessToken: "second", IsActive: true}}}
	p := newProxyPipelineTestHarness(t, upstream.URL, 2, fakes)
	attachTickets(t, p, codexticket.DefaultPolicy())
	for i, token := range fakes.tokens {
		a, _ := codexTicketAccount(token)
		p.codexTickets.Observe(a, "gpt-6-astra", ticketState([]string{"A", "B"}[i]), 200)
	}
	r := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"gpt-6-astra","input":[]}`))
	r.Header.Set(codexticket.Header, "foreign-client-state")
	w := httptest.NewRecorder()
	p.Proxy(w, r, RequestIntent{Endpoint: "/v1/responses"})
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	if len(got) != 2 || got[0] == got[1] {
		t.Fatal("wrong account ticket during failover")
	}
}

func TestCodexTicketStrictSelectionSkipsMissingTickets(t *testing.T) {
	var authorization string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-ticket\",\"status\":\"completed\",\"output\":[]}}\n\n")
	}))
	defer upstream.Close()
	fakes := &fakeProxyStore{tokens: []store.Token{{ID: 1, AccessToken: "no-ticket", IsActive: true}, {ID: 2, AccessToken: "has-ticket", IsActive: true}}}
	p := newProxyPipelineTestHarness(t, upstream.URL, 1, fakes)
	policy := codexticket.DefaultPolicy()
	policy.FailClosed = true
	attachTickets(t, p, policy)
	a, _ := codexTicketAccount(fakes.tokens[1])
	p.codexTickets.Observe(a, "gpt-6-astra", ticketState("B"), 200)
	r := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"gpt-6-astra","input":[]}`))
	w := httptest.NewRecorder()
	p.Proxy(w, r, RequestIntent{Endpoint: "/v1/responses"})
	if w.Code != 200 || authorization != "Bearer has-ticket" {
		t.Fatalf("strict selection status=%d auth=%s", w.Code, authorization)
	}
}

func TestCodexTicketProbeHeadersOnlyAndBusinessIsolation(t *testing.T) {
	seen := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 1 || !r.Close {
			t.Error("harvest reused transport")
		}
		if r.Header.Get("Version") != "0.153.4" || r.Header.Get("Authorization") != "Bearer access" {
			t.Error("incorrect probe identity")
		}
		if r.Header.Get(codexticket.Header) != "" {
			t.Error("probe sent prior ticket")
		}
		io.Copy(io.Discard, r.Body)
		w.Header().Set(codexticket.Header, ticketState("A"))
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		seen <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
			t.Error("probe body was not closed")
		}
	}))
	defer upstream.Close()
	fakes := &fakeProxyStore{tokens: []store.Token{{ID: 1, AccessToken: "access", IsActive: true}}}
	p := newProxyPipelineTestHarness(t, upstream.URL, 1, fakes)
	attachTickets(t, p, codexticket.DefaultPolicy())
	a, _ := codexTicketAccount(fakes.tokens[0])
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := p.probeCodexTicket(ctx, a, "gpt-6-astra", codexticket.DefaultPolicy())
	if err != nil || result.State != ticketState("A") {
		t.Fatalf("probe failed: %v", err)
	}
	<-seen
	if len(fakes.attempts) != 0 || p.transport.Stats().RequestsTotal != 0 || p.tokens.Stats().ActiveStreams != 0 {
		t.Fatal("probe leaked into business traffic or claim")
	}
}

func TestCodexTicketExcludesAgentAndPAT(t *testing.T) {
	for _, token := range []store.Token{{ID: 1, AccessToken: "at-personal"}, {ID: 2, AccessToken: "secret", RefreshToken: "agent_identity:runtime"}} {
		if token.ID == 2 {
			token.RefreshToken = ""
			token.AgentIdentity = &agentidentity.Credentials{}
		}
		if _, ok := codexTicketAccount(token); ok {
			t.Fatal("unsupported credentials entered ticket gate")
		}
	}
}

type unavailableHarvestChannelStore struct {
	*fakeProxyStore
	owner, channel    int64
	accountProxyCalls int
}

func (s *unavailableHarvestChannelStore) ProxyChannelURL(_ context.Context, owner, channel int64) (*url.URL, error) {
	s.owner, s.channel = owner, channel
	return nil, errors.New("channel deleted")
}
func (s *unavailableHarvestChannelStore) ResolveTokenProxy(context.Context, int64) (*url.URL, error) {
	s.accountProxyCalls++
	return nil, nil
}

func TestCodexTicketMissingChannelNeverUsesAccountOrDirectRoute(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(200) }))
	defer upstream.Close()
	fakes := &fakeProxyStore{tokens: []store.Token{{ID: 1, AccessToken: "access", IsActive: true}}}
	p := newProxyPipelineTestHarness(t, upstream.URL, 1, fakes)
	attachTickets(t, p, codexticket.DefaultPolicy())
	source := &unavailableHarvestChannelStore{fakeProxyStore: fakes}
	p.store = source
	a, _ := codexTicketAccount(fakes.tokens[0])
	policy := codexticket.DefaultPolicy()
	policy.HarvestProxyChannelID = 7
	policy.HarvestProxyOwnerID = 42
	_, err := p.probeCodexTicket(context.Background(), a, "gpt-6-astra", policy)
	if err == nil || calls.Load() != 0 || source.accountProxyCalls != 0 {
		t.Fatal("missing channel fell back to account/direct route")
	}
	if source.owner != 42 || source.channel != 7 {
		t.Fatal("wrong channel owner scope")
	}
	if p.tokens.Stats().ActiveStreams != 0 {
		t.Fatal("failed lookup leaked account claim")
	}
}
