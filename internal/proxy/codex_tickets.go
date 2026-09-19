package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/yym68686/oaix/internal/codexticket"
	"github.com/yym68686/oaix/internal/egress"
	"github.com/yym68686/oaix/internal/modelaccess"
	"github.com/yym68686/oaix/internal/store"
	"github.com/yym68686/oaix/internal/tokens"
	"github.com/yym68686/oaix/internal/transport"
)

func codexTicketAccount(token store.Token) (codexticket.Account, bool) {
	if token.ID <= 0 || token.IsAgentIdentity() || token.IsCodexPersonalAccessToken() || strings.TrimSpace(token.AccessToken) == "" {
		return codexticket.Account{}, false
	}
	accountID := ""
	if token.AccountID != nil {
		accountID = strings.TrimSpace(*token.AccountID)
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%d:%s", token.ID, token.OwnerUserID, accountID)))
	return codexticket.Account{TokenID: token.ID, Identity: hex.EncodeToString(sum[:])}, true
}

func (p *Pipeline) CodexTickets() *codexticket.Service { return p.codexTickets }

func (p *Pipeline) ticketAccounts() []codexticket.Account {
	if p.tokens == nil {
		return nil
	}
	policy := p.codexTickets.Policy()
	var out []codexticket.Account
	for _, runtimeToken := range p.tokens.Snapshot().Ready {
		t := runtimeToken.Token
		a, ok := codexTicketAccount(t)
		if !ok || !t.IsActive || t.DisabledAt != nil || t.CooldownUntil != nil && time.Now().Before(*t.CooldownUntil) {
			continue
		}
		plan := ""
		if t.PlanType != nil {
			plan = *t.PlanType
		}
		for _, model := range policy.Models {
			allowed := modelaccess.DefaultAllows(plan, model)
			if t.ModelAccessConfigured {
				allowed = modelaccess.Matches(t.AllowedModels, model)
			}
			if allowed {
				a.Models = append(a.Models, model)
			}
		}
		out = append(out, a)
	}
	return out
}

func (p *Pipeline) probeCodexTicket(ctx context.Context, account codexticket.Account, model string, policy codexticket.Policy) (codexticket.ProbeResult, error) {
	// Use a normal bounded claim so synthetic probes also respect per-account
	// concurrency, model access and current token availability. This does not
	// enter the business request/billing log pipeline or change token health.
	claim, err := p.tokens.Claim(ctx, tokens.Intent{Model: model, TargetTokenID: account.TokenID})
	if err != nil {
		return codexticket.ProbeResult{}, err
	}
	defer claim.Release()
	actual, ok := codexTicketAccount(claim.Token.Token)
	if !ok || actual.Identity != account.Identity {
		return codexticket.ProbeResult{}, errors.New("ticket account changed")
	}
	if policy.HarvestProxyChannelID > 0 {
		source, ok := p.store.(interface {
			ProxyChannelURL(context.Context, int64, int64) (*url.URL, error)
		})
		if !ok {
			return codexticket.ProbeResult{}, errors.New("harvest proxy channel unavailable")
		}
		// Resolve the selected channel for every attempt so edits take effect
		// without copying secrets. Missing/deleted channels never fall direct.
		u, err := source.ProxyChannelURL(ctx, policy.HarvestProxyOwnerID, policy.HarvestProxyChannelID)
		if err != nil || u == nil {
			return codexticket.ProbeResult{}, errors.New("harvest proxy channel unavailable")
		}
		ctx = egress.WithProxy(ctx, u)
	} else if policy.HarvestProxyURL != "" {
		u, err := egress.Parse(policy.HarvestProxyURL)
		if err != nil {
			return codexticket.ProbeResult{}, err
		}
		ctx = egress.WithProxy(ctx, u)
	} else {
		ctx = egress.ForToken(ctx, p.store, account.TokenID)
	}
	body, _ := json.Marshal(map[string]any{"model": model, "stream": true, "store": false, "instructions": "Reply with exactly: pong", "input": []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "input_text", "text": "ping"}}}}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.Upstream.ResponsesURL, bytes.NewReader(body))
	if err != nil {
		return codexticket.ProbeResult{}, errors.New("invalid ticket upstream URL")
	}
	req.Header.Set("Authorization", "Bearer "+claim.AccessToken())
	if id := claim.AccountID(); id != nil {
		req.Header.Set("ChatGPT-Account-ID", *id)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("OpenAI-Beta", "responses=experimental")
	req.Header.Set("Originator", "codex_cli_rs")
	req.Header.Set("Version", "0.153.4")
	req.Header.Set("User-Agent", "codex_cli_rs/0.153.4 (Debian 13.0.0; x86_64) WindowsTerminal")
	req.Header.Set("Session_id", newUUIDv7())
	resp, err := p.ticketTransport.DoWithOptions(ctx, req, transport.RequestOptions{ForceHTTP1: true, CloseConnection: true, RejectRedirects: true})
	if err != nil {
		return codexticket.ProbeResult{}, errors.New("ticket probe transport failed")
	}
	defer resp.Body.Close()
	result := codexticket.ProbeResult{State: strings.TrimSpace(resp.Header.Get(codexticket.Header)), Status: resp.StatusCode}
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
		result.RetryAfter = time.Duration(min(seconds, 3600)) * time.Second
	} else if until, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil {
		result.RetryAfter = time.Until(until)
	}
	return result, nil
}

func ticketIntentEligible(intent RequestIntent) bool {
	endpoint := intent.UpstreamEndpoint
	if endpoint == "" {
		endpoint = intent.Endpoint
	}
	return endpoint == "/v1/responses" || endpoint == "/v1/responses/compact"
}

func (p *Pipeline) applyCodexTicket(attempt Attempt, headers http.Header) error {
	if p.codexTickets == nil || !ticketIntentEligible(attempt.Intent) || attempt.Claim == nil || attempt.Claim.Token == nil {
		return nil
	}
	a, ok := codexTicketAccount(attempt.Claim.Token.Token)
	if !ok {
		return nil
	}
	return p.codexTickets.Apply(a, codexTicketModel(attempt.Intent, attempt.Document, attempt.Body), headers)
}

func (p *Pipeline) observeCodexTicket(attempt Attempt, resp *http.Response) {
	if p.codexTickets == nil || !ticketIntentEligible(attempt.Intent) || attempt.Claim == nil || attempt.Claim.Token == nil {
		return
	}
	a, ok := codexTicketAccount(attempt.Claim.Token.Token)
	if !ok {
		return
	}
	p.codexTickets.Observe(a, codexTicketModel(attempt.Intent, attempt.Document, attempt.Body), strings.TrimSpace(resp.Header.Get(codexticket.Header)), resp.StatusCode)
}

// Use the model sent upstream, including the image-compatibility rewrite.
func codexTicketModel(intent RequestIntent, document *RequestDocument, body []byte) string {
	if document == nil {
		document = newRequestDocument(body, "")
	}
	payload, err := document.Object()
	if err == nil {
		if model, ok := payload["model"].(string); ok && model != "" {
			if model == defaultImagesToolModel {
				return defaultImagesMainModel
			}
			return model
		}
	}
	return intent.Model
}
