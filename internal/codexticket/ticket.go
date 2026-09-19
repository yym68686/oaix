// Package codexticket owns short-lived Codex turn-state tickets. Configuration,
// ticket persistence and business request forwarding have separate lifecycles.
package codexticket

import (
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/yym68686/oaix/internal/egress"
)

const (
	SettingKey          = "codex_tickets"
	Header              = "X-Codex-Turn-State"
	TargetLength        = 292
	TTL                 = time.Hour
	RefreshBefore       = 10 * time.Minute
	AttemptTimeout      = 25 * time.Second
	ProbeInterval       = 6 * time.Second
	MaxConcurrentProbes = 2
)

var ErrUnavailable = errors.New("Codex turn-state ticket unavailable")

type Policy struct {
	Enabled         bool     `json:"enabled"`
	FailClosed      bool     `json:"fail_closed"`
	Models          []string `json:"models"`
	HarvestProxyURL string   `json:"-"`
}

func DefaultPolicy() Policy {
	return Policy{Enabled: true, Models: []string{"gpt-6-astra", "gpt-5.6-sol"}}
}

func (p Policy) Includes(model string) bool {
	if !p.Enabled {
		return false
	}
	for _, m := range p.Models {
		if m == strings.TrimSpace(model) {
			return true
		}
	}
	return false
}

func (p Policy) Validate() error {
	if len(p.Models) == 0 || len(p.Models) > 16 {
		return errors.New("models must contain 1 to 16 model names")
	}
	seen := make(map[string]bool)
	for _, m := range p.Models {
		if m == "" || len(m) > 128 || strings.ContainsAny(m, " \t\r\n\x00") || seen[m] {
			return errors.New("invalid or duplicate ticket model")
		}
		seen[m] = true
	}
	if p.HarvestProxyURL != "" {
		_, err := egress.Parse(p.HarvestProxyURL)
		return err
	}
	return nil
}

func MaskProxy(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := egress.Parse(raw)
	if err != nil {
		return ""
	}
	if u.User != nil {
		u.User = url.UserPassword(u.User.Username(), "***")
	}
	return u.String()
}

type Account struct {
	TokenID  int64
	Identity string
	Models   []string
}

type Ticket struct {
	TokenID    int64     `json:"token_id"`
	Model      string    `json:"model"`
	Identity   string    `json:"-"`
	State      string    `json:"-"`
	CapturedAt time.Time `json:"captured_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

func ValidState(state string) bool {
	if len(state) != TargetLength || !strings.HasPrefix(state, "gAAAAA") {
		return false
	}
	for _, c := range state {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '=') {
			return false
		}
	}
	return true
}

func (t Ticket) Valid(account Account, now time.Time) bool {
	return t.TokenID == account.TokenID && t.Identity == account.Identity && ValidState(t.State) &&
		!t.CapturedAt.IsZero() && !t.CapturedAt.After(now) && now.Before(t.ExpiresAt) &&
		t.ExpiresAt.After(t.CapturedAt) && t.ExpiresAt.Sub(t.CapturedAt) <= TTL
}
