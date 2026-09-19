package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/yym68686/oaix/internal/codexticket"
	"github.com/yym68686/oaix/internal/egress"
)

const createCodexTickets = `create table if not exists codex_turn_tickets (
	token_id integer not null references codex_tokens(id) on delete cascade,
	model varchar(128) not null,
	identity_hash varchar(64) not null,
	state_ciphertext text not null,
	captured_at timestamptz not null,
	expires_at timestamptz not null,
	primary key(token_id, model)
)`

type codexTicketPolicyRecord struct {
	codexticket.Policy
	ProxyCiphertext     string `json:"proxy_ciphertext,omitempty"`
	ProxyChannelOwnerID int64  `json:"proxy_channel_owner_id,omitempty"`
}

func (s *Store) LoadCodexTicketPolicy(ctx context.Context) (codexticket.Policy, error) {
	p := codexticket.DefaultPolicy()
	setting, err := s.GetSetting(ctx, codexticket.SettingKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, nil
	}
	if err != nil {
		return p, err
	}
	record := codexTicketPolicyRecord{Policy: p}
	if err = json.Unmarshal(setting.Value, &record); err != nil {
		return p, errors.New("invalid Codex ticket policy")
	}
	if record.ProxyCiphertext != "" {
		record.HarvestProxyURL, err = s.apiKeyCipher.Decrypt(record.ProxyCiphertext)
		if err != nil {
			return p, errors.New("cannot decrypt Codex ticket proxy")
		}
	}
	record.HarvestProxyOwnerID = record.ProxyChannelOwnerID
	return record.Policy, record.Policy.Validate()
}

type CodexTicketPolicyUpdate struct {
	Enabled        *bool
	FailClosed     *bool
	Models         *[]string
	ProxyURL       *string
	ClearProxy     bool
	ProxyChannelID *int64
	// Set by the authenticated handler, never accepted from JSON.
	ProxyChannelOwnerID int64
}

// A row lock prevents a concurrent partial settings update from losing a proxy.
// Empty/omitted/masked URLs preserve it; clearProxy is the explicit reset action.
func (s *Store) UpdateCodexTicketPolicy(ctx context.Context, update CodexTicketPolicyUpdate) error {
	enabled, failClosed, models := update.Enabled, update.FailClosed, update.Models
	proxyURL, clearProxy := update.ProxyURL, update.ClearProxy
	if update.ProxyChannelID != nil && (*update.ProxyChannelID < 0 || *update.ProxyChannelID > 0 && (clearProxy || proxyURL != nil && strings.TrimSpace(*proxyURL) != "")) {
		return errors.New("select a proxy channel or a URL, not both")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	initial, _ := json.Marshal(codexTicketPolicyRecord{Policy: codexticket.DefaultPolicy()})
	if _, err = tx.Exec(ctx, `insert into gateway_settings(key,value,updated_at) values($1,$2,now()) on conflict(key) do nothing`, codexticket.SettingKey, initial); err != nil {
		return err
	}
	var raw []byte
	if err = tx.QueryRow(ctx, `select value::jsonb from gateway_settings where key=$1 for update`, codexticket.SettingKey).Scan(&raw); err != nil {
		return err
	}
	record := codexTicketPolicyRecord{Policy: codexticket.DefaultPolicy()}
	if err = json.Unmarshal(raw, &record); err != nil {
		return errors.New("invalid stored Codex ticket policy")
	}
	record.HarvestProxyOwnerID = record.ProxyChannelOwnerID
	if enabled != nil {
		record.Enabled = *enabled
	}
	if failClosed != nil {
		record.FailClosed = *failClosed
	}
	if models != nil {
		record.Models = append([]string(nil), (*models)...)
	}
	if clearProxy {
		record.ProxyCiphertext = ""
		record.HarvestProxyChannelID, record.HarvestProxyOwnerID = 0, 0
	}
	if update.ProxyChannelID != nil && !clearProxy {
		id := *update.ProxyChannelID
		if id == 0 {
			record.HarvestProxyChannelID, record.HarvestProxyOwnerID = 0, 0
		} else {
			ownerID := update.ProxyChannelOwnerID
			if id == record.HarvestProxyChannelID {
				ownerID = record.HarvestProxyOwnerID
			}
			var found int64
			if err = tx.QueryRow(ctx, `select id from proxy_channels where id=$1 and owner_user_id=$2 for key share`, id, ownerID).Scan(&found); err != nil {
				return errors.New("harvest proxy channel not found in your proxy configuration")
			}
			record.HarvestProxyChannelID, record.HarvestProxyOwnerID = id, ownerID
			record.ProxyCiphertext = ""
		}
	}
	if proxyURL != nil && strings.TrimSpace(*proxyURL) != "" && !clearProxy {
		u, parseErr := egress.Parse(*proxyURL)
		if parseErr != nil {
			return parseErr
		}
		password := ""
		if u.User != nil {
			password, _ = u.User.Password()
		}
		if password == "***" {
			stored, decryptErr := s.apiKeyCipher.Decrypt(record.ProxyCiphertext)
			if decryptErr != nil || codexticket.MaskProxy(stored) != u.String() {
				return errors.New("masked proxy must match the saved proxy")
			}
		} else {
			record.ProxyCiphertext, err = s.apiKeyCipher.Encrypt(u.String())
			if err != nil {
				return err
			}
			record.HarvestProxyChannelID, record.HarvestProxyOwnerID = 0, 0
		}
	}
	if err = record.Policy.Validate(); err != nil {
		return err
	}
	record.ProxyChannelOwnerID = record.HarvestProxyOwnerID
	raw, err = json.Marshal(record)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `update gateway_settings set value=$2,updated_at=now() where key=$1`, codexticket.SettingKey, raw); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) LoadCodexTickets(ctx context.Context) ([]codexticket.Ticket, error) {
	rows, err := s.pool.Query(ctx, `select token_id,model,identity_hash,state_ciphertext,captured_at,expires_at from codex_turn_tickets where expires_at>now()`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tickets []codexticket.Ticket
	for rows.Next() {
		var t codexticket.Ticket
		var ciphertext string
		if err = rows.Scan(&t.TokenID, &t.Model, &t.Identity, &ciphertext, &t.CapturedAt, &t.ExpiresAt); err != nil {
			return nil, err
		}
		t.State, err = s.apiKeyCipher.Decrypt(ciphertext)
		if err != nil {
			return nil, errors.New("cannot decrypt Codex ticket")
		}
		tickets = append(tickets, t)
	}
	return tickets, rows.Err()
}

func (s *Store) SaveCodexTicket(ctx context.Context, t codexticket.Ticket) error {
	if !codexticket.ValidState(t.State) {
		return errors.New("invalid Codex ticket")
	}
	ciphertext, err := s.apiKeyCipher.Encrypt(t.State)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `insert into codex_turn_tickets(token_id,model,identity_hash,state_ciphertext,captured_at,expires_at)
		values($1,$2,$3,$4,$5,$6) on conflict(token_id,model) do update set
		identity_hash=excluded.identity_hash,state_ciphertext=excluded.state_ciphertext,captured_at=excluded.captured_at,expires_at=excluded.expires_at
		where codex_turn_tickets.captured_at < excluded.captured_at`, t.TokenID, t.Model, t.Identity, ciphertext, t.CapturedAt, t.ExpiresAt)
	return err
}

func redactCodexTicketSetting(item *Setting) {
	if item.Key != codexticket.SettingKey {
		return
	}
	var record codexTicketPolicyRecord
	if json.Unmarshal(item.Value, &record) != nil {
		item.Value = json.RawMessage(`{}`)
		return
	}
	item.Value, _ = json.Marshal(struct {
		codexticket.Policy
		ProxyConfigured bool `json:"harvest_proxy_configured"`
	}{record.Policy, record.ProxyCiphertext != "" || record.HarvestProxyChannelID > 0})
}
