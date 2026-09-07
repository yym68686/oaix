package store

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/yym68686/oaix/internal/egress"
)

const createProxyChannels = `create table if not exists proxy_channels (
	id bigserial primary key,
	owner_user_id bigint not null references platform_users(id) on delete cascade,
	name varchar(128) not null,
	url_ciphertext text not null,
	protocol varchar(16) not null,
	host text not null,
	port integer not null check (port between 1 and 65535),
	has_auth boolean not null default false,
	created_at timestamptz not null default now(),
	updated_at timestamptz not null default now(),
	unique (id, owner_user_id)
)`
const createProxyChannelsOwnerIndex = `create index if not exists ix_proxy_channels_owner on proxy_channels(owner_user_id, id)`
const createTokenProxyBindings = `create table if not exists token_proxy_bindings (
	token_id integer primary key references codex_tokens(id) on delete cascade,
	owner_user_id bigint not null references platform_users(id) on delete cascade,
	proxy_channel_id bigint not null,
	updated_at timestamptz not null default now(),
	foreign key (proxy_channel_id, owner_user_id) references proxy_channels(id, owner_user_id)
)`
const createTokenProxyChannelIndex = `create index if not exists ix_token_proxy_channel on token_proxy_bindings(proxy_channel_id)`

var ErrProxyLimit = errors.New("每个用户最多可添加 100 个代理渠道")

var ErrProxyInUse = errors.New("该代理仍被账号使用，请先在账号设置中更换代理或取消绑定")

type ProxyChannel struct {
	ID           int64     `json:"id"`
	Name         string    `json:"name"`
	Protocol     string    `json:"protocol"`
	Host         string    `json:"host"`
	Port         int       `json:"port"`
	HasAuth      bool      `json:"has_auth"`
	AccountCount int       `json:"account_count"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (s *Store) ListProxyChannels(ctx context.Context, ownerID int64) ([]ProxyChannel, error) {
	rows, err := s.pool.Query(ctx, `select p.id, p.name, p.protocol, p.host, p.port, p.has_auth,
		(select count(*) from token_proxy_bindings b where b.proxy_channel_id = p.id), p.created_at, p.updated_at
		from proxy_channels p where p.owner_user_id = $1 order by p.id desc`, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ProxyChannel{}
	for rows.Next() {
		var item ProxyChannel
		if err := rows.Scan(&item.ID, &item.Name, &item.Protocol, &item.Host, &item.Port, &item.HasAuth, &item.AccountCount, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// SaveProxyChannel encrypts the complete URL; list APIs never return credentials.
// An empty raw value on update preserves the existing credentials.
func (s *Store) SaveProxyChannel(ctx context.Context, ownerID, id int64, name, raw string) (int64, error) {
	name = strings.TrimSpace(name)
	if name == "" || len([]rune(name)) > 128 {
		return 0, errors.New("代理名称需为 1–128 个字符")
	}
	if id > 0 && strings.TrimSpace(raw) == "" {
		err := s.pool.QueryRow(ctx, `update proxy_channels set name = $3, updated_at = now() where id = $1 and owner_user_id = $2 returning id`, id, ownerID, name).Scan(&id)
		return id, err
	}
	u, err := egress.Parse(raw)
	if err != nil {
		return 0, err
	}
	ciphertext, err := s.apiKeyCipher.Encrypt(u.String())
	if err != nil {
		return 0, errors.New("无法加密代理配置")
	}
	if id > 0 {
		err = s.pool.QueryRow(ctx, `update proxy_channels set name=$3, url_ciphertext=$4, protocol=$5, host=$6, port=$7, has_auth=$8, updated_at=now()
			where id=$1 and owner_user_id=$2 returning id`, id, ownerID, name, ciphertext, u.Scheme, u.Hostname(), u.Port(), u.User != nil).Scan(&id)
	} else {
		// Serialize creation per owner to keep a bounded user-managed inventory.
		tx, beginErr := s.pool.Begin(ctx)
		if beginErr != nil {
			return 0, beginErr
		}
		defer tx.Rollback(ctx)
		var owner int64
		if err := tx.QueryRow(ctx, `select id from platform_users where id=$1 for update`, ownerID).Scan(&owner); err != nil {
			return 0, err
		}
		var count int
		if err := tx.QueryRow(ctx, `select count(*) from proxy_channels where owner_user_id=$1`, ownerID).Scan(&count); err != nil {
			return 0, err
		}
		if count >= 100 {
			return 0, ErrProxyLimit
		}
		err = tx.QueryRow(ctx, `insert into proxy_channels(owner_user_id, name, url_ciphertext, protocol, host, port, has_auth)
			values($1,$2,$3,$4,$5,$6,$7) returning id`, ownerID, name, ciphertext, u.Scheme, u.Hostname(), u.Port(), u.User != nil).Scan(&id)
		if err == nil {
			err = tx.Commit(ctx)
		}
	}
	return id, err
}

func (s *Store) DeleteProxyChannel(ctx context.Context, ownerID, id int64) error {
	result, err := s.pool.Exec(ctx, `delete from proxy_channels where id=$1 and owner_user_id=$2`, id, ownerID)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" {
		return ErrProxyInUse
	}
	if err == nil && result.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

func (s *Store) ProxyChannelURL(ctx context.Context, ownerID, id int64) (*url.URL, error) {
	var ciphertext string
	err := s.pool.QueryRow(ctx, `select url_ciphertext from proxy_channels where id=$1 and owner_user_id=$2`, id, ownerID).Scan(&ciphertext)
	if err != nil {
		return nil, err
	}
	return s.decryptProxy(ciphertext)
}

func (s *Store) decryptProxy(ciphertext string) (*url.URL, error) {
	raw, err := s.apiKeyCipher.Decrypt(ciphertext)
	if err != nil {
		return nil, errors.New("无法解密代理配置，请重新保存渠道")
	}
	return egress.Parse(raw)
}

func (s *Store) TokenProxyChannelID(ctx context.Context, tokenID int64) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `select proxy_channel_id from token_proxy_bindings where token_id=$1`, tokenID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return id, err
}

func (s *Store) SetTokenProxyChannel(ctx context.Context, scope ResourceScope, tokenID, channelID int64) error {
	if channelID < 0 {
		return errors.New("代理渠道 ID 无效")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var ownerID int64
	err = tx.QueryRow(ctx, `select owner_user_id from codex_tokens where id=$1 and merged_into_token_id is null
		and ($2 or owner_user_id=$3) for update`, tokenID, scope.AllowAll, scope.OwnerUserID).Scan(&ownerID)
	if err != nil {
		return err
	}
	if channelID == 0 {
		_, err = tx.Exec(ctx, `delete from token_proxy_bindings where token_id=$1`, tokenID)
	} else {
		// Lock the channel against concurrent deletion; only the account owner's
		// channels may be assigned, including when an administrator acts on it.
		var channel int64
		err = tx.QueryRow(ctx, `select id from proxy_channels where id=$1 and owner_user_id=$2 for key share`, channelID, ownerID).Scan(&channel)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `insert into token_proxy_bindings(token_id, owner_user_id, proxy_channel_id) values($1,$2,$3)
			on conflict(token_id) do update set owner_user_id=excluded.owner_user_id, proxy_channel_id=excluded.proxy_channel_id, updated_at=now()`, tokenID, ownerID, channelID)
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ResolveTokenProxy reads configuration independently of cached token facts.
// A stale owner binding fails closed, even if the account was transferred.
func (s *Store) ResolveTokenProxy(ctx context.Context, tokenID int64) (*url.URL, error) {
	if s == nil {
		return nil, nil
	}
	var ciphertext *string
	err := s.pool.QueryRow(ctx, `select p.url_ciphertext from token_proxy_bindings b
		left join codex_tokens t on t.id=b.token_id and t.owner_user_id=b.owner_user_id
		left join proxy_channels p on p.id=b.proxy_channel_id and p.owner_user_id=t.owner_user_id
		where b.token_id=$1`, tokenID).Scan(&ciphertext)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if ciphertext == nil {
		return nil, errors.New("账号代理归属已变更，请重新设置代理")
	}
	return s.decryptProxy(*ciphertext)
}
