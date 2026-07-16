package affinity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct {
	pool    *pgxpool.Pool
	timeout time.Duration
	stats   Metrics
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool, timeout: 3 * time.Second}
}

func (s *PostgresStore) Get(promptKey string) (Lane, bool) {
	atomic.AddInt64(&s.stats.Gets, 1)
	ctx, cancel := s.context()
	defer cancel()
	var lane Lane
	var secondary []byte
	var priceLocks []byte
	err := s.pool.QueryRow(ctx, `
		select primary_token_id, coalesce(secondary_token_ids, '[]'::jsonb), coalesce(marketplace_price_locks, '{}'::jsonb), updated_at, expires_at, policy_version
		from prompt_affinity_lanes
		where prompt_key_hash = $1 and expires_at > now()
	`, stableHash(promptKey)).Scan(&lane.PrimaryTokenID, &secondary, &priceLocks, &lane.UpdatedAt, &lane.ExpiresAt, &lane.PolicyVersion)
	if err != nil {
		return Lane{}, false
	}
	_ = json.Unmarshal(secondary, &lane.SecondaryTokenIDs)
	_ = json.Unmarshal(priceLocks, &lane.MarketplacePriceLocks)
	lane.NormalizeMarketplacePriceLocks()
	atomic.AddInt64(&s.stats.Hits, 1)
	return lane, true
}

func (s *PostgresStore) Put(promptKey string, lane Lane, ttl time.Duration) {
	atomic.AddInt64(&s.stats.Puts, 1)
	if ttl <= 0 {
		ttl = time.Hour
	}
	now := time.Now().UTC()
	lane.UpdatedAt = now
	lane.ExpiresAt = now.Add(ttl)
	if lane.PolicyVersion == 0 {
		lane.PolicyVersion = 1
	}
	lane.NormalizeMarketplacePriceLocks()
	secondary, _ := json.Marshal(lane.SecondaryTokenIDs)
	priceLocks, _ := json.Marshal(lane.MarketplacePriceLocks)
	ctx, cancel := s.context()
	defer cancel()
	_, _ = s.pool.Exec(ctx, `
		insert into prompt_affinity_lanes(
			prompt_key_hash, primary_token_id, secondary_token_ids, marketplace_price_locks, policy_version, expires_at, updated_at
		)
		values ($1, $2, $3, $4, $5, $6, $7)
		on conflict (prompt_key_hash) do update
		set primary_token_id = excluded.primary_token_id,
		    secondary_token_ids = excluded.secondary_token_ids,
		    marketplace_price_locks = excluded.marketplace_price_locks,
		    policy_version = excluded.policy_version,
		    expires_at = excluded.expires_at,
		    updated_at = excluded.updated_at
	`, stableHash(promptKey), lane.PrimaryTokenID, secondary, priceLocks, lane.PolicyVersion, lane.ExpiresAt, lane.UpdatedAt)
}

func (s *PostgresStore) BindPrimaryIfAbsent(promptKey string, tokenID int64, identityHash string, ttl time.Duration, policyVersion int) (Lane, bool) {
	atomic.AddInt64(&s.stats.Puts, 1)
	if tokenID <= 0 || identityHash == "" {
		return Lane{}, false
	}
	if ttl <= 0 {
		ttl = time.Hour
	}
	if policyVersion == 0 {
		policyVersion = 1
	}
	now := time.Now().UTC()
	lane := newPrimaryLane(tokenID, identityHash, policyVersion)
	priceLocks, _ := json.Marshal(lane.MarketplacePriceLocks)
	ctx, cancel := s.context()
	defer cancel()
	_, err := s.pool.Exec(ctx, `
		insert into prompt_affinity_lanes(
			prompt_key_hash, primary_token_id, secondary_token_ids, marketplace_price_locks, policy_version, expires_at, updated_at
		)
		values ($1, $2, '[]'::jsonb, $3, $4, $5, $6)
		on conflict (prompt_key_hash) do update
		set primary_token_id = excluded.primary_token_id,
		    secondary_token_ids = excluded.secondary_token_ids,
		    marketplace_price_locks = excluded.marketplace_price_locks,
		    policy_version = excluded.policy_version,
		    expires_at = excluded.expires_at,
		    updated_at = excluded.updated_at
		where prompt_affinity_lanes.expires_at <= now()
	`, stableHash(promptKey), tokenID, priceLocks, policyVersion, now.Add(ttl), now)
	if err != nil {
		return Lane{}, false
	}
	return s.Get(promptKey)
}

func (s *PostgresStore) RemoveToken(tokenID int64) {
	atomic.AddInt64(&s.stats.Removals, 1)
	ctx, cancel := s.context()
	defer cancel()
	rows, err := s.pool.Query(ctx, `
		select prompt_key_hash, primary_token_id, coalesce(secondary_token_ids, '[]'::jsonb), coalesce(marketplace_price_locks, '{}'::jsonb), updated_at, expires_at, policy_version
		from prompt_affinity_lanes
		where expires_at > now()
	`)
	if err != nil {
		return
	}
	type row struct {
		key  string
		lane Lane
	}
	var changed []row
	for rows.Next() {
		var item row
		var secondary []byte
		var priceLocks []byte
		if err := rows.Scan(&item.key, &item.lane.PrimaryTokenID, &secondary, &priceLocks, &item.lane.UpdatedAt, &item.lane.ExpiresAt, &item.lane.PolicyVersion); err != nil {
			rows.Close()
			return
		}
		_ = json.Unmarshal(secondary, &item.lane.SecondaryTokenIDs)
		_ = json.Unmarshal(priceLocks, &item.lane.MarketplacePriceLocks)
		if item.lane.PolicyVersion == PolicyVersionStrictToken {
			continue
		}
		next, ok := removeTokenFromLane(item.lane, tokenID)
		if !ok {
			continue
		}
		item.lane = next
		changed = append(changed, item)
	}
	rows.Close()
	if rows.Err() != nil {
		return
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback(ctx)
	for _, item := range changed {
		if item.lane.PrimaryTokenID == 0 && len(item.lane.SecondaryTokenIDs) == 0 {
			if _, err := tx.Exec(ctx, `delete from prompt_affinity_lanes where prompt_key_hash = $1`, item.key); err != nil {
				return
			}
			continue
		}
		secondary, _ := json.Marshal(item.lane.SecondaryTokenIDs)
		priceLocks, _ := json.Marshal(item.lane.MarketplacePriceLocks)
		if _, err := tx.Exec(ctx, `
			update prompt_affinity_lanes
			set primary_token_id = $2, secondary_token_ids = $3, marketplace_price_locks = $4, updated_at = now()
			where prompt_key_hash = $1
		`, item.key, item.lane.PrimaryTokenID, secondary, priceLocks); err != nil {
			return
		}
	}
	if _, err := tx.Exec(ctx, `delete from response_owner_bindings where token_id = $1`, tokenID); err != nil {
		return
	}
	_ = tx.Commit(ctx)
}

func (s *PostgresStore) BindResponseOwner(responseID string, binding ResponseOwnerBinding, ttl time.Duration) {
	atomic.AddInt64(&s.stats.ResponseBinds, 1)
	if binding.TokenID <= 0 {
		return
	}
	if ttl <= 0 {
		ttl = time.Hour
	}
	ctx, cancel := s.context()
	defer cancel()
	var priceBPS any
	var source any
	var lockedAt any
	var contractKey any
	if lock := binding.MarketplacePriceLock; lock != nil {
		priceBPS = lock.PriceBPS
		source = lock.Source
		if !lock.LockedAt.IsZero() {
			lockedAt = lock.LockedAt
		}
		if lock.ContractKey != "" {
			contractKey = lock.ContractKey
		}
	}
	_, _ = s.pool.Exec(ctx, `
		insert into response_owner_bindings(response_id_hash, token_id, marketplace_price_bps, marketplace_price_source, marketplace_price_locked_at, marketplace_price_contract_key, expires_at, updated_at)
		values ($1, $2, $3, $4, $5, $6, $7, now())
		on conflict (response_id_hash) do update
		set token_id = excluded.token_id,
		    marketplace_price_bps = excluded.marketplace_price_bps,
		    marketplace_price_source = excluded.marketplace_price_source,
		    marketplace_price_locked_at = excluded.marketplace_price_locked_at,
		    marketplace_price_contract_key = excluded.marketplace_price_contract_key,
		    expires_at = excluded.expires_at,
		    updated_at = now()
	`, stableHash(responseID), binding.TokenID, priceBPS, source, lockedAt, contractKey, time.Now().UTC().Add(ttl))
}

func (s *PostgresStore) GetResponseOwner(responseID string) (ResponseOwnerBinding, bool) {
	atomic.AddInt64(&s.stats.ResponseLookups, 1)
	ctx, cancel := s.context()
	defer cancel()
	var binding ResponseOwnerBinding
	var priceBPS *int
	var source *string
	var lockedAt *time.Time
	var contractKey *string
	if err := s.pool.QueryRow(ctx, `
		select token_id, marketplace_price_bps, marketplace_price_source, marketplace_price_locked_at, marketplace_price_contract_key, expires_at
		from response_owner_bindings
		where response_id_hash = $1 and expires_at > now()
	`, stableHash(responseID)).Scan(&binding.TokenID, &priceBPS, &source, &lockedAt, &contractKey, &binding.ExpiresAt); err != nil {
		return ResponseOwnerBinding{}, false
	}
	if priceBPS != nil {
		lock := MarketplacePriceLock{PriceBPS: *priceBPS}
		if source != nil {
			lock.Source = *source
		}
		if lockedAt != nil {
			lock.LockedAt = *lockedAt
		}
		if contractKey != nil {
			lock.ContractKey = *contractKey
		}
		binding.MarketplacePriceLock = &lock
	}
	return binding, true
}

func (s *PostgresStore) Stats() Metrics {
	return Metrics{
		Gets:            atomic.LoadInt64(&s.stats.Gets),
		Hits:            atomic.LoadInt64(&s.stats.Hits),
		Puts:            atomic.LoadInt64(&s.stats.Puts),
		Removals:        atomic.LoadInt64(&s.stats.Removals),
		ResponseBinds:   atomic.LoadInt64(&s.stats.ResponseBinds),
		ResponseLookups: atomic.LoadInt64(&s.stats.ResponseLookups),
	}
}

func (s *PostgresStore) context() (context.Context, context.CancelFunc) {
	timeout := s.timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return context.WithTimeout(context.Background(), timeout)
}

func stableHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func removeTokenFromLane(lane Lane, tokenID int64) (Lane, bool) {
	changed := false
	if lane.PrimaryTokenID == tokenID {
		changed = true
		if len(lane.SecondaryTokenIDs) > 0 {
			lane.PrimaryTokenID = lane.SecondaryTokenIDs[0]
			lane.SecondaryTokenIDs = lane.SecondaryTokenIDs[1:]
		} else {
			lane.PrimaryTokenID = 0
		}
	}
	filtered := lane.SecondaryTokenIDs[:0]
	for _, id := range lane.SecondaryTokenIDs {
		if id == tokenID {
			changed = true
			continue
		}
		filtered = append(filtered, id)
	}
	lane.SecondaryTokenIDs = filtered
	if lane.MarketplacePriceLocks != nil {
		delete(lane.MarketplacePriceLocks, tokenID)
	}
	lane.NormalizeMarketplacePriceLocks()
	return lane, changed
}

func tokenIndexKey(prefix string, tokenID int64) string {
	return prefix + strconv.FormatInt(tokenID, 10)
}
