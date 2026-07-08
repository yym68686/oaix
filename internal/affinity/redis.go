package affinity

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const redisPrefix = "oaix:affinity:"

type RedisStore struct {
	client  *redis.Client
	timeout time.Duration
	stats   Metrics
}

func NewRedisStore(client *redis.Client) *RedisStore {
	return &RedisStore{client: client, timeout: 2 * time.Second}
}

func NewRedisStoreFromURL(rawURL string) (*RedisStore, error) {
	options, err := redis.ParseURL(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, err
	}
	return NewRedisStore(redis.NewClient(options)), nil
}

func (s *RedisStore) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Close()
}

func (s *RedisStore) Get(promptKey string) (Lane, bool) {
	atomic.AddInt64(&s.stats.Gets, 1)
	ctx, cancel := s.context()
	defer cancel()
	payload, err := s.client.Get(ctx, laneKey(promptKey)).Bytes()
	if err != nil {
		return Lane{}, false
	}
	var lane Lane
	if err := json.Unmarshal(payload, &lane); err != nil {
		return Lane{}, false
	}
	if !lane.ExpiresAt.IsZero() && time.Now().UTC().After(lane.ExpiresAt) {
		return Lane{}, false
	}
	atomic.AddInt64(&s.stats.Hits, 1)
	return lane, true
}

func (s *RedisStore) Put(promptKey string, lane Lane, ttl time.Duration) {
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
	payload, _ := json.Marshal(lane)
	ctx, cancel := s.context()
	defer cancel()
	key := laneKey(promptKey)
	pipe := s.client.Pipeline()
	pipe.Set(ctx, key, payload, ttl)
	if lane.PrimaryTokenID > 0 {
		pipe.SAdd(ctx, redisTokenIndexKey(lane.PrimaryTokenID), key)
		pipe.Expire(ctx, redisTokenIndexKey(lane.PrimaryTokenID), ttl)
	}
	for _, tokenID := range lane.SecondaryTokenIDs {
		if tokenID <= 0 {
			continue
		}
		pipe.SAdd(ctx, redisTokenIndexKey(tokenID), key)
		pipe.Expire(ctx, redisTokenIndexKey(tokenID), ttl)
	}
	_, _ = pipe.Exec(ctx)
}

func (s *RedisStore) RemoveToken(tokenID int64) {
	atomic.AddInt64(&s.stats.Removals, 1)
	if tokenID <= 0 {
		return
	}
	ctx, cancel := s.context()
	defer cancel()
	indexKey := redisTokenIndexKey(tokenID)
	keys, err := s.client.SMembers(ctx, indexKey).Result()
	if err != nil {
		return
	}
	for _, key := range keys {
		payload, err := s.client.Get(ctx, key).Bytes()
		if err != nil {
			continue
		}
		var lane Lane
		if err := json.Unmarshal(payload, &lane); err != nil {
			continue
		}
		next, changed := removeTokenFromLane(lane, tokenID)
		if !changed {
			continue
		}
		if next.PrimaryTokenID == 0 && len(next.SecondaryTokenIDs) == 0 {
			_ = s.client.Del(ctx, key).Err()
			continue
		}
		next.UpdatedAt = time.Now().UTC()
		ttl := time.Until(next.ExpiresAt)
		if ttl <= 0 {
			ttl = time.Minute
		}
		payload, _ = json.Marshal(next)
		_ = s.client.Set(ctx, key, payload, ttl).Err()
	}
	_ = s.client.Del(ctx, indexKey, redisOwnerTokenIndexKey(tokenID)).Err()
}

func (s *RedisStore) BindResponseOwner(responseID string, binding ResponseOwnerBinding, ttl time.Duration) {
	atomic.AddInt64(&s.stats.ResponseBinds, 1)
	if binding.TokenID <= 0 {
		return
	}
	if ttl <= 0 {
		ttl = time.Hour
	}
	ctx, cancel := s.context()
	defer cancel()
	key := responseKey(responseID)
	payload, _ := json.Marshal(binding)
	pipe := s.client.Pipeline()
	pipe.Set(ctx, key, payload, ttl)
	pipe.SAdd(ctx, redisOwnerTokenIndexKey(binding.TokenID), key)
	pipe.Expire(ctx, redisOwnerTokenIndexKey(binding.TokenID), ttl)
	_, _ = pipe.Exec(ctx)
}

func (s *RedisStore) GetResponseOwner(responseID string) (ResponseOwnerBinding, bool) {
	atomic.AddInt64(&s.stats.ResponseLookups, 1)
	ctx, cancel := s.context()
	defer cancel()
	text, err := s.client.Get(ctx, responseKey(responseID)).Result()
	if err != nil {
		return ResponseOwnerBinding{}, false
	}
	var binding ResponseOwnerBinding
	if err := json.Unmarshal([]byte(text), &binding); err == nil && binding.TokenID > 0 {
		return binding, true
	}
	tokenID, err := strconv.ParseInt(text, 10, 64)
	if err != nil || tokenID <= 0 {
		return ResponseOwnerBinding{}, false
	}
	return ResponseOwnerBinding{TokenID: tokenID}, true
}

func (s *RedisStore) Stats() Metrics {
	return Metrics{
		Gets:            atomic.LoadInt64(&s.stats.Gets),
		Hits:            atomic.LoadInt64(&s.stats.Hits),
		Puts:            atomic.LoadInt64(&s.stats.Puts),
		Removals:        atomic.LoadInt64(&s.stats.Removals),
		ResponseBinds:   atomic.LoadInt64(&s.stats.ResponseBinds),
		ResponseLookups: atomic.LoadInt64(&s.stats.ResponseLookups),
	}
}

func (s *RedisStore) context() (context.Context, context.CancelFunc) {
	timeout := s.timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return context.WithTimeout(context.Background(), timeout)
}

func laneKey(promptKey string) string {
	return redisPrefix + "lane:" + stableHash(promptKey)
}

func responseKey(responseID string) string {
	return redisPrefix + "response:" + stableHash(responseID)
}

func redisTokenIndexKey(tokenID int64) string {
	return tokenIndexKey(redisPrefix+"token:", tokenID)
}

func redisOwnerTokenIndexKey(tokenID int64) string {
	return tokenIndexKey(redisPrefix+"owner-token:", tokenID)
}
