// Package redis provides Redis-backed implementations of the three hot stores
// (System Design §9), behind the same interfaces the in-memory adapters satisfy.
// This is the RAM-resident KV that lets the two planes run as separate processes:
// the off-clock workers write here, and the on-clock request reads here by key in
// single-digit milliseconds — no scan, no join, because the joins already happened
// off the clock.
//
// Every value is stored as JSON under a namespaced key (token:/block:/overlay:/
// feature:) so the three logical stores can share one Redis without colliding.
// A missing key maps to store.ErrNotFound for the token and policy stores (a hard
// condition the predicates act on) and to omission for the feature store (absence
// is a first-class "missing figure", never an optimistic zero, §8).
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
	"github.com/Sagarkhandagre897/AgentShield/internal/store"
)

// Key namespaces. Redis is a shared KV; prefixing keeps the token, block-state,
// overlay and feature rows from colliding in one instance.
const (
	nsToken   = "token:"
	nsBlock   = "block:"
	nsOverlay = "overlay:"
	nsFeature = "feature:"
)

// Dial opens a client to addr and verifies reachability with PING, so a
// misconfigured endpoint fails at startup rather than on the first read.
func Dial(ctx context.Context, addr string) (*goredis.Client, error) {
	c := goredis.NewClient(&goredis.Options{Addr: addr})
	if err := c.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("redis: dial %s: %w", addr, err)
	}
	return c, nil
}

// mapNotFound normalises go-redis's redis.Nil (key absent) to store.ErrNotFound.
func mapNotFound(err error) error {
	if errors.Is(err, goredis.Nil) {
		return store.ErrNotFound
	}
	return err
}

// PolicyStore is a Redis store.PolicyStore, keyed by token_id.
type PolicyStore struct{ c *goredis.Client }

// NewPolicyStore wraps a client as a policy store.
func NewPolicyStore(c *goredis.Client) *PolicyStore { return &PolicyStore{c: c} }

// GetOverlay returns the overlay for a token, or store.ErrNotFound.
func (s *PolicyStore) GetOverlay(ctx context.Context, tokenID string) (*domain.PolicyOverlay, error) {
	b, err := s.c.Get(ctx, nsOverlay+tokenID).Bytes()
	if err != nil {
		return nil, mapNotFound(err)
	}
	var o domain.PolicyOverlay
	if err := json.Unmarshal(b, &o); err != nil {
		return nil, fmt.Errorf("redis: decode overlay %s: %w", tokenID, err)
	}
	return &o, nil
}

// PutOverlay stores an overlay. Tighten-only enforcement lives with policy
// resolution (where the token bound is available), not here.
func (s *PolicyStore) PutOverlay(ctx context.Context, overlay *domain.PolicyOverlay) error {
	if overlay == nil || overlay.TokenID == "" {
		return store.ErrNotFound
	}
	b, err := json.Marshal(overlay)
	if err != nil {
		return err
	}
	return s.c.Set(ctx, nsOverlay+overlay.TokenID, b, 0).Err()
}

// TokenStore is a Redis store.TokenStore: the mandate and the event-sourced lien,
// both keyed by token_id.
type TokenStore struct{ c *goredis.Client }

// NewTokenStore wraps a client as a token / block-state store.
func NewTokenStore(c *goredis.Client) *TokenStore { return &TokenStore{c: c} }

// GetToken returns the mandate for a token_id, or store.ErrNotFound.
func (s *TokenStore) GetToken(ctx context.Context, tokenID string) (*domain.Token, error) {
	b, err := s.c.Get(ctx, nsToken+tokenID).Bytes()
	if err != nil {
		return nil, mapNotFound(err)
	}
	var t domain.Token
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, fmt.Errorf("redis: decode token %s: %w", tokenID, err)
	}
	return &t, nil
}

// PutToken stores a mandate after enforcing the containment invariant. A write
// that violates it is rejected, not clamped (§10) — the invariant holds on every
// backend, not just in memory.
func (s *TokenStore) PutToken(ctx context.Context, token *domain.Token) error {
	if token == nil {
		return store.ErrNotFound
	}
	if err := token.Validate(); err != nil {
		return err
	}
	b, err := json.Marshal(token)
	if err != nil {
		return err
	}
	return s.c.Set(ctx, nsToken+token.TokenID, b, 0).Err()
}

// GetBlockState returns the event-sourced lien for a token_id, or
// store.ErrNotFound.
func (s *TokenStore) GetBlockState(ctx context.Context, tokenID string) (*domain.BlockState, error) {
	b, err := s.c.Get(ctx, nsBlock+tokenID).Bytes()
	if err != nil {
		return nil, mapNotFound(err)
	}
	var bs domain.BlockState
	if err := json.Unmarshal(b, &bs); err != nil {
		return nil, fmt.Errorf("redis: decode block-state %s: %w", tokenID, err)
	}
	return &bs, nil
}

// PutBlockState stores the reconstructed lien state.
func (s *TokenStore) PutBlockState(ctx context.Context, state *domain.BlockState) error {
	if state == nil || state.TokenID == "" {
		return store.ErrNotFound
	}
	b, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return s.c.Set(ctx, nsBlock+state.TokenID, b, 0).Err()
}

// FeatureStore is a Redis store.FeatureStore. The clock reads it with one keyed
// multi-get across the entity ids (customer / token / agent / merchant / node).
type FeatureStore struct{ c *goredis.Client }

// NewFeatureStore wraps a client as a feature store.
func NewFeatureStore(c *goredis.Client) *FeatureStore { return &FeatureStore{c: c} }

// MultiGet returns the rows present for the given keys in one MGET round trip.
// A key with no row (Redis returns nil for that slot) is simply absent from the
// result — the caller treats absence as missing, which raises risk; it is never
// filled with a zero row (§8). A malformed stored value is likewise skipped
// rather than surfaced as a fabricated figure.
func (s *FeatureStore) MultiGet(ctx context.Context, keys []string) (map[string]*domain.FeatureRow, error) {
	out := make(map[string]*domain.FeatureRow, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	redisKeys := make([]string, len(keys))
	for i, k := range keys {
		redisKeys[i] = nsFeature + k
	}
	vals, err := s.c.MGet(ctx, redisKeys...).Result()
	if err != nil {
		return nil, err
	}
	for i, v := range vals {
		if v == nil { // key absent — leave it missing, never optimistic-zero
			continue
		}
		str, ok := v.(string)
		if !ok {
			continue
		}
		var r domain.FeatureRow
		if json.Unmarshal([]byte(str), &r) != nil {
			continue
		}
		out[keys[i]] = &r
	}
	return out, nil
}

// Put deposits (or replaces) one feature row, keyed by row.Key. Only the
// materialiser calls this; the engines deposit through it, never directly.
func (s *FeatureStore) Put(ctx context.Context, row *domain.FeatureRow) error {
	if row == nil || row.Key == "" {
		return store.ErrNotFound
	}
	b, err := json.Marshal(row)
	if err != nil {
		return err
	}
	return s.c.Set(ctx, nsFeature+row.Key, b, 0).Err()
}

// Compile-time checks that the Redis types satisfy the interfaces.
var (
	_ store.PolicyStore  = (*PolicyStore)(nil)
	_ store.TokenStore   = (*TokenStore)(nil)
	_ store.FeatureStore = (*FeatureStore)(nil)
)
