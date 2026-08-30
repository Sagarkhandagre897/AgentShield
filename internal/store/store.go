// Package store defines the three hot stores the synchronous plane reads on the
// clock (System Design §9), as interfaces. Each is reached by a single key and
// returns in single-digit milliseconds — no scan, no join — because the joins
// already happened off the clock.
//
// The decision service only ever calls the read methods. The write methods
// exist for the asynchronous workers (which reconstruct block-state and
// materialise feature rows) and for seeding in tests. Concrete backends
// (in-memory now; Redis/Dragonfly with the async plane) live in sub-packages.
package store

import (
	"context"
	"errors"

	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
)

// ErrNotFound is returned by keyed reads when the key is absent. For the token
// and policy stores this is a hard condition the predicates act on. For the
// feature store, absence is a normal, first-class case (a missing figure raises
// risk), so MultiGet reports it by omission rather than as an error.
var ErrNotFound = errors.New("store: not found")

// PolicyStore holds the customer's tightening overlay, keyed by token_id.
type PolicyStore interface {
	GetOverlay(ctx context.Context, tokenID string) (*domain.PolicyOverlay, error)
	PutOverlay(ctx context.Context, overlay *domain.PolicyOverlay) error
}

// TokenStore holds the mandate (Token) and the event-sourced lien (BlockState),
// both keyed by token_id. On the clock these are the availability that matters
// most: a request can survive a cold feature store, but not not knowing a
// token's limits (§9).
type TokenStore interface {
	GetToken(ctx context.Context, tokenID string) (*domain.Token, error)
	PutToken(ctx context.Context, token *domain.Token) error

	GetBlockState(ctx context.Context, tokenID string) (*domain.BlockState, error)
	PutBlockState(ctx context.Context, state *domain.BlockState) error
}

// FeatureStore holds the precomputed figures the workers deposit and the request
// reads by key. The clock does one keyed multi-get across the entity ids
// (customer / token / agent / merchant / node). Keys absent from the returned
// map are treated as missing — never as an optimistic zero (§8).
type FeatureStore interface {
	MultiGet(ctx context.Context, keys []string) (map[string]*domain.FeatureRow, error)
	Put(ctx context.Context, row *domain.FeatureRow) error
}
