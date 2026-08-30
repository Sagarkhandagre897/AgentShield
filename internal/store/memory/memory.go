// Package memory provides in-memory, concurrency-safe implementations of the
// three hot stores. They keep the synchronous plane testable with zero external
// dependencies: the deterministic spine can block and step-up correctly with no
// Redis, no Kafka and no network in the loop. Redis/Dragonfly adapters land with
// the asynchronous plane, behind the same interfaces.
//
// Every read returns a deep-enough copy that a caller cannot mutate stored state
// through the returned pointer.
package memory

import (
	"context"
	"sync"

	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
	"github.com/Sagarkhandagre897/AgentShield/internal/store"
)

// PolicyStore is an in-memory store.PolicyStore.
type PolicyStore struct {
	mu       sync.RWMutex
	overlays map[string]domain.PolicyOverlay
}

// NewPolicyStore returns an empty policy store.
func NewPolicyStore() *PolicyStore {
	return &PolicyStore{overlays: make(map[string]domain.PolicyOverlay)}
}

// GetOverlay returns the overlay for a token, or store.ErrNotFound.
func (s *PolicyStore) GetOverlay(_ context.Context, tokenID string) (*domain.PolicyOverlay, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	o, ok := s.overlays[tokenID]
	if !ok {
		return nil, store.ErrNotFound
	}
	return &o, nil
}

// PutOverlay stores an overlay. Tighten-only enforcement lives with policy
// resolution (where the token bound is available), not here.
func (s *PolicyStore) PutOverlay(_ context.Context, overlay *domain.PolicyOverlay) error {
	if overlay == nil || overlay.TokenID == "" {
		return store.ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.overlays[overlay.TokenID] = *overlay
	return nil
}

// TokenStore is an in-memory store.TokenStore.
type TokenStore struct {
	mu     sync.RWMutex
	tokens map[string]domain.Token
	blocks map[string]domain.BlockState
}

// NewTokenStore returns an empty token / block-state store.
func NewTokenStore() *TokenStore {
	return &TokenStore{
		tokens: make(map[string]domain.Token),
		blocks: make(map[string]domain.BlockState),
	}
}

// GetToken returns the mandate for a token_id, or store.ErrNotFound.
func (s *TokenStore) GetToken(_ context.Context, tokenID string) (*domain.Token, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.tokens[tokenID]
	if !ok {
		return nil, store.ErrNotFound
	}
	return &t, nil
}

// PutToken stores a mandate after enforcing the containment invariant. A write
// that violates it is rejected, not clamped (§10).
func (s *TokenStore) PutToken(_ context.Context, token *domain.Token) error {
	if token == nil {
		return store.ErrNotFound
	}
	if err := token.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens[token.TokenID] = *token
	return nil
}

// GetBlockState returns the event-sourced lien for a token_id, or
// store.ErrNotFound.
func (s *TokenStore) GetBlockState(_ context.Context, tokenID string) (*domain.BlockState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.blocks[tokenID]
	if !ok {
		return nil, store.ErrNotFound
	}
	// Copy the slice so callers cannot mutate stored nonces.
	nonces := make([]string, len(b.SeenNonces))
	copy(nonces, b.SeenNonces)
	b.SeenNonces = nonces
	return &b, nil
}

// PutBlockState stores the reconstructed lien state.
func (s *TokenStore) PutBlockState(_ context.Context, state *domain.BlockState) error {
	if state == nil || state.TokenID == "" {
		return store.ErrNotFound
	}
	nonces := make([]string, len(state.SeenNonces))
	copy(nonces, state.SeenNonces)
	cp := *state
	cp.SeenNonces = nonces
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blocks[state.TokenID] = cp
	return nil
}

// FeatureStore is an in-memory store.FeatureStore.
type FeatureStore struct {
	mu   sync.RWMutex
	rows map[string]domain.FeatureRow
}

// NewFeatureStore returns an empty feature store.
func NewFeatureStore() *FeatureStore {
	return &FeatureStore{rows: make(map[string]domain.FeatureRow)}
}

// MultiGet returns the rows present for the given keys. Keys with no row are
// simply absent from the result — the caller treats absence as missing, which
// raises risk; it is never filled with a zero row.
func (s *FeatureStore) MultiGet(_ context.Context, keys []string) (map[string]*domain.FeatureRow, error) {
	out := make(map[string]*domain.FeatureRow, len(keys))
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, k := range keys {
		if r, ok := s.rows[k]; ok {
			cp := r
			cp.SignalDeviations = append([]domain.SignalDeviation(nil), r.SignalDeviations...)
			out[k] = &cp
		}
	}
	return out, nil
}

// Put deposits (or replaces) one feature row, keyed by row.Key.
func (s *FeatureStore) Put(_ context.Context, row *domain.FeatureRow) error {
	if row == nil || row.Key == "" {
		return store.ErrNotFound
	}
	cp := *row
	cp.SignalDeviations = append([]domain.SignalDeviation(nil), row.SignalDeviations...)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[row.Key] = cp
	return nil
}

// Compile-time checks that the in-memory types satisfy the interfaces.
var (
	_ store.PolicyStore  = (*PolicyStore)(nil)
	_ store.TokenStore   = (*TokenStore)(nil)
	_ store.FeatureStore = (*FeatureStore)(nil)
)
