// Package materialise is the feature-materialiser: the single writer to the
// feature store (System Design §3, §8). Every figure the request reads on the
// clock is deposited here off the clock, and every write carries a computed_at
// stamp — which is what lets the reader treat staleness as a first-class fact
// and fail closed on it, rather than trusting a silently old number.
//
// It has two kinds of input:
//
//   - The model-free day-one signal, consumption_frac, which it computes itself
//     from the token ceiling and the reconstructed block-state whenever money
//     moves. This needs no model and is available from the very first capture.
//   - The learned figures the ML engines and the reputation-builder produce
//     (behaviour deviation, intent divergence, network risk, reputation), each
//     deposited through a typed method that merges only its own field into the
//     keyed row. Merge-by-field is why two engines writing the same entity key
//     (e.g. behaviour and reputation on an agent) coexist instead of clobbering.
//
// computed_at is latest-write-wins: a row's freshness reflects its most recent
// materialisation. A per-field freshness stamp — so a stalled engine's figure
// cannot be masked by another engine refreshing the same row — is a deferred
// schema evolution; today at most one figure lands per key.
package materialise

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Sagarkhandagre897/AgentShield/internal/bus"
	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
	"github.com/Sagarkhandagre897/AgentShield/internal/store"
)

// Group is the consumer-group name the materialiser subscribes under.
const Group = "feature-materialiser"

// Materialiser owns all writes to the feature store. Its methods serialise on a
// single mutex, matching its identity as the one writer: a read-modify-write of
// a shared row cannot lose an update.
type Materialiser struct {
	tokens   store.TokenStore
	features store.FeatureStore
	now      func() int64

	mu   sync.Mutex
	seen map[string]struct{} // event_ids already handled (idempotency)
}

// New returns a materialiser. now stamps computed_at on the figures it computes
// itself; if nil it defaults to wall-clock unix seconds.
func New(tokens store.TokenStore, features store.FeatureStore, now func() int64) *Materialiser {
	if now == nil {
		now = func() int64 { return time.Now().Unix() }
	}
	return &Materialiser{
		tokens:   tokens,
		features: features,
		now:      now,
		seen:     make(map[string]struct{}),
	}
}

// Register subscribes the materialiser to the bus under its consumer group.
func (m *Materialiser) Register(b bus.Bus) (func(), error) {
	return b.Subscribe(Group, m.Handle)
}

// Handle is the bus.Handler. It recomputes consumption_frac when money moves;
// other event types are no-ops here (the learned figures arrive through the
// Deposit methods). It is idempotent on event_id and marks an event handled only
// after the write commits, so redelivery re-applies safely.
func (m *Materialiser) Handle(ctx context.Context, ev domain.Event) error {
	if ev.Type != bus.EventPaymentCaptured {
		return nil // only a capture changes consumption; nothing else to materialise here
	}
	if ev.EventID == "" || ev.TokenID == "" {
		return nil
	}

	m.mu.Lock()
	_, done := m.seen[ev.EventID]
	m.mu.Unlock()
	if done {
		return nil
	}

	if err := m.refreshConsumption(ctx, ev.TokenID); err != nil {
		return err // leave unmarked so the bus redelivers
	}

	m.mu.Lock()
	m.seen[ev.EventID] = struct{}{}
	m.mu.Unlock()
	return nil
}

// refreshConsumption recomputes consumption_frac for a token from its lifetime
// ceiling and its reconstructed consumption, and deposits it on the token's row.
// It is model-free: available from the first capture, with no training.
func (m *Materialiser) refreshConsumption(ctx context.Context, tokenID string) error {
	tok, err := m.tokens.GetToken(ctx, tokenID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil // no mandate, no ceiling to divide by; nothing to compute
		}
		return err
	}
	if tok.TokenCeilingPaise <= 0 {
		return nil
	}

	var consumed int64
	bs, err := m.tokens.GetBlockState(ctx, tokenID)
	switch {
	case err == nil:
		consumed = bs.ConsumedTotal
	case errors.Is(err, store.ErrNotFound):
		consumed = 0 // no consumption folded yet
	default:
		return err
	}

	frac := clamp01(float64(consumed) / float64(tok.TokenCeilingPaise))
	return m.DepositConsumption(ctx, tokenID, frac, m.now())
}

// DepositConsumption sets the model-free consumption fraction on a token's row.
func (m *Materialiser) DepositConsumption(ctx context.Context, key string, frac float64, at int64) error {
	return m.merge(ctx, key, at, func(r *domain.FeatureRow) { r.ConsumptionFrac = clamp01(frac) })
}

// DepositBehaviour sets the agent-behaviour deviation and its per-signal
// breakdown (from the behaviour engine).
func (m *Materialiser) DepositBehaviour(ctx context.Context, key string, deviation float64, signals []domain.SignalDeviation, at int64) error {
	return m.merge(ctx, key, at, func(r *domain.FeatureRow) {
		r.BehaviourDeviation = clamp01(deviation)
		r.SignalDeviations = signals
	})
}

// DepositIntent sets the intent-divergence figure (from the intent engine).
func (m *Materialiser) DepositIntent(ctx context.Context, key string, divergence float64, at int64) error {
	return m.merge(ctx, key, at, func(r *domain.FeatureRow) { r.IntentDivergence = clamp01(divergence) })
}

// DepositNetwork sets the network-risk figure (from the graph engine).
func (m *Materialiser) DepositNetwork(ctx context.Context, key string, risk float64, at int64) error {
	return m.merge(ctx, key, at, func(r *domain.FeatureRow) { r.NetworkRisk = clamp01(risk) })
}

// DepositReputation sets the agent reputation (from the reputation-builder).
func (m *Materialiser) DepositReputation(ctx context.Context, key string, reputation float64, at int64) error {
	return m.merge(ctx, key, at, func(r *domain.FeatureRow) { r.Reputation = clamp01(reputation) })
}

// merge read-modify-writes the row for key, applying only the caller's field(s)
// and stamping computed_at. Serialised so concurrent deposits to the same key
// cannot lose an update.
func (m *Materialiser) merge(ctx context.Context, key string, at int64, apply func(*domain.FeatureRow)) error {
	if key == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	row := m.current(ctx, key)
	apply(row)
	row.Key = key
	row.ComputedAt = at
	return m.features.Put(ctx, row)
}

// current returns the existing row for key (a copy, safe to mutate) or a fresh
// row keyed by it.
func (m *Materialiser) current(ctx context.Context, key string) *domain.FeatureRow {
	if rows, err := m.features.MultiGet(ctx, []string{key}); err == nil {
		if r, ok := rows[key]; ok {
			return r
		}
	}
	return &domain.FeatureRow{Key: key}
}

func clamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}
