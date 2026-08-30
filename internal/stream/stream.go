// Package stream is the stream-processor: the off-clock worker that folds bus
// events into authoritative BlockState — consumed_today, consumed_total and the
// seen-nonce set the predicates read on the clock (System Design §3, §9). It is
// the single writer of block-state, so the synchronous plane never has to
// reconstruct a lien in the request path.
//
// Two rules shape what it folds:
//
//   - Consumption advances ONLY on payment.captured — money that actually moved.
//     A decision to ALLOW is not consumption; only a capture is. This is why a
//     cautious ALLOW that never settles never eats into a mandate.
//   - A nonce is marked seen when money moves (capture) or when a request is
//     ALLOWED, so P1 can refuse a replay of an authorised action. A STEP-UP or
//     BLOCK never spends a nonce — the same request may legitimately return.
//
// Delivery is at-least-once, so Handle is idempotent on event_id: each event is
// folded at most once, and the event is marked done only after the fold commits,
// so a fold that fails is safely redelivered.
package stream

import (
	"context"
	"errors"
	"sync"

	"github.com/Sagarkhandagre897/AgentShield/internal/bus"
	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
	"github.com/Sagarkhandagre897/AgentShield/internal/store"
)

// Group is the consumer-group name the stream-processor subscribes under.
const Group = "stream-processor"

// secondsPerDay is the width of the per-day consumption window.
const secondsPerDay = 86400

// Processor folds events into block-state. It owns no clock: it stamps windows
// from each event's occurred_at, which keeps replay of historical events
// deterministic.
type Processor struct {
	tokens store.TokenStore

	mu   sync.Mutex
	seen map[string]struct{} // event_ids already folded (idempotency)
}

// New returns a stream-processor writing to the given token/block-state store.
func New(tokens store.TokenStore) *Processor {
	return &Processor{tokens: tokens, seen: make(map[string]struct{})}
}

// Register subscribes the processor to the bus under its consumer group and
// returns the cancel func.
func (p *Processor) Register(b bus.Bus) (func(), error) {
	return b.Subscribe(Group, p.Handle)
}

// Handle is the bus.Handler. It returns a non-nil error only when a fold
// genuinely failed (a store error), so the bus redelivers; the event is recorded
// as folded only after the write commits, so redelivery re-applies it safely.
func (p *Processor) Handle(ctx context.Context, ev domain.Event) error {
	if ev.EventID == "" || ev.TokenID == "" {
		return nil // nothing to dedupe or key on; drop
	}

	p.mu.Lock()
	_, done := p.seen[ev.EventID]
	p.mu.Unlock()
	if done {
		return nil // at-least-once redelivery of an already-folded event
	}

	if err := p.apply(ctx, ev); err != nil {
		return err // leave unmarked so the bus redelivers
	}

	p.mu.Lock()
	p.seen[ev.EventID] = struct{}{}
	p.mu.Unlock()
	return nil
}

func (p *Processor) apply(ctx context.Context, ev domain.Event) error {
	switch ev.Type {
	case bus.EventPaymentCaptured:
		return p.foldCapture(ctx, ev)
	case bus.EventDecisionMade:
		return p.foldDecision(ctx, ev)
	default:
		// payment.failed moved no money; token.* is another projection's job.
		return nil
	}
}

// foldCapture advances consumption by the captured amount and spends the nonce.
// consumed_today resets when the capture falls in a later day than the last one.
func (p *Processor) foldCapture(ctx context.Context, ev domain.Event) error {
	amount, ok := bus.PayloadInt64(ev, bus.PayloadAmountPaise)
	if !ok || amount <= 0 {
		return nil // malformed capture; nothing to consume
	}
	bs, err := p.load(ctx, ev.TokenID)
	if err != nil {
		return err
	}
	if bs.LastComputedAt == 0 || dayOf(ev.OccurredAt) != dayOf(bs.LastComputedAt) {
		bs.ConsumedToday = 0
	}
	bs.ConsumedToday += amount
	bs.ConsumedTotal += amount
	bs.LastComputedAt = ev.OccurredAt
	if nonce, ok := bus.PayloadString(ev, bus.PayloadNonce); ok {
		bs.SeenNonces = addNonce(bs.SeenNonces, nonce)
	}
	return p.tokens.PutBlockState(ctx, bs)
}

// foldDecision spends the nonce of an ALLOWED request so its replay is refused.
// A non-ALLOW decision leaves no trace: the request may legitimately return.
func (p *Processor) foldDecision(ctx context.Context, ev domain.Event) error {
	if d, _ := bus.PayloadString(ev, bus.PayloadDecision); d != bus.DecisionAllow {
		return nil
	}
	nonce, ok := bus.PayloadString(ev, bus.PayloadNonce)
	if !ok || nonce == "" {
		return nil
	}
	bs, err := p.load(ctx, ev.TokenID)
	if err != nil {
		return err
	}
	updated := addNonce(bs.SeenNonces, nonce)
	if len(updated) == len(bs.SeenNonces) {
		return nil // already recorded; no write needed
	}
	bs.SeenNonces = updated
	return p.tokens.PutBlockState(ctx, bs)
}

// load returns the current block-state, or a fresh zero state keyed by token_id
// if none exists yet (the first event for a mandate seeds it).
func (p *Processor) load(ctx context.Context, tokenID string) (*domain.BlockState, error) {
	bs, err := p.tokens.GetBlockState(ctx, tokenID)
	if err == nil {
		return bs, nil
	}
	if errors.Is(err, store.ErrNotFound) {
		return &domain.BlockState{TokenID: tokenID}, nil
	}
	return nil, err
}

func dayOf(epochSeconds int64) int64 { return epochSeconds / secondsPerDay }

// addNonce appends a nonce unless it is already present, keeping the set unique.
func addNonce(nonces []string, nonce string) []string {
	if nonce == "" {
		return nonces
	}
	for _, n := range nonces {
		if n == nonce {
			return nonces
		}
	}
	return append(nonces, nonce)
}
