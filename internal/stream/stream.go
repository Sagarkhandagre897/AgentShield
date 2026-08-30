// Package stream is the stream-processor: the off-clock worker that folds bus
// events into authoritative BlockState — consumed_today, consumed_total and the
// seen-nonce set the predicates read on the clock (System Design §3, §9). It is
// the single writer of block-state, so the synchronous plane never has to
// reconstruct a lien in the request path.
//
// It is also the single writer of the provenance CHAIN and the encrypted VAULT
// (the architecture diagram assigns both the CHAIN and the VAULT write to this
// processor, not the decision service). For the CHAIN: the decision service replies
// and announces decision.made carrying the full record's fields; this processor
// reconstructs the domain.ProvenanceRecord off the clock and appends it to the
// ledger. Every decision is recorded — ALLOW, STEP_UP and BLOCK alike — so the
// audit trail is complete, not just the allowed slice. For the VAULT: an
// envelope.sealed event carries a session's raw PII once, and this processor seals
// each field into the encrypted, erasable store — the only worker that touches it,
// and never on the clock.
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

// ProvenanceSink appends one decision's record to the CHAIN, off the caller's
// clock. Both the in-memory chain.Sink and the durable pgchain.Sink satisfy it
// structurally (no import cycle), so the composition root plugs in whichever the
// deployment uses. Emit is fire-and-forget by the sink contract; a durable sink
// reports its own failures out of band rather than returning them here.
type ProvenanceSink interface {
	Emit(ctx context.Context, rec *domain.ProvenanceRecord)
}

// VaultSink seals one field of a session's PII into the encrypted VAULT. vault.Sink
// satisfies it structurally (no import cycle), so the composition root plugs in the
// durable store. Unlike the CHAIN's Emit, Seal RETURNS its error: sealing is the
// whole purpose of the envelope.sealed fold, so a transient failure must redeliver
// rather than be dropped. field is the wire field name (bus.PayloadRawInstruction /
// bus.PayloadContact); the sink maps it to a vault.Field.
type VaultSink interface {
	Seal(ctx context.Context, sessionID, field, plaintext string) error
}

// Processor folds events into block-state and records decisions on the CHAIN. It
// owns no clock: it stamps windows from each event's occurred_at, which keeps
// replay of historical events deterministic.
type Processor struct {
	tokens store.TokenStore
	chain  ProvenanceSink // CHAIN writer; nil means this processor keeps no ledger
	vault  VaultSink      // VAULT writer; nil means sealing events are ignored (no PII store)

	mu   sync.Mutex
	seen map[string]struct{} // event_ids already folded (idempotency)
}

// New returns a stream-processor writing block-state to the given token store and,
// when the sinks are non-nil, provenance to the CHAIN and sealed PII to the VAULT
// behind them. Pass nil sinks to run without a ledger or PII store (nonce-spending
// and consumption still work); the composition root supplies the in-memory or
// durable stores as the deployment requires.
func New(tokens store.TokenStore, chainSink ProvenanceSink, vaultSink VaultSink) *Processor {
	return &Processor{tokens: tokens, chain: chainSink, vault: vaultSink, seen: make(map[string]struct{})}
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
	case bus.EventEnvelopeSealed:
		return p.foldEnvelopeSealed(ctx, ev)
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

// foldDecision records the decision on the CHAIN and spends the nonce of an
// ALLOWED request so its replay is refused. Every decision is recorded — ALLOW,
// STEP_UP and BLOCK — but only an ALLOW spends a nonce; a non-ALLOW may legitimately
// return with the same nonce.
//
// The fallible nonce write runs BEFORE the CHAIN append so that a store error
// redelivers the event before any provenance is written, and a retry cannot append
// a duplicate record. Same-event redelivery within the process is already caught by
// Handle's seen-set, so the record is appended exactly once per evaluation here.
func (p *Processor) foldDecision(ctx context.Context, ev domain.Event) error {
	if d, _ := bus.PayloadString(ev, bus.PayloadDecision); d == bus.DecisionAllow {
		if err := p.spendNonce(ctx, ev); err != nil {
			return err // leave the CHAIN untouched so the retry appends once
		}
	}
	if p.chain != nil {
		p.chain.Emit(ctx, bus.ProvenanceFromEvent(ev))
	}
	return nil
}

// foldEnvelopeSealed seals a session's raw PII into the VAULT — the once-per-session
// write the architecture diagram assigns to this processor. It seals each field the
// event carries (raw instruction text, contact) under its own vault.Field; the wire
// payload key doubles as the field name (bus.PayloadRawInstruction == the vault
// field), so no mapping table is needed. An absent field is skipped by the sink.
//
// Unlike the CHAIN append, a seal error is RETURNED so the bus redelivers: losing
// the raw text is not acceptable, and vault.Seal is an idempotent upsert, so a
// redelivered seal overwrites the same row identically. With no VAULT configured
// (single-process without Postgres) the event is a no-op — there is no PII store to
// write, and nothing on the clock depends on it.
func (p *Processor) foldEnvelopeSealed(ctx context.Context, ev domain.Event) error {
	if p.vault == nil {
		return nil // no PII store in this deployment; nothing to seal into
	}
	sessionID, _ := bus.PayloadString(ev, bus.PayloadSessionID)
	if sessionID == "" {
		return nil // no VAULT key; nothing to seal against
	}
	for _, field := range []string{bus.PayloadRawInstruction, bus.PayloadContact} {
		plaintext, _ := bus.PayloadString(ev, field)
		if err := p.vault.Seal(ctx, sessionID, field, plaintext); err != nil {
			return err // redeliver; the seen-set is not marked, so no double-seal races here
		}
	}
	return nil
}

// spendNonce adds an ALLOWED request's nonce to the lien, so P1 refuses a replay.
// It is a no-op when the nonce is already recorded.
func (p *Processor) spendNonce(ctx context.Context, ev domain.Event) error {
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
