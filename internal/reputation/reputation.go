// Package reputation is the reputation-builder: the off-clock worker that turns
// an agent's settled outcomes into one slow-moving trust figure (System Design
// §3, §7). Reputation is subtracted from risk on the clock, so a well-behaved
// agent earns a little slack and an unproven one earns none.
//
// It learns only from GROUND TRUTH that has settled — captures that stuck,
// failures, and disputes/chargebacks — never from "no complaint" and never from
// our own past decisions, which would be a feedback loop that launders a bad
// call into a training label. A capture is a positive; a failure is a soft
// negative; a dispute is the strongest negative and weighs heaviest.
//
// A thin history is shrunk toward a neutral prior, so a brand-new agent sits at
// neutral (earning no slack) rather than being trusted or condemned on a handful
// of events. The estimate is the Beta-Bernoulli posterior mean:
//
//	reputation = (successes + k·p0) / (successes + failures + k)
//
// with p0 the prior mean and k its strength in pseudo-observations.
//
// The builder never writes the feature store itself — that is the materialiser's
// job as single writer — so it deposits through a Depositor. Outcomes are
// additive and commutative, so token-partitioned events bucketed by agent need
// no cross-token ordering; it only needs to fold each event once, so Handle is
// idempotent on event_id.
package reputation

import (
	"context"
	"sync"
	"time"

	"github.com/Sagarkhandagre897/AgentShield/internal/bus"
	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
)

// Group is the consumer-group name the reputation-builder subscribes under.
const Group = "reputation-builder"

// Depositor is the write the builder uses to publish a reputation figure. The
// materialiser satisfies it; taking an interface keeps the single-writer
// invariant and makes the builder testable without a feature store.
type Depositor interface {
	DepositReputation(ctx context.Context, key string, reputation float64, at int64) error
}

// Params tunes the prior and how harshly a dispute counts.
type Params struct {
	PriorMean     float64 // neutral cold-start reputation
	PriorStrength float64 // pseudo-observations of the prior (how much history it takes to move off neutral)
	DisputeWeight float64 // how many plain failures one dispute is worth
}

// DefaultParams is a sensible starting point: neutral 0.5, ~20 observations to
// move meaningfully, a dispute worth five failures.
func DefaultParams() Params {
	return Params{PriorMean: 0.5, PriorStrength: 20, DisputeWeight: 5}
}

type counters struct{ successes, failures float64 }

// Builder accumulates per-agent outcome counters in memory and deposits the
// derived reputation. Counters are commutative, so their order does not matter;
// what matters is folding each event exactly once.
type Builder struct {
	deposit Depositor
	params  Params
	now     func() int64

	mu     sync.Mutex
	agents map[string]*counters
	seen   map[string]struct{}
}

// New returns a reputation-builder. now stamps the deposited figure's
// computed_at; if nil it defaults to wall-clock unix seconds.
func New(deposit Depositor, params Params, now func() int64) *Builder {
	if now == nil {
		now = func() int64 { return time.Now().Unix() }
	}
	return &Builder{
		deposit: deposit,
		params:  params,
		now:     now,
		agents:  make(map[string]*counters),
		seen:    make(map[string]struct{}),
	}
}

// Register subscribes the builder to the bus under its consumer group.
func (b *Builder) Register(bs bus.Bus) (func(), error) {
	return bs.Subscribe(Group, b.Handle)
}

// Handle folds one settled outcome into the agent's reputation. It returns an
// error only when the deposit failed, having rolled the counter change back so
// the bus can redeliver without double-counting; success is recorded only after
// the deposit commits.
func (b *Builder) Handle(ctx context.Context, ev domain.Event) error {
	success, fail, ok := b.classify(ev)
	if !ok {
		return nil // not a settled outcome
	}
	agentID, ok := bus.PayloadString(ev, bus.PayloadAgentID)
	if !ok || agentID == "" || ev.EventID == "" {
		return nil // cannot attribute or dedupe
	}

	b.mu.Lock()
	if _, done := b.seen[ev.EventID]; done {
		b.mu.Unlock()
		return nil // at-least-once redelivery of an already-folded outcome
	}
	c := b.agents[agentID]
	if c == nil {
		c = &counters{}
		b.agents[agentID] = c
	}
	c.successes += success
	c.failures += fail
	rep := b.reputation(c)
	b.mu.Unlock()

	if err := b.deposit.DepositReputation(ctx, agentID, rep, b.now()); err != nil {
		b.mu.Lock()
		c.successes -= success // roll back; the outcome was not persisted
		c.failures -= fail
		b.mu.Unlock()
		return err
	}

	b.mu.Lock()
	b.seen[ev.EventID] = struct{}{}
	b.mu.Unlock()
	return nil
}

// classify maps an event type to its outcome weights. ok is false for events
// that are not settled outcomes.
func (b *Builder) classify(ev domain.Event) (success, fail float64, ok bool) {
	switch ev.Type {
	case bus.EventPaymentCaptured:
		return 1, 0, true
	case bus.EventPaymentFailed:
		return 0, 1, true
	case bus.EventPaymentDisputed:
		return 0, b.params.DisputeWeight, true
	default:
		return 0, 0, false
	}
}

// reputation is the Beta-Bernoulli posterior mean, clamped to [0,1].
func (b *Builder) reputation(c *counters) float64 {
	num := c.successes + b.params.PriorStrength*b.params.PriorMean
	den := c.successes + c.failures + b.params.PriorStrength
	if den <= 0 {
		return b.params.PriorMean
	}
	return clamp01(num / den)
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
