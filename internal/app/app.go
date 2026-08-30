// Package app is the in-process composition root: it wires the shared stores,
// the in-memory bus and CHAIN, the three off-clock workers and the synchronous
// decision service into one running system (System Design §3).
//
// Why one process: the dev build backs everything with in-memory stores and an
// in-memory bus, and memory is not shared across OS processes — the decision
// service and the workers must hold the SAME instances to interact at all. So
// the two planes' invariant meeting points are wired right here:
//
//   - bus, going down: the decision service publishes decision.made and the
//     stream-processor consumes it (spending the nonce of an ALLOW).
//   - feature store, going up: the materialiser is the SINGLE writer; the
//     reputation-builder deposits through it, and the decision service reads it.
//
// When durable adapters (Kafka, Redis, PostgreSQL) replace the in-memory ones,
// each plane becomes its own deployment and this root splits along the same
// seams — the wiring, not the contracts, is what changes.
package app

import (
	"context"
	"time"

	"github.com/Sagarkhandagre897/AgentShield/internal/bus"
	busmem "github.com/Sagarkhandagre897/AgentShield/internal/bus/memory"
	"github.com/Sagarkhandagre897/AgentShield/internal/chain"
	"github.com/Sagarkhandagre897/AgentShield/internal/decision"
	"github.com/Sagarkhandagre897/AgentShield/internal/features"
	"github.com/Sagarkhandagre897/AgentShield/internal/materialise"
	"github.com/Sagarkhandagre897/AgentShield/internal/reputation"
	"github.com/Sagarkhandagre897/AgentShield/internal/score"
	"github.com/Sagarkhandagre897/AgentShield/internal/store"
	"github.com/Sagarkhandagre897/AgentShield/internal/store/memory"
	"github.com/Sagarkhandagre897/AgentShield/internal/stream"
)

// Config tunes the assembled system. Every field is optional: with a zero Config
// the service authenticates via mTLS, the workers use the wall clock, and the bus
// retries a failing handler three times before dead-lettering.
//
// The four backend fields split the process. Left nil (the dev/test default), New
// builds in-memory stores and an in-memory bus and runs the three workers in THIS
// process — one runnable system with no external infra. Supplied together (Redis
// stores + a Kafka bus), New wires only the decision service and does NOT run the
// workers: they live in cmd/worker, and registering them here would double-process
// every event. Close then owns the provided bus's shutdown.
type Config struct {
	Identify   func(ctx context.Context) string // caller identity (default: mTLS peer cert)
	Now        func() int64                     // clock for the workers and the service (default: wall clock)
	BusRetries int                              // at-least-once retry bound for the in-memory bus

	Tokens   store.TokenStore   // durable token/block-state store (default: in-memory)
	Policies store.PolicyStore  // durable policy store (default: in-memory)
	Features store.FeatureStore // durable feature store (default: in-memory)
	Bus      bus.Bus            // durable bus (default: in-memory)
}

// App is the wired in-process system. The stores, bus and CHAIN are exported so a
// caller (the gRPC entrypoint, a demo, a test) can seed mandates, inject webhook
// events and verify provenance; Decision is what the transport serves.
type App struct {
	Tokens   store.TokenStore
	Policies store.PolicyStore
	Features store.FeatureStore
	Bus      bus.Bus
	Chain    *chain.Chain
	Decision *decision.Service

	Stream       *stream.Processor
	Materialiser *materialise.Materialiser
	Reputation   *reputation.Builder

	cancels []func()
}

// New assembles and starts the system. In single-process mode the workers
// subscribe to the bus before New returns, so the async plane is live once it
// does; Close stops them. In split-process mode (durable backends supplied) the
// workers run elsewhere and New wires only the decision service.
func New(cfg Config) (*App, error) {
	now := cfg.Now
	if now == nil {
		now = func() int64 { return time.Now().Unix() }
	}

	// Split the process on whether durable backends were supplied. All four must
	// come together — a decision host reading Redis but publishing to an in-memory
	// bus no worker consumes would silently drop every outcome.
	external := cfg.Tokens != nil && cfg.Policies != nil && cfg.Features != nil && cfg.Bus != nil

	var (
		tokens   store.TokenStore
		policies store.PolicyStore
		fstore   store.FeatureStore
		b        bus.Bus
	)
	if external {
		tokens, policies, fstore, b = cfg.Tokens, cfg.Policies, cfg.Features, cfg.Bus
	} else {
		tokens = memory.NewTokenStore()
		policies = memory.NewPolicyStore()
		fstore = memory.NewFeatureStore()
		b = busmem.New(cfg.BusRetries)
	}
	ledger := chain.New()

	a := &App{
		Tokens:   tokens,
		Policies: policies,
		Features: fstore,
		Bus:      b,
		Chain:    ledger,
	}

	// Run the workers in-process only in single-process mode. When durable
	// backends are supplied, cmd/worker owns them; registering here would fold
	// every event twice.
	if !external {
		// The materialiser is the single writer to the feature store; every other
		// producer deposits through it. The reputation-builder is one such producer,
		// so it takes the materialiser as its Depositor.
		mat := materialise.New(tokens, fstore, now)
		rep := reputation.New(mat, reputation.DefaultParams(), now)
		strm := stream.New(tokens)
		a.Stream, a.Materialiser, a.Reputation = strm, mat, rep

		// Subscribe every worker, collecting cancels so Close can stop them. A failed
		// registration tears down what was already wired.
		for _, register := range []func(bus.Bus) (func(), error){
			strm.Register, mat.Register, rep.Register,
		} {
			cancel, err := register(b)
			if err != nil {
				a.stopWorkers()
				_ = b.Close()
				return nil, err
			}
			a.cancels = append(a.cancels, cancel)
		}
	}

	a.Decision = decision.New(decision.Config{
		Tokens:   tokens,
		Policies: policies,
		Features: features.NewReader(fstore, features.DefaultStalenessBudgetSeconds),
		Scorer:   score.NewLinearScorer(score.DefaultWeights),
		Params:   score.Params{InterruptionCostPaise: score.DefaultInterruptionCostPaise},
		Sink:     chain.NewSink(ledger), // provenance, going down
		Events:   b,                     // decision.made, going down (the bus meeting point)
		Identify: cfg.Identify,
		Now:      now,
	})

	return a, nil
}

// Close stops the workers and the bus. It leaves the stores and the CHAIN intact
// so their final state can still be inspected.
func (a *App) Close() error {
	a.stopWorkers()
	return a.Bus.Close()
}

func (a *App) stopWorkers() {
	for _, cancel := range a.cancels {
		cancel()
	}
	a.cancels = nil
}
