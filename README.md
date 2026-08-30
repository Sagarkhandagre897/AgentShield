# AgentShield

A real-time **Agent Trust & Risk layer** that answers at `POST /v1/orders`,
before any rupee moves. When an AI agent asks Razorpay to create an order,
the request is shown to AgentShield first and it returns exactly one of three
answers — **ALLOW**, **STEP-UP**, or **BLOCK**. Only ALLOW proceeds to the
payments API and the rails. Because the gate runs before money moves, a wrong
"no" costs a re-confirmation, not a reversal — which is what lets the whole
system fail closed without fear.

The full specification lives in [`design_docs/`](design_docs/). This README is
the map of the code.

## Two planes that meet at exactly two points

- **Synchronous plane — on the clock.** One stateless Go service in front of
  three keyed stores. It resolves a token, runs six integer checks, reads a row
  of precomputed figures, scores and decides — and replies *before* it tells the
  bus anything. p99 ≤ 50 ms, ~21 ms typical.
- **Asynchronous plane — off the clock.** A durable bus and six workers (Go for
  stream-joins, Python for the models) that recompute baselines, models,
  embeddings and reputation, and leave each result behind as one calibrated
  figure with a `computed_at` stamp.

They never call each other. They meet only at the bus (events go down) and the
online feature store (one figure comes up).

### Two invariants that never bend

1. **Only the six predicates (P1–P6) can BLOCK.** The ML engines may only raise
   risk or ask for a STEP-UP — never refuse on their own.
2. **When anything is missing, stale or slow, the system fails closed to a
   STEP-UP.** It never guesses in order to stay fast, and a blank is never
   filled with an optimistic zero.

## Repository layout (monorepo)

```
proto/agentshield/v1/     the Evaluate gRPC + Protobuf contract (the only wire method)
gen/go/                   generated Go bindings (regenerate with `make proto`)
internal/domain/          the six core schemas — token, policy overlay, intent
                          envelope, feature row, provenance record, event
cmd/decision/             the decision service entrypoint            (synchronous plane)
internal/store/           keyed store interfaces + implementations
internal/predicate/       the deterministic spine, P1–P6
internal/features/        the keyed feature reader + staleness handling
internal/score/           the ensemble aggregator + expected-loss decide()
internal/decision/        orchestration of the seven stages
services/                 Python ML workers                          (asynchronous plane, later)
design_docs/              the System Design document and figures
```

The synchronous plane is being built first, one component per commit, in
dependency order. The asynchronous plane (bus, workers, ML engines) is added on
top of a boundary that already holds.

## Build

```bash
make proto   # regenerate gRPC bindings from the .proto (only when it changes)
make build   # compile everything
make test    # run the suite
```

Requires Go 1.25+, `protoc` with `protoc-gen-go` and `protoc-gen-go-grpc`.

---
Prepared by Sagar Khandagre — Agentic security.
