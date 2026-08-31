# AgentShield

A real-time **Agent Trust & Risk layer** that answers at `POST /v1/orders`,
before any rupee moves. When an AI agent asks Razorpay to create an order,
the request is shown to AgentShield first and it returns exactly one of three
answers — **ALLOW**, **STEP-UP**, or **BLOCK**. Only ALLOW proceeds to the
payments API and the rails. Because the gate runs before money moves, a wrong
"no" costs a re-confirmation, not a reversal — which is what lets the whole
system fail closed without fear.

The full specification lives in [`design_docs/`](design_docs/). This README is
the map of the code. For *why the product exists* — the agentic security attacks
and payment frauds it is built to stop, corroborated against the OWASP Agentic
Top 10 and the current payment-fraud literature — see
[`design_docs/PROBLEMS_SOLVED.md`](design_docs/PROBLEMS_SOLVED.md).

## Two planes that meet at exactly two points

- **Synchronous plane — on the clock.** One stateless Go service in front of
  three keyed stores. It resolves a token, runs six integer checks, reads a row
  of precomputed figures, scores and decides — and replies *before* it tells the
  bus anything. Budget p99 ≤ 50 ms; a live loopback run measures ~3 ms typical
  (p50) and p99 ≈ 6 ms — see
  [`design_docs/LIVE_TEST_RESULTS.md`](design_docs/LIVE_TEST_RESULTS.md#on-clock-latency).
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

## Run it live

`make test` proves the two planes in-process. To watch real traffic cross the
wire — a decision service answering over gRPC, a worker folding events off a
real bus, verdicts landing in a durable Postgres CHAIN — bring the split-process
stack up and drive a generated world through it. Three steps.

**1. Bring up the infrastructure** (Redpanda + Redis + Postgres, and the one-shot
that creates the six topics). These three are the *only* containers — the two app
hosts run on the host, so Docker Desktop shows exactly three:

```bash
docker compose -f deploy/docker-compose.yml up -d --wait redpanda redis postgres
docker compose -f deploy/docker-compose.yml up -d topic-init
docker compose -f deploy/docker-compose.yml wait topic-init   # exits 0 once topics exist
```

**2. Start the two planes against it.** Setting `REDIS_ADDR` + `KAFKA_SEEDS`
selects split-process mode; in split mode the worker owns the durable CHAIN +
VAULT (`POSTGRES_DSN`), and the decision service runs dev-mode (no mTLS, caller
identity `dev-caller`). Leave both running in their own shells:

```bash
# off-clock plane — stream-processor, materialiser, reputation-builder, labeler
REDIS_ADDR=localhost:6379 KAFKA_SEEDS=localhost:19092 \
POSTGRES_DSN='postgres://agentshield:agentshield@localhost:5432/agentshield?sslmode=disable' \
go run ./cmd/worker
```

```bash
# on-clock plane — the Evaluate gRPC service, listening on :8443
REDIS_ADDR=localhost:6379 KAFKA_SEEDS=localhost:19092 \
AGENTSHIELD_INTERRUPTION_COST_PAISE=100000 \
AGENTSHIELD_STALENESS_BUDGET_SECONDS=0 \
AGENTSHIELD_ADDR=:8443 \
go run ./cmd/decision
```

**3. Drive traffic through the live stack.** `demo/live_test.py` assumes the
stack and both hosts are already up and leaves everything running. It spawns only
`cmd/driverkit` (the same NDJSON arm the eval harness uses), replays a generated
scenario through the tested orchestrator, and prints the on-clock verdict spread,
a marquee of representative verdicts read back from the durable CHAIN (one per
class, including a P1 replay BLOCK), and the settled training labels:

```bash
/home/sagar/.venvs/agentshield/bin/python demo/live_test.py --seed 7
```

It needs no install (pure stdlib) and sets its own import path. A green run reads:

```
timeline: 78 debits -> 31 ALLOW / 40 STEP_UP / 7 BLOCK
debits=78 evaluated=78 decision_acc=1.0 code_acc=1.0
MARQUEE — representative verdicts, read back from the durable CHAIN
  legit ALLOW           eval_00001  ALLOW/OK_ALLOW            ALLOW/OK_ALLOW            ✓
  replay BLOCK (P1)     eval_00048  BLOCK/BLOCKED_DUPLICATE   BLOCK/BLOCKED_DUPLICATE   ✓
  revoked-token BLOCK   eval_00053  BLOCK/BLOCKED_AUTHORITY   BLOCK/BLOCKED_AUTHORITY   ✓
```

The full scored output of one such run — per-family accuracy, the marquee, and the
durable CHAIN/VAULT state read back from Postgres — is recorded in
[`design_docs/LIVE_TEST_RESULTS.md`](design_docs/LIVE_TEST_RESULTS.md).

**Measure the on-clock latency** against the same already-running stack.
`demo/latency_test.py` primes one legit request onto the full ALLOW path (reusing
live_test's seed/seal/pre-warm phases) and hands the timing loop to the
`cmd/latencyprobe` Go binary — 2000 sequential `Evaluate` calls, a fresh
`evaluation_id` + `nonce` on each so none trips P1 replay and every one stays on
the full seven-stage ALLOW path:

```bash
/home/sagar/.venvs/agentshield/bin/python demo/latency_test.py --seed 7
```

A green run reports **~3 ms typical** (p50) with p99 ≈ 6 ms, comfortably inside the
50 ms budget; the full distribution and the "is there an off-the-shelf tool" answer
(ghz / grpcurl) are in
[`design_docs/LIVE_TEST_RESULTS.md`](design_docs/LIVE_TEST_RESULTS.md#on-clock-latency).

Nothing is torn down — teardown stays your call:
`docker compose -f deploy/docker-compose.yml down -v` (the `-v` also drops the
CHAIN/VAULT/bus volumes for a clean slate).

> `python -m agentshield_driver --seed 7` (from `services/driver/`) is the other
> way to see the same loop: it owns the *whole* lifecycle — brings the stack and
> both hosts up, scores the run, and tears it all back down. `demo/live_test.py`
> is the "leave it up and poke at it" counterpart.

---
Prepared by Sagar Khandagre — Agentic security.
