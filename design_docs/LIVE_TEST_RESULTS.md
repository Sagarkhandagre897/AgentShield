# AgentShield — live end-to-end test results

This document records a full live run of the split-process stack: a generated
world of tokens, envelopes and payment debits driven across the wire through the
on-clock gRPC decision service and the off-clock worker plane, then scored
against the generator's ground truth and read back from the durable stores.

It is produced by [`demo/live_test.py`](../demo/live_test.py) — see the
**"Run it live"** section of the [top-level README](../README.md) for the three
commands that bring the stack up and drive it. This file captures the output of
one such run so the accuracy is documented, not just demonstrated.

## How it was produced

Against an already-running stack (docker infra + `cmd/worker` + `cmd/decision`),
from a **clean slate** (`docker compose … down -v` then a fresh `up`), one command:

```bash
/home/sagar/.venvs/agentshield/bin/python demo/live_test.py --seed 7
```

It seeds 50 tokens + 2 policy overlays, seals 50 intent envelopes into the VAULT,
pre-warms the feature store, replays 78 payment debits through the live
`Evaluate` gRPC, settles the off-clock confirmations/disputes/cancellations, and
collects the training labels the labeler emits.

| | |
|---|---|
| Scenario seed | `7` (deterministic) |
| Go toolchain | `go1.25.0` |
| Python | `3.12.3` (venv, pure stdlib — no install) |
| Docker / Compose | `29.5.3` / `2.40.3` |
| Repo commit | `bd92879` |
| Decision config | `INTERRUPTION_COST_PAISE=100000`, `STALENESS_BUDGET_SECONDS=0`, dev mode (no mTLS) |

## Headline result

| Metric | Value |
|---|---|
| Debits evaluated | **78 / 78** (100% coverage) |
| Decision accuracy | **1.0** |
| Code accuracy | **1.0** |
| Families exercised | **9 / 9** at 1.0 decision + 1.0 code |
| Training labels settled | **84** |
| CHAIN hash-link integrity | **0 broken links** across 78 rows |

## On-clock verdict spread

The 78 debits split across all three answers as the generator intended:

```
timeline: 78 debits -> 31 ALLOW / 40 STEP_UP / 7 BLOCK
```

## Per-family accuracy

Every one of the nine attack/behaviour families scored a perfect decision **and**
code match against ground truth:

| Family | n | decision | code |
|---|---:|---:|---:|
| legit | 29 | 1.0 | 1.0 |
| intent_drift | 4 | 1.0 | 1.0 |
| mule_fan_in | 6 | 1.0 | 1.0 |
| replay | 3 | 1.0 | 1.0 |
| scope_overrun | 4 | 1.0 | 1.0 |
| shared_device_ring | 10 | 1.0 | 1.0 |
| stale_revoked_token | 4 | 1.0 | 1.0 |
| synchronised_fleet | 6 | 1.0 | 1.0 |
| velocity_bustout | 12 | 1.0 | 1.0 |
| **total** | **78** | **1.0** | **1.0** |

## Marquee — representative verdicts, read back from the durable CHAIN

One debit per verdict class, with the *live* decision read back out of the
Postgres `provenance` chain (not the in-memory reply) — so a P1 replay BLOCK and
a graph-ring STEP-UP are legible on their own, not just as an aggregate:

| what | eval | expected | live (CHAIN) | ok |
|---|---|---|---|:--:|
| legit ALLOW | `eval_00001` | ALLOW / OK_ALLOW | ALLOW / OK_ALLOW | ✓ |
| intent-drift STEP_UP | `eval_00027` | STEP_UP / STEPUP_RISK | STEP_UP / STEPUP_RISK | ✓ |
| scope-overrun STEP_UP | `eval_00031` | STEP_UP / STEPUP_SCOPE | STEP_UP / STEPUP_SCOPE | ✓ |
| graph-ring STEP_UP | `eval_00057` | STEP_UP / STEPUP_RISK | STEP_UP / STEPUP_RISK | ✓ |
| replay BLOCK (P1) | `eval_00048` | BLOCK / BLOCKED_DUPLICATE | BLOCK / BLOCKED_DUPLICATE | ✓ |
| revoked-token BLOCK | `eval_00053` | BLOCK / BLOCKED_AUTHORITY | BLOCK / BLOCKED_AUTHORITY | ✓ |

## Durable evidence, read straight from Postgres

Independent of the harness's own scoring, querying the durable stores after the
run corroborates every number above. The CHAIN (`provenance`) holds one
append-only, hash-linked row per evaluation:

```sql
SELECT decision, count(*) FROM provenance GROUP BY decision;   -- ALLOW 31 · STEP_UP 40 · BLOCK 7
SELECT code, count(*)     FROM provenance GROUP BY code;        -- see below
SELECT count(*)           FROM provenance;                      -- 78
```

| decision | rows | | code | rows |
|---|---:|---|---|---:|
| ALLOW | 31 | | OK_ALLOW | 31 |
| STEP_UP | 40 | | STEPUP_RISK | 30 |
| BLOCK | 7 | | STEPUP_SCOPE | 10 |
| **total** | **78** | | BLOCKED_AUTHORITY | 4 |
| | | | BLOCKED_DUPLICATE | 3 |

**Hash-chain integrity** — every row's `prev_hash` matches the previous row's
`hash`, so the ledger is unbroken and tamper-evident:

```sql
SELECT count(*) FROM provenance a
 WHERE seq > 1 AND prev_hash IS DISTINCT FROM
       (SELECT hash FROM provenance b WHERE b.seq = a.seq - 1);   -- 0
```

**VAULT** — the off-clock stream-processor sealed the session PII (raw
instruction text + contact) as AES-256-GCM ciphertext, session-id as AAD:

```sql
SELECT count(*) AS rows, count(DISTINCT session_id) AS sessions FROM vault;   -- 100 rows · 50 sessions
```

(Two fields per session × 50 sessions = 100 sealed rows; the plane reads none of
this on the clock — only the envelope digest travels on a request.)

## Settled training labels

The off-clock labeler drained **84** settled outcomes — the learning loop's
ground truth for the next model refit:

```
labels: 84 observed {'confirmed_step_up': 40, 'dispute': 42, 'cancellation': 2}
```

## What this proves

- The full **on-clock → off-clock** loop runs across real transport: gRPC to the
  decision service, Redpanda for the bus, Redis for the hot stores, Postgres for
  the durable CHAIN + VAULT.
- The **six predicates are the sole blockers**: every BLOCK on the chain carries a
  `BLOCKED_*` code (P1 replay `BLOCKED_DUPLICATE`, P4 authority `BLOCKED_AUTHORITY`);
  the ML engines only ever raised risk to a `STEPUP_*`, never refused.
- **Fail-closed** holds: the 40 STEP-UPs are the guarded middle, not silent allows.
- The decision the caller saw on the clock is exactly the decision that landed on
  the durable, hash-linked chain — the reply and the ledger agree, row for row.

## Reproduce it

Full instructions are in the [README "Run it live" section](../README.md#run-it-live).
In short: `docker compose -f deploy/docker-compose.yml up -d --wait` (+ `topic-init`),
start `cmd/worker` and `cmd/decision` in split-process mode, then run
`demo/live_test.py --seed 7`. The seed is deterministic, so the numbers above
reproduce exactly on a clean slate.

---
Prepared by Sagar Khandagre — Agentic security.
