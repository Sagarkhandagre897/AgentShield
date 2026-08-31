# AgentShield — how a request is decided on the clock

This is the plain-words version of what happens when an AI agent tries to move
money. When the agent asks Razorpay to create an order, the request is shown to
AgentShield **first**, and everything below runs on the clock — a budget of
**p99 ≤ 50 ms** (a live run measures ~3 ms typical). By the end, the caller always
gets exactly one of three answers — **ALLOW**, **STEP-UP**, or **BLOCK** — and
never a transport error standing in for a decision.

One rule sits underneath all seven stages: **when in doubt, fail closed.** If
AgentShield cannot read something it needs, or a figure is missing or stale, it
does not guess an optimistic "yes" — it asks for a step-up.

```
ingress → resolve token → predicates (P1–P6) → read features → score → decide → respond
```

## The seven stages, in order

**1. Ingress — "who is calling, and what are they asking?"**
The service authenticates the caller over mTLS and takes in the order (token id,
customer, agent, merchant, amount, nonce, the sealed-instruction digest, …). The
caller's identity is pinned from the TLS certificate — **not** from anything in
the request body — so a request cannot simply *claim* to be someone it isn't. The
clock starts here.

**2. Resolve token — "look up the permission slip."**
Three quick keyed reads: the **token** (the standing permission), its
**block-state** (how much has been spent, which nonces were already seen), and the
**policy overlay** (the caps and allow-lists for this token). A "not found" is a
normal case — the predicates handle it. But if a store genuinely errors so we
cannot know the limits, we stop right here and fail closed to a STEP-UP. We never
proceed on a blank.

**3. Run the predicates (P1–P6) — "the six hard checks."**
This is the deterministic spine, and the **only** place a BLOCK can ever come
from. They run cheapest-and-most-decisive first, and the first one to fire ends
the request (stages 4–6 are skipped):

| Order | Predicate | The question, in plain words | If it fires |
|---|---|---|---|
| 1 | **P1 Replay** | have I already seen this exact request? | **BLOCK** — duplicate |
| 2 | **P5 Identity** | can I prove who this is, and that the slip is theirs? | **BLOCK** — identity |
| 3 | **P4 Authority** | has the permission expired, been cancelled, or run out? | **BLOCK** — authority |
| 4 | **P6 Binding** | does the amount asked match the amount bound to this order? | **BLOCK** — binding |
| 5 | **P2 Scope** | is it bigger than allowed, or to the wrong kind of place? | **STEP-UP** — scope |
| 6 | **P3 Unbound** | is this debit actually backed by the instruction we were given? | **STEP-UP** — unbound |

Identity (P5) deliberately runs before the checks that assume a token exists, so
P4/P6/P2 can trust the token is really there. If none of the six fires, the request
is clean so far and moves on.

**4. Read features — "fetch the precomputed figures."**
One keyed multi-get pulls the row of figures the off-clock plane left behind —
behaviour deviation, network risk, intent divergence, agent reputation — each
stamped with *when* it was computed. If any figure we need is missing or older
than the staleness budget, the view is marked **degraded**. The on-clock plane
computes nothing itself; it only reads what is already sitting there.

**5. Score — "fold the figures into one number."**
The scorer combines the figures (plus the model-free consumption fraction, and
reputation, which *lowers* risk) into a single calibrated probability **p** — the
chance this debit is misuse. If the view came back degraded in stage 4, there is
nothing honest to score, so we skip straight to a fail-closed STEP-UP.

**6. Decide — "is it worth interrupting the customer?"**
The question is not "is p high?" but **expected loss vs. the cost of interrupting.**
Multiply p by the rupees at risk; if that expected loss is larger than the fixed
interruption cost, ask for a **STEP-UP** — otherwise **ALLOW**. This step has no
BLOCK branch at all: the ML side can only ever raise risk to a step-up, never
refuse. One extra floor — if the tool involved is rated **critical** and we were
about to ALLOW, we bump it to a STEP-UP anyway (still never a block).

**7. Respond — "answer first, tell the story later."**
Build the lean verdict — just the decision, the reason code, and whether it is
retryable — and return it to the caller. On purpose it carries **no** score, band,
or threshold, so nobody can reverse-engineer the scoring by poking at it. Only
*after* the reply, off the caller's clock, does the service announce the full
record on the bus (a **fingerprint** of the request, never the raw request), and
the off-clock stream-processor writes it to the durable, hash-linked ledger. This
service never touches the ledger itself.

## The three ways a request can end

| Answer | When it happens | Comes from | Retryable? |
|---|---|---|---|
| **ALLOW** | passed all six predicates and the expected loss was under the interruption cost | stage 6 | — proceeds to the payments API |
| **STEP-UP** | a soft predicate (P2/P3), the score, or any fail-closed condition | stages 3, 5, 6 | **yes** — re-confirm with a human |
| **BLOCK** | a hard predicate (P1/P4/P5/P6) fired | stage 3 only | **no** — terminal |

## Two things you will see happen a lot

- **A blocking predicate short-circuits everything.** When P1/P4/P5/P6 fires at
  stage 3, stages 4–6 never run — which is why a replay or a revoked token is
  answered in well under a millisecond.
- **Fail-closed is the default.** Any time a read fails, or a needed figure is
  missing or stale, the answer is a STEP-UP with the reason `STEPUP_FAILCLOSED`.
  AgentShield would rather ask twice than guess once.

## Why answer *before* the money moves?

Because the gate sits in front of the payments API, a wrong "no" costs a
re-confirmation, not a reversal. That is exactly what lets the whole system fail
closed without fear: the worst case of caution is a small interruption — never
lost money, and never a chargeback to claw back afterwards.

---
Prepared by Sagar Khandagre.
