# AgentShield — what problem does it solve?

This document states the problem AgentShield exists to solve: the class of
**agentic security attacks** and **agentic payment frauds** that appear the
moment an AI agent — not a human — is the one asking a payments API to move
money. The attack catalogue is drawn from the product spec
([`PRODUCT.md`](PRODUCT.md)) and every threat is corroborated against public,
independently-published industry taxonomies. Links are inline and collected under
[Sources](#sources). Web landscape captured as of 2026-08-31.

## The problem in one line

Traditional fraud asks *"is this transaction fraudulent?"*. Agentic payments
force a strictly larger question:

> **Can we trust the entire chain of decisions that produced this transaction?**
> — PRODUCT.md §2

The canonical failure the product is built around: a user says *"buy groceries
under ₹2,000,"* the agent reads some untrusted tool output, and buys a ₹1,800
subscription instead. Every classic signal is clean — known merchant, known
customer, known device, valid method — so a transaction-level fraud model waves
it through. The defect is not in the transaction; it is that **Intent ≠
Transaction**. Closing that gap, *before money moves*, is the whole product.

## Why now — agentic payments are already shipping

This is not a hypothetical threat surface. In 2025–2026 the payment networks
shipped the rails that make agent-initiated debits real, and the volume followed:

- Visa, Mastercard, Stripe and PayPal have all shipped **agentic commerce
  protocols**; Cloudflare's **Agent Pay** and the **Agent Payments Protocol
  (AP2)** define how an agent acquires a spending mandate and checks out on a
  human's behalf. ([RisingWave][rw])
- Scale is arriving with them: ~**$262B** in AI-agent holiday orders (Salesforce),
  AI-driven US retail visits up **393% YoY** in early 2026 (Adobe), and nearly
  **8 billion** agent requests logged in the first two months of 2026
  (DataDome). ([Corgilabs][corgi])
- The economics favour the attacker: BCG projects agentic AI could cut the cost
  to run a scam by **~90%** within two years, driving a **2×-or-more** surge in
  fraud volume. ([BCG][bcg])
- The field now has a canonical taxonomy: the **OWASP Top 10 for Agentic
  Applications** (ASI01–ASI10), published by the OWASP GenAI Security Project in
  December 2025. ([OWASP breakdown][owasp] · [promptfoo][promptfoo])

## The external threat catalogues (web)

Three independently-published lists frame the same problem space. AgentShield's
own attacks (next section) map onto them, which is the point: these are the
recognised threats, not invented ones.

**OWASP Top 10 for Agentic Applications 2026 — ASI01–ASI10** ([breakdown][owasp]):
ASI01 Agent Goal Hijack · ASI02 Tool Misuse & Exploitation · ASI03 Identity &
Privilege Abuse · ASI04 Agentic Supply-Chain Vulnerabilities · ASI05 Unexpected
Code Execution · ASI06 Memory & Context Poisoning · ASI07 Insecure Inter-Agent
Communication · ASI08 Cascading Failures · ASI09 Human-Agent Trust Exploitation ·
ASI10 Rogue Agents. (Built from real incidents — EchoLeak, the Amazon Q case, the
Replit production-database meltdown.)

**RisingWave — "7 agentic-payment fraud patterns human rules miss"** ([link][rw]):
Hijacked Agent Burst · Mandate Replay · Scope Escalation · Multi-Agent Collusion ·
Cross-Merchant Correlation · Cadence Anomaly · Geographic Impossibility. This list
is striking because it lands, independently, on nearly the same set AgentShield is
tested against (see [nine families](#what-the-system-is-actually-tested-against--nine-families)).

**Sardine — "7 agentic attacks now live in 2026"** ([link][sardine]): polymorphic
phishing agents · invoice-timed / predictive payment fraud · deepfake-as-a-service
KYC bypass · synthetic-identity farms · Ghost-Touch / biometric session hijacking ·
CVE-weaponisation agents · automated chain-hopping money laundering. Its thesis —
fraud has shifted from *"fraud-at-scale"* to *"fraud-with-agency"* — is the same
one PRODUCT.md opens with.

## Why a classic fraud model does not answer this

AgentShield is deliberately **not** "another fraud model," and the reasoning is
load-bearing:

- **Fraud models learn "normal" from humans.** They key on session rhythm, time
  on page, typing cadence, the 1 a.m. impulse buy. An agent breaks every one of
  those signals at once — it *"moves too fast, too cleanly, and too
  consistently"* — which is exactly what those models were built to flag, so
  they make confident mistakes in both directions. ([Corgilabs][corgi])
- **The question itself is different.** Razorpay already runs mature
  transaction-risk systems (Shield, ACS, Vulcan, COD Intelligence, Bumblebee,
  Chargeback Shield). Those answer *"is this payment bad?"* — probabilistic,
  learned. AgentShield answers *"was this payment asked for?"* — a
  **deterministic** comparison against a registered consent artefact
  (PRODUCT.md §19, §63).
- **It needs no training labels, so it can exist before the fraud does.** The
  core check compares the debit to a stored mandate + intent envelope, not to a
  learned model of past fraud — which is why it can ship on day one, when *"every
  agent in the world is 0 days old"* (PRODUCT.md §8, §53).

## The boundary that does the work — two invariants

1. **Only the six deterministic predicates (P1–P6) can BLOCK.** The ML engines
   (behavioural, graph, reputation, semantic-intent) may only raise risk to a
   **STEP-UP** — they never refuse on their own. *"A probabilistic signal alone
   never blocks — not once, anywhere in this product."* (PRODUCT.md §22, §44)
2. **When anything is missing, stale or slow, the system fails closed to a
   STEP-UP** — never an optimistic ALLOW.

The one identity it enforces end-to-end:
`AUTHORIZED ACTION = USER-INTENDED ACTION = EXECUTED TRANSACTION` (PRODUCT.md §43).

### The six predicates

| Predicate | Fires when | Verdict (implemented code) |
|---|---|---|
| **P1 replay** | idempotency key / presentment id already seen for this token | **BLOCK** (`BLOCKED_DUPLICATE`) |
| **P2 scope overrun** | amount > per-debit cap, or category/merchant outside the overlay | **RE-CONFIRM → STEP-UP** (`STEPUP_SCOPE`) |
| **P3 unbound presentation** | no intent envelope, or its digest ≠ the one held | **STEP-UP** (`UNATTESTED`) |
| **P4 stale / revoked** | token expired, cancellation seen, or block already exhausted | **BLOCK** (`BLOCKED_AUTHORITY`) |
| **P5 unverifiable identity** | caller credential invalid, or token ≠ this customer's | **BLOCK** (`BLOCKED_IDENTITY`) |
| **P6 binding mismatch** | presented amount ≠ the amount bound to this order at eval time | **BLOCK** (`BLOCKED_BINDING`) |

(The `BLOCKED_*` / `STEPUP_*` strings are the reason codes recorded on the durable
CHAIN — see [`LIVE_TEST_RESULTS.md`](LIVE_TEST_RESULTS.md). PRODUCT.md itself labels
these P1–P6 with the caller-facing codes `AUTHORIZATION_VIOLATION`,
`INTENT_MISMATCH`, `UNATTESTED`.)

## The attacks AgentShield is built to catch

Each row is an attack the spec calls out, how it moves money, the AgentShield
defence, and the public taxonomy it corresponds to.

| # | Attack (PRODUCT.md) | How it moves money | AgentShield defence | Public taxonomy |
|---|---|---|---|---|
| 1 | **Prompt injection** (§36) | untrusted content changes the agent's chosen action | not stoppable at the pay layer; detected as **intent mismatch** → STEP-UP/BLOCK; an injection **cannot widen** server-side authority | OWASP **ASI01** Goal Hijack ([owasp]) |
| 2 | **MCP tool poisoning** (§37) | poisoned tool metadata/response reshapes the plan | tool output is a **feature, never policy** → STEP-UP; verified capability tiers on first-party tools *can* BLOCK | OWASP **ASI02** Tool Misuse, **ASI04** Supply-Chain ([owasp]) |
| 3 | **Delegation abuse / slow drain** (§38) | many small in-policy debits drain a standing mandate | block-consumption features (`fraction_of_block_consumed`, `projected_exhaustion_seconds`) → STEP-UP; P2 caps → RE-CONFIRM | RisingWave **Hijacked Agent Burst** ([rw]); Sardine *fraud-with-agency* ([sardine]) |
| 4 | **Agent impersonation** (§39) | a fake agent presents a request | **P5** identity BLOCK; real defence is **containment** — a fake agent can't widen the mandate or mint a token | OWASP **ASI03** Identity & Privilege Abuse ([owasp]); Corgilabs agent spoofing ([corgi]) |
| 5 | **Session hijacking** (§40) | a stolen live session sends valid-looking debits | timing/cadence discontinuity on an unchanged policy → **STEP-UP** (never BLOCK — a busy 2 a.m. agent looks identical) | Sardine **Ghost-Touch / session hijack** ([sardine]) |
| 6 | **Parameter manipulation** (§41) | agent thinks ₹1,500, the request says ₹15,000 | four independent ceilings, **none from the request** — **P3/P2/P4/P6** → BLOCK | OWASP **ASI02** Tool Misuse ([owasp]); RisingWave **Scope Escalation** ([rw]) |
| 7 | **Multi-agent manipulation** (§42) | an upstream agent's number is trusted blindly (₹1,999→₹19,999) | *trust does not compose*; authority comes from token+policy, not the caller; the request body's own "limit" field is **deleted** | OWASP **ASI07** Insecure Inter-Agent Comms, **ASI08** Cascading Failures ([owasp]); RisingWave **Multi-Agent Collusion** ([rw]) |
| 8 | **Intent drift** (§12/§54) | committed "groceries ₹2,000" → "subscription ₹1,899, unknown merchant" | committed **intent envelope** at step 2; structural mismatch → **P6** BLOCK; semantic drift → STEP-UP | OWASP **ASI01** Goal Hijack, **ASI06** Memory/Context Poisoning ([owasp]); RisingWave **Cross-Merchant / Cadence** ([rw]) |
| 9 | **Repudiation** — *"I never asked my agent to buy that"* (§27) | genuine and false repudiation look identical at debit time | not a detector — an append-only **hash-chained provenance** record, mirrored into the order, able to work *against* the operator | OWASP **ASI09** Human-Agent Trust Exploitation ([owasp]); Corgilabs *"the AI bought it"* disputes ([corgi]) |
| 10 | **Settlement / contact redirect** (§14) | `modify_bank_account` etc. redirect *every future* payment | rated **CRITICAL**; cheapest defence in the product is a **digest comparison** on those fields | OWASP **ASI02** Tool Misuse ([owasp]); Sardine **invoice / bank-detail swap** ([sardine]) |

The load-bearing one is **#3, the slow drain**: every debit is under the per-txn
cap, right category, known merchant — individually unremarkable. Rate-based fraud
features miss it because nothing is loud; the mandate is simply being drained
faster than the customer would. The rail-unique signal that catches it —
`fraction_of_block_consumed` / `projected_exhaustion_seconds` — is the one
PRODUCT.md treats as most novel (§18, §38, §52).

## What the system is actually tested against — nine families

The live harness exercises **nine attack/behaviour families**, all scored 1.0 on
decision and code against ground truth (see
[`LIVE_TEST_RESULTS.md`](LIVE_TEST_RESULTS.md)). The right-hand column is the
independent RisingWave list — the near-1:1 overlap is the strongest external
signal that these are the *real* problems.

| AgentShield family | What it is | Primary defence | Independent industry match |
|---|---|---|---|
| `legit` | in-policy baseline chain | predicates pass → low risk → **ALLOW** | — |
| `velocity_bustout` | many small in-policy debits drain the block | block-consumption features → **STEP-UP** | RisingWave #1 Hijacked Agent Burst ([rw]) |
| `replay` | a captured mandate/debit replayed | **P1** → **BLOCK** | RisingWave #2 Mandate Replay ([rw]) |
| `scope_overrun` | debit outside the authorised cap/category/merchant | **P2** → RE-CONFIRM/STEP-UP | RisingWave #3 Scope Escalation ([rw]) |
| `synchronised_fleet` | one owner splits spend across many agents | graph/network risk → **STEP-UP** | RisingWave #4 Multi-Agent Collusion ([rw]) |
| `mule_fan_in` | many agents/debits converge on one destination | graph risk + settlement-redirect check | RisingWave #5 Cross-Merchant Correlation ([rw]) |
| `intent_drift` | robotic cadence / merchant-category divergence | semantic + cadence features → **STEP-UP** | RisingWave #5/#6 Cross-Merchant + Cadence ([rw]) |
| `shared_device_ring` | many agents share a device/IP fingerprint | behavioural + graph (weak on *declared* telemetry) | RisingWave #7 Geographic Impossibility ([rw]) |
| `stale_revoked_token` | debit on an expired/cancelled/exhausted mandate | **P4** → **BLOCK** | mandate-lifecycle check |

## Honest coverage (PRODUCT.md §64)

The spec grades its own defence rather than claiming uniform protection:

- **Strong (deterministic):** delegation abuse (P2), parameter manipulation (P6),
  replay (P1), stale/revoked token (P4). These are comparisons against stored
  artefacts, not predictions — they do not degrade with novelty.
- **Moderate:** agent compromise / intent divergence — solid *when a committed
  intent envelope exists*; on an unattested request the answer is honestly
  `UNATTESTED`, not an invented low score.
- **Weak in the MVP:** tool poisoning (rides on declared, untrusted data) and
  session hijacking (no behavioural baseline on day one, when every agent is
  0 days old).

Naming the weak rows is deliberate: a boundary that claims uniform defence is
less credible than one that says exactly where it is thin.

## Sources

- **OWASP Top 10 for Agentic Applications 2026 (ASI01–ASI10)** — OWASP GenAI
  Security Project (Dec 2025). Readable breakdown:
  [neuraltrust.ai][owasp]; framework overview: [promptfoo.dev][promptfoo].
- **RisingWave — Detecting Fraud in Agentic Payments: 7 Patterns Human Rules
  Miss** — [risingwave.com][rw].
- **Sardine — 7 Agentic Attacks Now Live in 2026** — [sardine.ai][sardine].
- **Corgilabs — Agentic Commerce Fraud: When AI Agents Start Buying** —
  [corgilabs.ai][corgi].
- **BCG — How Agentic AI Will Industrialize Financial Scams** — [bcg.com][bcg].
- Internal: [`PRODUCT.md`](PRODUCT.md) (the product spec) and
  [`LIVE_TEST_RESULTS.md`](LIVE_TEST_RESULTS.md) (the scored live run).

[owasp]: https://neuraltrust.ai/blog/owasp-agentic-ai-top-10
[promptfoo]: https://www.promptfoo.dev/docs/red-team/owasp-agentic-ai/
[rw]: https://risingwave.com/blog/detecting-fraud-agentic-payments-7-patterns/
[sardine]: https://www.sardine.ai/blog/agentic-attacks
[corgi]: https://www.corgilabs.ai/insights/agentic-commerce-fraud
[bcg]: https://www.bcg.com/publications/2026/how-agentic-ai-will-industrialize-financial-scams

---
Prepared by Sagar Khandagre.



