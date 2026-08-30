"""orchestrator — the choreography half of the live driver.

Given a live :class:`~agentshield_driver.kit.BaseKit` and a generated scenario dict,
:func:`run_scenario` replays the world against the real system in the order the two
planes demand, then scores it. It owns everything the contract-bound Go driverkit
does not: the timeline order, the barrier logic, the settlement phasing, and the
oracle join.

The phases, in order:

  1. seed      — write the mandates and the customers' tightening overlays.
  2. seal      — seal each session's intent envelope into the VAULT (the only PII).
  3. pre-warm  — deposit an engine-stand-in figure on every entity key a debit will
                 read (a blank row is treated as missing and fails the read closed),
                 then poll until the off-clock materialiser has folded them all.
  4. timeline  — evaluate each debit in order; a debit that depends on a prior fold
                 carries a barrier the driver waits out first. An ALLOWed debit whose
                 recipe captures is captured immediately (a bare capture teaches the
                 labeler nothing but drives the consumption/nonce barriers).
  5. settle    — after every evaluate (so each decision.made has armed the labeler),
                 emit the deferred step-up confirmations, then the disputes, then the
                 cancellations — the settled outcomes the labeler distils into labels.
  6. collect   — drain the labels the real labeler produced, and score verdicts +
                 labels against the generator's ground truth.

Why the phasing: a step-up confirming capture races the decision.made that arms the
labeler (different topics, different partitions). Deferring every confirmation to a
settlement phase run after all evaluates means the arming fold has landed first, so
the capture is read as a confirmed step-up rather than an unarmed no-op.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Set

from .kit import BaseKit
from .oracle import normalise_decision, score_run

# Families whose misuse is only visible in the graph: the network-risk figure on the
# payers/members is what must earn the step-up, so the driver pre-warms it high.
GRAPH_FAMILIES = {"mule_fan_in", "shared_device_ring", "synchronised_fleet"}
INTENT_DRIFT_FAMILY = "intent_drift"

# The 12 proto OrderContext fields, in the order the generator's order_context()
# emits them. A debit dict carries these at top level alongside its metadata.
_ORDER_KEYS = (
    "evaluation_id", "token_id", "customer_id", "agent_id", "merchant_id", "session_id",
    "amount_paise", "cart_hash", "envelope_digest", "tool_risk", "nonce", "ts",
)

# The elevated figure the pre-warm deposits so the soft/graph families clear the
# interruption cost; the baseline every other key gets so no read is degraded.
_RISK_HIGH = 0.9
_BASELINE = 0.0


@dataclass
class Timings:
    """The waits the choreography needs, all in seconds bar the label drain.

    The barriers (feature pre-warm, per-token fold) poll actively and return the
    instant their condition holds, bounded by their timeout; the settle sleeps are
    unconditional pauses that let a just-emitted event fold before the next phase
    reads its effect. Defaults are generous for a laptop docker stack; a faster or
    slower box can dial them without touching the phase machine."""

    feature_poll_timeout: float = 20.0
    barrier_poll_timeout: float = 20.0
    poll_interval: float = 0.1
    settle_after_evaluate: float = 3.0
    settle_after_capture: float = 2.0
    settle_after_settlement: float = 2.0
    labels_timeout_ms: int = 15000


@dataclass
class RunLog:
    """What a run accumulates for the oracle and for debugging.

    ``verdicts_by_eval`` is the join key the oracle needs (eval_id -> evaluate
    response). ``captured_evals`` is the set of debits that actually settled a
    capture (immediate or deferred), so the dispute phase only chargebacks money
    that moved. ``warnings`` collects every barrier that timed out — a non-fatal
    signal that a fold never landed, which the results surface rather than hide."""

    verdicts_by_eval: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    captured_evals: Set[str] = field(default_factory=set)
    warnings: List[str] = field(default_factory=list)


def run_scenario(
    kit: BaseKit,
    scenario: Dict[str, Any],
    *,
    timings: Timings | None = None,
    log: Callable[[str], None] = print,
) -> Dict[str, Any]:
    """Replay ``scenario`` against the live system behind ``kit`` and score it.

    Drives the six phases in order and returns the oracle's results dict, augmented
    with the run's ``warnings`` and the scenario ``meta``. Raises
    :class:`~agentshield_driver.kit.KitError` only on a hard transport/op failure;
    a fold that never lands is a warning, not an exception, so a degraded run still
    scores (and the degradation shows up as a missed verdict)."""
    timings = timings or Timings()
    rl = RunLog()

    kit.ping()
    _seed(kit, scenario, log)
    _seal(kit, scenario, log)
    _prewarm(kit, scenario, timings, rl, log)
    _timeline(kit, scenario, timings, rl, log)

    log("settle: draining decision.made arms before the deferred confirmations")
    time.sleep(timings.settle_after_evaluate)
    _deferred_captures(kit, scenario, rl, log)
    time.sleep(timings.settle_after_capture)
    _disputes(kit, scenario, rl, log)
    _cancellations(kit, scenario, log)
    time.sleep(timings.settle_after_settlement)

    labels = _collect(kit, scenario, rl, timings, log)
    results = score_run(scenario, rl.verdicts_by_eval, labels)
    results["warnings"] = rl.warnings
    results["meta"] = scenario.get("meta", {})
    return results


# --- phase 1: seed -----------------------------------------------------------
def _seed(kit: BaseKit, scenario: Dict[str, Any], log: Callable[[str], None]) -> None:
    """Write every mandate and its customer overlay through the real store contract.
    The driverkit unmarshals each dict into domain.Token / domain.PolicyOverlay, so
    the same containment/narrowing invariants a product write is checked by apply."""
    tokens = scenario.get("tokens", [])
    overlays = scenario.get("overlays", [])
    for t in tokens:
        kit.seed_token(t)
    for o in overlays:
        kit.seed_overlay(o)
    log(f"seed: {len(tokens)} tokens, {len(overlays)} overlays")


# --- phase 2: seal -----------------------------------------------------------
def _session_token_ts(scenario: Dict[str, Any]) -> Dict[str, tuple]:
    """Map each session_id onto (token_id, ts) from the first debit that runs under
    it. A sealed envelope needs a non-empty token_id (the stream-processor drops a
    seal keyed on empty), so the session is bound to the mandate its traffic uses."""
    out: Dict[str, tuple] = {}
    for d in scenario.get("timeline", []):
        sid = d.get("session_id", "")
        if sid and sid not in out:
            out[sid] = (d["token_id"], d["ts"])
    return out


def _seal(kit: BaseKit, scenario: Dict[str, Any], log: Callable[[str], None]) -> None:
    """Seal each session's raw intent envelope into the VAULT — the one PII-bearing
    event in the whole run. token_id (the partition key) is taken from the session's
    first debit; a session with no traffic is skipped (nothing would read it)."""
    smap = _session_token_ts(scenario)
    base_ts = int(scenario.get("meta", {}).get("base_ts", 0))
    sealed = 0
    for e in scenario.get("envelopes", []):
        sid = e["session_id"]
        bound = smap.get(sid)
        if bound is None:
            continue
        token_id, ts = bound
        kit.seal_envelope(
            event_id=f"seal:{sid}",
            token_id=token_id,
            session_id=sid,
            occurred_at=int(ts or base_ts),
            raw_instruction=e.get("raw_instruction", ""),
            contact=e.get("contact", ""),
        )
        sealed += 1
    log(f"seal: {sealed} envelopes sealed into the VAULT")


# --- phase 3: pre-warm -------------------------------------------------------
def _distinct_keys(scenario: Dict[str, Any]) -> Set[str]:
    """Every non-empty entity key any debit will read. A debit's feature read wants
    all four of (customer, token, agent, merchant) present, or the read is degraded
    and fails closed — so the pre-warm must touch every one across the timeline."""
    keys: Set[str] = set()
    for d in scenario.get("timeline", []):
        for f in ("customer_id", "token_id", "agent_id", "merchant_id"):
            v = d.get(f, "")
            if v:
                keys.add(v)
    return keys


def _overlay_targets(scenario: Dict[str, Any]) -> tuple:
    """The keys that need an elevated figure to earn their expected step-up: the
    drift tokens (high intent divergence) and the graph-family customers (high
    network risk). Everything else rides the baseline."""
    drift_tokens: Set[str] = set()
    graph_customers: Set[str] = set()
    for d in scenario.get("timeline", []):
        fam = d.get("family", "")
        if fam == INTENT_DRIFT_FAMILY:
            drift_tokens.add(d["token_id"])
        elif fam in GRAPH_FAMILIES:
            graph_customers.add(d["customer_id"])
    return drift_tokens, graph_customers


def _prewarm(
    kit: BaseKit, scenario: Dict[str, Any], timings: Timings,
    rl: RunLog, log: Callable[[str], None],
) -> None:
    """Deposit an engine-stand-in figure on every key a debit reads, then wait for
    the off-clock materialiser to fold them all. Baseline behaviour makes each row
    present (a missing row fails the read closed); the intent/network overlays lift
    the drift/graph keys over the interruption cost so their step-ups are earned."""
    base_ts = int(scenario.get("meta", {}).get("base_ts", 0))
    keys = _distinct_keys(scenario)
    drift_tokens, graph_customers = _overlay_targets(scenario)

    for k in keys:
        kit.deposit_feature(
            event_id=f"warm:b:{k}", token_id=k, kind="behaviour",
            feature_key=k, occurred_at=base_ts, value=_BASELINE,
        )
    for tid in drift_tokens:
        kit.deposit_feature(
            event_id=f"warm:i:{tid}", token_id=tid, kind="intent",
            feature_key=tid, occurred_at=base_ts, value=_RISK_HIGH,
        )
    for cid in graph_customers:
        kit.deposit_feature(
            event_id=f"warm:n:{cid}", token_id=cid, kind="network",
            feature_key=cid, occurred_at=base_ts, value=_RISK_HIGH,
        )
    log(
        f"pre-warm: {len(keys)} keys baselined, "
        f"{len(drift_tokens)} intent + {len(graph_customers)} network overlays"
    )
    _await_features(kit, keys, timings, rl, log)


def _await_features(
    kit: BaseKit, keys: Set[str], timings: Timings,
    rl: RunLog, log: Callable[[str], None],
) -> None:
    """Poll get_feature on each key until its row has been folded, or warn on
    timeout. The materialiser is off the clock, so a debit sent before its keys land
    would read a missing row and fail closed — this barrier prevents that race."""
    deadline = time.time() + timings.feature_poll_timeout
    pending = set(keys)
    while pending:
        still: Set[str] = set()
        for k in pending:
            if kit.get_feature(k).get("feature") is None:
                still.add(k)
        pending = still
        if not pending:
            return
        if time.time() > deadline:
            rl.warnings.append(
                f"pre-warm timeout: {len(pending)} keys never folded, "
                f"e.g. {sorted(pending)[:5]}"
            )
            return
        time.sleep(timings.poll_interval)


# --- phase 4: timeline -------------------------------------------------------
def _order_context(d: Dict[str, Any]) -> Dict[str, Any]:
    """Pull exactly the 12 proto OrderContext fields out of a debit dict — the wire
    request the driverkit hands to gRPC Evaluate, with none of the ground-truth or
    settlement metadata the debit also carries."""
    return {k: d[k] for k in _ORDER_KEYS}


def _timeline(
    kit: BaseKit, scenario: Dict[str, Any], timings: Timings,
    rl: RunLog, log: Callable[[str], None],
) -> None:
    """Evaluate every debit in timeline order. A barrier'd debit waits for the prior
    fold it depends on (a spent nonce, accumulated consumption) before its evaluate.
    An ALLOWed debit whose recipe captures is captured immediately — a bare capture
    that teaches the labeler nothing but spends the nonce and folds the consumption
    the later barriers key on. Step-up confirmations are deferred to settlement."""
    spent: Dict[str, Set[str]] = {}   # token -> nonces an immediate capture has spent
    imm_sum: Dict[str, int] = {}      # token -> paise those immediate captures moved
    stepups = allows = blocks = 0

    for d in scenario.get("timeline", []):
        token = d["token_id"]
        if d.get("barrier"):
            _await_barrier(
                kit, token, spent.get(token, set()), imm_sum.get(token, 0),
                timings, rl, log,
            )

        resp = kit.evaluate(_order_context(d))
        rl.verdicts_by_eval[d["evaluation_id"]] = resp
        decision = normalise_decision(resp.get("decision", ""))
        stepups += int(decision == "STEP_UP")
        allows += int(decision == "ALLOW")
        blocks += int(decision == "BLOCK")

        s = d.get("settlement", {})
        cw = s.get("capture_when", "never")
        if decision == "ALLOW" and cw in ("allow", "allow_or_stepup"):
            amt = int(s.get("capture_amount_paise", 0))
            kit.capture(
                event_id=f"cap:{d['evaluation_id']}", token_id=token,
                occurred_at=int(d["ts"]), amount_paise=amt,
                nonce=d["nonce"], agent_id=d.get("agent_id", ""),
            )
            spent.setdefault(token, set()).add(d["nonce"])
            imm_sum[token] = imm_sum.get(token, 0) + amt
            rl.captured_evals.add(d["evaluation_id"])

    log(f"timeline: {len(scenario.get('timeline', []))} debits "
        f"-> {allows} ALLOW / {stepups} STEP_UP / {blocks} BLOCK")


def _await_barrier(
    kit: BaseKit, token: str, need_nonces: Set[str], need_sum: int,
    timings: Timings, rl: RunLog, log: Callable[[str], None],
) -> None:
    """Wait until the token's reconstructed block-state reflects the prior immediate
    captures this debit depends on: every tracked nonce shows as spent AND the day's
    consumption has caught up to what those captures moved. Covers both the replay
    barrier (nonce spend) and the bust-out crossing debit (accumulated consumption)."""
    if not need_nonces and need_sum <= 0:
        return
    deadline = time.time() + timings.barrier_poll_timeout
    while True:
        block = kit.get_block(token).get("block")
        if block is not None:
            seen = set(block.get("seen_nonces") or [])
            consumed = int(block.get("consumed_today", 0))
            if need_nonces.issubset(seen) and consumed >= need_sum:
                return
        if time.time() > deadline:
            rl.warnings.append(
                f"barrier timeout on token {token}: "
                f"need nonces {sorted(need_nonces)} / consumed >= {need_sum}"
            )
            return
        time.sleep(timings.poll_interval)


# --- phase 5: settle ---------------------------------------------------------
def _deferred_captures(
    kit: BaseKit, scenario: Dict[str, Any], rl: RunLog, log: Callable[[str], None],
) -> None:
    """Emit the confirming captures for the step-ups a human passed. Run after the
    settle delay so every decision.made has armed the labeler's pending[(token,nonce)];
    the capture reuses the debit's nonce, so the labeler reads it as a confirmed
    step-up (LEGIT) rather than the unarmed no-op an in-flight race would produce."""
    n = 0
    for d in scenario.get("timeline", []):
        resp = rl.verdicts_by_eval.get(d["evaluation_id"])
        if resp is None:
            continue
        s = d.get("settlement", {})
        if s.get("capture_when") != "allow_or_stepup":
            continue
        if normalise_decision(resp.get("decision", "")) != "STEP_UP":
            continue
        kit.capture(
            event_id=f"cap:{d['evaluation_id']}", token_id=d["token_id"],
            occurred_at=int(d["ts"]), amount_paise=int(s.get("capture_amount_paise", 0)),
            nonce=d["nonce"], agent_id=d.get("agent_id", ""),
        )
        rl.captured_evals.add(d["evaluation_id"])
        n += 1
    log(f"settle: {n} deferred step-up confirmations captured")


def _disputes(
    kit: BaseKit, scenario: Dict[str, Any], rl: RunLog, log: Callable[[str], None],
) -> None:
    """Chargeback every captured debit whose recipe disputes — the strongest settled
    negative (a MISUSE label). Only debits that actually moved money can be disputed,
    so this keys off captured_evals rather than the recipe alone."""
    n = 0
    for d in scenario.get("timeline", []):
        s = d.get("settlement", {})
        if not s.get("then_dispute"):
            continue
        if d["evaluation_id"] not in rl.captured_evals:
            continue
        kit.dispute(
            event_id=f"dis:{d['evaluation_id']}", token_id=d["token_id"],
            occurred_at=int(d["ts"]), nonce=d["nonce"], agent_id=d.get("agent_id", ""),
        )
        n += 1
    log(f"settle: {n} disputes chargeback'd")


def _cancellations(
    kit: BaseKit, scenario: Dict[str, Any], log: Callable[[str], None],
) -> None:
    """Pull each cancelled mandate — a token.cancelled the labeler distils into a
    light MISUSE (cancellation) label. Unconditional: it does not depend on any
    verdict, so it settles regardless of what the live system decided."""
    base_ts = int(scenario.get("meta", {}).get("base_ts", 0))
    cancels = scenario.get("cancellations", [])
    for tid in cancels:
        kit.cancel(event_id=f"cancel:{tid}", token_id=tid, occurred_at=base_ts)
    log(f"settle: {len(cancels)} mandates cancelled")


# --- phase 6: collect --------------------------------------------------------
def _collect(
    kit: BaseKit, scenario: Dict[str, Any], rl: RunLog,
    timings: Timings, log: Callable[[str], None],
) -> List[Dict[str, Any]]:
    """Drain the labels the live labeler produced. ``expect`` is the guaranteed floor
    — every dispute and every cancellation emits a MISUSE label unconditionally — so
    the drain blocks until at least those arrive, then snapshots whatever else (the
    confirmed step-ups) folded alongside them."""
    disputes = sum(
        1 for d in scenario.get("timeline", [])
        if d.get("settlement", {}).get("then_dispute") and d["evaluation_id"] in rl.captured_evals
    )
    expect = disputes + len(scenario.get("cancellations", []))
    resp = kit.collect_labels(expect=expect, timeout_ms=timings.labels_timeout_ms)
    labels = resp.get("labels") or []
    log(f"collect: drained {len(labels)} labels (floor was {expect})")
    return labels

