"""engine — the behaviour engine's async runner (System Design §11).

It closes the three layers into one worker on the off-clock plane: consume the
gate events (evaluations and payments), fold each into the per-principal baselines
(Layer 1), score the resulting deviation vector through the calibrated model
(Layer 2) with the isolation-forest floor beneath it (Layer 3), and deposit ONE
behaviour_deviation — plus the per-signal breakdown with counts — through the
shared publisher. The Go materialiser (the single writer) merges it onto the
keyed feature row; the engine never writes a store.

Nothing here runs on the request. The request reads the number by key.

The figure is keyed on the principal it describes — the agent_id when the event
carries one, else the customer_id, else the token_id. Merchant is read from the
payload when present (merchant_id); until the richer gate events flow it simply
leaves the merchant-novelty signal unobserved rather than fabricating one.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import List, Optional

from .baselines import BaselineBank, Signal, baseline_deviation
from .model import BehaviourScorer, IsolationFloor, combine

try:  # the shared wire contract; bootstrap onto sys.path when run in-tree
    from agentshield_shared.schema import Event, SignalDeviation
except ModuleNotFoundError:  # pragma: no cover
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))
    from agentshield_shared.schema import Event, SignalDeviation

# Payload keys the engine reads. amount/agent/customer mirror the shared schema;
# merchant_id is behaviour-local and optional (not yet on every gate event).
PAYLOAD_AMOUNT_PAISE = "amount_paise"
PAYLOAD_AGENT_ID = "agent_id"
PAYLOAD_CUSTOMER_ID = "customer_id"
PAYLOAD_MERCHANT_ID = "merchant_id"

# The gate events the engine learns from (§11: "source · our gate events").
_LEARN_FROM = {"decision.made", "payment.captured"}

# The consumer group and the topics those events arrive on.
GROUP = "behaviour-engine"
TOPICS = ("evaluations.v1", "payments.v1")


@dataclass
class Deposit:
    """What the engine computed for one observation — ready to hand to the shared
    publisher's deposit_behaviour()."""

    feature_key: str
    deviation: float
    signals: List[SignalDeviation]
    occurred_at: int
    token_id: str


def _principal_of(ev: Event) -> str:
    """The entity the behaviour figure lands on. Agent first (the figure is about
    an agent acting unlike itself), then customer, then the token as a last key."""
    p = ev.payload.get(PAYLOAD_AGENT_ID) or ev.payload.get(PAYLOAD_CUSTOMER_ID) or ev.token_id
    return p or ""


def _amount_of(ev: Event) -> float:
    try:
        return float(ev.payload.get(PAYLOAD_AMOUNT_PAISE) or 0.0)
    except (TypeError, ValueError):
        return 0.0


class BehaviourEngine:
    """The three layers, wired. Holds the baseline bank (Layer 1) and, when the ML
    wheels are present and a model has been trained/loaded, the calibrated scorer
    (Layer 2) and the isolation-forest floor (Layer 3). Without them it deposits
    the Layer-1 aggregate — a defensible cold-start figure, never a crash."""

    def __init__(self, scorer: Optional[BehaviourScorer] = None, floor: Optional[IsolationFloor] = None):
        self.bank = BaselineBank()
        self.scorer = scorer
        self.floor = floor

    def observe(self, ev: Event) -> Optional[Deposit]:
        """Fold one gate event and return the deposit to publish, or None when the
        event is not one we learn from or carries nothing to key on."""
        if ev.type not in _LEARN_FROM:
            return None
        key = _principal_of(ev)
        if not key:
            return None

        amount = _amount_of(ev)
        merchant = ev.payload.get(PAYLOAD_MERCHANT_ID) or ""
        signals, fv = self.bank.observe(key, amount, merchant, ev.occurred_at)

        calibrated = self.scorer.predict(fv) if self.scorer else None
        floor_score = self.floor.score(fv) if self.floor else 0.0
        deviation = combine(calibrated, floor_score, baseline_deviation(signals))

        breakdown = [SignalDeviation(s.signal, s.deviation, s.obs_count) for s in signals]
        return Deposit(key, deviation, breakdown, ev.occurred_at, ev.token_id)

    def run(self, seeds: str, group: str = GROUP) -> None:
        """Consume the gate events forever, depositing a behaviour figure per
        observation. The producer is flushed before the handler returns, so the
        deposit is durable before the source offset commits — at-least-once all the
        way through, and the Go materialiser dedupes on the deposit's stable id."""
        from agentshield_shared.bus import DepositPublisher, EventConsumer  # lazy: needs the kafka client

        pub = DepositPublisher(seeds)
        consumer = EventConsumer(seeds, group, list(TOPICS))

        def handle(ev: Event) -> None:
            dep = self.observe(ev)
            if dep is None:
                return
            pub.deposit_behaviour(dep.feature_key, dep.deviation, dep.occurred_at, dep.signals, dep.token_id)
            pub.flush(10.0)

        try:
            consumer.run(handle)
        finally:
            consumer.close()


def load_models(model_dir: str) -> tuple[Optional[BehaviourScorer], Optional[IsolationFloor]]:
    """Load a trained scorer and floor from a directory if both the wheels and the
    files are present; otherwise return (None, None) and let the engine run on
    Layer 1 alone. Retraining is a daily-to-weekly offline job (§11), not this
    worker's concern — it only serves the current model."""
    from . import model as _model

    if not _model.available():
        return None, None
    scorer = floor = None
    gbdt_path = os.path.join(model_dir, "behaviour_gbdt.pkl")
    floor_path = os.path.join(model_dir, "behaviour_floor.pkl")
    if os.path.exists(gbdt_path):
        scorer = BehaviourScorer()
        scorer.load(gbdt_path)
    if os.path.exists(floor_path):
        floor = IsolationFloor()
        floor.load(floor_path)
    return scorer, floor


def main() -> None:
    seeds = os.environ.get("KAFKA_SEEDS")
    if not seeds:
        raise SystemExit("KAFKA_SEEDS is required (e.g. localhost:19092)")
    scorer = floor = None
    model_dir = os.environ.get("BEHAVIOUR_MODEL_DIR")
    if model_dir:
        scorer, floor = load_models(model_dir)
    print(f"behaviour-engine: consuming {list(TOPICS)} from {seeds} "
          f"(scorer={'on' if scorer else 'cold'}, floor={'on' if floor else 'off'})", flush=True)
    BehaviourEngine(scorer, floor).run(seeds)


if __name__ == "__main__":  # pragma: no cover
    main()


