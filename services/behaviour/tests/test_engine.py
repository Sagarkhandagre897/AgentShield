"""Engine-level tests (System Design §11) — Layer 1 + the combination rule.

They pin the seams the whole engine turns on: which entity a figure is keyed to,
which events it learns from, that the floor is a genuine lower bound on the
figure, and that the per-signal breakdown it deposits is the shared wire type the
Go materialiser reads. No ML wheels needed — the combination rule is exercised
directly. Runnable with pytest or directly.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from agentshield_behaviour.engine import BehaviourEngine  # noqa: E402
from agentshield_behaviour.model import combine  # noqa: E402
from agentshield_shared.schema import (  # noqa: E402
    EVENT_DECISION_MADE,
    EVENT_TOKEN_CONFIRMED,
    Event,
    SignalDeviation,
)


def _debit(agent="", customer="", token="tok_1", amount=10000, merchant="m_A", at=1000, eid="e1"):
    payload = {"amount_paise": amount, "merchant_id": merchant}
    if agent:
        payload["agent_id"] = agent
    if customer:
        payload["customer_id"] = customer
    return Event(event_id=eid, type=EVENT_DECISION_MADE, token_id=token, occurred_at=at, payload=payload)


def test_keying_prefers_agent_then_customer_then_token():
    eng = BehaviourEngine()
    assert eng.observe(_debit(agent="a1", customer="c1")).feature_key == "a1"
    assert eng.observe(_debit(customer="c1")).feature_key == "c1"
    assert eng.observe(_debit()).feature_key == "tok_1"


def test_only_learns_from_gate_events():
    eng = BehaviourEngine()
    assert eng.observe(Event(event_id="t", type=EVENT_TOKEN_CONFIRMED, token_id="tok_1")) is None
    assert eng.observe(_debit(agent="a1")) is not None


def test_no_principal_is_dropped():
    eng = BehaviourEngine()
    ev = Event(event_id="e", type=EVENT_DECISION_MADE, token_id="", payload={"amount_paise": 100})
    assert eng.observe(ev) is None


def test_deposit_carries_shared_signal_type():
    eng = BehaviourEngine()
    dep = eng.observe(_debit(agent="a1"))
    assert dep.occurred_at == 1000 and dep.token_id == "tok_1"
    assert dep.signals and all(isinstance(s, SignalDeviation) for s in dep.signals)
    assert "obs_count" in dep.signals[0].to_dict()  # decodes on the Go side


def test_floor_is_a_lower_bound():
    # A quiet model cannot suppress an anomaly the floor caught.
    assert combine(calibrated=0.2, floor=0.8, baseline=0.1) == 0.8
    # Cold start (no model) falls back to the Layer-1 aggregate.
    assert combine(calibrated=None, floor=0.0, baseline=0.5) == 0.5
    # A confident model raises above the floor, and the figure clamps to [0, 1].
    assert combine(calibrated=0.9, floor=0.1, baseline=0.3) == 0.9
    assert combine(calibrated=1.5, floor=0.0, baseline=0.0) == 1.0


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
