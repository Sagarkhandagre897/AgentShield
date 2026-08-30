"""Engine-level tests (System Design §13) — structural path, no wheels needed.

They pin the seams the engine turns on: edges are folded from the gate events, a
settled dispute seeds suspicion that reaches a token sharing a device with the
fraud while a stranger stays clean, figures are deposited under the bare id (the
key the request reads) only for the keyed node types, and the structural floor is
a genuine lower bound on the figure. The learned model is exercised in
test_model.py. Runnable with pytest or directly.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from agentshield_graph.engine import GraphEngine  # noqa: E402
from agentshield_graph.model import combine_risk  # noqa: E402
from agentshield_shared.schema import (  # noqa: E402
    EVENT_DECISION_MADE,
    EVENT_FEATURE_BEHAVIOUR,
    EVENT_PAYMENT_DISPUTED,
    Event,
)


def _ev(etype, token, at=1000, eid=None, **entities):
    payload = {k: v for k, v in entities.items() if v}
    return Event(event_id=eid or f"e_{token}_{at}", type=etype, token_id=token, occurred_at=at, payload=payload)


def _ring_engine():
    eng = GraphEngine()
    # t1 and t2 share device d; t1 is later disputed (a settled-fraud seed).
    eng.observe(_ev(EVENT_DECISION_MADE, "t1", agent_id="a1", device_id="d"))
    eng.observe(_ev(EVENT_DECISION_MADE, "t2", agent_id="a2", device_id="d"))
    eng.observe(_ev(EVENT_PAYMENT_DISPUTED, "t1", at=2000))
    # A stranger in a separate component.
    eng.observe(_ev(EVENT_DECISION_MADE, "t9", agent_id="a9", merchant_id="m9"))
    return eng


def test_dispute_seed_reaches_the_neighbour_not_the_stranger():
    deps = {d.feature_key: d for d in _ring_engine().deposit_all(computed_at=5000)}
    assert deps["t2"].risk > deps["t9"].risk  # shares a device with the fraud token
    assert deps["t9"].risk < 0.3  # a clean stranger stays low


def test_deposits_only_keyed_node_types_under_the_bare_id():
    deps = {d.feature_key: d for d in _ring_engine().deposit_all()}
    assert "t2" in deps and "a2" in deps and "m9" in deps  # token/agent/merchant
    assert "d" not in deps  # a device is structure only — nothing reads it by key


def test_token_nodes_carry_the_partition_key():
    deps = {d.feature_key: d for d in _ring_engine().deposit_all()}
    assert deps["t2"].token_id == "t2"  # token node → token_id set
    assert deps["a2"].token_id == ""    # agent node → no token partition


def test_computed_at_is_stamped():
    deps = _ring_engine().deposit_all(computed_at=777)
    assert deps and all(d.computed_at == 777 for d in deps)


def test_structural_floor_is_a_lower_bound():
    # The floor and propagation are lower bounds; the learned model only raises.
    assert combine_risk(0.6, 0.2, None) == 0.6
    assert combine_risk(0.1, 0.3, 0.5) == 0.5
    assert combine_risk(0.1, 0.0, None) == 0.1
    assert combine_risk(0.9, 0.0, 2.0) == 1.0  # clamped to [0, 1]


def test_non_edge_events_and_keyless_events_are_ignored():
    eng = GraphEngine()
    eng.observe(_ev(EVENT_FEATURE_BEHAVIOUR, "t1", agent_id="a1"))  # not an edge event
    eng.observe(_ev(EVENT_DECISION_MADE, "", agent_id="a1"))       # nothing to key on
    assert len(eng.graph) == 0
    assert eng.deposit_all() == []


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
