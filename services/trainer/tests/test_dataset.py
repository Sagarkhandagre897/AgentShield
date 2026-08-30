"""Dataset-replay tests (System Design §11, §13) — pure stdlib, no wheels.

They pin train/serve parity and the no-leak rule: the behaviour example for a
settled token is the principal's vector as it stood at the last event AT OR BEFORE
the outcome (a later event must not leak back into the row), the floor's population
covers every observation (not just the labelled subset), and a token is seeded in
the graph only when an edge actually put it there. Runnable with pytest or directly.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "behaviour"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "graph"))

from agentshield_shared.schema import (  # noqa: E402
    EVENT_DECISION_MADE,
    EVENT_PAYMENT_CAPTURED,
    LABEL_LEGIT,
    LABEL_MISUSE,
    REASON_CONFIRMED_STEP_UP,
    REASON_DISPUTE,
    Event,
)
from agentshield_behaviour.baselines import BaselineBank  # noqa: E402
from agentshield_graph.graph import NODE_TOKEN, node_id  # noqa: E402
from agentshield_trainer.dataset import (  # noqa: E402
    behaviour_examples,
    behaviour_population,
    graph_dataset,
)
from agentshield_trainer.labels import Label, LabelSet  # noqa: E402


def _ev(etype, token, at, agent=None, amount=0, merchant=None, eid=None):
    payload = {}
    if agent:
        payload["agent_id"] = agent
    if amount:
        payload["amount_paise"] = amount
    if merchant:
        payload["merchant_id"] = merchant
    return Event(event_id=eid or f"{etype}_{token}_{at}", type=etype, token_id=token, occurred_at=at, payload=payload)


def _labels(*triples):
    """(token, value, occurred_at) → a LabelSet, weight 1.0."""
    ls = LabelSet()
    for token, value, at in triples:
        reason = REASON_DISPUTE if value == LABEL_MISUSE else REASON_CONFIRMED_STEP_UP
        ls.add(token, Label(value, 1.0, reason, at))
    return ls


def test_behaviour_example_is_point_in_time():
    # t1 acts at 100 (amount 1000) and again at 300 (amount 5000); the outcome
    # settled at 200. The example must be the 100-event vector — the 300 event is
    # after the label and would leak the future into the row.
    events = [
        _ev(EVENT_DECISION_MADE, "t1", at=100, agent="a1", amount=1000, merchant="m1"),
        _ev(EVENT_PAYMENT_CAPTURED, "t1", at=300, agent="a1", amount=5000, merchant="m2"),
    ]
    X, y, w = behaviour_examples(events, _labels(("t1", LABEL_MISUSE, 200)))

    bank = BaselineBank()
    _sig, expected = bank.observe("a1", 1000.0, "m1", 100)  # only the pre-label event
    assert len(X) == 1
    assert X[0] == expected
    assert y == [1] and w == [1.0]


def test_unlabelled_or_post_label_only_tokens_produce_no_row():
    events = [
        _ev(EVENT_DECISION_MADE, "t2", at=100, agent="a2", amount=1000),   # unlabelled
        _ev(EVENT_DECISION_MADE, "t5", at=300, agent="a5", amount=1000),   # only after its label
    ]
    labels = _labels(("t5", LABEL_MISUSE, 200))  # settled at 200, activity at 300
    X, y, w = behaviour_examples(events, labels)
    assert X == [] and y == [] and w == []


def test_behaviour_population_covers_every_observation():
    events = [
        _ev(EVENT_DECISION_MADE, "t1", at=100, agent="a1", amount=1000),
        _ev(EVENT_PAYMENT_CAPTURED, "t1", at=300, agent="a1", amount=5000),
        _ev(EVENT_DECISION_MADE, "t2", at=100, agent="a2", amount=1000),   # unlabelled
        _ev(EVENT_DECISION_MADE, "t3", at=100, agent="a3", amount=1000),
        _ev(EVENT_PAYMENT_CAPTURED, "t3", at=300, agent="a3", amount=1500),
    ]
    pop = behaviour_population(events)
    assert len(pop) == 5  # the floor sees everyone, labelled or not


def test_graph_dataset_seeds_present_tokens_with_the_label_value():
    events = [
        _ev(EVENT_DECISION_MADE, "t1", at=100, agent="a1", merchant="m1"),
        _ev(EVENT_DECISION_MADE, "t3", at=100, agent="a3", merchant="m3"),
    ]
    # t1 disputed (misuse), t3 confirmed (legit), t4 labelled but never had an edge.
    labels = _labels(("t1", LABEL_MISUSE, 200), ("t3", LABEL_LEGIT, 200), ("t4", LABEL_MISUSE, 200))
    graph, seeds = graph_dataset(events, labels)

    assert seeds == {
        node_id(NODE_TOKEN, "t1"): LABEL_MISUSE,
        node_id(NODE_TOKEN, "t3"): LABEL_LEGIT,
    }
    assert node_id(NODE_TOKEN, "t4") not in seeds  # no edge → nowhere to attach


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
