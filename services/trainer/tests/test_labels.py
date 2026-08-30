"""Label-reader tests (System Design §6) — pure stdlib, no wheels.

They pin the one rule that makes the labels trustworthy: the set is folded ONLY
from outcome.labeled events (never a decision, never silence), misuse dominates a
legitimate label whichever arrived first, and within one class the later outcome
wins. Runnable with pytest or directly (`python3 tests/test_labels.py`).
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from agentshield_shared.schema import (  # noqa: E402
    EVENT_DECISION_MADE,
    EVENT_OUTCOME_LABELED,
    LABEL_LEGIT,
    LABEL_MISUSE,
    PAYLOAD_LABEL,
    PAYLOAD_REASON,
    PAYLOAD_WEIGHT,
    REASON_CANCELLATION,
    REASON_CONFIRMED_STEP_UP,
    REASON_DISPUTE,
    Event,
)
from agentshield_trainer.labels import Label, LabelSet  # noqa: E402


def _label_ev(token, value, weight, reason, at, eid=None):
    return Event(
        event_id=eid or f"lab_{token}_{at}",
        type=EVENT_OUTCOME_LABELED,
        token_id=token,
        occurred_at=at,
        payload={PAYLOAD_LABEL: value, PAYLOAD_WEIGHT: weight, PAYLOAD_REASON: reason},
    )


def test_folds_only_outcome_events():
    events = [
        Event(event_id="d1", type=EVENT_DECISION_MADE, token_id="t1", occurred_at=10),
        _label_ev("t1", LABEL_MISUSE, 1.0, REASON_DISPUTE, at=20),
    ]
    ls = LabelSet.from_events(events)
    assert len(ls) == 1  # the decision.made contributes no label
    assert ls.get("t1").is_misuse


def test_misuse_dominates_legit_regardless_of_order():
    # legit first, then misuse
    a = LabelSet.from_events([
        _label_ev("t1", LABEL_LEGIT, 1.0, REASON_CONFIRMED_STEP_UP, at=10),
        _label_ev("t1", LABEL_MISUSE, 1.0, REASON_DISPUTE, at=20),
    ])
    # misuse first, then legit
    b = LabelSet.from_events([
        _label_ev("t1", LABEL_MISUSE, 1.0, REASON_DISPUTE, at=20),
        _label_ev("t1", LABEL_LEGIT, 1.0, REASON_CONFIRMED_STEP_UP, at=30),
    ])
    assert a.get("t1").is_misuse and b.get("t1").is_misuse


def test_within_a_class_the_later_outcome_wins():
    ls = LabelSet.from_events([
        _label_ev("t1", LABEL_MISUSE, 1.0, REASON_DISPUTE, at=10),
        _label_ev("t1", LABEL_MISUSE, 0.5, REASON_CANCELLATION, at=20),
    ])
    lab = ls.get("t1")
    assert lab.reason == REASON_CANCELLATION and lab.occurred_at == 20


def test_weight_and_reason_are_carried():
    ls = LabelSet.from_events([_label_ev("t1", LABEL_MISUSE, 0.5, REASON_CANCELLATION, at=10)])
    lab = ls.get("t1")
    assert lab.value == LABEL_MISUSE and lab.weight == 0.5 and lab.reason == REASON_CANCELLATION


def test_empty_token_is_ignored():
    ls = LabelSet()
    ls.add("", Label(LABEL_MISUSE, 1.0, REASON_DISPUTE, 10))
    assert len(ls) == 0


def test_is_misuse_property():
    assert Label(LABEL_MISUSE, 1.0, REASON_DISPUTE, 1).is_misuse
    assert not Label(LABEL_LEGIT, 1.0, REASON_CONFIRMED_STEP_UP, 1).is_misuse


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
