"""Retrain-orchestrator tests (System Design §6, §11, §13).

They pin what a retrain run actually does: every learned layer gets a report line
(trained, or skipped with a reason), intent is never fitted (§12 — it is a sealed-
embedding comparison, not a supervised model), an empty corpus is a clean no-op
that writes nothing, and — when the wheels are present — the behaviour layers train
and persist under the exact filenames their engine loads, so a freshly retrained
model is picked up online. Graph training needs torch; it skips cleanly without it.
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "behaviour"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "graph"))

from agentshield_shared.schema import (  # noqa: E402
    EVENT_DECISION_MADE,
    EVENT_OUTCOME_LABELED,
    EVENT_PAYMENT_CAPTURED,
    LABEL_LEGIT,
    LABEL_MISUSE,
    PAYLOAD_LABEL,
    PAYLOAD_REASON,
    PAYLOAD_WEIGHT,
    REASON_CONFIRMED_STEP_UP,
    REASON_DISPUTE,
    Event,
)
from agentshield_behaviour.model import available as behaviour_available  # noqa: E402
from agentshield_graph.model import available as graph_available  # noqa: E402
from agentshield_trainer.train import (  # noqa: E402
    BEHAVIOUR_FLOOR,
    BEHAVIOUR_GBDT,
    GRAPH_SAGE,
    retrain,
)

try:
    import pytest  # noqa: E402
except ImportError:  # pragma: no cover - direct run without pytest
    pytest = None

def _ev(etype, token, at, agent, amount, merchant, eid=None):
    return Event(
        event_id=eid or f"{etype}_{token}_{at}",
        type=etype,
        token_id=token,
        occurred_at=at,
        payload={"agent_id": agent, "amount_paise": amount, "merchant_id": merchant},
    )


def _label(token, value, weight, reason, at):
    return Event(
        event_id=f"lab_{token}_{at}",
        type=EVENT_OUTCOME_LABELED,
        token_id=token,
        occurred_at=at,
        payload={PAYLOAD_LABEL: value, PAYLOAD_WEIGHT: weight, PAYLOAD_REASON: reason},
    )


def _stream(n=25):
    """A corpus with both classes: n disputed (misuse) tokens with large debits and
    n confirmed-step-up (legit) tokens with small ones — each acts before its label
    settles, so each yields one point-in-time example."""
    events = []
    for i in range(n):
        ab, tb = f"ab{i}", f"tb{i}"
        events += [
            _ev(EVENT_DECISION_MADE, tb, 100, ab, 9000 + i, f"mb{i}"),
            _ev(EVENT_PAYMENT_CAPTURED, tb, 110, ab, 9500 + i, f"mb{i}"),
            _label(tb, LABEL_MISUSE, 1.0, REASON_DISPUTE, 200),
        ]
        ag, tg = f"ag{i}", f"tg{i}"
        events += [
            _ev(EVENT_DECISION_MADE, tg, 100, ag, 100 + i, f"mg{i}"),
            _ev(EVENT_PAYMENT_CAPTURED, tg, 110, ag, 120 + i, f"mg{i}"),
            _label(tg, LABEL_LEGIT, 1.0, REASON_CONFIRMED_STEP_UP, 200),
        ]
    return events


def test_report_covers_every_layer():
    with tempfile.TemporaryDirectory() as d:
        rep = retrain(_stream(), d)
    names = {l.name for l in rep.layers}
    assert names == {"behaviour_scorer", "behaviour_floor", "graph_sage", "intent"}
    assert rep.n_events > 0 and rep.n_labels == 50


def test_intent_is_never_retrained():
    with tempfile.TemporaryDirectory() as d:
        rep = retrain(_stream(4), d)
    intent = rep.layer("intent")
    assert intent is not None and not intent.trained
    assert "sealed-embedding" in intent.reason


def test_empty_corpus_is_a_clean_noop():
    with tempfile.TemporaryDirectory() as d:
        rep = retrain([], d)
        assert not rep.trained_any
        assert os.path.isdir(d)  # dir created
        assert os.listdir(d) == []  # but nothing written


def _skipif(cond, reason):
    if pytest is not None:
        return pytest.mark.skipif(cond, reason=reason)
    return lambda fn: fn  # direct-run: decorator is a no-op


@_skipif(not behaviour_available(), "behaviour ML wheels not installed")
def test_behaviour_trains_and_persists_where_the_engine_loads_it():
    from agentshield_behaviour.engine import load_models

    with tempfile.TemporaryDirectory() as d:
        rep = retrain(_stream(), d)
        scorer, floor = rep.layer("behaviour_scorer"), rep.layer("behaviour_floor")
        assert scorer.trained and os.path.exists(os.path.join(d, BEHAVIOUR_GBDT))
        assert floor.trained and os.path.exists(os.path.join(d, BEHAVIOUR_FLOOR))
        # Train/serve parity: the engine's own loader picks the artifacts up.
        loaded_scorer, loaded_floor = load_models(d)
        assert loaded_scorer is not None and loaded_scorer.fitted
        assert loaded_floor is not None and loaded_floor.fitted


@_skipif(not behaviour_available(), "behaviour ML wheels not installed")
def test_behaviour_scorer_skips_a_single_class_corpus():
    # Only misuse labels: isotonic calibration has nothing to calibrate against.
    events = []
    for i in range(5):
        events += [
            _ev(EVENT_DECISION_MADE, f"t{i}", 100, f"a{i}", 9000, f"m{i}"),
            _label(f"t{i}", LABEL_MISUSE, 1.0, REASON_DISPUTE, 200),
        ]
    with tempfile.TemporaryDirectory() as d:
        rep = retrain(events, d)
    scorer = rep.layer("behaviour_scorer")
    assert not scorer.trained and "both classes" in scorer.reason
    # The floor is label-free — it still fits on the population.
    assert rep.layer("behaviour_floor").trained


@_skipif(behaviour_available(), "runs only where the behaviour wheels are absent")
def test_behaviour_is_a_recorded_noop_without_wheels():
    with tempfile.TemporaryDirectory() as d:
        rep = retrain(_stream(4), d)
    assert not rep.layer("behaviour_scorer").trained
    assert "ml wheels absent" in rep.layer("behaviour_scorer").reason


@_skipif(not graph_available(), "graph ML wheels (torch/torch-geometric) not installed")
def test_graph_trains_when_wheels_present():
    with tempfile.TemporaryDirectory() as d:
        rep = retrain(_stream(), d)
        g = rep.layer("graph_sage")
        assert g.trained and os.path.exists(os.path.join(d, GRAPH_SAGE))


@_skipif(graph_available(), "runs only where the graph wheels are absent")
def test_graph_is_a_recorded_noop_without_wheels():
    with tempfile.TemporaryDirectory() as d:
        rep = retrain(_stream(4), d)
    g = rep.layer("graph_sage")
    assert not g.trained and "ml wheels absent" in g.reason


if __name__ == "__main__":
    fns = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    ran = 0
    for name, fn in fns:
        mark = getattr(fn, "pytestmark", None)
        skip = any(getattr(m, "name", "") == "skipif" and m.args and m.args[0] for m in (mark or []))
        if skip:
            print(f"skip {name}")
            continue
        fn()
        ran += 1
        print(f"ok  {name}")
    print(f"\n{ran} passed")
