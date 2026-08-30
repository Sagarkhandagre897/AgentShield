"""dataset — replay the event log into labelled training sets (System Design §11, §13).

The trainer must fit on exactly what the engine computes online, or the model
learns a feature it will never see. So this module does not define features — it
replays the historical events through the engines' OWN extractors:

  behaviour_examples    folds every learn-from event through a BaselineBank
                        (Layer 1, §11) and, for each labelled token, keeps the
                        principal's feature vector as it stood at the last event at
                        or before the outcome settled — a point-in-time snapshot,
                        no leakage of the future into the row. These are the
                        (X, y, w) the supervised scorer (Layer 2) fits on.
  behaviour_population   the SAME fold, but every feature vector produced by every
                        observation — the label-free view the isolation-forest
                        floor (Layer 3) fits on: "everyone we have seen", not just
                        the settled subset, so an unlabelled anomaly still lands
                        inside the forest.
  graph_dataset         folds the edge events through the graph engine to build the
                        heterogeneous entity graph (§13), and turns the labelled
                        tokens into settled-fraud / settled-clean seeds on their
                        token nodes — what label propagation and GraphSAGE learn
                        from.

All pure stdlib: feature extraction and graph construction carry no ML wheels. The
learned fitting lives in train.py behind each engine's available() guard.
"""

from __future__ import annotations

import os
import sys
from typing import Iterable, List, Optional, Tuple

# Bootstrap the sibling engine packages + the shared contract onto sys.path when
# run in-tree (each service is its own package, not installed).
_HERE = os.path.dirname(__file__)
for _rel in (("..", "..", "shared"), ("..", "..", "behaviour"), ("..", "..", "graph")):
    sys.path.insert(0, os.path.join(_HERE, *_rel))

from agentshield_shared.schema import Event  # noqa: E402

from agentshield_behaviour import engine as behaviour_engine  # noqa: E402
from agentshield_behaviour.baselines import BaselineBank  # noqa: E402
from agentshield_graph.engine import GraphEngine  # noqa: E402
from agentshield_graph.graph import NODE_TOKEN, node_id  # noqa: E402

from .labels import LabelSet  # noqa: E402


def _fold_behaviour(
    events: Iterable[Event], labels: Optional[LabelSet]
) -> Tuple[List[List[float]], dict]:
    """One pass through a shared BaselineBank — the population sees every learn-from
    event, exactly as it does online. Returns:

      population  every feature vector produced (order = event order) — the floor's
                  label-free training set.
      snapshot    token_id -> (occurred_at, feature_vector) at the latest event that
                  is not after the label settled — the point-in-time row for the
                  supervised scorer. Empty when ``labels`` is None."""
    bank = BaselineBank()
    population: List[List[float]] = []
    snapshot: dict[str, tuple[int, List[float]]] = {}

    for ev in sorted(events, key=lambda e: e.occurred_at):
        if ev.type not in behaviour_engine._LEARN_FROM:
            continue
        key = behaviour_engine._principal_of(ev)
        if not key:
            continue
        amount = behaviour_engine._amount_of(ev)
        merchant = ev.payload.get(behaviour_engine.PAYLOAD_MERCHANT_ID) or ""
        _signals, fv = bank.observe(key, amount, merchant, ev.occurred_at)
        population.append(list(fv))

        if labels is None:
            continue
        lab = labels.get(ev.token_id)
        if lab is None or ev.occurred_at > lab.occurred_at:
            continue  # unlabelled, or after the outcome settled (would leak)
        prev = snapshot.get(ev.token_id)
        if prev is None or ev.occurred_at >= prev[0]:
            snapshot[ev.token_id] = (ev.occurred_at, list(fv))

    return population, snapshot


def behaviour_examples(
    events: Iterable[Event], labels: LabelSet
) -> Tuple[List[List[float]], List[int], List[float]]:
    """Build (X, y, weights) for the supervised behaviour scorer.

    One example per labelled token — the principal's feature vector snapshotted at
    the last event at or before the outcome settled. Labels come only from settled
    outcomes, so only settled tokens produce rows; the weight rides along so a soft
    label (a cancellation) can count for less than a hard one (a dispute)."""
    _population, snapshot = _fold_behaviour(events, labels)

    X: List[List[float]] = []
    y: List[int] = []
    w: List[float] = []
    for token, (_at, fv) in snapshot.items():
        lab = labels.get(token)
        if lab is None:
            continue
        X.append(fv)
        y.append(int(round(lab.value)))
        w.append(lab.weight)
    return X, y, w


def behaviour_population(events: Iterable[Event]) -> List[List[float]]:
    """Every feature vector the replay produces — the label-free training set for
    the isolation-forest floor. The floor fits on the whole population it has seen,
    not the labelled subset, so a pattern the labels have not caught up to still
    registers as an outlier (§11, Layer 3)."""
    population, _snapshot = _fold_behaviour(events, None)
    return population


def graph_dataset(events: Iterable[Event], labels: LabelSet):
    """Build (graph, seeds) for the graph engine's learned + propagated layers.

    The edge events fold into the same heterogeneous graph the engine builds
    online; each labelled token becomes a seed on its token node (1.0 settled-bad,
    0.0 settled-good). Only tokens that actually appear as nodes are seeded — a
    label for a token we never saw an edge for has nowhere to attach."""
    eng = GraphEngine()
    for ev in events:
        eng.observe(ev)

    present = set(eng.graph.nodes())
    seeds: dict[str, float] = {}
    for token, lab in labels.items():
        node = node_id(NODE_TOKEN, token)
        if node in present:
            seeds[node] = lab.value
    return eng.graph, seeds
