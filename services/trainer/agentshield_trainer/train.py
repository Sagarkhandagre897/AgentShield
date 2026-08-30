"""train — the offline retrain orchestrator (System Design §6, §11, §13).

The daily-to-weekly job, distilled: read the settled labels the labeler put on
outcomes.v1, replay the historical events through the engines' OWN feature
extraction (dataset.py), fit each learned layer, and persist it under the exact
filename the engine loads. It never runs on the clock; it produces the artifacts
the engines pick up on their next start.

Two invariants carry through (§6, train/serve parity):
  - Labels come only from settled outcomes. This module reads what the labeler
    emitted and manufactures nothing — "no complaint arrived" and "we allowed it"
    cannot leak in.
  - Every learned layer is gated behind its engine's own available(). Without the
    ML wheels the job is a clean no-op per layer (reason recorded), never a crash —
    the same cold-start posture the engines serve under.

What is (and is not) retrained:
  behaviour_scorer  LightGBM + isotonic — needs both classes among the labels.
  behaviour_floor   isolation forest — label-free, fits on the whole population.
  graph_sage        GraphSAGE + isotonic — needs a fraud seed and a clean seed.
  intent            NOT retrained — intent alignment (§12) is a sealed-embedding
                    comparison, not a supervised model; there are no weights to fit.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from typing import Iterable, List, Optional

# Bootstrap the sibling engine packages + the shared contract onto sys.path when
# run in-tree (each service is its own package, not installed).
_HERE = os.path.dirname(__file__)
for _rel in (("..", "..", "shared"), ("..", "..", "behaviour"), ("..", "..", "graph")):
    sys.path.insert(0, os.path.join(_HERE, *_rel))

from agentshield_shared.schema import Event  # noqa: E402

from agentshield_behaviour import model as behaviour_model  # noqa: E402
from agentshield_behaviour.model import BehaviourScorer, IsolationFloor  # noqa: E402
from agentshield_graph import model as graph_model  # noqa: E402
from agentshield_graph.model import GraphSAGEModel  # noqa: E402

from .dataset import behaviour_examples, behaviour_population, graph_dataset  # noqa: E402
from .labels import LabelSet  # noqa: E402

# Artifact filenames — MUST match what the engines load (behaviour engine's
# load_models(), graph engine's load_model()), or a freshly trained model is
# never picked up.
BEHAVIOUR_GBDT = "behaviour_gbdt.pkl"
BEHAVIOUR_FLOOR = "behaviour_floor.pkl"
GRAPH_SAGE = "graph_sage.pkl"

@dataclass
class LayerReport:
    """The outcome for one learned layer: whether it was fitted, why not if it was
    skipped, how many examples it saw, and where the artifact landed."""

    name: str
    trained: bool = False
    reason: str = ""
    n_examples: int = 0
    artifact: str = ""

    def __str__(self) -> str:
        if self.trained:
            return f"  {self.name}: trained on {self.n_examples} → {self.artifact}"
        return f"  {self.name}: skipped ({self.reason}; {self.n_examples} examples)"


@dataclass
class TrainReport:
    """What one retrain run did — one LayerReport per learned layer, plus the size
    of the corpus it ran over. Returned so a caller (a scheduler, a test) can assert
    on exactly what was fitted."""

    model_dir: str
    n_events: int = 0
    n_labels: int = 0
    layers: List[LayerReport] = field(default_factory=list)

    def add(self, r: LayerReport) -> LayerReport:
        self.layers.append(r)
        return r

    def layer(self, name: str) -> Optional[LayerReport]:
        return next((l for l in self.layers if l.name == name), None)

    @property
    def trained_any(self) -> bool:
        return any(l.trained for l in self.layers)

    def __str__(self) -> str:
        head = (f"retrain over {self.n_events} events, {self.n_labels} settled labels "
                f"→ {self.model_dir}")
        return "\n".join([head, *(str(l) for l in self.layers)])


def _train_behaviour(events: List[Event], labels: LabelSet, model_dir: str, report: TrainReport) -> None:
    """Fit Layer 2 (the calibrated scorer) on the labelled snapshots and Layer 3
    (the label-free floor) on the whole population. Each is a clean no-op — with a
    recorded reason — when the wheels are absent or the data is too thin."""
    ml = behaviour_model.available()
    ml_reason = "ml wheels absent (numpy/lightgbm/scikit-learn)"

    X, y, _w = behaviour_examples(events, labels)
    scorer = LayerReport("behaviour_scorer", n_examples=len(X))
    if not ml:
        scorer.reason = ml_reason
    elif not X:
        scorer.reason = "no labelled examples"
    elif len(set(y)) < 2:
        scorer.reason = "need both classes (misuse and clean) to calibrate"
    else:
        m = BehaviourScorer()
        m.train(X, y)
        scorer.artifact = os.path.join(model_dir, BEHAVIOUR_GBDT)
        m.save(scorer.artifact)
        scorer.trained = True
    report.add(scorer)

    pop = behaviour_population(events)
    floor = LayerReport("behaviour_floor", n_examples=len(pop))
    if not ml:
        floor.reason = ml_reason
    elif not pop:
        floor.reason = "no observations to fit the floor"
    else:
        f = IsolationFloor()
        f.train(pop)
        floor.artifact = os.path.join(model_dir, BEHAVIOUR_FLOOR)
        f.save(floor.artifact)
        floor.trained = True
    report.add(floor)


def _train_graph(events: List[Event], labels: LabelSet, model_dir: str, report: TrainReport) -> None:
    """Fit the GraphSAGE layer (§13) on the entity graph with the settled-fraud /
    settled-clean seeds. Needs both a fraud and a clean seed to calibrate — the
    same guard the model enforces — else it is a recorded no-op."""
    graph, seeds = graph_dataset(events, labels)
    rep = LayerReport("graph_sage", n_examples=len(seeds))
    classes = {int(round(v)) for v in seeds.values()}
    if not graph_model.available():
        rep.reason = "ml wheels absent (torch/torch-geometric/scikit-learn)"
    elif not seeds:
        rep.reason = "no seeded token nodes"
    elif len(classes) < 2:
        rep.reason = "need both a fraud seed and a clean seed to calibrate"
    else:
        m = GraphSAGEModel()
        m.train(graph, seeds)
        rep.artifact = os.path.join(model_dir, GRAPH_SAGE)
        m.save(rep.artifact)
        rep.trained = True
    report.add(rep)


def _note_intent(report: TrainReport) -> None:
    """Intent alignment (§12) is a sealed-embedding comparison against a fixed
    product embedder, not a supervised model — there are no weights the settled
    labels could fit. Recorded here so a run's report is complete, not silent."""
    report.add(LayerReport(
        "intent", trained=False,
        reason="not supervised — sealed-embedding comparison (§12), nothing to fit",
    ))


def retrain(events: Iterable[Event], model_dir: str, labels: Optional[LabelSet] = None) -> TrainReport:
    """Replay the events, fit every learned layer that has the wheels and the data,
    and persist each under the filename its engine loads. Returns a TrainReport of
    exactly what was fitted. Labels are read from the events (the labeler's
    outcome.labeled) unless supplied — this module never manufactures one."""
    events = list(events)  # replayed several times; a generator would exhaust
    if labels is None:
        labels = LabelSet.from_events(events)
    os.makedirs(model_dir, exist_ok=True)

    report = TrainReport(model_dir=model_dir, n_events=len(events), n_labels=len(labels))
    _train_behaviour(events, labels, model_dir, report)
    _train_graph(events, labels, model_dir, report)
    _note_intent(report)
    return report


def load_events(path: str) -> List[Event]:
    """Read a JSONL event log (one event JSON per line) into Events — the corpus a
    manual retrain runs over."""
    out: List[Event] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(Event.from_json(line))
    return out


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description="AgentShield offline retrain (§6, §11, §13)")
    ap.add_argument("events", help="path to a JSONL event log (one Event per line)")
    ap.add_argument("model_dir", help="directory to write the model artifacts into")
    args = ap.parse_args(argv)

    report = retrain(load_events(args.events), args.model_dir)
    print(report, flush=True)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

