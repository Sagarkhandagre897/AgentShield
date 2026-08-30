"""Learned-layer tests (System Design §13, Layer 2) — need torch + torch-geometric.

They pin what the learned model must do: fit the settled-fraud/clean seeds so a
fraud-connected node scores above a clean one, emit a calibrated per-node
probability in [0, 1], and predict identically once persisted and reloaded.
Skipped whole when torch / torch-geometric are absent — the structural floor and
propagation stand alone there. Runnable with pytest or directly, with a clean
no-op skip in either mode when the wheels are missing.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agentshield_graph.graph import NODE_AGENT, NODE_DEVICE, NODE_TOKEN, Graph, node_id  # noqa: E402
from agentshield_graph.model import GraphSAGEModel, available  # noqa: E402

_HAVE = available()

try:
    import pytest  # noqa: E402

    pytestmark = pytest.mark.skipif(not _HAVE, reason="torch/torch-geometric not installed")
except ImportError:  # pragma: no cover
    pytest = None


def _seeded_graph():
    """A fraud cluster (tokens sharing a device with a disputed token) and a clean
    cluster, with one settled seed each."""
    g = Graph()
    fraud = node_id(NODE_TOKEN, "f0")
    dev = node_id(NODE_DEVICE, "d")
    for i in range(4):
        g.add_edge(node_id(NODE_TOKEN, f"f{i}"), dev, 1)
        g.add_edge(node_id(NODE_TOKEN, f"f{i}"), node_id(NODE_AGENT, f"fa{i}"), 1)
    clean = node_id(NODE_TOKEN, "c0")
    for i in range(4):
        g.add_edge(node_id(NODE_TOKEN, f"c{i}"), node_id(NODE_AGENT, "ca"), 1)
    return g, {fraud: 1.0, clean: 0.0}, fraud, clean


def test_learned_model_separates_fraud_from_clean():
    g, seeds, fraud, clean = _seeded_graph()
    m = GraphSAGEModel(epochs=150)
    assert m.risk(g) == {}  # unfitted → empty, engine leans on the floor
    m.train(g, seeds)
    assert m.fitted
    risk = m.risk(g)
    assert set(risk) == set(g.nodes())
    assert all(0.0 <= v <= 1.0 for v in risk.values())  # a calibrated probability
    assert risk[fraud] > risk[clean]


def test_training_needs_both_a_fraud_and_a_clean_seed():
    g, _, fraud, _ = _seeded_graph()
    m = GraphSAGEModel(epochs=10)
    try:
        m.train(g, {fraud: 1.0})  # only a fraud seed
        raised = False
    except ValueError:
        raised = True
    assert raised, "single-class seeds must be rejected"


def test_persisted_model_predicts_identically():
    import tempfile

    g, seeds, fraud, _ = _seeded_graph()
    m = GraphSAGEModel(epochs=150)
    m.train(g, seeds)
    before = m.risk(g)[fraud]
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "graph_sage.pkl")
        m.save(p)
        m2 = GraphSAGEModel()
        m2.load(p)
        assert abs(m2.risk(g)[fraud] - before) < 1e-5


if __name__ == "__main__":
    if not _HAVE:
        print("skipped (torch/torch-geometric not installed)")
        raise SystemExit(0)
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
