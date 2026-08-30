"""model — the learned graph layer and the combination rule (System Design §13).

    "The architecture is inductive (GraphSAGE-style, via PyG or DGL): a node that
     appeared this morning gets an embedding from its neighbourhood without a full
     retrain. The supervised signal sharpens what the structure already suggests —
     it is not the only thing holding the figure up."

Layer 2 here is the inductive GraphSAGE model: it embeds every node from its
neighbourhood and turns the embedding into a calibrated per-node risk, trained
semi-supervised from the few settled-fraud seeds. torch / torch-geometric /
scikit-learn are imported lazily, so the structural floor and label propagation
(``structural.py``, pure stdlib) run and are tested without the wheels — the
engine falls back to the floor when the learned model is absent, exactly the
cold-start posture §13 describes.

The combination rule keeps the floor a genuine lower bound: network_risk is the
max of the structural floor, the propagated suspicion, and (when present) the
learned figure. A learned model can raise the number, never suppress a structure
that already looks like a ring.
"""

from __future__ import annotations

import math
import pickle
from typing import Dict, List, Mapping, Optional, Sequence

from .graph import Graph, node_type
from .structural import structural_signals

try:
    import numpy as np
    import torch
    import torch.nn.functional as F
    from sklearn.isotonic import IsotonicRegression
    from torch_geometric.nn import SAGEConv

    _HAVE_ML = True
except Exception:  # pragma: no cover - exercised only where the wheels are absent
    _HAVE_ML = False

# The node feature order the learned model reads — the structural signals plus a
# volume term. Stable so a persisted model lines up with a freshly built graph.
FEATURE_NAMES = ["degree", "fan_out", "shared_attributes", "component_size", "log_degree"]


def available() -> bool:
    """True when torch / torch-geometric / scikit-learn are importable — the
    learned GraphSAGE layer is live. When False, network_risk rests on the
    structural floor and label propagation alone."""
    return _HAVE_ML


def _require_ml() -> None:
    if not _HAVE_ML:
        raise RuntimeError("torch/torch-geometric/scikit-learn not installed; `pip install -e services/graph`")


def combine_risk(structural: float, propagated: float, learned: Optional[float]) -> float:
    """The one figure the engine deposits per node. The structural floor and the
    propagated suspicion are lower bounds the learned model may raise; before the
    model exists, they hold the figure up on their own. Clamped to [0, 1]."""
    base = max(structural, propagated)
    if learned is not None:
        base = max(base, learned)
    return max(0.0, min(1.0, base))


def _feature_row(graph: Graph, node: str, component_size: int) -> List[float]:
    s = structural_signals(graph, node, component_size)
    return [
        s["degree"],
        s["fan_out"],
        s["shared_attributes"],
        s["component_size"],
        math.log1p(s["degree"]),
    ]


def node_feature_matrix(graph: Graph) -> tuple[List[str], List[List[float]]]:
    """The ordered node list and their feature rows — the inductive input the SAGE
    model reads. Order is stable within a build so edges line up with rows."""
    sizes = graph.component_sizes()
    order = list(graph.nodes())
    rows = [_feature_row(graph, n, sizes.get(n, 1)) for n in order]
    return order, rows


class GraphSAGEModel:
    """Layer 2: a two-layer inductive GraphSAGE encoder with a linear head,
    trained semi-supervised on the settled-fraud seeds, its output turned into a
    calibrated probability by isotonic regression. Unfitted, risk() returns an
    empty map and the engine leans on the floor."""

    def __init__(self, hidden: int = 32, epochs: int = 200, lr: float = 0.01) -> None:
        self._net = None
        self._iso = None
        self._in_dim = len(FEATURE_NAMES)
        self._hidden = hidden
        self._epochs = epochs
        self._lr = lr

    @property
    def fitted(self) -> bool:
        return self._net is not None

    def _build_net(self):
        class _SAGE(torch.nn.Module):
            def __init__(self, in_dim, hidden):
                super().__init__()
                self.c1 = SAGEConv(in_dim, hidden)
                self.c2 = SAGEConv(hidden, hidden)
                self.head = torch.nn.Linear(hidden, 1)

            def forward(self, x, edge_index):
                h = F.relu(self.c1(x, edge_index))
                h = F.relu(self.c2(h, edge_index))
                return self.head(h).squeeze(-1)

        return _SAGE(self._in_dim, self._hidden)

    def _edge_index(self, order: Sequence[str], graph: Graph):
        idx = {n: i for i, n in enumerate(order)}
        src, dst = [], []
        for a in order:
            for b in graph.neighbours(a):
                if b in idx:  # undirected → both directions
                    src.append(idx[a])
                    dst.append(idx[b])
        if not src:  # no edges → a self-loop keeps SAGEConv well-defined
            src, dst = list(range(len(order))), list(range(len(order)))
        return torch.tensor([src, dst], dtype=torch.long)

    def train(self, graph: Graph, seeds: Mapping[str, float]) -> None:
        """Fit the encoder on the graph with a masked loss over the seeded nodes
        (settled outcomes), then calibrate the seeded scores. Needs both a fraud
        seed and a clean seed to calibrate against."""
        _require_ml()
        order, rows = node_feature_matrix(graph)
        if not order:
            raise ValueError("empty graph")
        labelled = {n: v for n, v in seeds.items() if n in set(order)}
        if len(set(round(v) for v in labelled.values())) < 2:
            raise ValueError("need both a fraud seed and a clean seed to calibrate")

        x = torch.tensor(rows, dtype=torch.float)
        edge_index = self._edge_index(order, graph)
        idx = {n: i for i, n in enumerate(order)}
        mask = torch.tensor([idx[n] for n in labelled], dtype=torch.long)
        y = torch.tensor([float(labelled[n]) for n in labelled], dtype=torch.float)

        self._net = self._build_net()
        opt = torch.optim.Adam(self._net.parameters(), lr=self._lr)
        self._net.train()
        for _ in range(self._epochs):
            opt.zero_grad()
            out = self._net(x, edge_index)
            loss = F.binary_cross_entropy_with_logits(out[mask], y)
            loss.backward()
            opt.step()

        self._net.eval()
        with torch.no_grad():
            raw = torch.sigmoid(self._net(x, edge_index)[mask]).cpu().numpy()
        self._iso = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
        self._iso.fit(raw, y.cpu().numpy())

    def risk(self, graph: Graph) -> Dict[str, float]:
        """Inductive forward over the current graph → a calibrated risk per node.
        Empty when unfitted."""
        if not self.fitted:
            return {}
        order, rows = node_feature_matrix(graph)
        if not order:
            return {}
        x = torch.tensor(rows, dtype=torch.float)
        edge_index = self._edge_index(order, graph)
        self._net.eval()
        with torch.no_grad():
            raw = torch.sigmoid(self._net(x, edge_index)).cpu().numpy()
        cal = self._iso.predict(raw)
        return {n: float(cal[i]) for i, n in enumerate(order)}

    def save(self, path: str) -> None:
        with open(path, "wb") as f:
            pickle.dump({"state": self._net.state_dict() if self._net else None,
                         "iso": self._iso, "hidden": self._hidden}, f)

    def load(self, path: str) -> None:
        _require_ml()
        with open(path, "rb") as f:
            blob = pickle.load(f)
        self._hidden = blob["hidden"]
        self._iso = blob["iso"]
        if blob["state"] is not None:
            self._net = self._build_net()
            self._net.load_state_dict(blob["state"])
            self._net.eval()
