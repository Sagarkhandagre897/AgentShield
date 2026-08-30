"""structural — the model-free floor and label propagation (System Design §13).

    "Beneath the learned embedding sits a model-free floor ... a node with no
     learned embedding yet still has a structural signal — degree, fan-out, count
     of shared attributes, size of the component it sits in. It is meaningful from
     the first sighting, so a brand-new node is never simply blank."

    "It computes inductive node embeddings, runs cluster/ring detection over them,
     and uses label propagation from the few settled-fraud seeds to spread
     suspicion through tightly connected structure."

Two pure-stdlib pieces live here — the ones that hold the figure up before, and
beneath, the learned GraphSAGE model:

  structural_floor  — degree, fan-out, shared attributes, component size, squashed
                      and folded by noisy-OR into a [0, 1] floor. A lower bound: a
                      fresh, sparsely connected node reads low; a hub tangled into a
                      large component with many shared handles reads high.
  propagate         — label propagation from settled-fraud seeds, spreading
                      suspicion through the adjacency. No embeddings needed — it
                      operates on the structure directly, so it runs torch-free.

Labels come only from settled outcomes (a dispute is misuse); the seeds are those
disputes, never our own past decisions.
"""

from __future__ import annotations

import math
from typing import Dict, List, Mapping

from .graph import ATTRIBUTE_TYPES, Graph, node_type

# Squash scales — how much of each structural signal reads as "a lot". Chosen so
# an ordinary node (a token tied to its agent, merchant and device in a small
# component) floors low, and only a genuine hub, mule fan-out, heavily shared
# handle or large tangled component lifts it toward 1.
_DEGREE_SCALE = 30.0
_FANOUT_SCALE = 15.0
_SHARED_SCALE = 8.0
_COMPONENT_SCALE = 40.0


def _squash(raw: float, scale: float) -> float:
    """Non-negative magnitude → [0, 1), monotone and saturating (shared with the
    behaviour engine's Layer 1 in spirit)."""
    if raw <= 0:
        return 0.0
    return 1.0 - math.exp(-raw / scale)


def fan_out(graph: Graph, node: str) -> int:
    """The largest number of distinct neighbours of any single type — an agent
    touching twenty tokens, a device shared by twenty agents. This is the mule /
    ring fan-out that a single-principal view cannot see."""
    by_type: Dict[str, int] = {}
    for nbr in graph.neighbours(node):
        by_type[node_type(nbr)] = by_type.get(node_type(nbr), 0) + 1
    return max(by_type.values(), default=0)


def shared_attributes(graph: Graph, node: str) -> int:
    """How many other entities share an attribute node (device, policy) with this
    one — the co-occurrence that ties a ring together. Summed over the node's
    attribute neighbours: each contributes (its degree − 1) others that share it."""
    total = 0
    for nbr in graph.neighbours(node):
        if node_type(nbr) in ATTRIBUTE_TYPES:
            total += max(0, graph.degree(nbr) - 1)
    return total


def structural_signals(graph: Graph, node: str, component_size: int) -> Dict[str, float]:
    """The raw structural signals for one node — degree, fan-out, shared
    attributes, component size. Returned raw so a caller can inspect them; the
    floor squashes and folds them."""
    return {
        "degree": float(graph.degree(node)),
        "fan_out": float(fan_out(graph, node)),
        "shared_attributes": float(shared_attributes(graph, node)),
        "component_size": float(component_size),
    }


def structural_floor(signals: Mapping[str, float]) -> float:
    """Fold the structural signals into a [0, 1] floor by noisy-OR: any one strong
    structural signal lifts it, and a sparsely connected node barely moves it. A
    genuine lower bound on network_risk — the learned model may raise it, never
    suppress it."""
    devs = [
        _squash(signals.get("degree", 0.0), _DEGREE_SCALE),
        _squash(signals.get("fan_out", 0.0), _FANOUT_SCALE),
        _squash(signals.get("shared_attributes", 0.0), _SHARED_SCALE),
        _squash(signals.get("component_size", 0.0), _COMPONENT_SCALE),
    ]
    prod = 1.0
    for d in devs:
        prod *= 1.0 - d
    return 1.0 - prod


def propagate(graph: Graph, seeds: Mapping[str, float], iterations: int = 30, damping: float = 0.85) -> Dict[str, float]:
    """Label propagation from the settled-fraud seeds. Seed nodes are clamped to
    their label; every other node relaxes toward the damped mean of its
    neighbours. Suspicion spreads through tightly connected structure and
    attenuates with distance, so a node one hop from a ring lights up and a
    stranger stays near zero. Pure stdlib — no embeddings required."""
    score: Dict[str, float] = {n: float(seeds.get(n, 0.0)) for n in graph.nodes()}
    if not seeds:
        return score
    for _ in range(iterations):
        nxt: Dict[str, float] = {}
        for n in graph.nodes():
            if n in seeds:
                nxt[n] = float(seeds[n])  # clamped to the settled label
                continue
            nbrs = graph.neighbours(n)
            if not nbrs:
                nxt[n] = 0.0
                continue
            nxt[n] = damping * (sum(score[m] for m in nbrs) / len(nbrs))
        score = nxt
    return {n: max(0.0, min(1.0, v)) for n, v in score.items()}
