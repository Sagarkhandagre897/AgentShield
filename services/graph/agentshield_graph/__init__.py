"""agentshield_graph — the graph engine (System Design §13).

Some risk is only visible in the shape of the neighbourhood — collusion rings,
mule fan-out, a fresh agent sharing a device with twenty settled-bad tokens. The
engine learns that neighbourhood off the clock and leaves one calibrated
network_risk per node on the shelf for the request to read by key:

    graph       — the heterogeneous, time-aware entity graph, built from the gate
                  events; every node keyed on a stable id. Pure stdlib.
    structural  — the model-free floor (degree, fan-out, shared attributes,
                  component size) and label propagation from settled-fraud seeds.
                  Pure stdlib; meaningful from a node's first sighting.
    model       — an inductive GraphSAGE encoder → a calibrated per-node risk,
                  trained semi-supervised. torch / torch-geometric imported lazily.
    engine      — fold edges, remember disputes as seeds, and deposit one
                  network_risk per keyed node on a batch cadence.

The learned model sharpens what the structure suggests; it never suppresses it —
network_risk is the max of the floor, the propagation and the learned figure. The
request does no graph walk; it only ever reads the number by key.
"""

from .engine import Deposit, GraphEngine, load_model
from .graph import (
    Graph,
    bare_id,
    node_id,
    node_type,
)
from .model import GraphSAGEModel, available, combine_risk, node_feature_matrix
from .structural import fan_out, propagate, shared_attributes, structural_floor, structural_signals

__all__ = [
    "Deposit",
    "GraphEngine",
    "load_model",
    "Graph",
    "bare_id",
    "node_id",
    "node_type",
    "GraphSAGEModel",
    "available",
    "combine_risk",
    "node_feature_matrix",
    "fan_out",
    "propagate",
    "shared_attributes",
    "structural_floor",
    "structural_signals",
]
