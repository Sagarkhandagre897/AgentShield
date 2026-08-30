"""Structural-layer tests (System Design §13) — pure stdlib.

They pin the model-free pieces that hold network_risk up before, and beneath, the
learned model: fan-out and shared-attribute counts read the ring/mule shape, the
structural floor is low for a fresh sparse node and high for a tangled hub, and
label propagation spreads a settled-fraud seed to its neighbourhood while a
stranger stays at zero. Runnable with pytest or directly.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agentshield_graph.graph import (  # noqa: E402
    NODE_AGENT,
    NODE_DEVICE,
    NODE_TOKEN,
    Graph,
    node_id,
)
from agentshield_graph.structural import (  # noqa: E402
    fan_out,
    propagate,
    shared_attributes,
    structural_floor,
)


def _fan(g, agent, n):
    a = node_id(NODE_AGENT, agent)
    for i in range(n):
        g.add_edge(node_id(NODE_TOKEN, f"{agent}_t{i}"), a, 1)
    return a


def test_fan_out_counts_distinct_neighbours_of_a_type():
    g = Graph()
    a = _fan(g, "a1", 5)
    assert fan_out(g, a) == 5  # one agent, five tokens


def test_shared_attributes_see_co_occurrence():
    g = Graph()
    d = node_id(NODE_DEVICE, "d1")
    for i in range(3):  # three tokens share one device
        g.add_edge(node_id(NODE_TOKEN, f"t{i}"), d, 1)
    # Each token shares the device with the two others.
    assert shared_attributes(g, node_id(NODE_TOKEN, "t0")) == 2


def test_floor_is_low_for_fresh_and_high_for_a_hub():
    fresh = structural_floor({"degree": 1, "fan_out": 1, "shared_attributes": 0, "component_size": 2})
    hub = structural_floor({"degree": 20, "fan_out": 20, "shared_attributes": 10, "component_size": 50})
    assert fresh < 0.3
    assert hub > 0.7
    assert hub > fresh


def test_propagation_spreads_from_a_seed_and_attenuates():
    g = Graph()
    t1, d, t2 = node_id(NODE_TOKEN, "t1"), node_id(NODE_DEVICE, "d"), node_id(NODE_TOKEN, "t2")
    g.add_edge(t1, d, 1)
    g.add_edge(t2, d, 1)  # t2 shares a device with the fraud token
    g.add_edge(node_id(NODE_TOKEN, "far"), node_id(NODE_AGENT, "far_a"), 1)  # separate component

    scores = propagate(g, {t1: 1.0})
    assert scores[t2] > 0.0  # suspicion reached the neighbour
    assert scores[node_id(NODE_TOKEN, "far")] == 0.0  # never reached the stranger
    assert scores[t2] < 1.0  # attenuated with distance, never clamped up


def test_no_seeds_means_no_suspicion():
    g = Graph()
    g.add_edge(node_id(NODE_TOKEN, "t1"), node_id(NODE_AGENT, "a1"), 1)
    assert all(v == 0.0 for v in propagate(g, {}).values())


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
