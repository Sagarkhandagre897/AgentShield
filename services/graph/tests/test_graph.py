"""Graph tests (System Design §13) — pure stdlib.

They pin the structure the whole engine reads: nodes are namespaced by type so a
mis-key cannot merge two entities, edges are undirected and self-edges dropped,
and connected components (the tangle a ring sits in) are counted correctly.
Runnable with pytest or directly.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agentshield_graph.graph import (  # noqa: E402
    NODE_AGENT,
    NODE_MERCHANT,
    NODE_TOKEN,
    Graph,
    bare_id,
    node_id,
    node_type,
)


def test_node_key_namespaces_by_type():
    a = node_id(NODE_AGENT, "x1")
    m = node_id(NODE_MERCHANT, "x1")
    assert a != m  # same raw id, different entities — never merged
    assert node_type(a) == NODE_AGENT and bare_id(a) == "x1"


def test_edges_are_undirected_and_self_edges_dropped():
    g = Graph()
    t, a = node_id(NODE_TOKEN, "t1"), node_id(NODE_AGENT, "a1")
    g.add_edge(t, a, 100)
    g.add_edge(a, a, 100)  # self-edge: dropped
    assert a in g.neighbours(t) and t in g.neighbours(a)
    assert g.degree(a) == 1 and a not in g.neighbours(a)


def test_components_count_the_tangle():
    g = Graph()
    # Two separate pairs → two components of size 2.
    g.add_edge(node_id(NODE_TOKEN, "t1"), node_id(NODE_AGENT, "a1"), 1)
    g.add_edge(node_id(NODE_TOKEN, "t2"), node_id(NODE_AGENT, "a2"), 1)
    sizes = g.component_sizes()
    assert sizes[node_id(NODE_TOKEN, "t1")] == 2
    assert len(set(g.components().values())) == 2
    # Tie them through a shared merchant → one component of four.
    g.add_edge(node_id(NODE_TOKEN, "t1"), node_id(NODE_MERCHANT, "m"), 2)
    g.add_edge(node_id(NODE_TOKEN, "t2"), node_id(NODE_MERCHANT, "m"), 2)
    assert g.component_sizes()[node_id(NODE_MERCHANT, "m")] == 5


def test_first_seen_and_edge_time_are_kept():
    g = Graph()
    t, a = node_id(NODE_TOKEN, "t1"), node_id(NODE_AGENT, "a1")
    g.add_edge(t, a, 500)
    assert g.first_seen(t) == 500
    assert g.edge_time(a, t) == 500  # order-independent


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
