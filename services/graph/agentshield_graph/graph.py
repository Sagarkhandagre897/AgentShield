"""graph — the heterogeneous, time-aware entity graph (System Design §13).

    "Nodes are the stable entities — customer, token, merchant, agent, policy,
     device node — and edges are the debits and shared attributes that tie them.
     Edges carry time, so the model sees a ring forming rather than a static
     snapshot."

This module is the graph itself, built incrementally from the gate events — pure
stdlib. Every node is keyed on a stable identifier and namespaced by type
(``agent:a1``), so an agent and a merchant that happen to share a raw id are two
distinct nodes:

    A MIS-KEY HERE IS A MIS-IDENTIFICATION — a figure keyed on a reassignable
    handle (a VPA, a UMN) would silently attach one entity's whole neighbourhood
    to another. Nodes are keyed on the stable id, never on the payer-port handle.

The learned embedding and the structural floor both read this graph; neither
writes back to the request path.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set, Tuple

# Node types (§13). Attribute nodes (device, policy) are the shared handles that
# tie principals together — they are where rings and mule fan-out become visible.
NODE_CUSTOMER = "customer"
NODE_TOKEN = "token"
NODE_MERCHANT = "merchant"
NODE_AGENT = "agent"
NODE_POLICY = "policy"
NODE_DEVICE = "device"
ATTRIBUTE_TYPES = frozenset({NODE_DEVICE, NODE_POLICY})


def node_id(node_type: str, raw: str) -> str:
    """A namespaced, stable node key. Two entities of different types never
    collide, and the raw id is expected to be the stable identifier (never a
    reassignable VPA/UMN)."""
    return f"{node_type}:{raw}"


def node_type(node: str) -> str:
    return node.split(":", 1)[0]


def bare_id(node: str) -> str:
    """The raw identifier without the type prefix — the key the request reads a
    figure by (behaviour keys an agent on the bare id, so a network_risk for that
    agent must land on the same row)."""
    return node.split(":", 1)[1] if ":" in node else node


@dataclass
class Graph:
    """An undirected multigraph over the stable entities. Fixed structures: an
    adjacency map, the first- and last-seen timestamps per node, and the set of
    edge kinds seen between a pair (so time-aware without keeping every edge)."""

    _adj: Dict[str, Set[str]] = field(default_factory=dict)
    _first_seen: Dict[str, int] = field(default_factory=dict)
    _last_seen: Dict[str, int] = field(default_factory=dict)
    _edge_ts: Dict[Tuple[str, str], int] = field(default_factory=dict)

    def _touch(self, node: str, ts: int) -> None:
        if node not in self._adj:
            self._adj[node] = set()
            self._first_seen[node] = ts
        self._last_seen[node] = max(self._last_seen.get(node, ts), ts)

    def add_edge(self, a: str, b: str, ts: int = 0) -> None:
        """Add (or refresh the time on) an undirected edge. Self-edges are
        dropped — a node ties to others, never to itself."""
        if a == b:
            return
        self._touch(a, ts)
        self._touch(b, ts)
        self._adj[a].add(b)
        self._adj[b].add(a)
        self._edge_ts[(a, b) if a < b else (b, a)] = ts

    def neighbours(self, node: str) -> Set[str]:
        return self._adj.get(node, set())

    def degree(self, node: str) -> int:
        return len(self._adj.get(node, ()))

    def nodes(self) -> Iterable[str]:
        return self._adj.keys()

    def first_seen(self, node: str) -> int:
        return self._first_seen.get(node, 0)

    def edge_time(self, a: str, b: str) -> Optional[int]:
        return self._edge_ts.get((a, b) if a < b else (b, a))

    def components(self) -> Dict[str, int]:
        """Connected-component id per node, by union-find. Component *size* is a
        structural signal: a node in a large tangled component is more suspicious
        than an isolated pair."""
        parent: Dict[str, str] = {n: n for n in self._adj}

        def find(x: str) -> str:
            root = x
            while parent[root] != root:
                root = parent[root]
            while parent[x] != root:  # path compression
                parent[x], x = root, parent[x]
            return root

        for a, nbrs in self._adj.items():
            for b in nbrs:
                ra, rb = find(a), find(b)
                if ra != rb:
                    parent[ra] = rb
        return {n: find(n) for n in self._adj}

    def component_sizes(self) -> Dict[str, int]:
        comp = self.components()
        sizes: Dict[str, int] = {}
        for root in comp.values():
            sizes[root] = sizes.get(root, 0) + 1
        return {n: sizes[root] for n, root in comp.items()}

    def __len__(self) -> int:
        return len(self._adj)
