"""engine — the graph engine's async runner (System Design §13).

    "The graph is built, the embeddings are computed and the propagation runs
     entirely in the asynchronous plane; the engine deposits one calibrated
     network_risk per node with a computed_at stamp. The request does no graph
     walk — it reads the node's figure by key ... The graph rebuilds and re-embeds
     on a batch cadence; the request only ever reads."

The worker folds every gate event into the entity graph (edges are the debits and
the shared attributes), remembers the settled-fraud seeds (a dispute is misuse),
and on a batch cadence recomputes each node's figure — the structural floor, the
propagated suspicion, and, when the learned model is loaded, the GraphSAGE
figure — combining them so the floor stays a lower bound. It deposits one
network_risk per keyed node through the shared publisher; the Go materialiser (the
single writer) merges it onto the row the request reads by key.

Every node is keyed on its stable id; the figure is deposited under the bare id
(the key the request already reads an agent or token by). A mis-key here is a
mis-identification — never key on a reassignable VPA/UMN.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional

from .graph import (
    NODE_AGENT,
    NODE_CUSTOMER,
    NODE_DEVICE,
    NODE_MERCHANT,
    NODE_POLICY,
    NODE_TOKEN,
    Graph,
    bare_id,
    node_id,
    node_type,
)
from .model import GraphSAGEModel, combine_risk
from .structural import propagate, structural_floor, structural_signals

try:  # the shared wire contract; bootstrap onto sys.path when run in-tree
    from agentshield_shared.schema import (
        EVENT_DECISION_MADE,
        EVENT_PAYMENT_CAPTURED,
        EVENT_PAYMENT_DISPUTED,
        EVENT_PAYMENT_FAILED,
        EVENT_TOKEN_CANCELLED,
        EVENT_TOKEN_CONFIRMED,
        Event,
    )
except ModuleNotFoundError:  # pragma: no cover
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))
    from agentshield_shared.schema import (
        EVENT_DECISION_MADE,
        EVENT_PAYMENT_CAPTURED,
        EVENT_PAYMENT_DISPUTED,
        EVENT_PAYMENT_FAILED,
        EVENT_TOKEN_CANCELLED,
        EVENT_TOKEN_CONFIRMED,
        Event,
    )

# Payload keys that name an entity to tie to the token. device_id / policy_id are
# graph-local shared attributes (optional, not yet on every gate event).
PAYLOAD_AGENT_ID = "agent_id"
PAYLOAD_CUSTOMER_ID = "customer_id"
PAYLOAD_MERCHANT_ID = "merchant_id"
PAYLOAD_DEVICE_ID = "device_id"
PAYLOAD_POLICY_ID = "policy_id"

_ENTITY_KEYS = (
    (NODE_AGENT, PAYLOAD_AGENT_ID),
    (NODE_CUSTOMER, PAYLOAD_CUSTOMER_ID),
    (NODE_MERCHANT, PAYLOAD_MERCHANT_ID),
    (NODE_DEVICE, PAYLOAD_DEVICE_ID),
    (NODE_POLICY, PAYLOAD_POLICY_ID),
)

# The engine learns edges from every gate event; a dispute is the settled-fraud
# seed. Node types the request reads a figure by (attribute nodes are structure
# only — nothing reads a device or policy by key).
_EDGE_EVENTS = {
    EVENT_DECISION_MADE,
    EVENT_PAYMENT_CAPTURED,
    EVENT_PAYMENT_FAILED,
    EVENT_PAYMENT_DISPUTED,
    EVENT_TOKEN_CONFIRMED,
    EVENT_TOKEN_CANCELLED,
}
DEPOSIT_TYPES = frozenset({NODE_AGENT, NODE_CUSTOMER, NODE_TOKEN, NODE_MERCHANT})
GROUP = "graph-engine"
TOPICS = ("evaluations.v1", "payments.v1", "tokens.v1")


@dataclass
class Deposit:
    """One node's network_risk — ready for deposit_network()."""

    feature_key: str
    risk: float
    computed_at: int
    token_id: str


class GraphEngine:
    """The entity graph, the settled-fraud seeds and the batch scorer. Holds the
    structural floor + label propagation (always) and, when the wheels and a
    trained model are present, the learned GraphSAGE figure (which can only raise
    the floor, never suppress it)."""

    def __init__(self, model: Optional[GraphSAGEModel] = None):
        self.graph = Graph()
        self.model = model
        self._seeds: Dict[str, float] = {}
        self._latest_ts = 0

    def observe(self, ev: Event) -> None:
        """Fold one gate event into the graph and update the fraud seeds. No
        deposit here — the graph engine deposits on a batch tick, not per event."""
        if ev.type not in _EDGE_EVENTS or not ev.token_id:
            return
        self._latest_ts = max(self._latest_ts, ev.occurred_at)
        token = node_id(NODE_TOKEN, ev.token_id)
        for ntype, pkey in _ENTITY_KEYS:
            raw = ev.payload.get(pkey)
            if raw:  # an edge is what brings a node into the graph
                self.graph.add_edge(token, node_id(ntype, str(raw)), ev.occurred_at)
        if ev.type == EVENT_PAYMENT_DISPUTED:  # a settled dispute is misuse — a seed
            self._seeds[token] = 1.0

    def deposit_all(self, computed_at: Optional[int] = None) -> List[Deposit]:
        """Recompute every keyed node's figure and return the deposits: the
        structural floor and the propagated suspicion (always), raised by the
        learned figure when the model is loaded."""
        at = computed_at if computed_at is not None else self._latest_ts
        sizes = self.graph.component_sizes()
        propagated = propagate(self.graph, self._seeds)
        learned = self.model.risk(self.graph) if (self.model and self.model.fitted) else {}

        deposits: List[Deposit] = []
        for node in self.graph.nodes():
            if node_type(node) not in DEPOSIT_TYPES:
                continue
            floor = structural_floor(structural_signals(self.graph, node, sizes.get(node, 1)))
            risk = combine_risk(floor, propagated.get(node, 0.0), learned.get(node))
            token_id = bare_id(node) if node_type(node) == NODE_TOKEN else ""
            deposits.append(Deposit(bare_id(node), risk, at, token_id))
        return deposits

    def run(self, seeds: str, group: str = GROUP, batch: int = 128) -> None:
        """Consume every gate event forever, folding edges; every `batch` events
        recompute and deposit each node's network_risk. The producer is flushed
        before the offsets commit — at-least-once, and the Go materialiser dedupes
        on the deposit's stable id (type+key+computed_at)."""
        from agentshield_shared.bus import DepositPublisher, EventConsumer  # lazy: needs the kafka client

        pub = DepositPublisher(seeds)
        consumer = EventConsumer(seeds, group, list(TOPICS))
        n = 0

        def handle(ev: Event) -> None:
            nonlocal n
            self.observe(ev)
            n += 1
            if n % batch == 0:
                for dep in self.deposit_all():
                    pub.deposit_network(dep.feature_key, dep.risk, dep.computed_at, dep.token_id)
                pub.flush(10.0)

        try:
            consumer.run(handle)
        finally:
            consumer.close()


def load_model(model_dir: str) -> Optional[GraphSAGEModel]:
    """Load a trained GraphSAGE model if the wheels and the file are present;
    otherwise None and the engine runs on the structural floor + propagation.
    Training is an offline batch job (§13), not this worker's concern."""
    from . import model as _model

    if not _model.available():
        return None
    path = os.path.join(model_dir, "graph_sage.pkl")
    if not os.path.exists(path):
        return None
    m = GraphSAGEModel()
    m.load(path)
    return m


def main() -> None:
    seeds = os.environ.get("KAFKA_SEEDS")
    if not seeds:
        raise SystemExit("KAFKA_SEEDS is required (e.g. localhost:19092)")
    model = None
    model_dir = os.environ.get("GRAPH_MODEL_DIR")
    if model_dir:
        model = load_model(model_dir)
    print(f"graph-engine: consuming {list(TOPICS)} from {seeds} "
          f"(model={'on' if model else 'structural-only'})", flush=True)
    GraphEngine(model).run(seeds)


if __name__ == "__main__":  # pragma: no cover
    main()
