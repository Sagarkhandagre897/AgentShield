"""agentshield_trainer — the offline retrain loop (System Design §6, §11, §13).

The learning loop distilled to one job: take the settled labels the labeler put
on outcomes.v1, replay the historical events through the SAME feature extraction
the online engines use, pair each labeled entity with its features as they stood
before the outcome, and fit + persist the learned layers — the behaviour GBDT and
its isolation-forest floor, and the graph's GraphSAGE model. It never runs on the
clock; it produces the artifacts the engines load and serve.

Two invariants carry through from the design:

  - Labels come only from settled outcomes (§6). This module never manufactures a
    label; it consumes exactly what the labeler emitted, so "no complaint arrived"
    and "we allowed it" can never leak in.
  - Train/serve parity. The feature vectors are built by reusing the engines' own
    BaselineBank and entity Graph, so what the trainer fits on is byte-for-byte
    what the engine will compute online — no second, drifting feature definition.
"""
