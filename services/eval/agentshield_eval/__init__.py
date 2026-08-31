"""agentshield_eval — the Phase-7 eval core.

The live driver (``services/driver``) replays a generated Scenario against the real
AgentShield and writes a ``results.json``: the generator's ground truth
(``expected_decision``/``expected_code`` per debit, from ``families.LEGEND``) joined
to what the live system actually did, rolled up per family and overall, plus the
settled labels the Go labeler produced.

This package turns that already-scored results dict into the *measurements* an eval
asks for — confusion matrices over the decision and code enums, per-family accuracy,
the settled-label breakdown, and an expected-loss framing (how much at-risk value the
system gated vs let through) — as plain numpy + stdlib, so the metrics and their
tests run without the notebook stack. The narrative + charts live in
``notebook/eval.ipynb``, which imports these functions.

It measures only; like the oracle it asserts nothing and changes nothing.
"""

from __future__ import annotations

__all__ = [
    "DECISION_ORDER",
    "load_results",
    "headline",
    "confusion",
    "family_table",
    "label_breakdown",
    "expected_loss",
    "rupees",
]

from .metrics import (
    DECISION_ORDER,
    confusion,
    expected_loss,
    family_table,
    headline,
    label_breakdown,
    load_results,
    rupees,
)
