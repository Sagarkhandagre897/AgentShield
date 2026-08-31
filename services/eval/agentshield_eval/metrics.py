"""metrics — numpy + stdlib measurements over a driver ``results.json``.

Every function here takes the already-scored results dict the driver wrote (see the
package doc) and returns plain Python / numpy — no plotting, no pandas — so the core
and its tests stay light. The notebook imports these and draws them.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import numpy as np

# The decision axis in escalation order: allow through -> step up to re-confirm ->
# hard block. Both the generator's ground truth and the live system speak these three;
# fixing the order makes each confusion-matrix axis read low -> high friction.
DECISION_ORDER = ["ALLOW", "STEP_UP", "BLOCK"]


def load_results(path: Union[str, Path]) -> Dict[str, Any]:
    """Load a driver ``results.json`` into the results dict the rest of this module reads."""
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def rupees(paise: float) -> str:
    """Format an integer paise amount as ``₹`` (100 paise = ₹1) for human-facing labels."""
    return f"₹{paise / 100:,.2f}"


def headline(results: Dict[str, Any]) -> Dict[str, Any]:
    """The one-line scoreboard: totals + accuracies, plus ``run_seconds`` when the live
    driver wrote it (absent for an in-process score) and the barrier-warning count."""
    o = results["overall"]
    out: Dict[str, Any] = {
        "debits": o["debits"],
        "evaluated": o["evaluated"],
        "decision_accuracy": o["decision_accuracy"],
        "code_accuracy": o["code_accuracy"],
        "warnings": len(results.get("warnings", [])),
    }
    if "run_seconds" in results:
        out["run_seconds"] = results["run_seconds"]
    return out


def _labels_for(
    per_debit: Sequence[Dict[str, Any]], exp_key: str, act_key: str,
    order: Optional[Sequence[str]] = None,
) -> List[str]:
    """The label set present across both the expected and (evaluated) actual columns.
    With an ``order``, known labels lead in that order and any extras trail sorted."""
    present = {r[exp_key] for r in per_debit}
    present |= {r[act_key] for r in per_debit if r["evaluated"]}
    present.discard("")
    if order:
        return [l for l in order if l in present] + sorted(present - set(order))
    return sorted(present)


def confusion(
    per_debit: Sequence[Dict[str, Any]], kind: str = "decision",
    labels: Optional[Sequence[str]] = None,
) -> Tuple[List[str], "np.ndarray"]:
    """Expected-vs-actual count matrix for the ``decision`` or ``code`` enum.

    Rows are the expected (ground-truth) class, columns the actual (live) class, over
    one shared ordered label set. Unevaluated debits (no live verdict) are dropped — a
    confusion cell compares two verdicts and there isn't one. Returns ``(labels, m)``
    with ``m[i, j]`` = #(expected == labels[i] and actual == labels[j])."""
    if kind == "decision":
        exp_key, act_key, order = "expected_decision", "actual_decision", DECISION_ORDER
    elif kind == "code":
        exp_key, act_key, order = "expected_code", "actual_code", None
    else:
        raise ValueError(f"kind must be 'decision' or 'code', got {kind!r}")
    labels = list(labels) if labels is not None else _labels_for(per_debit, exp_key, act_key, order)
    index = {l: i for i, l in enumerate(labels)}
    m = np.zeros((len(labels), len(labels)), dtype=int)
    for r in per_debit:
        if not r["evaluated"]:
            continue
        i, j = index.get(r[exp_key]), index.get(r[act_key])
        if i is not None and j is not None:
            m[i, j] += 1
    return labels, m


def family_table(results: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Per-family rows sorted by name: count, evaluated, decision/code accuracy, and
    whether it is a misuse family (from the per-debit ``is_misuse`` flag)."""
    by_fam = results["by_family"]
    misuse_fams = {r["family"] for r in results["per_debit"] if r["is_misuse"]}
    return [
        {
            "family": fam,
            "count": by_fam[fam]["count"],
            "evaluated": by_fam[fam]["evaluated"],
            "decision_accuracy": by_fam[fam]["decision_accuracy"],
            "code_accuracy": by_fam[fam]["code_accuracy"],
            "is_misuse": fam in misuse_fams,
        }
        for fam in sorted(by_fam)
    ]


def label_breakdown(results: Dict[str, Any]) -> Dict[str, Any]:
    """The settled-label view: reasons observed from the live labeler beside the
    settlements' expected reasons (so over/under-labeling shows), plus the value tally."""
    labs = results.get("labels", {})
    observed = dict(labs.get("observed_by_reason", {}))
    expected = dict(labs.get("expected_by_reason", {}))
    rows = [
        {"reason": r, "observed": observed.get(r, 0), "expected": expected.get(r, 0)}
        for r in sorted(set(observed) | set(expected))
    ]
    return {
        "observed_count": labs.get("observed_count", 0),
        "by_reason": rows,
        "by_value": dict(labs.get("observed_by_value", {})),
    }


def expected_loss(results: Dict[str, Any]) -> Dict[str, Any]:
    """Frame the run in money and friction — the terms the decision rule is written in.

    Ground truth, not the misuse flag, sets the bar: a debit the generator expects to
    *gate* (STEP_UP to re-confirm, or BLOCK) carries at-risk value the system should
    hold; some misuse debits (a bust-out's early legs, a replay's first leg) are
    themselves expected to ALLOW and so are not at-risk. The system *gates* an at-risk
    debit by returning anything but ALLOW and *leaks* it by allowing it through; a debit
    the generator expects to ALLOW but the system gates is false friction. Perfect
    fidelity gates every at-risk paisa and adds no friction."""
    pd = [r for r in results["per_debit"] if r["evaluated"]]
    gate_expected = [r for r in pd if r["expected_decision"] in ("STEP_UP", "BLOCK")]
    allow_expected = [r for r in pd if r["expected_decision"] == "ALLOW"]
    gated = [r for r in gate_expected if r["actual_decision"] != "ALLOW"]
    leaked = [r for r in gate_expected if r["actual_decision"] == "ALLOW"]
    friction = [r for r in allow_expected if r["actual_decision"] != "ALLOW"]
    at_risk = sum(r["amount_paise"] for r in gate_expected)
    gated_paise = sum(r["amount_paise"] for r in gated)
    return {
        "gate_expected": len(gate_expected),
        "gated": len(gated),
        "leaked": len(leaked),
        "recall": round(len(gated) / (len(gate_expected) or 1), 4),
        "at_risk_paise": at_risk,
        "gated_paise": gated_paise,
        "leaked_paise": sum(r["amount_paise"] for r in leaked),
        "value_gated_frac": round(gated_paise / (at_risk or 1), 4),
        "allow_expected": len(allow_expected),
        "false_friction": len(friction),
        "false_friction_rate": round(len(friction) / (len(allow_expected) or 1), 4),
    }
