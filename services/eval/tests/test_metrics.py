"""test_metrics — the eval core over a hand-built results dict (precise numbers) and
one end-to-end FakeKit run (a real, perfectly-scored results shape).

The hand-built fixture deliberately contains a leaked misuse debit, a false-friction
legit debit, and an unevaluated debit, so every branch of the confusion / expected-loss
math is exercised with numbers checkable by hand.
"""

from __future__ import annotations

import numpy as np
import pytest

import agentshield_eval as ev


@pytest.fixture
def results():
    """Six debits: 3 correct, 1 leaked misuse (STEP_UP->ALLOW), 1 false-friction legit
    (ALLOW->STEP_UP), 1 unevaluated legit (dropped from confusion + loss)."""
    def d(eid, fam, mis, amt, exp, act, ev_ok):
        return {
            "evaluation_id": eid, "family": fam, "is_misuse": mis, "amount_paise": amt,
            "expected_decision": exp[0], "expected_code": exp[1],
            "actual_decision": act[0] if ev_ok else "", "actual_code": act[1] if ev_ok else "",
            "evaluated": ev_ok,
            "decision_match": ev_ok and exp[0] == act[0], "code_match": ev_ok and exp[1] == act[1],
        }
    per_debit = [
        d("e1", "legit", False, 100, ("ALLOW", "OK_ALLOW"), ("ALLOW", "OK_ALLOW"), True),
        d("e2", "legit", False, 200, ("ALLOW", "OK_ALLOW"), ("STEP_UP", "STEPUP_RISK"), True),
        d("e3", "scope_overrun", True, 500, ("STEP_UP", "STEPUP_SCOPE"), ("STEP_UP", "STEPUP_SCOPE"), True),
        d("e4", "replay", True, 700, ("BLOCK", "BLOCKED_DUPLICATE"), ("BLOCK", "BLOCKED_DUPLICATE"), True),
        d("e5", "velocity_bustout", True, 1000, ("STEP_UP", "STEPUP_SCOPE"), ("ALLOW", "OK_ALLOW"), True),
        d("e6", "legit", False, 300, ("ALLOW", "OK_ALLOW"), ("ALLOW", "OK_ALLOW"), False),
    ]
    return {
        "overall": {"debits": 6, "evaluated": 5, "decision_matches": 3, "code_matches": 3,
                    "decision_accuracy": 0.5, "code_accuracy": 0.5},
        "by_family": {
            "legit": {"count": 3, "evaluated": 2, "decision_matches": 1, "code_matches": 1,
                      "decision_accuracy": 0.3333, "code_accuracy": 0.3333},
            "scope_overrun": {"count": 1, "evaluated": 1, "decision_matches": 1, "code_matches": 1,
                              "decision_accuracy": 1.0, "code_accuracy": 1.0},
            "replay": {"count": 1, "evaluated": 1, "decision_matches": 1, "code_matches": 1,
                       "decision_accuracy": 1.0, "code_accuracy": 1.0},
            "velocity_bustout": {"count": 1, "evaluated": 1, "decision_matches": 0, "code_matches": 0,
                                 "decision_accuracy": 0.0, "code_accuracy": 0.0},
        },
        "per_debit": per_debit,
        "labels": {"observed_count": 3,
                   "observed_by_reason": {"dispute": 2, "cancellation": 1},
                   "observed_by_value": {"misuse": 3},
                   "expected_by_reason": {"dispute": 3, "cancellation": 1}},
        "warnings": [],
        "run_seconds": 22.94,
    }

def test_headline_surfaces_totals_and_run_seconds(results):
    h = ev.headline(results)
    assert h["debits"] == 6 and h["evaluated"] == 5
    assert h["decision_accuracy"] == 0.5 and h["code_accuracy"] == 0.5
    assert h["warnings"] == 0
    assert h["run_seconds"] == 22.94  # surfaced only when the live driver wrote it


def test_headline_omits_run_seconds_when_absent(results):
    results.pop("run_seconds")
    assert "run_seconds" not in ev.headline(results)


def test_decision_confusion_counts_and_escalation_order(results):
    labels, m = ev.confusion(results["per_debit"], kind="decision")
    assert labels == ["ALLOW", "STEP_UP", "BLOCK"]  # low -> high friction
    # rows=expected, cols=actual; the unevaluated e6 is dropped.
    assert m.tolist() == [[1, 1, 0], [1, 1, 0], [0, 0, 1]]
    assert int(m.trace()) == 3          # correct decisions among the 5 evaluated
    assert int(m.sum()) == 5            # every evaluated debit lands in one cell


def test_code_confusion_uses_code_columns(results):
    labels, m = ev.confusion(results["per_debit"], kind="code")
    assert set(labels) == {"OK_ALLOW", "STEPUP_RISK", "STEPUP_SCOPE", "BLOCKED_DUPLICATE"}
    assert int(m.sum()) == 5
    # the leaked e5 (expected STEPUP_SCOPE, actual OK_ALLOW) is an off-diagonal cell
    i, j = labels.index("STEPUP_SCOPE"), labels.index("OK_ALLOW")
    assert m[i, j] == 1


def test_confusion_rejects_unknown_kind(results):
    with pytest.raises(ValueError):
        ev.confusion(results["per_debit"], kind="nope")


def test_expected_loss_gated_leaked_and_friction(results):
    el = ev.expected_loss(results)
    assert el["gate_expected"] == 3 and el["gated"] == 2 and el["leaked"] == 1
    assert el["recall"] == round(2 / 3, 4)
    assert el["at_risk_paise"] == 2200 and el["gated_paise"] == 1200 and el["leaked_paise"] == 1000
    assert el["value_gated_frac"] == round(1200 / 2200, 4)
    assert el["allow_expected"] == 2 and el["false_friction"] == 1  # e6 unevaluated -> excluded
    assert el["false_friction_rate"] == 0.5


def test_label_breakdown_pairs_observed_and_expected(results):
    lb = ev.label_breakdown(results)
    assert lb["observed_count"] == 3
    assert lb["by_value"] == {"misuse": 3}
    rows = {r["reason"]: r for r in lb["by_reason"]}
    assert rows["dispute"] == {"reason": "dispute", "observed": 2, "expected": 3}
    assert rows["cancellation"] == {"reason": "cancellation", "observed": 1, "expected": 1}


def test_family_table_sorted_with_misuse_flag(results):
    rows = ev.family_table(results)
    assert [r["family"] for r in rows] == sorted(r["family"] for r in rows)  # name-sorted
    flag = {r["family"]: r["is_misuse"] for r in rows}
    assert flag["legit"] is False
    assert flag["scope_overrun"] and flag["replay"] and flag["velocity_bustout"]


def test_rupees_formats_paise():
    assert ev.rupees(100000) == "₹1,000.00"


def test_end_to_end_fakekit_run_is_perfect():
    """A full FakeKit run reproduces the intended verdicts, so the confusion matrices
    are purely diagonal, misuse recall is 1.0 and there is zero false friction."""
    from agentshield_driver.fakekit import FakeKit
    from agentshield_driver.orchestrator import Timings, run_scenario
    from agentshield_generator.generate import build_scenario

    fast = Timings(feature_poll_timeout=2.0, barrier_poll_timeout=2.0, poll_interval=0.0,
                   settle_after_evaluate=0.0, settle_after_capture=0.0,
                   settle_after_settlement=0.0, labels_timeout_ms=0)
    res = run_scenario(FakeKit(), build_scenario().to_dict(), timings=fast, log=lambda _m: None)

    h = ev.headline(res)
    assert h["decision_accuracy"] == 1.0 and h["code_accuracy"] == 1.0 and h["warnings"] == 0
    _, dm = ev.confusion(res["per_debit"], kind="decision")
    assert int(dm.trace()) == int(dm.sum())          # every cell on the diagonal
    el = ev.expected_loss(res)
    assert el["recall"] == 1.0 and el["leaked"] == 0 and el["false_friction"] == 0
