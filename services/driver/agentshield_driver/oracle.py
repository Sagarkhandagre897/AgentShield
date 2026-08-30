"""oracle — score a live run against the generator's ground truth.

The generator tags every debit with the verdict it is *meant* to receive
(``expected_decision`` / ``expected_code``, from ``families.LEGEND``) and records,
on each settlement, the label the run is *meant* to teach the downstream labeler
(``expected_label`` / ``expected_reason``). This module joins those intentions to
what the live system actually did — the verdicts the decision service returned and
the settled labels the Go labeler produced — and rolls the comparison up per family
and overall.

It asserts nothing and changes nothing: it only measures. The verdict a debit
received is the live decision service's; the labels are the live labeler's. The
one translation it performs is cosmetic — the driverkit reports the proto ``Answer``
enum name (``ANSWER_ALLOW``), while the generator speaks the bare bus decision
(``ALLOW``) — so :func:`normalise_decision` strips the ``ANSWER_`` prefix before
comparing. Codes already match the ``pb.Code`` enum names byte-for-byte.
"""

from __future__ import annotations

from typing import Any, Dict, List

_ANSWER_PREFIX = "ANSWER_"


def normalise_decision(raw: str) -> str:
    """Fold the driverkit's proto ``Answer`` enum name onto the bare bus decision the
    generator's oracle speaks: ``ANSWER_ALLOW`` -> ``ALLOW``. A value already without
    the prefix (or empty) is returned unchanged."""
    if not raw:
        return ""
    if raw.startswith(_ANSWER_PREFIX):
        return raw[len(_ANSWER_PREFIX):]
    return raw


def score_run(
    scenario: Dict[str, Any],
    verdicts_by_eval: Dict[str, Dict[str, Any]],
    labels: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Join the scenario's ground truth to the live verdicts and settled labels.

    ``verdicts_by_eval`` maps a debit's evaluation_id to the driverkit ``evaluate``
    response (its raw ``decision``/``code``). ``labels`` is the list of settled label
    records the driverkit drained from outcomes.v1. Returns a results dict with a
    per-debit comparison, per-family and overall verdict accuracy, and a label
    summary (observed vs the settlements' expected labels)."""
    per_debit: List[Dict[str, Any]] = []
    families: Dict[str, Dict[str, int]] = {}

    for d in scenario.get("timeline", []):
        eval_id = d["evaluation_id"]
        family = d["family"]
        exp_decision = d["expected_decision"]
        exp_code = d["expected_code"]

        resp = verdicts_by_eval.get(eval_id)
        if resp is None:
            act_decision, act_code, evaluated = "", "", False
        else:
            act_decision = normalise_decision(resp.get("decision", ""))
            act_code = resp.get("code", "")
            evaluated = True

        decision_match = evaluated and act_decision == exp_decision
        code_match = evaluated and act_code == exp_code

        per_debit.append({
            "evaluation_id": eval_id,
            "family": family,
            "is_misuse": d["is_misuse"],
            "amount_paise": d["amount_paise"],
            "expected_decision": exp_decision,
            "expected_code": exp_code,
            "actual_decision": act_decision,
            "actual_code": act_code,
            "evaluated": evaluated,
            "decision_match": decision_match,
            "code_match": code_match,
        })

        fam = families.setdefault(
            family, {"count": 0, "evaluated": 0, "decision_matches": 0, "code_matches": 0}
        )
        fam["count"] += 1
        fam["evaluated"] += int(evaluated)
        fam["decision_matches"] += int(decision_match)
        fam["code_matches"] += int(code_match)

    for fam in families.values():
        n = fam["count"] or 1
        fam["decision_accuracy"] = round(fam["decision_matches"] / n, 4)
        fam["code_accuracy"] = round(fam["code_matches"] / n, 4)

    total = len(per_debit)
    dm = sum(1 for r in per_debit if r["decision_match"])
    cm = sum(1 for r in per_debit if r["code_match"])
    ev = sum(1 for r in per_debit if r["evaluated"])

    return {
        "overall": {
            "debits": total,
            "evaluated": ev,
            "decision_matches": dm,
            "code_matches": cm,
            "decision_accuracy": round(dm / (total or 1), 4),
            "code_accuracy": round(cm / (total or 1), 4),
        },
        "by_family": families,
        "per_debit": per_debit,
        "labels": _score_labels(scenario, labels),
    }


def _score_labels(scenario: Dict[str, Any], labels: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Summarise the settled labels the run produced against what the settlements
    expected. Labels come only from settled outcomes (the Go labeler), so this is a
    tally, not a per-debit join: the notebook (component 3) does the finer analysis."""
    observed_by_reason: Dict[str, int] = {}
    observed_by_value: Dict[str, int] = {}
    for lab in labels:
        reason = lab.get("reason", "")
        observed_by_reason[reason] = observed_by_reason.get(reason, 0) + 1
        val = "misuse" if float(lab.get("label", 0.0)) >= 0.5 else "legit"
        observed_by_value[val] = observed_by_value.get(val, 0) + 1

    expected_by_reason: Dict[str, int] = {}
    for d in scenario.get("timeline", []):
        s = d.get("settlement", {})
        reason = s.get("expected_reason") or ""
        if reason:
            expected_by_reason[reason] = expected_by_reason.get(reason, 0) + 1
    # Unconditional cancellations settle a light MISUSE too.
    cancels = len(scenario.get("cancellations", []))
    if cancels:
        expected_by_reason["cancellation"] = expected_by_reason.get("cancellation", 0) + cancels

    return {
        "observed_count": len(labels),
        "observed_by_reason": observed_by_reason,
        "observed_by_value": observed_by_value,
        "expected_by_reason": expected_by_reason,
    }
