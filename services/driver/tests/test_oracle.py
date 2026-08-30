"""test_oracle — unit tests for the verdict/label scoring, no infra and no FakeKit.

These pin the pure join in :mod:`agentshield_driver.oracle`: the ANSWER_ prefix
fold, the per-debit verdict/code comparison, the per-family and overall roll-ups,
and the label tally (observed vs the settlements' expected reasons). A hand-built
scenario keeps the arithmetic checkable by eye."""

from __future__ import annotations

from agentshield_driver.oracle import normalise_decision, score_run


def test_normalise_decision_strips_answer_prefix():
    assert normalise_decision("ANSWER_ALLOW") == "ALLOW"
    assert normalise_decision("ANSWER_STEP_UP") == "STEP_UP"
    assert normalise_decision("ANSWER_BLOCK") == "BLOCK"
    # Already-bare and empty values pass through unchanged.
    assert normalise_decision("ALLOW") == "ALLOW"
    assert normalise_decision("") == ""


def _debit(eval_id, family, exp_dec, exp_code, *, is_misuse=True, amount=600000, settlement=None):
    return {
        "evaluation_id": eval_id,
        "family": family,
        "is_misuse": is_misuse,
        "amount_paise": amount,
        "expected_decision": exp_dec,
        "expected_code": exp_code,
        "settlement": settlement or {},
    }


def test_score_run_joins_verdicts_and_rolls_up():
    scenario = {
        "timeline": [
            _debit("e1", "legit", "ALLOW", "OK_ALLOW", is_misuse=False),
            _debit("e2", "replay", "BLOCK", "BLOCKED_DUPLICATE"),
            _debit("e3", "intent_drift", "STEP_UP", "STEPUP_RISK",
                   settlement={"then_dispute": True, "expected_reason": "dispute"}),
        ],
        "cancellations": ["tok_x"],
    }
    verdicts = {
        # e1 matches exactly.
        "e1": {"decision": "ANSWER_ALLOW", "code": "OK_ALLOW"},
        # e2 got a step-up instead of the expected block (decision + code miss).
        "e2": {"decision": "ANSWER_STEP_UP", "code": "STEPUP_FAILCLOSED"},
        # e3 matches the decision but not the code.
        "e3": {"decision": "ANSWER_STEP_UP", "code": "STEPUP_SCOPE"},
    }
    labels = [
        {"token_id": "tok_a", "label": 1.0, "reason": "dispute"},
        {"token_id": "tok_x", "label": 1.0, "reason": "cancellation"},
        {"token_id": "tok_b", "label": 0.0, "reason": "confirmed_step_up"},
    ]

    res = score_run(scenario, verdicts, labels)

    # overall: 2 of 3 decisions match (e1, e3), 1 of 3 codes match (e1).
    assert res["overall"]["debits"] == 3
    assert res["overall"]["evaluated"] == 3
    assert res["overall"]["decision_matches"] == 2
    assert res["overall"]["code_matches"] == 1
    assert res["overall"]["decision_accuracy"] == round(2 / 3, 4)

    # per-family accuracy is per its own count.
    assert res["by_family"]["legit"]["decision_accuracy"] == 1.0
    assert res["by_family"]["replay"]["decision_accuracy"] == 0.0
    assert res["by_family"]["intent_drift"]["decision_accuracy"] == 1.0
    assert res["by_family"]["intent_drift"]["code_accuracy"] == 0.0

    # labels: observed tallies by reason and value, expected from the settlements.
    labs = res["labels"]
    assert labs["observed_count"] == 3
    assert labs["observed_by_reason"] == {"dispute": 1, "cancellation": 1, "confirmed_step_up": 1}
    assert labs["observed_by_value"] == {"misuse": 2, "legit": 1}
    assert labs["expected_by_reason"]["dispute"] == 1
    assert labs["expected_by_reason"]["cancellation"] == 1


def test_score_run_marks_unevaluated_debits():
    scenario = {"timeline": [_debit("e1", "legit", "ALLOW", "OK_ALLOW", is_misuse=False)]}
    res = score_run(scenario, {}, [])  # no verdict for e1
    row = res["per_debit"][0]
    assert row["evaluated"] is False
    assert row["decision_match"] is False
    assert res["overall"]["evaluated"] == 0
