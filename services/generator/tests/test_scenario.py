"""Scenario tests — pure stdlib, no wheels.

They pin the properties that make a generated scenario a trustworthy oracle for the
Phase-7 live run:

  * it is reproducible from the seed alone (no clock, no ambient randomness);
  * every family the System Design names is present, and each debit's recorded
    verdict matches the family LEGEND (the deterministic families exactly; the two
    families with an allowed prefix within their known set);
  * the money invariants hold (token containment; a debit is exactly the 12 proto
    OrderContext fields);
  * the label line is respected — a settlement only ever describes a settled outcome
    (a capture, a chargeback, a pulled mandate), never asserts a training label, and
    the only LEGIT-labelled settlement is a confirmed step-up;
  * the one PII carrier is the sealed envelope; no debit on the wire holds raw PII,
    and a bound debit's digest equals a re-seal of its envelope.

Runnable with pytest or directly (`python3 tests/test_scenario.py`).
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agentshield_generator import families as F  # noqa: E402
from agentshield_generator.generate import (  # noqa: E402
    DRIFT_MERCHANTS,
    EXPIRE_IN_PAST,
    Config,
    build_scenario,
)

# The exact 12 fields of proto OrderContext — the wire contract a debit must be.
ORDER_CONTEXT_FIELDS = {
    "evaluation_id", "token_id", "customer_id", "agent_id", "merchant_id",
    "session_id", "amount_paise", "cart_hash", "envelope_digest", "tool_risk",
    "nonce", "ts",
}


def _scn(seed: int = 7):
    return build_scenario(Config(seed=seed))


# --- reproducibility --------------------------------------------------------

def test_build_is_deterministic():
    a = build_scenario(Config(seed=7)).to_json()
    b = build_scenario(Config(seed=7)).to_json()
    assert a == b, "same seed must yield byte-identical JSON"


def test_different_seed_differs():
    a = build_scenario(Config(seed=7)).to_json()
    b = build_scenario(Config(seed=8)).to_json()
    assert a != b, "a different seed should produce a different world"


# --- coverage ---------------------------------------------------------------

def test_all_families_present():
    counts = _scn().counts_by_family()
    for fam in F.ALL_FAMILIES:
        assert counts.get(fam, 0) > 0, f"family {fam} missing from the default scenario"


def test_graph_structures_have_multiple_members():
    counts = _scn().counts_by_family()
    # A ring/fan-in/fleet is only a structure if it has several members.
    assert counts[F.FAMILY_MULE_FAN_IN] >= 3
    assert counts[F.FAMILY_SHARED_DEVICE_RING] >= 3
    assert counts[F.FAMILY_SYNCHRONISED_FLEET] >= 3


# --- the oracle matches the legend -----------------------------------------

def test_oracle_matches_legend():
    """Every debit's recorded verdict is consistent with its family's LEGEND.
    Two families carry a known allowed prefix and so admit a second shape:
    an ordinary/allowed leg alongside the family's headline verdict."""
    allowed = (F.DECISION_ALLOW, F.CODE_OK_ALLOW)
    for d in _scn().timeline:
        legend = F.LEGEND[d.family]
        expected = (d.expected_decision, d.expected_code)
        ideal = (legend.ideal_decision, legend.ideal_code)
        if d.family == F.FAMILY_LEGIT:
            # ordinary allow, or the cautious CRITICAL-tool step-up
            assert expected in (allowed, (F.DECISION_STEP_UP, F.CODE_STEPUP_RISK))
        elif d.family == F.FAMILY_VELOCITY_BUSTOUT:
            # the pre-crossing legs allow; the crossing leg is the legend verdict
            assert expected in (allowed, ideal)
        else:
            assert expected == ideal, f"{d.family}: {expected} != legend {ideal}"


def test_misuse_flag_tracks_family():
    for d in _scn().timeline:
        assert d.is_misuse == (d.family in F.MISUSE_FAMILIES)


# --- money invariants -------------------------------------------------------

def test_token_containment():
    for t in _scn().tokens:
        assert 0 < t.max_amount_paise <= t.max_per_day_paise <= t.token_ceiling_paise


def test_order_context_is_exactly_twelve_fields():
    for d in _scn().timeline:
        assert set(d.order_context().keys()) == ORDER_CONTEXT_FIELDS


def test_every_debit_has_a_cart_hash():
    # P6 (binding) blocks an empty cart_hash; a generated debit is never accidentally
    # unbound — the families that BIND-fail are simply not modelled here.
    for d in _scn().timeline:
        assert d.cart_hash, f"debit {d.evaluation_id} has an empty cart_hash"


# --- deterministic-family mechanics ----------------------------------------

def test_replay_reuses_a_spent_nonce():
    scn = _scn()
    by_token = {}
    for d in scn.timeline:
        by_token.setdefault(d.token_id, []).append(d)
    replays = [d for d in scn.timeline if d.family == F.FAMILY_REPLAY]
    assert replays, "expected at least one replay"
    for r in replays:
        siblings = by_token[r.token_id]
        seed = [d for d in siblings if d.family == F.FAMILY_LEGIT and d.nonce == r.nonce]
        assert seed, "a replay must reuse the nonce of a prior legit debit on the same token"
        assert seed[0].evaluation_id != r.evaluation_id, "the replay must carry a fresh evaluation_id"
        assert r.barrier, "a replay depends on the seed's nonce-spend folding first"
        assert (r.expected_decision, r.expected_code) == (F.DECISION_BLOCK, F.CODE_BLOCKED_DUPLICATE)


def test_barrier_only_where_a_prior_fold_is_needed():
    for d in _scn().timeline:
        if d.barrier:
            assert d.family in (F.FAMILY_REPLAY, F.FAMILY_VELOCITY_BUSTOUT)


def test_velocity_bustout_crosses_the_daily_cap():
    scn = _scn()
    tok = {t.token_id: t for t in scn.tokens}
    legs = [d for d in scn.timeline if d.family == F.FAMILY_VELOCITY_BUSTOUT]
    assert legs, "expected a velocity bust-out"
    # at least one leg is expected to step up on scope (the crossing leg)
    crossings = [d for d in legs if (d.expected_decision, d.expected_code) == (F.DECISION_STEP_UP, F.CODE_STEPUP_SCOPE)]
    assert crossings, "a bust-out must have a leg that crosses the per-day cap"
    for d in crossings:
        assert d.barrier, "the crossing leg needs prior captures folded into consumption"
    # each individual leg stays under the per-debit cap — it is the SUM that busts
    for d in legs:
        assert d.amount_paise <= tok[d.token_id].max_amount_paise


def test_scope_overrun_has_both_flavours():
    scn = _scn()
    tok = {t.token_id: t for t in scn.tokens}
    overlay_denies = {o.token_id for o in scn.overlays if "deny" in o.merchant_rules.values()}
    legs = [d for d in scn.timeline if d.family == F.FAMILY_SCOPE_OVERRUN]
    over_cap = [d for d in legs if d.amount_paise > tok[d.token_id].max_amount_paise]
    deny = [d for d in legs if d.token_id in overlay_denies]
    assert over_cap, "expected an over-cap scope overrun"
    assert deny, "expected a merchant-deny scope overrun"
    for d in legs:
        assert (d.expected_decision, d.expected_code) == (F.DECISION_STEP_UP, F.CODE_STEPUP_SCOPE)


def test_stale_and_revoked_flavours():
    scn = _scn()
    tok = {t.token_id: t for t in scn.tokens}
    cancelled = set(scn.cancellations)
    legs = [d for d in scn.timeline if d.family == F.FAMILY_STALE_REVOKED]
    assert legs
    expired = [d for d in legs if tok[d.token_id].expire_at == EXPIRE_IN_PAST]
    revoked = [d for d in legs if tok[d.token_id].token_id in cancelled]
    assert expired, "expected an expired-mandate debit"
    assert revoked, "expected a revoked-mandate debit"
    for d in revoked:
        assert tok[d.token_id].status == "cancelled"
    for d in legs:
        assert (d.expected_decision, d.expected_code) == (F.DECISION_BLOCK, F.CODE_BLOCKED_AUTHORITY)


def test_cancellations_are_confirmed_revocations():
    scn = _scn()
    tok = {t.token_id: t for t in scn.tokens}
    assert scn.cancellations, "a revoked-token family implies at least one cancellation"
    for tid in scn.cancellations:
        assert tok[tid].status == "cancelled"


# --- the label line ---------------------------------------------------------

def test_settlements_only_describe_settled_outcomes():
    for d in _scn().timeline:
        s = d.settlement
        assert s.capture_when in ("allow", "allow_or_stepup", "never")
        if s.capture_when == "never":
            # a blocked debit moves no money and teaches no label
            assert not s.then_dispute
            assert s.expected_label is None


def test_only_confirmed_step_up_yields_a_legit_label():
    for d in _scn().timeline:
        s = d.settlement
        if s.expected_label == F.LABEL_LEGIT:
            assert s.expected_reason == F.REASON_CONFIRMED_STEP_UP
            assert s.capture_when == "allow_or_stepup"
            assert not d.is_misuse


def test_disputes_are_misuse_full_weight():
    for d in _scn().timeline:
        s = d.settlement
        if s.then_dispute:
            assert s.expected_label == F.LABEL_MISUSE
            assert s.expected_reason == F.REASON_DISPUTE
            assert d.is_misuse


def test_ordinary_legit_capture_teaches_nothing():
    # a bare, undisputed capture is not a label (§6): most legit traffic is unlabelled
    scn = _scn()
    ordinary = [
        d for d in scn.timeline
        if d.family == F.FAMILY_LEGIT and d.settlement.capture_when == "allow"
    ]
    assert ordinary, "expected ordinary allowed legit debits"
    for d in ordinary:
        assert d.settlement.expected_label is None
        assert not d.settlement.then_dispute


# --- PII / VAULT ------------------------------------------------------------

def test_raw_pii_lives_only_in_envelopes():
    scn = _scn().to_dict()
    for d in scn["timeline"]:
        assert "raw_instruction" not in d, "raw PII must never ride a debit"
        assert "contact" not in d
    for e in scn["envelopes"]:
        assert e["raw_instruction"] and e["contact"], "the sealed envelope carries the PII"


def test_bound_debit_digest_matches_its_envelope():
    scn = _scn()
    env_by_session = {e.session_id: e for e in scn.envelopes}
    for d in scn.timeline:
        if d.envelope_digest:
            env = env_by_session[d.session_id]
            assert d.envelope_digest == env.digest(), "a debit's digest must equal a re-seal of its envelope"


def test_intent_drift_targets_an_off_envelope_merchant():
    drift_ids = {m for m, _cat in DRIFT_MERCHANTS}
    drifts = [d for d in _scn().timeline if d.family == F.FAMILY_INTENT_DRIFT]
    assert drifts
    for d in drifts:
        assert d.merchant_id in drift_ids, "an intent-drift debit diverts to an off-envelope merchant"
        assert d.envelope_digest, "yet it is still bound to a sealed envelope (the spine passes)"


# --- shape / serialisation --------------------------------------------------

def test_validate_and_json_roundtrip():
    import json

    scn = _scn()
    scn.validate()  # raises on any inconsistency
    doc = json.loads(scn.to_json())
    assert set(doc.keys()) == {"meta", "tokens", "overlays", "envelopes", "cancellations", "timeline"}
    assert doc["meta"]["totals"]["debits"] == len(scn.timeline)
    assert doc["meta"]["counts_by_family"] == scn.counts_by_family()
    assert set(doc["meta"]["legend"].keys()) == set(F.LEGEND.keys())


if __name__ == "__main__":  # pragma: no cover
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
