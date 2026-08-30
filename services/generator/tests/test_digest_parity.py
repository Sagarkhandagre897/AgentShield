"""Digest-parity tests — guard the one place the generator duplicates engine logic.

``scenario.envelope_digest`` is an inlined replica of the intent engine's envelope
digest (§12), copied rather than imported so the generator stays pure stdlib and
never drags in numpy. That copy is a liability the moment the real digest changes,
so this test pins the two together byte-for-byte: the generator's ``normalise_purpose``
and ``envelope_digest`` must agree with ``agentshield_intent.envelope`` for a matrix
of inputs and for every envelope in a freshly generated scenario.

If the intent package can't be imported (its ML deps aren't installed), the parity
check is skipped — the copy is still exercised by test_scenario.py; only the
cross-check against the source of truth needs the intent package present.

Runnable with pytest or directly (`python3 tests/test_digest_parity.py`).
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "intent"))

from agentshield_generator.generate import Config, build_scenario  # noqa: E402
from agentshield_generator.scenario import envelope_digest, normalise_purpose  # noqa: E402

try:
    # Importing the submodule still runs agentshield_intent.__init__, which pulls in
    # numpy via alignment/embed; a missing wheel means we skip the cross-check.
    from agentshield_intent.envelope import IntentEnvelope
    from agentshield_intent.envelope import normalise_purpose as intent_normalise_purpose

    INTENT_AVAILABLE = True
    INTENT_SKIP_REASON = ""
except Exception as exc:  # pragma: no cover - depends on the environment
    INTENT_AVAILABLE = False
    INTENT_SKIP_REASON = f"agentshield_intent not importable: {exc}"


# A matrix that exercises every folding branch and every normalisation the digest
# depends on: purpose synonyms and casing, category/merchant whitespace and case,
# amount coercion, and both empty and populated constraints.
PURPOSE_CASES = [
    "purchase", "Purchase", "BUY", "order", "shop", "shopping",
    "subscription", "subscribe", "recurring", "renewal", "membership",
    "top-up", "top_up", "topup", "recharge", "wallet", "refill",
    "", "  ", "nonsense",
]

ENVELOPE_CASES = [
    ("purchase", "groceries", 1_000_000, "bigbasket", {}),
    ("BUY", "  Electronics ", 250000, "Croma", {}),
    ("subscribe", "subscription", 49900, "NETFLIX", {"cadence": "monthly"}),
    ("top_up", "wallet-topup", 500000, "  paytm  ", {"a": 1, "b": [1, 2, 3]}),
    ("", "gift-cards", 0, "", {}),
    ("nonsense", "crypto", 99999999, "wazirx", {"nested": {"x": True}}),
]


def _require_intent():
    if not INTENT_AVAILABLE:
        import pytest

        pytest.skip(INTENT_SKIP_REASON)


def test_normalise_purpose_matches_intent():
    _require_intent()
    for p in PURPOSE_CASES:
        assert normalise_purpose(p) == intent_normalise_purpose(p), f"purpose fold drift on {p!r}"


def test_envelope_digest_matches_intent():
    _require_intent()
    for purpose, category, amount, pref, constraints in ENVELOPE_CASES:
        ours = envelope_digest(purpose, category, amount, pref, constraints)
        theirs = IntentEnvelope(
            purpose=purpose,
            category=category,
            max_amount_paise=amount,
            merchant_preference=pref,
            constraints=constraints,
        ).digest()
        assert ours == theirs, f"digest drift on {(purpose, category, amount, pref, constraints)!r}"


def test_generated_envelopes_seal_like_the_engine():
    _require_intent()
    scn = build_scenario(Config(seed=7))
    assert scn.envelopes, "the scenario should contain sealed envelopes"
    for e in scn.envelopes:
        theirs = IntentEnvelope(
            purpose=e.purpose,
            category=e.category,
            max_amount_paise=e.max_amount_paise,
            merchant_preference=e.merchant_preference,
            constraints=e.constraints,
        ).digest()
        assert e.digest() == theirs, f"session {e.session_id}: generator digest != engine digest"


if __name__ == "__main__":  # pragma: no cover
    if not INTENT_AVAILABLE:
        print(f"SKIP  all parity tests — {INTENT_SKIP_REASON}")
        raise SystemExit(0)
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
