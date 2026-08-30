"""Engine-level tests (System Design §12) — the seal + score + deposit path.

They pin the seams the engine turns on: the figure is keyed on the session, a
sealed envelope makes a matching debit read low and an off-merchant one read
higher, a session that sealed nothing reads unattested (not innocent), and only
evaluation debits are judged. The deterministic hashing embedder is used, so no
model download is needed. Runnable with pytest or directly.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from agentshield_intent.alignment import UNATTESTED_DIVERGENCE  # noqa: E402
from agentshield_intent.embed import HashingEmbedder  # noqa: E402
from agentshield_intent.engine import IntentEngine  # noqa: E402
from agentshield_intent.envelope import IntentEnvelope  # noqa: E402
from agentshield_shared.schema import EVENT_DECISION_MADE, EVENT_PAYMENT_CAPTURED, Event  # noqa: E402

_ENV = {"purpose": "purchase", "category": "coffee", "max_amount_paise": 50000, "merchant_preference": "blue tokai"}


def _debit(session="s1", token="tok_1", merchant="blue tokai", category="coffee",
           purpose="purchase", envelope=None, at=1000, eid="e1", etype=EVENT_DECISION_MADE):
    payload = {"session_id": session, "merchant_id": merchant, "category": category, "purpose": purpose}
    if envelope is not None:
        payload["intent_envelope"] = envelope
    return Event(event_id=eid, type=etype, token_id=token, occurred_at=at, payload=payload)


def _fresh():
    return IntentEngine(HashingEmbedder())  # deterministic, no model download


def test_figure_is_keyed_on_the_session():
    dep = _fresh().observe(_debit(envelope=_ENV))
    assert dep.feature_key == "s1" and dep.token_id == "tok_1" and dep.occurred_at == 1000


def test_matching_debit_reads_low():
    dep = _fresh().observe(_debit(envelope=_ENV))  # seals and scores the same debit
    assert dep.divergence < 0.3


def test_off_merchant_reads_higher_than_matching():
    eng = _fresh()
    match = eng.observe(_debit(envelope=_ENV))  # seal on the first debit
    off = eng.observe(_debit(merchant="casino royale", category="gambling", eid="e2", at=2000))
    assert off.divergence > match.divergence and off.divergence > 0.3


def test_no_envelope_is_unattested_not_innocent():
    dep = _fresh().observe(_debit(session="s2", envelope=None))
    assert dep.divergence == UNATTESTED_DIVERGENCE


def test_only_learns_from_evaluation_debits():
    assert _fresh().observe(_debit(etype=EVENT_PAYMENT_CAPTURED)) is None


def test_dropped_without_a_key():
    eng = _fresh()
    ev = Event(event_id="e", type=EVENT_DECISION_MADE, token_id="", payload={"merchant_id": "x"})
    assert eng.observe(ev) is None


def test_sealing_is_idempotent():
    eng = _fresh()
    a = eng.seal("k", IntentEnvelope.from_dict(_ENV))
    b = eng.seal("k", IntentEnvelope.from_dict(_ENV))
    assert a is b  # same digest → not re-embedded


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
