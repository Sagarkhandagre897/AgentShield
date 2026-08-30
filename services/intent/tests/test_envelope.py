"""Envelope tests (System Design §12) — pure stdlib.

They pin the fingerprint the request rides on: the digest is stable and order-
independent over the same sealed content, distinct envelopes seal distinctly, and
the semantic text embedded is the soft part only — the exact fields (max amount,
constraints) travel but are not folded into what the model sees. Runnable with
pytest or directly.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agentshield_intent.envelope import (  # noqa: E402
    PURPOSE_PURCHASE,
    PURPOSE_SUBSCRIPTION,
    IntentEnvelope,
    normalise_purpose,
)


def _env(**kw):
    base = dict(purpose="purchase", category="coffee", max_amount_paise=50000, merchant_preference="Blue Tokai")
    base.update(kw)
    return IntentEnvelope(**base)


def test_digest_is_stable_and_order_independent():
    a = IntentEnvelope(purpose="purchase", category="coffee", constraints={"x": 1, "y": 2})
    b = IntentEnvelope(purpose="purchase", category="coffee", constraints={"y": 2, "x": 1})
    assert a.digest() == b.digest()  # sorted keys → same fingerprint
    assert len(a.digest()) == 64  # sha256 hex


def test_distinct_envelopes_seal_distinctly():
    assert _env().digest() != _env(max_amount_paise=999999).digest()
    assert _env().digest() != _env(category="electronics").digest()


def test_semantic_text_is_the_soft_part_only():
    txt = _env().semantic_text()
    assert "coffee" in txt and "blue tokai" in txt.lower() and "purchase" in txt
    # The exact fields never enter the text the model embeds.
    assert "50000" not in txt


def test_normalise_purpose_folds_synonyms():
    assert normalise_purpose("BUY") == PURPOSE_PURCHASE
    assert normalise_purpose("recurring") == PURPOSE_SUBSCRIPTION
    assert normalise_purpose("nonsense") == ""  # unknown, never a fourth class


def test_round_trips_through_dict():
    e = _env(constraints={"region": "IN"})
    r = IntentEnvelope.from_dict(e.to_dict())
    assert r.digest() == e.digest() and r.constraints == {"region": "IN"}


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
