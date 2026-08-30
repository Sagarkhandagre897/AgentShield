"""Alignment tests (System Design §12) — pure stdlib (hashing embedder).

They pin the judged part: a debit that looks like the sealed envelope diverges
near zero, an unrelated one climbs, a contradicted purpose lifts the figure on its
own, an unattested session reads high but stays a soft figure (never 1.0), and the
result is always a clamped [0, 1] distance — never a verdict. Runnable with
pytest or directly.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agentshield_intent.alignment import (  # noqa: E402
    UNATTESTED_DIVERGENCE,
    divergence,
    purpose_mismatch,
    semantic_distance,
)
from agentshield_intent.embed import HashingEmbedder  # noqa: E402

_emb = HashingEmbedder()


def test_aligned_debit_diverges_near_zero():
    v = _emb.encode("coffee blue tokai")
    assert semantic_distance(v, v) == 0.0
    assert divergence(v, v, "purchase", "purchase") < 0.05


def test_unrelated_debit_diverges_more_than_aligned():
    env = _emb.encode("coffee blue tokai")
    same = divergence(env, _emb.encode("coffee blue tokai"))
    other = divergence(env, _emb.encode("online casino gambling chips"))
    assert other > same
    assert other > 0.3


def test_purpose_mismatch_lifts_on_its_own():
    v = _emb.encode("streaming subscription")
    # Same words, but a purchase where a subscription was declared.
    assert divergence(v, v, "subscription", "purchase") > divergence(v, v, "subscription", "subscription")


def test_purpose_mismatch_needs_two_known_purposes():
    assert purpose_mismatch("purchase", "") == 0.0  # unknown observed → no evidence
    assert purpose_mismatch("", "purchase") == 0.0
    assert purpose_mismatch("purchase", "purchase") == 0.0
    assert purpose_mismatch("purchase", "top-up") > 0.0


def test_unattested_is_high_but_soft():
    assert 0.5 < UNATTESTED_DIVERGENCE < 1.0  # raises risk, never a hard block


def test_divergence_is_clamped():
    a = _emb.encode("aaa")
    b = _emb.encode("zzz different tokens entirely")
    assert 0.0 <= divergence(a, b, "purchase", "top-up") <= 1.0


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
