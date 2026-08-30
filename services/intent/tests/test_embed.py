"""Embedder tests (System Design §12).

The hashing fallback is pure stdlib and always exercised: normalised output, shared
tokens read as similar, disjoint tokens as near-orthogonal. The product embedder
(all-MiniLM-L6-v2) test is skipped unless sentence-transformers is installed — it
would otherwise download the model. Runnable with pytest or directly.
"""

import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agentshield_intent.embed import (  # noqa: E402
    HashingEmbedder,
    SentenceTransformerEmbedder,
    available,
    cosine,
    load_embedder,
)

try:
    import pytest  # noqa: E402
except ImportError:  # pragma: no cover
    pytest = None


def test_hashing_is_normalised_and_deterministic():
    e = HashingEmbedder()
    v = e.encode("coffee blue tokai")
    assert len(v) == 64
    assert abs(math.sqrt(sum(x * x for x in v)) - 1.0) < 1e-9  # unit norm
    assert e.encode("coffee blue tokai") == v  # deterministic


def test_hashing_similar_beats_disjoint():
    e = HashingEmbedder()
    base = e.encode("coffee blue tokai")
    assert cosine(base, e.encode("coffee blue tokai")) == 1.0  # identical
    shared = cosine(base, e.encode("coffee tokai"))
    disjoint = cosine(base, e.encode("gambling casino chips"))
    assert shared > disjoint


def test_cosine_handles_empty_and_mismatch():
    assert cosine([], [1.0]) == 0.0
    assert cosine([1.0, 2.0], [1.0]) == 0.0  # width mismatch → no claimed similarity
    assert cosine([0.0, 0.0], [1.0, 1.0]) == 0.0  # zero vector


def test_load_embedder_can_force_the_fallback():
    assert isinstance(load_embedder(prefer_model=False), HashingEmbedder)


def test_product_embedder_matches_semantics():
    if not available():
        msg = "sentence-transformers not installed"
        if pytest is not None:
            pytest.skip(msg)
        print(f"  (skipped: {msg})")
        return
    e = SentenceTransformerEmbedder()
    coffee = e.encode("a cup of coffee at a cafe")
    assert abs(math.sqrt(sum(x * x for x in coffee)) - 1.0) < 1e-3  # normalised
    near = cosine(coffee, e.encode("buying an espresso from a coffee shop"))
    far = cosine(coffee, e.encode("wiring money to an offshore casino"))
    assert near > far  # a real model sees synonymy the hashing one cannot


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
