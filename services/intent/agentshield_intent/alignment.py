"""alignment — the judged part of intent (System Design §12).

    "How close is this merchant and category to what was asked? This is cosine
     similarity on the embeddings plus a small purpose classifier (purchase vs
     subscription vs top-up). It produces the intent_divergence figure — a soft
     distance, never a verdict."

Two things are deliberately NOT here. The exact part — amount within the
envelope's stated maximum, explicit constraints — is a crisp comparison, handled
deterministically on the clock as predicate P3 and policy, not by this engine. And
there is no threshold: this produces a distance in [0, 1] that can only raise risk
or ask for a step-up downstream; it never decides.

    NO ENVELOPE MEANS UNATTESTED, NOT INNOCENT — if a session sealed no envelope,
    the debit is unattested: not scored as aligned, not quietly passed. The absence
    raises risk, exactly as a missing figure does elsewhere. That is what
    UNATTESTED_DIVERGENCE encodes — high, but still a soft figure, not a block.
"""

from __future__ import annotations

from .embed import cosine
from .envelope import PURPOSE_UNKNOWN

# A session with no sealed envelope. High — silence is never read as consent —
# but a soft figure, not a 1.0 verdict: the clock still owns the decision.
UNATTESTED_DIVERGENCE = 0.85

# A declared purpose the debit contradicts (purchase where a subscription was
# asked for, say). A hint, not a gate — it lifts the divergence, it does not set it.
_PURPOSE_MISMATCH = 0.5


def semantic_distance(sealed_embedding, debit_embedding) -> float:
    """Cosine similarity mapped to a distance in [0, 1]: identical direction → 0,
    orthogonal → 0.5, opposed → 1. A debit that looks just like the envelope is
    near zero; one pointing elsewhere climbs."""
    return max(0.0, min(1.0, (1.0 - cosine(sealed_embedding, debit_embedding)) / 2.0))


def purpose_mismatch(declared: str, observed: str) -> float:
    """0 when the purposes agree or either is unknown (no evidence either way);
    a fixed penalty when a known observed purpose contradicts a known declared
    one. The classifier is small on purpose — it nudges, it does not judge."""
    if not declared or not observed or declared == PURPOSE_UNKNOWN or observed == PURPOSE_UNKNOWN:
        return 0.0
    return 0.0 if declared == observed else _PURPOSE_MISMATCH


def divergence(sealed_embedding, debit_embedding, declared_purpose: str = "", observed_purpose: str = "") -> float:
    """The intent_divergence figure: the semantic distance and the purpose
    mismatch folded by noisy-OR, so either alone can lift it and neither can
    suppress the other. In [0, 1] — a soft distance the clock reads by key,
    consistent with the behaviour engine's aggregation."""
    sem = semantic_distance(sealed_embedding, debit_embedding)
    pm = purpose_mismatch(declared_purpose, observed_purpose)
    return max(0.0, min(1.0, 1.0 - (1.0 - sem) * (1.0 - pm)))
