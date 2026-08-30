"""agentshield_intent — the intent-alignment engine (System Design §12).

Seal the user's instruction once, then judge every debit against that sealed
fingerprint — all on the off-clock plane, leaving one soft intent_divergence on
the shelf for the request to read by key:

    envelope  — the structured intent { purpose, category, max_amount_paise,
                merchant_preference, constraints } and its stable digest.
    embed     — all-MiniLM-L6-v2 via sentence-transformers, imported lazily, with
                a deterministic hashing fallback so the engine runs torch-free.
    alignment — cosine distance on the embeddings + a small purpose classifier →
                a soft intent_divergence in [0, 1]; a session with no envelope is
                unattested (raises risk), never innocent.
    engine    — seal once per session, score each debit, deposit one figure.

The exact part of alignment — amount within the stated maximum, explicit
constraints — is a deterministic check on the clock (predicate P3 and policy),
not this engine's concern. The language model only extracts and embeds; its
output is a feature, never a verdict.
"""

from .alignment import UNATTESTED_DIVERGENCE, divergence, purpose_mismatch, semantic_distance
from .embed import Embedder, HashingEmbedder, SentenceTransformerEmbedder, available, cosine, load_embedder
from .engine import Deposit, IntentEngine
from .envelope import IntentEnvelope, SealedEnvelope, normalise_purpose

__all__ = [
    "UNATTESTED_DIVERGENCE",
    "divergence",
    "purpose_mismatch",
    "semantic_distance",
    "Embedder",
    "HashingEmbedder",
    "SentenceTransformerEmbedder",
    "available",
    "cosine",
    "load_embedder",
    "Deposit",
    "IntentEngine",
    "IntentEnvelope",
    "SealedEnvelope",
    "normalise_purpose",
]
