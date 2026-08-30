"""embed — the sentence-transformer, with a stdlib fallback (System Design §12).

    "A sentence-transformer (bge-small or all-MiniLM-L6-v2) embeds it."

The real embedder is all-MiniLM-L6-v2 via sentence-transformers. It is imported
lazily and the model is loaded on first use, so importing this module (and the
whole intent engine) never drags in torch. When the wheel or the model is absent,
the engine falls back to a deterministic hashing embedder: a cold-start
stand-in — coarse but real cosine geometry over shared tokens — exactly as the
behaviour engine's Layer-1 aggregate stands in until its calibrated model exists.

Both embedders return an L2-normalised vector, so cosine similarity is just a dot
product and lives in [-1, 1].
"""

from __future__ import annotations

import hashlib
import math
import re
from typing import List, Protocol, Sequence

_TOKEN = re.compile(r"[a-z0-9]+")


def _tokens(text: str) -> List[str]:
    return _TOKEN.findall((text or "").lower())


def cosine(a: Sequence[float], b: Sequence[float]) -> float:
    """Cosine similarity in [-1, 1]. Zero for an empty vector on either side —
    no shared direction, so no claimed similarity."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    return max(-1.0, min(1.0, dot / (na * nb)))


class Embedder(Protocol):
    def encode(self, text: str) -> List[float]:  # pragma: no cover - interface
        ...


class HashingEmbedder:
    """Deterministic fallback: hash each token into a fixed-width vector (signed,
    so cancellation is possible) and L2-normalise. Shared tokens land on the same
    dimensions with the same sign, so overlapping text reads as similar and
    unrelated text as near-orthogonal — enough geometry to exercise the divergence
    pipeline and its tests without downloading a model. Not the product embedder:
    it has no notion of synonymy, only of shared tokens."""

    def __init__(self, dim: int = 64) -> None:
        self.dim = dim

    def encode(self, text: str) -> List[float]:
        vec = [0.0] * self.dim
        for tok in _tokens(text):
            h = hashlib.blake2b(tok.encode("utf-8"), digest_size=8).digest()
            idx = int.from_bytes(h[:4], "big") % self.dim
            sign = 1.0 if (h[4] & 1) else -1.0
            vec[idx] += sign
        norm = math.sqrt(sum(v * v for v in vec))
        if norm <= 0.0:
            return vec
        return [v / norm for v in vec]


class SentenceTransformerEmbedder:
    """The product embedder: all-MiniLM-L6-v2 via sentence-transformers, loaded
    lazily on first encode(). Output is normalised so cosine is a dot product."""

    def __init__(self, model_name: str = "all-MiniLM-L6-v2") -> None:
        self.model_name = model_name
        self._model = None

    def _ensure(self) -> None:
        if self._model is None:
            from sentence_transformers import SentenceTransformer  # lazy: pulls torch

            self._model = SentenceTransformer(self.model_name)

    def encode(self, text: str) -> List[float]:
        self._ensure()
        vec = self._model.encode(text or "", normalize_embeddings=True)
        return [float(x) for x in vec]


def available() -> bool:
    """True when sentence-transformers is importable — the product embedder is
    live. When False the engine falls back to HashingEmbedder rather than crash."""
    try:
        import sentence_transformers  # noqa: F401
    except Exception:
        return False
    return True


def load_embedder(prefer_model: bool = True, model_name: str = "all-MiniLM-L6-v2") -> Embedder:
    """The product embedder when the wheel is present and asked for; otherwise the
    deterministic fallback. Defaults to preferring the model — pass prefer_model
    False to force the stdlib embedder (tests, or a torch-free deployment)."""
    if prefer_model and available():
        return SentenceTransformerEmbedder(model_name)
    return HashingEmbedder()
