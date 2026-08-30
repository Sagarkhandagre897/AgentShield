"""envelope — the sealed intent envelope (System Design §12).

    "Before any tool call, an LLM reads the user's instruction and extracts a
     structured intent envelope: { purpose, category, max_amount_paise,
     merchant_preference, constraints }. A sentence-transformer embeds it, and the
     whole thing is sealed: the envelope_digest is written to order.notes so it
     travels on every later request, and the raw text goes to the erasable VAULT."

This module is the envelope itself and its digest — pure stdlib. The LLM
extraction happens once, upstream (the only place a language model runs); the
engine receives the structured envelope and seals it. The digest is a stable
fingerprint of the sealed content: it is what rides on the request, and it lets a
later debit prove it is being judged against the envelope that was actually
sealed — not a substituted one.

    THE LINE THE LLM NEVER CROSSES — the model extracts and embeds; it never sees
    a policy, a score or a threshold, and it never decides. Its output becomes a
    feature. So this module carries no threshold and reaches no verdict.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List

# The purpose classes the small classifier ranges over (§12: "a small purpose
# classifier — purchase vs subscription vs top-up"). PURPOSE_UNKNOWN is the
# absence of a declared/observed purpose, never a fourth class.
PURPOSE_PURCHASE = "purchase"
PURPOSE_SUBSCRIPTION = "subscription"
PURPOSE_TOPUP = "top-up"
PURPOSE_UNKNOWN = ""
PURPOSES = (PURPOSE_PURCHASE, PURPOSE_SUBSCRIPTION, PURPOSE_TOPUP)


def normalise_purpose(value: str) -> str:
    """Fold a free-text purpose onto one of the known classes, or UNKNOWN. Kept
    deliberately small — the classifier is a hint to the divergence, not a gate."""
    v = (value or "").strip().lower().replace("_", "-")
    if v in PURPOSES:
        return v
    if v in ("buy", "order", "shop", "shopping"):
        return PURPOSE_PURCHASE
    if v in ("subscribe", "recurring", "renewal", "membership"):
        return PURPOSE_SUBSCRIPTION
    if v in ("topup", "recharge", "wallet", "refill"):
        return PURPOSE_TOPUP
    return PURPOSE_UNKNOWN


@dataclass
class IntentEnvelope:
    """What the user actually asked for, structured. max_amount_paise and the
    explicit constraints are checked deterministically on the clock (predicate P3
    and policy) — the engine here judges only the soft, semantic part, so those
    fields travel but are not scored into the divergence."""

    purpose: str = PURPOSE_UNKNOWN
    category: str = ""
    max_amount_paise: int = 0
    merchant_preference: str = ""
    constraints: Dict[str, Any] = field(default_factory=dict)

    def semantic_text(self) -> str:
        """The text the sentence-transformer embeds: the soft, judged part of the
        envelope — what was asked for, in words. The exact fields (max amount,
        constraints) are handled deterministically elsewhere and left out here."""
        parts = [self.purpose, self.category, self.merchant_preference]
        return " · ".join(p for p in (s.strip() for s in parts) if p)

    def canonical(self) -> Dict[str, Any]:
        return {
            "purpose": normalise_purpose(self.purpose),
            "category": self.category.strip().lower(),
            "max_amount_paise": int(self.max_amount_paise),
            "merchant_preference": self.merchant_preference.strip().lower(),
            "constraints": self.constraints,
        }

    def digest(self) -> str:
        """A stable fingerprint of the sealed envelope — the envelope_digest that
        rides on order.notes. Order-independent over the dict fields (sorted keys),
        so the same envelope always seals to the same digest."""
        blob = json.dumps(self.canonical(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "purpose": self.purpose,
            "category": self.category,
            "max_amount_paise": self.max_amount_paise,
            "merchant_preference": self.merchant_preference,
            "constraints": self.constraints,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "IntentEnvelope":
        return cls(
            purpose=str(d.get("purpose", PURPOSE_UNKNOWN)),
            category=str(d.get("category", "")),
            max_amount_paise=int(d.get("max_amount_paise", 0) or 0),
            merchant_preference=str(d.get("merchant_preference", "")),
            constraints=dict(d.get("constraints") or {}),
        )


@dataclass
class SealedEnvelope:
    """The sealed result the engine keeps per session: the embedding of the
    envelope's semantic text, its declared purpose, and the digest. The raw text
    is not kept here — it lives in the erasable VAULT; only the fingerprint and
    the vector stay on the off-clock plane."""

    embedding: List[float]
    purpose: str
    digest: str
