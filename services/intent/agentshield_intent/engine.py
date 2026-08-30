"""engine — the intent-alignment engine's async runner (System Design §12).

    "Understand the instruction once; then every debit is a cheap comparison
     against a sealed fingerprint."

The engine splits intent alignment into a slow part done once and a cheap part
done every debit, and neither is on the clock:

  seal(session)   — once per session: embed the structured envelope and keep the
                    vector, the declared purpose and the digest, keyed by session.
                    The raw text is not kept here — it lives in the erasable VAULT.
  observe(debit)  — every debit on evaluations.v1: embed the debit's merchant and
                    category, take cosine distance from the sealed envelope, fold
                    in the small purpose classifier, and deposit ONE soft
                    intent_divergence through the shared publisher. The Go
                    materialiser (the single writer) merges it onto the keyed row.

A session that sealed no envelope is unattested, not innocent: the debit still
gets a deposit, at UNATTESTED_DIVERGENCE, so the absence raises risk rather than
passing quietly. Nothing here decides — the figure is a distance the clock reads.

The figure is keyed on the session the envelope was sealed in (the token id as a
last resort). The structured envelope reaches the engine on the request that
seals it — carried on the decision.made payload under intent_envelope, session-
local and optional, exactly as merchant_id is behaviour-local for the behaviour
engine — so no new cross-plane event type is needed to seal once per session.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Optional

from .alignment import UNATTESTED_DIVERGENCE, divergence
from .embed import Embedder, load_embedder
from .envelope import IntentEnvelope, SealedEnvelope, normalise_purpose

try:  # the shared wire contract; bootstrap onto sys.path when run in-tree
    from agentshield_shared.schema import EVENT_DECISION_MADE, Event
except ModuleNotFoundError:  # pragma: no cover
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))
    from agentshield_shared.schema import EVENT_DECISION_MADE, Event

# Payload keys the engine reads. session_id/category/purpose/merchant_id and the
# sealed envelope are intent-local and optional (not yet on every gate event),
# mirroring how merchant_id is behaviour-local for the behaviour engine.
PAYLOAD_SESSION_ID = "session_id"
PAYLOAD_CATEGORY = "category"
PAYLOAD_PURPOSE = "purpose"
PAYLOAD_MERCHANT_ID = "merchant_id"
PAYLOAD_INTENT_ENVELOPE = "intent_envelope"

# Intent is judged per debit (§12: "session start, evaluations"). Payments carry
# no intent to judge, so — unlike the behaviour engine — we learn only from here.
_LEARN_FROM = {EVENT_DECISION_MADE}
GROUP = "intent-engine"
TOPICS = ("evaluations.v1",)


@dataclass
class Deposit:
    """What the engine computed for one debit — ready for deposit_intent()."""

    feature_key: str
    divergence: float
    occurred_at: int
    token_id: str


def _key_of(ev: Event) -> str:
    """The identity the intent figure lands on: the session the envelope was
    sealed in, else the token as a last key."""
    return ev.payload.get(PAYLOAD_SESSION_ID) or ev.token_id or ""


def _debit_text(merchant: str, category: str) -> str:
    """The words of the debit the embedder compares against the envelope — its
    merchant and category, the soft part. Amount and constraints are the exact
    part and are judged deterministically on the clock, not embedded here."""
    return " · ".join(p for p in (category.strip(), merchant.strip()) if p)


class IntentEngine:
    """The envelope store plus the divergence scorer. Holds one embedder (the
    product model when present, else the deterministic fallback) and the per-
    session sealed envelopes. Fixed-size per session: a vector, a purpose, a
    digest — the raw instruction is never kept here."""

    def __init__(self, embedder: Optional[Embedder] = None):
        self.embedder = embedder if embedder is not None else load_embedder()
        self._sealed: dict[str, SealedEnvelope] = {}

    def seal(self, session_key: str, envelope: IntentEnvelope) -> SealedEnvelope:
        """Seal an envelope for a session — embed its semantic text once and keep
        the vector, purpose and digest. Idempotent per (session, digest): re-
        sealing the same envelope is a no-op, so a redelivered sealing debit does
        not re-embed."""
        existing = self._sealed.get(session_key)
        digest = envelope.digest()
        if existing is not None and existing.digest == digest:
            return existing
        sealed = SealedEnvelope(
            embedding=self.embedder.encode(envelope.semantic_text()),
            purpose=normalise_purpose(envelope.purpose),
            digest=digest,
        )
        self._sealed[session_key] = sealed
        return sealed

    def observe(self, ev: Event) -> Optional[Deposit]:
        """Fold one debit and return the deposit to publish, or None when the
        event is not one we learn from or carries nothing to key on. Seals the
        envelope first when the debit carries one (the once-per-session path)."""
        if ev.type not in _LEARN_FROM:
            return None
        key = _key_of(ev)
        if not key:
            return None

        raw_env = ev.payload.get(PAYLOAD_INTENT_ENVELOPE)
        if isinstance(raw_env, dict):
            self.seal(key, IntentEnvelope.from_dict(raw_env))

        sealed = self._sealed.get(key)
        if sealed is None:
            # Unattested: no envelope was ever sealed for this session. The
            # absence is the signal — it raises risk, it is not passed as aligned.
            return Deposit(key, UNATTESTED_DIVERGENCE, ev.occurred_at, ev.token_id)

        merchant = ev.payload.get(PAYLOAD_MERCHANT_ID) or ""
        category = ev.payload.get(PAYLOAD_CATEGORY) or ""
        observed_purpose = normalise_purpose(ev.payload.get(PAYLOAD_PURPOSE) or "")
        debit_vec = self.embedder.encode(_debit_text(merchant, category))
        div = divergence(sealed.embedding, debit_vec, sealed.purpose, observed_purpose)
        return Deposit(key, div, ev.occurred_at, ev.token_id)

    def run(self, seeds: str, group: str = GROUP) -> None:
        """Consume the evaluation events forever, depositing one intent_divergence
        per debit. The producer is flushed before the handler returns, so the
        deposit is durable before the source offset commits — at-least-once, and
        the Go materialiser dedupes on the deposit's stable id."""
        from agentshield_shared.bus import DepositPublisher, EventConsumer  # lazy: needs the kafka client

        pub = DepositPublisher(seeds)
        consumer = EventConsumer(seeds, group, list(TOPICS))

        def handle(ev: Event) -> None:
            dep = self.observe(ev)
            if dep is None:
                return
            pub.deposit_intent(dep.feature_key, dep.divergence, dep.occurred_at, dep.token_id)
            pub.flush(10.0)

        try:
            consumer.run(handle)
        finally:
            consumer.close()


def main() -> None:
    seeds = os.environ.get("KAFKA_SEEDS")
    if not seeds:
        raise SystemExit("KAFKA_SEEDS is required (e.g. localhost:19092)")
    # Prefer the product embedder unless explicitly asked to stay torch-free.
    prefer = os.environ.get("INTENT_EMBEDDER", "model").lower() != "hashing"
    engine = IntentEngine(load_embedder(prefer_model=prefer))
    kind = type(engine.embedder).__name__
    print(f"intent-engine: consuming {list(TOPICS)} from {seeds} (embedder={kind})", flush=True)
    engine.run(seeds)


if __name__ == "__main__":  # pragma: no cover
    main()
