"""scenario — the data model a generated Scenario is made of.

A Scenario is pure, serialisable data: the world the live driver (Phase 7
component 2) will replay against the real system. It has three parts —

  * setup     — the mandates, the customer overlays, and the sealed intent
                envelopes (each carrying the raw PII bound for the VAULT). The
                driver seeds these before any traffic.
  * timeline  — an ordered list of Debits. Each is a full OrderContext (the 12
                fields the gRPC Evaluate takes) plus the generator's ground truth
                and a *settlement recipe*: what the world does in reaction, once
                the live verdict is known.
  * outcomes  — the settled facts that are unconditional (a pulled mandate). The
                per-debit disputes/captures live on each Debit's settlement because
                they depend on what the system actually decided.

The wire dicts below match the Go contracts field-for-field: ``order_context()``
is exactly the proto ``OrderContext``; ``Token``/``Overlay`` mirror
``internal/domain``; the envelope digest is computed the same way the intent engine
computes it (§12), so a debit's ``envelope_digest`` equals what a re-seal of the
same envelope would produce.

Nothing here reaches a verdict or emits a label — it only describes the world and
what is *known to have settled*, leaving the deciding to the live system and the
labelling to the Go labeler (§6).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from . import families

# ---------------------------------------------------------------------------
# The envelope digest — computed exactly as the intent engine does (§12), so the
# fingerprint on the request matches a re-seal of the same structured envelope. A
# parity test pins this against agentshield_intent.envelope.
# ---------------------------------------------------------------------------

_PURPOSES = ("purchase", "subscription", "top-up")


def normalise_purpose(value: str) -> str:
    """Fold a free-text purpose onto one of the known classes, or "" (unknown).
    Mirrors agentshield_intent.envelope.normalise_purpose byte-for-byte."""
    v = (value or "").strip().lower().replace("_", "-")
    if v in _PURPOSES:
        return v
    if v in ("buy", "order", "shop", "shopping"):
        return "purchase"
    if v in ("subscribe", "recurring", "renewal", "membership"):
        return "subscription"
    if v in ("topup", "recharge", "wallet", "refill"):
        return "top-up"
    return ""


def envelope_digest(
    purpose: str,
    category: str,
    max_amount_paise: int,
    merchant_preference: str,
    constraints: Dict[str, Any],
) -> str:
    """The stable fingerprint the intent engine seals (§12): sorted-key JSON over
    the canonical envelope, SHA-256'd. This is what rides on order.notes and lands
    on OrderContext.envelope_digest."""
    canonical = {
        "purpose": normalise_purpose(purpose),
        "category": (category or "").strip().lower(),
        "max_amount_paise": int(max_amount_paise),
        "merchant_preference": (merchant_preference or "").strip().lower(),
        "constraints": constraints or {},
    }
    blob = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Setup objects
# ---------------------------------------------------------------------------


@dataclass
class Token:
    """A mandate to seed into the token store (mirror domain.Token). The
    containment invariant (per-debit ≤ per-day ≤ lifetime) is enforced here, just
    as the Go domain enforces it on every write — a violating token is a generator
    bug, not something the store should have to clamp."""

    token_id: str
    customer_id: str
    type: str
    max_amount_paise: int
    max_per_day_paise: int
    token_ceiling_paise: int
    frequency: str
    expire_at: int
    status: str

    def validate(self) -> None:
        if not (self.max_amount_paise > 0 and self.max_per_day_paise > 0 and self.token_ceiling_paise > 0):
            raise ValueError(f"token {self.token_id}: ceilings must be positive")
        if not (self.max_amount_paise <= self.max_per_day_paise <= self.token_ceiling_paise):
            raise ValueError(
                f"token {self.token_id}: containment violated "
                f"({self.max_amount_paise} <= {self.max_per_day_paise} <= {self.token_ceiling_paise})"
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "token_id": self.token_id,
            "customer_id": self.customer_id,
            "type": self.type,
            "max_amount_paise": self.max_amount_paise,
            "max_per_day_paise": self.max_per_day_paise,
            "token_ceiling_paise": self.token_ceiling_paise,
            "frequency": self.frequency,
            "expire_at": self.expire_at,
            "status": self.status,
        }


@dataclass
class Overlay:
    """A customer's tightening of a token (mirror domain.PolicyOverlay). An overlay
    may only narrow — the generator never emits one that widens a cap, because the
    Go store would reject it and it would not model a real customer control."""

    token_id: str
    allowed_categories: List[str] = field(default_factory=list)
    merchant_rules: Dict[str, str] = field(default_factory=dict)  # merchant_id -> "allow" | "deny"
    per_window_caps: Dict[str, int] = field(default_factory=dict)  # window -> cap in paise
    overlay_version: int = 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "token_id": self.token_id,
            "allowed_categories": self.allowed_categories,
            "merchant_rules": self.merchant_rules,
            "per_window_caps": self.per_window_caps,
            "overlay_version": self.overlay_version,
        }


@dataclass
class SealedEnvelope:
    """A sealed intent envelope for one session (§12). The structured fields are
    what the intent engine embeds and seals; ``envelope_digest`` is the fingerprint
    that rides on every later debit. ``raw_instruction`` and ``contact`` are the raw
    PII the driver seals into the erasable VAULT via an envelope.sealed event — the
    only PII in the whole scenario, and it never rides a request."""

    session_id: str
    purpose: str
    category: str
    max_amount_paise: int
    merchant_preference: str
    raw_instruction: str
    contact: str
    constraints: Dict[str, Any] = field(default_factory=dict)

    def digest(self) -> str:
        return envelope_digest(
            self.purpose, self.category, self.max_amount_paise, self.merchant_preference, self.constraints
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "purpose": self.purpose,
            "category": self.category,
            "max_amount_paise": self.max_amount_paise,
            "merchant_preference": self.merchant_preference,
            "constraints": self.constraints,
            "envelope_digest": self.digest(),
            # PII — bound for the VAULT (vault.FieldInstruction / vault.FieldContact),
            # sealed once by the driver and never placed on an analytic topic.
            "raw_instruction": self.raw_instruction,
            "contact": self.contact,
        }


# ---------------------------------------------------------------------------
# Timeline objects
# ---------------------------------------------------------------------------


@dataclass
class Settlement:
    """What the world does after a debit, conditioned on the LIVE verdict — the
    recipe the driver runs so that every training label the run produces comes from
    a genuine settled outcome (§6), never from the generator asserting one.

      capture_when  — when money actually moves and a payment.captured is emitted:
                        "allow"            only if the system allowed it
                        "allow_or_stepup"  if allowed, or if stepped-up and the
                                           human then passed it (the capture reuses
                                           the debit's nonce, so a step-up it
                                           confirms becomes a LEGIT label)
                        "never"            the debit is meant to be blocked; no money
      then_dispute  — after a capture, a payment.disputed settles later (a
                      chargeback) — the strongest settled negative, a MISUSE label.
      expected_label / expected_reason — the label this settlement is meant to teach
                      the downstream labeler, recorded for the eval notebook only.
    """

    capture_when: str = "never"
    capture_amount_paise: int = 0
    then_dispute: bool = False
    expected_label: Optional[float] = None
    expected_reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "capture_when": self.capture_when,
            "capture_amount_paise": self.capture_amount_paise,
            "then_dispute": self.then_dispute,
            "expected_label": self.expected_label,
            "expected_reason": self.expected_reason,
        }


@dataclass
class Debit:
    """One evaluation the driver sends to gRPC Evaluate, plus the generator's ground
    truth and settlement recipe. ``order_context()`` is exactly the 12 proto fields;
    everything else is metadata the wire never carries."""

    # --- OrderContext (the 12 proto fields) ---
    evaluation_id: str
    token_id: str
    customer_id: str
    agent_id: str
    merchant_id: str
    session_id: str
    amount_paise: int
    cart_hash: str
    envelope_digest: str
    tool_risk: int
    nonce: str
    ts: int

    # --- ground truth (the eval oracle) ---
    is_misuse: bool
    family: str
    expected_decision: str
    expected_code: str

    # --- world reaction ---
    settlement: Settlement = field(default_factory=Settlement)

    # --- driver synchronisation hint ---
    # True when this debit's expected verdict depends on a PRIOR debit's settlement
    # having been folded off the clock (a spent nonce for a replay, accumulated
    # consumption for a bust-out's crossing debit). The driver must let that async
    # fold land before sending this one, or the race would mask the pattern.
    barrier: bool = False

    def order_context(self) -> Dict[str, Any]:
        """Exactly the proto OrderContext, ready for the driver to build a request."""
        return {
            "evaluation_id": self.evaluation_id,
            "token_id": self.token_id,
            "customer_id": self.customer_id,
            "agent_id": self.agent_id,
            "merchant_id": self.merchant_id,
            "session_id": self.session_id,
            "amount_paise": self.amount_paise,
            "cart_hash": self.cart_hash,
            "envelope_digest": self.envelope_digest,
            "tool_risk": self.tool_risk,
            "nonce": self.nonce,
            "ts": self.ts,
        }

    def to_dict(self) -> Dict[str, Any]:
        d = self.order_context()
        d.update(
            {
                "is_misuse": self.is_misuse,
                "family": self.family,
                "expected_decision": self.expected_decision,
                "expected_code": self.expected_code,
                "settlement": self.settlement.to_dict(),
                "barrier": self.barrier,
            }
        )
        return d


# ---------------------------------------------------------------------------
# The whole scenario
# ---------------------------------------------------------------------------


@dataclass
class Scenario:
    """A complete, replayable world. ``tokens``/``overlays``/``envelopes`` are the
    setup; ``timeline`` is the ordered debits; ``cancellations`` names the tokens the
    driver pulls (a token.cancelled settled outcome → a light MISUSE label). ``meta``
    records the seed and knobs so a run is reproducible from the JSON alone."""

    tokens: List[Token] = field(default_factory=list)
    overlays: List[Overlay] = field(default_factory=list)
    envelopes: List[SealedEnvelope] = field(default_factory=list)
    timeline: List[Debit] = field(default_factory=list)
    cancellations: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        """Cheap internal consistency checks — the invariants the Go stores would
        also enforce, caught here so a malformed scenario never reaches a live run."""
        for t in self.tokens:
            t.validate()
        token_ids = {t.token_id for t in self.tokens}
        session_ids = {e.session_id for e in self.envelopes}
        for d in self.timeline:
            if d.token_id not in token_ids:
                raise ValueError(f"debit {d.evaluation_id}: references unknown token {d.token_id}")
            # A bound debit (non-empty digest) must have a real sealed envelope behind it.
            if d.envelope_digest and d.session_id not in session_ids:
                raise ValueError(f"debit {d.evaluation_id}: bound to unknown session {d.session_id}")
        for tid in self.cancellations:
            if tid not in token_ids:
                raise ValueError(f"cancellation references unknown token {tid}")

    def counts_by_family(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for d in self.timeline:
            out[d.family] = out.get(d.family, 0) + 1
        return out

    def to_dict(self) -> Dict[str, Any]:
        return {
            "meta": self.meta,
            "tokens": [t.to_dict() for t in self.tokens],
            "overlays": [o.to_dict() for o in self.overlays],
            "envelopes": [e.to_dict() for e in self.envelopes],
            "cancellations": self.cancellations,
            "timeline": [d.to_dict() for d in self.timeline],
        }

    def to_json(self, indent: Optional[int] = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=False)
