"""schema — the wire contract mirror.

These dataclasses are the Python side of the Go ``internal/domain`` and
``internal/bus`` packages. The field names below are the JSON keys the Go
structs carry (their ``json:"..."`` tags), so a message an engine publishes here
decodes straight into ``domain.Event`` / ``domain.FeatureRow`` on the Go side and
is read by ``bus.PayloadFloat64`` / ``bus.PayloadSignals`` without translation.

Keeping this in pure stdlib (no confluent_kafka import) means the contract is
importable and testable without a broker on the box.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

# --- Bus event types (mirror internal/bus/bus.go) --------------------------
EVENT_DECISION_MADE = "decision.made"
EVENT_PAYMENT_CAPTURED = "payment.captured"
EVENT_PAYMENT_FAILED = "payment.failed"
EVENT_PAYMENT_DISPUTED = "payment.disputed"
EVENT_TOKEN_CONFIRMED = "token.confirmed"
EVENT_TOKEN_CANCELLED = "token.cancelled"
EVENT_FEATURE_BEHAVIOUR = "feature.behaviour.deposited"
EVENT_FEATURE_INTENT = "feature.intent.deposited"
EVENT_FEATURE_NETWORK = "feature.network.deposited"
EVENT_OUTCOME_LABELED = "outcome.labeled"

# --- Payload keys (mirror internal/bus/bus.go) -----------------------------
PAYLOAD_AMOUNT_PAISE = "amount_paise"
PAYLOAD_NONCE = "nonce"
PAYLOAD_DECISION = "decision"
PAYLOAD_CUSTOMER_ID = "customer_id"
PAYLOAD_AGENT_ID = "agent_id"
PAYLOAD_FEATURE_KEY = "feature_key"
PAYLOAD_DEVIATION = "deviation"
PAYLOAD_DIVERGENCE = "divergence"
PAYLOAD_RISK = "risk"
PAYLOAD_SIGNAL_DEVIATIONS = "signal_deviations"
PAYLOAD_LABEL = "label"
PAYLOAD_WEIGHT = "weight"
PAYLOAD_REASON = "reason"

# --- Label values and reasons (mirror internal/bus/bus.go) -----------------
LABEL_MISUSE = 1.0
LABEL_LEGIT = 0.0
REASON_DISPUTE = "dispute"
REASON_CANCELLATION = "cancellation"
REASON_CONFIRMED_STEP_UP = "confirmed_step_up"

# --- Topics (mirror internal/bus/kafka/kafka.go) ---------------------------
TOPIC_EVALUATIONS = "evaluations.v1"
TOPIC_PAYMENTS = "payments.v1"
TOPIC_TOKENS = "tokens.v1"
TOPIC_FEATURES = "features.v1"
TOPIC_OUTCOMES = "outcomes.v1"

@dataclass
class SignalDeviation:
    """One per-signal deviation with the observation count it was computed from
    (mirror domain.SignalDeviation)."""

    signal: str
    deviation: float
    obs_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {"signal": self.signal, "deviation": self.deviation, "obs_count": self.obs_count}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "SignalDeviation":
        return cls(
            signal=d.get("signal", ""),
            deviation=float(d.get("deviation", 0.0)),
            obs_count=int(d.get("obs_count", 0)),
        )


@dataclass
class Event:
    """The bus envelope (mirror domain.Event). token_id is the partition key;
    consumers dedupe on event_id."""

    event_id: str
    type: str
    token_id: str = ""
    occurred_at: int = 0
    payload: dict[str, Any] = field(default_factory=dict)
    source: str = ""
    hmac: str = ""

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "event_id": self.event_id,
            "type": self.type,
            "token_id": self.token_id,
            "occurred_at": self.occurred_at,
            "payload": self.payload,
            "source": self.source,
        }
        if self.hmac:  # omitempty on the Go side
            d["hmac"] = self.hmac
        return d

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), separators=(",", ":")).encode("utf-8")

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Event":
        return cls(
            event_id=d.get("event_id", ""),
            type=d.get("type", ""),
            token_id=d.get("token_id", ""),
            occurred_at=int(d.get("occurred_at", 0)),
            payload=d.get("payload") or {},
            source=d.get("source", ""),
            hmac=d.get("hmac", ""),
        )

    @classmethod
    def from_json(cls, raw: bytes | str) -> "Event":
        return cls.from_dict(json.loads(raw))


@dataclass
class FeatureRow:
    """The precomputed row the request reads by key (mirror domain.FeatureRow).
    computed_at is always present — it is what makes staleness first-class."""

    key: str
    behaviour_deviation: float = 0.0
    signal_deviations: list[SignalDeviation] = field(default_factory=list)
    intent_divergence: float = 0.0
    network_risk: float = 0.0
    reputation: float = 0.0
    consumption_frac: float = 0.0
    computed_at: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "behaviour_deviation": self.behaviour_deviation,
            "signal_deviations": [s.to_dict() for s in self.signal_deviations],
            "intent_divergence": self.intent_divergence,
            "network_risk": self.network_risk,
            "reputation": self.reputation,
            "consumption_frac": self.consumption_frac,
            "computed_at": self.computed_at,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "FeatureRow":
        return cls(
            key=d.get("key", ""),
            behaviour_deviation=float(d.get("behaviour_deviation", 0.0)),
            signal_deviations=[SignalDeviation.from_dict(s) for s in (d.get("signal_deviations") or [])],
            intent_divergence=float(d.get("intent_divergence", 0.0)),
            network_risk=float(d.get("network_risk", 0.0)),
            reputation=float(d.get("reputation", 0.0)),
            consumption_frac=float(d.get("consumption_frac", 0.0)),
            computed_at=int(d.get("computed_at", 0)),
        )


def topic_for(event_type: str) -> str | None:
    """Map an event type to its topic (mirror kafka.topicFor). Returns None for
    an unknown type."""
    if event_type == EVENT_DECISION_MADE:
        return TOPIC_EVALUATIONS
    if event_type in (EVENT_PAYMENT_CAPTURED, EVENT_PAYMENT_FAILED, EVENT_PAYMENT_DISPUTED):
        return TOPIC_PAYMENTS
    if event_type in (EVENT_TOKEN_CONFIRMED, EVENT_TOKEN_CANCELLED):
        return TOPIC_TOKENS
    if event_type in (EVENT_FEATURE_BEHAVIOUR, EVENT_FEATURE_INTENT, EVENT_FEATURE_NETWORK):
        return TOPIC_FEATURES
    if event_type == EVENT_OUTCOME_LABELED:
        return TOPIC_OUTCOMES
    return None
