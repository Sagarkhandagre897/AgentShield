"""bus — the Kafka/Redpanda client for the off-clock engines.

Two directions across the seam:

  * consume — an engine subscribes to the topics it learns from (evaluations,
    payments, tokens) and folds each event off the clock.
  * deposit — an engine publishes its one calibrated figure as a
    ``feature.*.deposited`` event on features.v1; the Go feature-materialiser
    (the single writer) merges it into the keyed row. The engine never writes a
    store.

Delivery is at-least-once: the consumer commits an offset only after the handler
returns, and every deposit carries a stable event_id so a redelivery folds once.
confluent_kafka is imported lazily so this module (and schema.py through it)
stays importable for contract tests without the client installed.
"""

from __future__ import annotations

import hashlib
from typing import Callable, Iterable, Sequence

from . import schema
from .schema import Event, SignalDeviation

try:  # the client is only needed to actually talk to a broker
    from confluent_kafka import Consumer as _Consumer
    from confluent_kafka import Producer as _Producer

    _HAVE_KAFKA = True
except ImportError:  # pragma: no cover - exercised only where the lib is absent
    _Consumer = _Producer = None
    _HAVE_KAFKA = False


def _require_kafka() -> None:
    if not _HAVE_KAFKA:
        raise RuntimeError(
            "confluent-kafka is not installed; `pip install -e services/shared` to talk to Redpanda"
        )


def _deposit_id(event_type: str, feature_key: str, occurred_at: int) -> str:
    """A stable event_id for one (type, key, computation), so an at-least-once
    redelivery folds exactly once on the Go side."""
    return hashlib.sha1(f"{event_type}:{feature_key}:{occurred_at}".encode()).hexdigest()


def build_behaviour_event(
    feature_key: str,
    deviation: float,
    occurred_at: int,
    signals: Sequence[SignalDeviation] | None = None,
    token_id: str = "",
) -> Event:
    """Construct a feature.behaviour.deposited event (mirror
    bus.FeatureBehaviourDepositEvent)."""
    return Event(
        event_id=_deposit_id(schema.EVENT_FEATURE_BEHAVIOUR, feature_key, occurred_at),
        type=schema.EVENT_FEATURE_BEHAVIOUR,
        token_id=token_id,
        occurred_at=occurred_at,
        source="behaviour-engine",
        payload={
            schema.PAYLOAD_FEATURE_KEY: feature_key,
            schema.PAYLOAD_DEVIATION: deviation,
            schema.PAYLOAD_SIGNAL_DEVIATIONS: [s.to_dict() for s in (signals or [])],
        },
    )


def build_intent_event(feature_key: str, divergence: float, occurred_at: int, token_id: str = "") -> Event:
    """Construct a feature.intent.deposited event (mirror bus.FeatureIntentDepositEvent)."""
    return Event(
        event_id=_deposit_id(schema.EVENT_FEATURE_INTENT, feature_key, occurred_at),
        type=schema.EVENT_FEATURE_INTENT,
        token_id=token_id,
        occurred_at=occurred_at,
        source="intent-engine",
        payload={schema.PAYLOAD_FEATURE_KEY: feature_key, schema.PAYLOAD_DIVERGENCE: divergence},
    )


def build_network_event(feature_key: str, risk: float, occurred_at: int, token_id: str = "") -> Event:
    """Construct a feature.network.deposited event (mirror bus.FeatureNetworkDepositEvent)."""
    return Event(
        event_id=_deposit_id(schema.EVENT_FEATURE_NETWORK, feature_key, occurred_at),
        type=schema.EVENT_FEATURE_NETWORK,
        token_id=token_id,
        occurred_at=occurred_at,
        source="graph-engine",
        payload={schema.PAYLOAD_FEATURE_KEY: feature_key, schema.PAYLOAD_RISK: risk},
    )


def build_envelope_sealed_event(
    token_id: str,
    session_id: str,
    occurred_at: int,
    raw_instruction: str = "",
    contact: str = "",
) -> Event:
    """Construct an envelope.sealed event — the one PII-bearing message on the bus
    (mirror bus.EnvelopeSealedEvent). session_id is the VAULT key (in the payload);
    token_id is the partition key AND is required — the Go stream-processor drops an
    event with an empty token_id, so a seal with none would silently never land.
    raw_instruction and contact are the plaintext the stream-processor seals field-by-
    field into the encrypted VAULT; an empty field is simply not sealed. The event_id
    is stable for a (session, instant) so an at-least-once redelivery folds once."""
    return Event(
        event_id=_deposit_id(schema.EVENT_ENVELOPE_SEALED, session_id, occurred_at),
        type=schema.EVENT_ENVELOPE_SEALED,
        token_id=token_id,
        occurred_at=occurred_at,
        source="intent-engine",
        payload={
            schema.PAYLOAD_SESSION_ID: session_id,
            schema.PAYLOAD_RAW_INSTRUCTION: raw_instruction,
            schema.PAYLOAD_CONTACT: contact,
        },
    )


class DepositPublisher:
    """The off-clock producer client. Its main job is publishing an engine's
    calibrated figures to features.v1 (the up meeting point, merged by the Go
    feature-materialiser — the single writer). It also carries the one PII-bearing
    event, envelope.sealed, to vault.v1, so a producer or the demo generator can seed
    the encrypted VAULT through the same client. The record key is token_id when set,
    else the feature_key, so an entity's messages keep their order on one partition."""

    def __init__(self, seeds: str):
        _require_kafka()
        self._p = _Producer({"bootstrap.servers": seeds})

    def _publish(self, ev: Event) -> None:
        topic = schema.topic_for(ev.type)
        if topic is None:
            raise ValueError(f"no topic for event type {ev.type!r}")
        key = ev.token_id or ev.payload.get(schema.PAYLOAD_FEATURE_KEY, "")
        self._p.produce(topic, key=key.encode("utf-8"), value=ev.to_json())

    def deposit_behaviour(
        self,
        feature_key: str,
        deviation: float,
        occurred_at: int,
        signals: Sequence[SignalDeviation] | None = None,
        token_id: str = "",
    ) -> None:
        self._publish(build_behaviour_event(feature_key, deviation, occurred_at, signals, token_id))

    def deposit_intent(self, feature_key: str, divergence: float, occurred_at: int, token_id: str = "") -> None:
        self._publish(build_intent_event(feature_key, divergence, occurred_at, token_id))

    def deposit_network(self, feature_key: str, risk: float, occurred_at: int, token_id: str = "") -> None:
        self._publish(build_network_event(feature_key, risk, occurred_at, token_id))

    def seal_envelope(
        self,
        token_id: str,
        session_id: str,
        occurred_at: int,
        raw_instruction: str = "",
        contact: str = "",
    ) -> None:
        """Publish an envelope.sealed event to vault.v1 so the Go stream-processor
        seals the session's raw PII into the encrypted VAULT. token_id is required
        (the processor drops events with none)."""
        self._publish(build_envelope_sealed_event(token_id, session_id, occurred_at, raw_instruction, contact))

    def flush(self, timeout: float = 10.0) -> int:
        """Block until queued deposits are acknowledged; returns messages still
        in flight (0 when all delivered)."""
        return self._p.flush(timeout)


class EventConsumer:
    """Subscribes a consumer group to the given topics and folds each event with
    at-least-once semantics: offsets commit only after the handler returns."""

    def __init__(self, seeds: str, group: str, topics: Iterable[str]):
        _require_kafka()
        self._c = _Consumer(
            {
                "bootstrap.servers": seeds,
                "group.id": group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self._c.subscribe(list(topics))

    def run(self, handler: Callable[[Event], None], poll_timeout: float = 1.0) -> None:
        """Poll forever, folding each decoded event. A handler that raises leaves
        the offset uncommitted, so the event is redelivered — safe because the Go
        handlers dedupe on event_id. An undecodable message is committed and
        skipped rather than wedging the partition."""
        while True:
            msg = self._c.poll(poll_timeout)
            if msg is None:
                continue
            if msg.error():
                continue
            try:
                ev = Event.from_json(msg.value())
            except (ValueError, TypeError):
                self._c.commit(msg, asynchronous=False)
                continue
            handler(ev)
            self._c.commit(msg, asynchronous=False)

    def close(self) -> None:
        self._c.close()
