// Package bus is the asynchronous plane's spine — the durable event bus the two
// planes meet at, going down (System Design §3, §11). The synchronous plane
// publishes to it and never reads back; the off-clock workers subscribe and fold
// events into store state and feature rows.
//
// token_id is the partition key: it is what guarantees that everything about one
// mandate is processed in order. Delivery is at-least-once, so every handler MUST
// be idempotent on event_id — the bus may redeliver, and a real broker
// (Kafka/Redpanda) will on rebalance or retry.
//
// The interface lives here; the in-memory broker that keeps the plane runnable
// without a broker lives in the memory sub-package, and a Kafka adapter lands
// behind the same interface.
package bus

import (
	"context"
	"encoding/json"

	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
)

// Known event types carried on the bus. token_id is always the partition key;
// the type selects which workers act on the event.
const (
	EventDecisionMade    = "decision.made"    // one per Evaluate, emitted after the reply
	EventPaymentCaptured = "payment.captured" // money actually moved — the only source of truth for consumption
	EventPaymentFailed   = "payment.failed"
	EventPaymentDisputed = "payment.disputed" // chargeback/dispute — the strongest settled negative, arrives on settlement lag
	EventTokenConfirmed  = "token.confirmed"
	EventTokenCancelled  = "token.cancelled"

	// EventEnvelopeSealed carries a session's raw PII toward the VAULT, once per
	// session (§9, §12). It is the ONE PII-bearing event on the bus: the intent
	// envelope's raw instruction text and the contact behind the session, which the
	// stream-processor (the single VAULT writer, per the architecture diagram) seals
	// into the encrypted, erasable store off the clock. Only the envelope DIGEST ever
	// rides a request; the raw text travels here exactly once and then lives sealed.
	// It has its own topic (vault.v1) to keep raw PII off the analytic topics.
	EventEnvelopeSealed = "envelope.sealed"

	// EventErasureRequested is a DPDP right-to-erasure request for a session's
	// sealed PII (§9). It carries NO plaintext — only the session_id whose rows the
	// VAULT must delete and whose data key it must shred (crypto-shredding, which is
	// what makes the deletion real: a backup that still holds the ciphertext becomes
	// undecryptable once the key is gone). It rides the same vault.v1 channel as
	// envelope.sealed and is keyed on the mandate's token_id, so it folds in order
	// AFTER that session's seals on the one partition — a late seal can never
	// resurrect erased PII behind it. The stream-processor (the single VAULT writer)
	// is the sole consumer; erasure is an off-clock operation, never on the request
	// path. An operator/console originates it (cmd/dpdp-erase); nothing else does.
	EventErasureRequested = "erasure.requested"

	// EventOutcomeLabeled is the labeler's output on outcomes.v1 (§6). It is the
	// ONLY event that carries a training label, and it is emitted off the clock
	// by the labeler, which distils the settled payment/token lifecycle into one
	// label per settled outcome. A label may come only from a settled outcome —
	// a dispute, a cancellation, a confirmed step-up — never from "no complaint
	// arrived," and never from our own past verdicts, which would teach the
	// models to agree with yesterday's mistakes.
	EventOutcomeLabeled = "outcome.labeled"

	// Feature-deposit events — the up meeting point for the off-clock ML engines
	// (§8, §10). An engine computes its one calibrated figure off the clock and
	// publishes it here; the feature-materialiser (the single writer) consumes it
	// and merges the field into the keyed row. This is §10 verbatim: "new engines
	// add columns to the feature row and types to the event envelope — not new
	// objects on the hot path." The engine never writes the store itself.
	EventFeatureBehaviour = "feature.behaviour.deposited" // behaviour engine  → behaviour_deviation (§11)
	EventFeatureIntent    = "feature.intent.deposited"    // intent engine     → intent_divergence  (§12)
	EventFeatureNetwork   = "feature.network.deposited"   // graph engine      → network_risk       (§13)
)

// Payload keys carried in Event.Payload. In-process the values keep their
// natural Go types; a JSON broker adapter would decode numbers as float64 or
// json.Number, which the Payload* readers below normalise.
const (
	PayloadAmountPaise = "amount_paise" // int64
	PayloadNonce       = "nonce"        // string
	PayloadDecision    = "decision"     // string, one of the Decision* values
	PayloadCustomerID  = "customer_id"  // string
	PayloadAgentID     = "agent_id"     // string; who a settled outcome is attributed to (reputation)

	// Feature-deposit payload keys. A deposit is keyed by the entity the figure
	// belongs to (a customer / token / agent / merchant / node id), carried in
	// feature_key — never in token_id, which is the partition key for money and
	// mandate events. The figure itself and its computed_at travel alongside.
	PayloadFeatureKey       = "feature_key"       // string; the entity id the figure lands on
	PayloadDeviation        = "deviation"         // float64; behaviour_deviation
	PayloadDivergence       = "divergence"        // float64; intent_divergence
	PayloadRisk             = "risk"              // float64; network_risk
	PayloadSignalDeviations = "signal_deviations" // []domain.SignalDeviation; per-signal breakdown (behaviour)

	// Outcome-label payload keys. A label event is keyed (like every bus message)
	// on token_id for per-token ordering; these carry the label itself, how much
	// to trust it, and why it was assigned so a trainer can filter by reason.
	PayloadLabel  = "label"  // float64; 1.0 = misuse, 0.0 = legitimate — the training target
	PayloadWeight = "weight" // float64; confidence in [0,1] — a dispute weighs full, a bare cancellation less
	PayloadReason = "reason" // string; one of the Reason* values below

	// Provenance payload keys — the rest of the ProvenanceRecord a decision.made
	// event carries so the stream-processor (the CHAIN's single writer, per the
	// architecture diagram) can rebuild the full record off the clock and append
	// it, without the decision service ever touching the ledger. evaluation_id,
	// decision and timestamp already ride the envelope (EventID / PayloadDecision
	// / OccurredAt); these are what remains. request_digest is a hash of the order
	// computed on the clock, so the raw request never travels the bus — only its
	// fingerprint, enough to bind the audit record to what was asked.
	PayloadCode            = "code"             // string; the pb.Code verdict reason
	PayloadPredicateFailed = "predicate_failed" // string; which of P1-P6 refused, if any
	PayloadPolicyVersion   = "policy_version"   // int; the overlay version in force
	PayloadRequestDigest   = "request_digest"   // string; SHA-256 fingerprint of the order
	PayloadEvidenceDigest  = "evidence_digest"  // string; fingerprint of the evidence scored

	// Envelope-sealing payload keys — the raw PII an envelope.sealed event carries
	// to the VAULT. session_id is the VAULT key (the store is keyed by session, not
	// token); the two field values are the plaintext the stream-processor seals, each
	// under its vault.Field. Both field values are optional — a session may seal an
	// instruction with no contact on file — but session_id is required to key the row.
	PayloadSessionID      = "session_id"           // string; the VAULT key
	PayloadRawInstruction = "raw_instruction_text" // string; the raw purpose text the LLM read (vault.FieldInstruction)
	PayloadContact        = "contact"              // string; the contact behind the session (vault.FieldContact)
)

// Label values a settled outcome resolves to. 1.0 is the positive (misuse)
// class the models are trained to raise risk on; 0.0 is a confirmed-legitimate
// outcome. There is deliberately no "unknown" label — a lifecycle event that
// does not settle into one of these produces no label at all.
const (
	LabelMisuse = 1.0
	LabelLegit  = 0.0
)

// Reasons a label was assigned — the settled outcome it came from. Each is an
// external fact about what happened to the money or the mandate, independent of
// what we decided.
const (
	ReasonDispute         = "dispute"           // a chargeback/dispute — the strongest settled negative
	ReasonCancellation    = "cancellation"      // the mandate was pulled — a soft negative, weighed lightly
	ReasonConfirmedStepUp = "confirmed_step_up" // the human passed a step-up and money then moved — a legitimate outcome
)

// Decision payload values mirror the verdict answers as plain strings, so the
// bus contract stays free of the generated protobuf package.
const (
	DecisionAllow  = "ALLOW"
	DecisionStepUp = "STEP_UP"
	DecisionBlock  = "BLOCK"
)

// DecisionMadeEvent builds the event the decision service emits after each
// reply. amountPaise and the nonce let the stream-processor spend the nonce on
// an ALLOW; the decision string gates that.
func DecisionMadeEvent(eventID, tokenID string, occurredAt int64, decision, nonce string, amountPaise int64) domain.Event {
	return domain.Event{
		EventID:    eventID,
		Type:       EventDecisionMade,
		TokenID:    tokenID,
		OccurredAt: occurredAt,
		Source:     "decision",
		Payload: map[string]any{
			PayloadDecision:    decision,
			PayloadNonce:       nonce,
			PayloadAmountPaise: amountPaise,
		},
	}
}

// PaymentCapturedEvent builds the event a settlement webhook emits when money
// actually moved — the only source of truth for consumption.
func PaymentCapturedEvent(eventID, tokenID string, occurredAt, amountPaise int64, nonce string) domain.Event {
	return domain.Event{
		EventID:    eventID,
		Type:       EventPaymentCaptured,
		TokenID:    tokenID,
		OccurredAt: occurredAt,
		Source:     "webhook",
		Payload: map[string]any{
			PayloadAmountPaise: amountPaise,
			PayloadNonce:       nonce,
		},
	}
}

// PaymentFailedEvent builds the event for an attempt that did not move money.
// It carries no consumption; consumption only ever advances on capture.
func PaymentFailedEvent(eventID, tokenID string, occurredAt int64, nonce string) domain.Event {
	return domain.Event{
		EventID:    eventID,
		Type:       EventPaymentFailed,
		TokenID:    tokenID,
		OccurredAt: occurredAt,
		Source:     "webhook",
		Payload:    map[string]any{PayloadNonce: nonce},
	}
}

// PaymentDisputedEvent builds the event for a chargeback or dispute — the
// strongest settled negative for reputation, and one that arrives late.
func PaymentDisputedEvent(eventID, tokenID string, occurredAt int64, nonce string) domain.Event {
	return domain.Event{
		EventID:    eventID,
		Type:       EventPaymentDisputed,
		TokenID:    tokenID,
		OccurredAt: occurredAt,
		Source:     "webhook",
		Payload:    map[string]any{PayloadNonce: nonce},
	}
}

// EnvelopeSealedEvent builds the once-per-session event that carries a session's
// raw PII to the VAULT. sessionID is the VAULT key (in the payload); tokenID is the
// partition key, so a mandate's sealing lands in order with its other events. The
// raw instruction text and contact are optional plaintext the stream-processor
// seals field-by-field; an empty field is simply not sealed. This is the only event
// that carries raw PII, and it is consumed by exactly one worker — the VAULT writer.
func EnvelopeSealedEvent(eventID, tokenID, sessionID string, occurredAt int64, rawInstruction, contact string) domain.Event {
	return domain.Event{
		EventID:    eventID,
		Type:       EventEnvelopeSealed,
		TokenID:    tokenID,
		OccurredAt: occurredAt,
		Source:     "intent-engine",
		Payload: map[string]any{
			PayloadSessionID:      sessionID,
			PayloadRawInstruction: rawInstruction,
			PayloadContact:        contact,
		},
	}
}

// ErasureRequestedEvent builds a DPDP right-to-erasure request for a session's
// sealed PII. sessionID is the VAULT key to erase (rows deleted, key shredded);
// tokenID is the partition key, so the erasure lands in order after that mandate's
// seals on the one partition. It carries NO plaintext — a deletion request names
// what to forget, never the data itself. The stream-processor (the single VAULT
// writer) is the sole consumer; the operator entrypoint (cmd/dpdp-erase) publishes
// it off the clock.
func ErasureRequestedEvent(eventID, tokenID, sessionID string, occurredAt int64) domain.Event {
	return domain.Event{
		EventID:    eventID,
		Type:       EventErasureRequested,
		TokenID:    tokenID,
		OccurredAt: occurredAt,
		Source:     "dpdp-erase",
		Payload: map[string]any{
			PayloadSessionID: sessionID,
		},
	}
}

// WithAgent stamps the agent a settled outcome is attributed to; the
// reputation-builder keys on it. The payment webhook sets it from the order
// record. Other producers may leave it unset, and consumers that do not need it
// (the stream-processor, the materialiser) ignore it.
func WithAgent(ev domain.Event, agentID string) domain.Event {
	if ev.Payload == nil {
		ev.Payload = map[string]any{}
	}
	ev.Payload[PayloadAgentID] = agentID
	return ev
}

// WithProvenance decorates a decision.made event with the provenance fields the
// stream-processor needs to rebuild the full domain.ProvenanceRecord and append
// it to the CHAIN off the clock (mirroring WithAgent). The record's evaluation_id,
// decision and timestamp already ride the event envelope (EventID / PayloadDecision
// / OccurredAt), so only the remaining fields are stamped here; empty optional
// fields are omitted. PrevHash is not carried — the CHAIN stamps it at append time.
func WithProvenance(ev domain.Event, rec *domain.ProvenanceRecord) domain.Event {
	if rec == nil {
		return ev
	}
	if ev.Payload == nil {
		ev.Payload = map[string]any{}
	}
	ev.Payload[PayloadCode] = rec.Code
	ev.Payload[PayloadPolicyVersion] = rec.PolicyVersion
	if rec.PredicateFailed != "" {
		ev.Payload[PayloadPredicateFailed] = rec.PredicateFailed
	}
	if rec.RequestDigest != "" {
		ev.Payload[PayloadRequestDigest] = rec.RequestDigest
	}
	if rec.EvidenceDigest != "" {
		ev.Payload[PayloadEvidenceDigest] = rec.EvidenceDigest
	}
	return ev
}

// ProvenanceFromEvent rebuilds the domain.ProvenanceRecord a decision.made event
// carries: the evaluation_id, decision and timestamp come from the envelope, the
// rest from the payload WithProvenance stamped. PrevHash is left empty — the CHAIN
// stamps the link when it appends. This is the read side of the reply-then-record
// seam: the decision service publishes, the stream-processor reconstructs and writes.
func ProvenanceFromEvent(ev domain.Event) *domain.ProvenanceRecord {
	decision, _ := PayloadString(ev, PayloadDecision)
	code, _ := PayloadString(ev, PayloadCode)
	predicateFailed, _ := PayloadString(ev, PayloadPredicateFailed)
	requestDigest, _ := PayloadString(ev, PayloadRequestDigest)
	evidenceDigest, _ := PayloadString(ev, PayloadEvidenceDigest)
	policyVersion, _ := PayloadInt64(ev, PayloadPolicyVersion)
	return &domain.ProvenanceRecord{
		EvaluationID:    ev.EventID,
		RequestDigest:   requestDigest,
		Decision:        decision,
		Code:            code,
		PredicateFailed: predicateFailed,
		EvidenceDigest:  evidenceDigest,
		PolicyVersion:   int(policyVersion),
		TS:              ev.OccurredAt,
	}
}

// FeatureBehaviourDepositEvent carries a behaviour engine's calibrated deviation
// (and its per-signal breakdown) toward the materialiser. featureKey is the
// entity the figure lands on (an agent / customer id); occurredAt is the figure's
// computed_at. event_id must be stable for the (key, computation) so redelivery
// folds once. tokenID may be "" for a non-token entity; it is only a partition
// hint here, not the deposit key.
func FeatureBehaviourDepositEvent(eventID, tokenID, featureKey string, occurredAt int64, deviation float64, signals []domain.SignalDeviation) domain.Event {
	return domain.Event{
		EventID: eventID, Type: EventFeatureBehaviour, TokenID: tokenID,
		OccurredAt: occurredAt, Source: "behaviour-engine",
		Payload: map[string]any{
			PayloadFeatureKey:       featureKey,
			PayloadDeviation:        deviation,
			PayloadSignalDeviations: signals,
		},
	}
}

// FeatureIntentDepositEvent carries the intent engine's divergence figure toward
// the materialiser, keyed by featureKey (a token / session-scoped entity id).
func FeatureIntentDepositEvent(eventID, tokenID, featureKey string, occurredAt int64, divergence float64) domain.Event {
	return domain.Event{
		EventID: eventID, Type: EventFeatureIntent, TokenID: tokenID,
		OccurredAt: occurredAt, Source: "intent-engine",
		Payload: map[string]any{PayloadFeatureKey: featureKey, PayloadDivergence: divergence},
	}
}

// FeatureNetworkDepositEvent carries the graph engine's network-risk figure toward
// the materialiser, keyed by featureKey (a node id — a stable identifier, never a
// reassignable handle, §13).
func FeatureNetworkDepositEvent(eventID, tokenID, featureKey string, occurredAt int64, risk float64) domain.Event {
	return domain.Event{
		EventID: eventID, Type: EventFeatureNetwork, TokenID: tokenID,
		OccurredAt: occurredAt, Source: "graph-engine",
		Payload: map[string]any{PayloadFeatureKey: featureKey, PayloadRisk: risk},
	}
}

// OutcomeLabeledEvent builds the labeler's output on outcomes.v1: one training
// label distilled from a settled outcome. tokenID keys it (per-token ordering);
// occurredAt is the settling event's time, which the trainer uses for a
// point-in-time join back to the feature vector as it stood then. label is the
// target (LabelMisuse / LabelLegit), weight the confidence, reason the settled
// outcome it came from. Any agent_id / customer_id carried by the settling event
// rides along so the trainer can attribute the label without another lookup.
func OutcomeLabeledEvent(eventID, tokenID string, occurredAt int64, label, weight float64, reason string) domain.Event {
	return domain.Event{
		EventID:    eventID,
		Type:       EventOutcomeLabeled,
		TokenID:    tokenID,
		OccurredAt: occurredAt,
		Source:     "labeler",
		Payload: map[string]any{
			PayloadLabel:  label,
			PayloadWeight: weight,
			PayloadReason: reason,
		},
	}
}

// PayloadInt64 reads an integer payload value, normalising the int64 / int /
// float64 / json.Number forms a broker may deliver. The second result is false
// when the key is absent or not a number.
func PayloadInt64(ev domain.Event, key string) (int64, bool) {
	switch v := ev.Payload[key].(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case float64:
		return int64(v), true
	case json.Number:
		n, err := v.Int64()
		return n, err == nil
	default:
		return 0, false
	}
}

// PayloadString reads a string payload value. The second result is false when
// the key is absent or not a string.
func PayloadString(ev domain.Event, key string) (string, bool) {
	s, ok := ev.Payload[key].(string)
	return s, ok
}

// PayloadFloat64 reads a floating-point payload value, normalising the float64 /
// float32 / int / int64 / json.Number forms a broker may deliver. The second
// result is false when the key is absent or not a number.
func PayloadFloat64(ev domain.Event, key string) (float64, bool) {
	switch v := ev.Payload[key].(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

// PayloadSignals reads the per-signal deviation breakdown. In-process the value
// keeps its Go type; a JSON broker delivers it as []any of maps, which this
// re-marshals through the SignalDeviation shape. A missing or malformed value
// yields nil — the row simply carries no breakdown.
func PayloadSignals(ev domain.Event) []domain.SignalDeviation {
	raw, ok := ev.Payload[PayloadSignalDeviations]
	if !ok || raw == nil {
		return nil
	}
	if s, ok := raw.([]domain.SignalDeviation); ok {
		return s
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var out []domain.SignalDeviation
	if json.Unmarshal(b, &out) != nil {
		return nil
	}
	return out
}

// Handler processes one delivered event. Returning a non-nil error asks the bus
// to redeliver the event; because delivery is at-least-once, a handler must be
// idempotent on ev.EventID.
type Handler func(ctx context.Context, ev domain.Event) error

// Bus is the publish/subscribe boundary. Each Subscribe registers an independent
// consumer group: every group sees every event (fan-out), and within a group
// events are delivered in per-partition (token_id) order.
type Bus interface {
	// Publish sends one event to every current subscriber, preserving order.
	Publish(ctx context.Context, ev domain.Event) error
	// Subscribe registers a consumer group by name and returns a cancel func
	// that stops it.
	Subscribe(group string, h Handler) (cancel func(), err error)
	// Close stops all delivery and releases resources.
	Close() error
}
