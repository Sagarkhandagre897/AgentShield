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
	EventTokenConfirmed  = "token.confirmed"
	EventTokenCancelled  = "token.cancelled"
)

// Payload keys carried in Event.Payload. In-process the values keep their
// natural Go types; a JSON broker adapter would decode numbers as float64 or
// json.Number, which the Payload* readers below normalise.
const (
	PayloadAmountPaise = "amount_paise" // int64
	PayloadNonce       = "nonce"        // string
	PayloadDecision    = "decision"     // string, one of the Decision* values
	PayloadCustomerID  = "customer_id"  // string
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
