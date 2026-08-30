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
