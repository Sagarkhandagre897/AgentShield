// Package labeler is the outcomes.v1 labeler: the off-clock worker that distils
// the settled payment and mandate lifecycle into training labels (System Design
// §6). It is the sole producer of outcomes.v1 — the topic the offline trainers
// read to learn from ground truth.
//
// A label may come ONLY from a settled outcome, and only from three of them:
//
//	dispute            a chargeback/dispute — the charge was repudiated.  MISUSE.
//	cancellation       the mandate was pulled — a soft negative.          MISUSE (light).
//	confirmed step-up  a step-up we asked for was passed and money then
//	                   moved — the human affirmatively approved.          LEGITIMATE.
//
// Two rules the design is emphatic about (§6) shape what is deliberately absent:
//
//   - Never from "no complaint arrived." A capture that simply was not disputed
//     is not evidence of legitimacy — the victim may not have noticed yet — so a
//     bare capture produces no label. Only a capture that CONFIRMS a step-up does,
//     because there the human was challenged and said yes.
//   - Never from our own past verdicts. The step-up is used only as a gate that an
//     EXTERNAL event (the capture) must then satisfy; the label is grounded in
//     that external outcome, not in the fact that we allowed or stepped up.
//
// Labels are emitted onto the bus (keyed on token_id for per-token ordering) with
// a weight — full for a dispute, lighter for a bare cancellation — so a trainer
// can trust a repudiation more than a churned mandate. Delivery is at-least-once,
// so the labeler is idempotent on the settling event's id, and each label it
// emits carries a stable id derived from that event, so a redelivery re-emits the
// same label rather than a second one.
package labeler

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/Sagarkhandagre897/AgentShield/internal/bus"
	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
)

// Group is the consumer-group name the labeler subscribes under.
const Group = "labeler"

// Publisher is the only write the labeler needs: it emits label events back onto
// the bus. bus.Bus satisfies it; taking the narrow interface keeps the labeler
// testable without a broker.
type Publisher interface {
	Publish(ctx context.Context, ev domain.Event) error
}

// Params tunes how much a bare mandate cancellation is trusted as a negative.
type Params struct {
	CancellationWeight float64 // confidence of a cancellation label, in [0,1]
}

// DefaultParams weighs a dispute at full confidence and a bare cancellation at
// half — a pulled mandate is a weaker signal than a repudiated charge.
func DefaultParams() Params { return Params{CancellationWeight: 0.5} }

// Labeler folds the settled lifecycle into labels. It keeps two pieces of state:
// the set of settling events already folded (idempotency), and the step-ups
// still awaiting an external confirmation, keyed by (token_id, nonce).
type Labeler struct {
	publish Publisher
	params  Params

	mu      sync.Mutex
	seen    map[string]struct{}
	pending map[string]struct{} // token\x1fnonce of step-ups awaiting a capture
}

// New returns a labeler publishing onto pub.
func New(pub Publisher, params Params) *Labeler {
	return &Labeler{
		publish: pub,
		params:  params,
		seen:    make(map[string]struct{}),
		pending: make(map[string]struct{}),
	}
}

// Register subscribes the labeler to the bus under its consumer group.
func (l *Labeler) Register(bs bus.Bus) (func(), error) {
	return bs.Subscribe(Group, l.Handle)
}

func pendingKey(tokenID, nonce string) string { return tokenID + "\x1f" + nonce }

// Handle folds one lifecycle event. A step-up decision is recorded as pending; a
// capture matching a pending step-up, a dispute, or a cancellation each emit one
// label; everything else is ignored (including the labeler's own outcome events
// and the feature deposits, which are not settled outcomes). It returns an error
// only when a publish failed, without marking the event folded, so the bus
// redelivers and the same label (stable id) is re-emitted rather than lost.
func (l *Labeler) Handle(ctx context.Context, ev domain.Event) error {
	if ev.TokenID == "" || ev.EventID == "" {
		return nil // nothing to key on or dedupe by
	}
	l.mu.Lock()
	_, done := l.seen[ev.EventID]
	l.mu.Unlock()
	if done {
		return nil // at-least-once redelivery of an already-folded event
	}

	switch ev.Type {
	case bus.EventDecisionMade:
		return l.onDecision(ev)
	case bus.EventPaymentCaptured:
		return l.onCapture(ctx, ev)
	case bus.EventPaymentDisputed:
		return l.onDispute(ctx, ev)
	case bus.EventTokenCancelled:
		return l.onCancellation(ctx, ev)
	default:
		return nil // not a settling outcome
	}
}

// onDecision arms a step-up as awaiting external confirmation. A step-up is a
// challenge, not an outcome, so it emits no label — it only marks the capture
// that may later confirm it. Without a nonce the confirming capture cannot be
// matched, so it is not armed.
func (l *Labeler) onDecision(ev domain.Event) error {
	if d, _ := bus.PayloadString(ev, bus.PayloadDecision); d != bus.DecisionStepUp {
		return nil // an ALLOW/BLOCK is our own verdict — never a label source
	}
	nonce, ok := bus.PayloadString(ev, bus.PayloadNonce)
	if !ok || nonce == "" {
		return nil
	}
	l.mu.Lock()
	l.pending[pendingKey(ev.TokenID, nonce)] = struct{}{}
	l.seen[ev.EventID] = struct{}{}
	l.mu.Unlock()
	return nil
}

// onCapture emits a LEGIT label only when the capture confirms a pending step-up
// — the human was challenged and money then moved. A capture with no pending
// step-up is a silent clean capture and, per §6, yields no label.
func (l *Labeler) onCapture(ctx context.Context, ev domain.Event) error {
	nonce, _ := bus.PayloadString(ev, bus.PayloadNonce)
	key := pendingKey(ev.TokenID, nonce)

	l.mu.Lock()
	_, armed := l.pending[key]
	l.mu.Unlock()
	if !armed {
		return nil // not a confirmed step-up — no label from silence
	}

	if err := l.emit(ctx, ev, bus.LabelLegit, 1.0, bus.ReasonConfirmedStepUp); err != nil {
		return err
	}
	l.mu.Lock()
	delete(l.pending, key)
	l.seen[ev.EventID] = struct{}{}
	l.mu.Unlock()
	return nil
}

// onDispute emits the strongest negative: the charge was repudiated. It also
// drops any step-up still pending for the token — a repudiated outcome overrides
// a would-be confirmation.
func (l *Labeler) onDispute(ctx context.Context, ev domain.Event) error {
	if err := l.emit(ctx, ev, bus.LabelMisuse, 1.0, bus.ReasonDispute); err != nil {
		return err
	}
	l.mu.Lock()
	for k := range l.pending {
		if strings.HasPrefix(k, ev.TokenID+"\x1f") {
			delete(l.pending, k)
		}
	}
	l.seen[ev.EventID] = struct{}{}
	l.mu.Unlock()
	return nil
}

// onCancellation emits a soft negative: the mandate was pulled. It is weighed
// below a dispute because a cancellation is noisier — it can be ordinary churn.
func (l *Labeler) onCancellation(ctx context.Context, ev domain.Event) error {
	if err := l.emit(ctx, ev, bus.LabelMisuse, l.params.CancellationWeight, bus.ReasonCancellation); err != nil {
		return err
	}
	l.mu.Lock()
	l.seen[ev.EventID] = struct{}{}
	l.mu.Unlock()
	return nil
}

// emit publishes one label event derived from the settling event. The label
// event's id is stable — derived from the settling event and the reason — so a
// redelivery re-emits the same label, which the trainer dedupes on. Any agent_id
// / customer_id on the settling event rides along for attribution.
func (l *Labeler) emit(ctx context.Context, src domain.Event, label, weight float64, reason string) error {
	out := bus.OutcomeLabeledEvent(
		fmt.Sprintf("label:%s:%s", reason, src.EventID),
		src.TokenID, src.OccurredAt, label, weight, reason,
	)
	if agentID, ok := bus.PayloadString(src, bus.PayloadAgentID); ok && agentID != "" {
		out.Payload[bus.PayloadAgentID] = agentID
	}
	if custID, ok := bus.PayloadString(src, bus.PayloadCustomerID); ok && custID != "" {
		out.Payload[bus.PayloadCustomerID] = custID
	}
	return l.publish.Publish(ctx, out)
}

