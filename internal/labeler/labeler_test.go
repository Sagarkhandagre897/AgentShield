package labeler_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Sagarkhandagre897/AgentShield/internal/bus"
	busmem "github.com/Sagarkhandagre897/AgentShield/internal/bus/memory"
	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
	"github.com/Sagarkhandagre897/AgentShield/internal/labeler"
)

const (
	tok   = "tok_1"
	agent = "agent_1"
	cust  = "cust_1"
	at    = int64(1_700_000_000)
)

// spyPub records the labels the labeler publishes and can fail the next N calls.
type spyPub struct {
	mu       sync.Mutex
	events   []domain.Event
	failNext int32
	ch       chan domain.Event
}

func (s *spyPub) Publish(_ context.Context, ev domain.Event) error {
	if atomic.AddInt32(&s.failNext, -1) >= 0 {
		return errors.New("publish boom")
	}
	s.mu.Lock()
	s.events = append(s.events, ev)
	s.mu.Unlock()
	if s.ch != nil {
		s.ch <- ev
	}
	return nil
}

func (s *spyPub) snap() []domain.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.Event, len(s.events))
	copy(out, s.events)
	return out
}

func newLabeler(p labeler.Publisher) *labeler.Labeler {
	return labeler.New(p, labeler.DefaultParams())
}

func feed(t *testing.T, l *labeler.Labeler, evs ...domain.Event) {
	t.Helper()
	for _, ev := range evs {
		if err := l.Handle(context.Background(), ev); err != nil {
			t.Fatalf("handle %s: %v", ev.EventID, err)
		}
	}
}

func stepUp(id, nonce string) domain.Event {
	return bus.DecisionMadeEvent(id, tok, at, bus.DecisionStepUp, nonce, 50000)
}
func allow(id, nonce string) domain.Event {
	return bus.DecisionMadeEvent(id, tok, at, bus.DecisionAllow, nonce, 50000)
}
func capture(id, nonce string) domain.Event {
	return bus.PaymentCapturedEvent(id, tok, at, 50000, nonce)
}
func dispute(id string) domain.Event {
	ev := bus.WithAgent(bus.PaymentDisputedEvent(id, tok, at, "n_"+id), agent)
	ev.Payload[bus.PayloadCustomerID] = cust
	return ev
}
func cancellation(id string) domain.Event {
	return domain.Event{EventID: id, Type: bus.EventTokenCancelled, TokenID: tok, OccurredAt: at, Payload: map[string]any{}}
}

// one requires exactly one label was published and returns it.
func one(t *testing.T, s *spyPub) domain.Event {
	t.Helper()
	evs := s.snap()
	if len(evs) != 1 {
		t.Fatalf("want exactly one label, got %d", len(evs))
	}
	return evs[0]
}

func label(t *testing.T, ev domain.Event) (float64, float64, string) {
	t.Helper()
	if ev.Type != bus.EventOutcomeLabeled {
		t.Fatalf("not a label event: %q", ev.Type)
	}
	if ev.TokenID != tok {
		t.Fatalf("label must be keyed on token_id, got %q", ev.TokenID)
	}
	lab, _ := bus.PayloadFloat64(ev, bus.PayloadLabel)
	w, _ := bus.PayloadFloat64(ev, bus.PayloadWeight)
	r, _ := bus.PayloadString(ev, bus.PayloadReason)
	return lab, w, r
}

// __TESTS__

func TestDisputeEmitsMisuse(t *testing.T) {
	s := &spyPub{}
	l := newLabeler(s)
	feed(t, l, dispute("d1"))

	ev := one(t, s)
	lab, w, r := label(t, ev)
	if lab != bus.LabelMisuse || w != 1.0 || r != bus.ReasonDispute {
		t.Fatalf("dispute → misuse@1.0/dispute, got label=%v weight=%v reason=%q", lab, w, r)
	}
	if a, _ := bus.PayloadString(ev, bus.PayloadAgentID); a != agent {
		t.Fatalf("agent_id must ride along, got %q", a)
	}
	if c, _ := bus.PayloadString(ev, bus.PayloadCustomerID); c != cust {
		t.Fatalf("customer_id must ride along, got %q", c)
	}
}

func TestCancellationIsSoftMisuse(t *testing.T) {
	s := &spyPub{}
	l := newLabeler(s)
	feed(t, l, cancellation("c1"))

	lab, w, r := label(t, one(t, s))
	if lab != bus.LabelMisuse || r != bus.ReasonCancellation {
		t.Fatalf("cancellation → misuse/cancellation, got label=%v reason=%q", lab, r)
	}
	if w != labeler.DefaultParams().CancellationWeight || !(w < 1.0) {
		t.Fatalf("a cancellation must weigh below a dispute, got weight=%v", w)
	}
}

func TestConfirmedStepUpEmitsLegit(t *testing.T) {
	s := &spyPub{}
	l := newLabeler(s)
	// A step-up arms nothing on its own; the confirming capture makes the label.
	feed(t, l, stepUp("e1", "n1"))
	if len(s.snap()) != 0 {
		t.Fatalf("a step-up alone must emit no label")
	}
	feed(t, l, capture("p1", "n1"))

	lab, w, r := label(t, one(t, s))
	if lab != bus.LabelLegit || w != 1.0 || r != bus.ReasonConfirmedStepUp {
		t.Fatalf("confirmed step-up → legit@1.0, got label=%v weight=%v reason=%q", lab, w, r)
	}
}

func TestBareCaptureIsNotLabeled(t *testing.T) {
	s := &spyPub{}
	l := newLabeler(s)
	// No preceding step-up: a clean capture that simply was not disputed is not
	// evidence of legitimacy (§6 — never from "no complaint arrived").
	feed(t, l, capture("p1", "n1"))
	if n := len(s.snap()); n != 0 {
		t.Fatalf("a bare capture must not be labeled, got %d labels", n)
	}
}

func TestAllowDecisionIsNotLabeled(t *testing.T) {
	s := &spyPub{}
	l := newLabeler(s)
	// An ALLOW is our own verdict — never a label source — and it arms no step-up,
	// so a later capture on the same nonce is still not labeled.
	feed(t, l, allow("e1", "n1"), capture("p1", "n1"))
	if n := len(s.snap()); n != 0 {
		t.Fatalf("an allow verdict must not become a label, got %d labels", n)
	}
}

func TestDisputeOverridesPendingStepUp(t *testing.T) {
	s := &spyPub{}
	l := newLabeler(s)
	feed(t, l, stepUp("e1", "n1"), dispute("d1"))
	// The dispute label is out; the step-up it overrode must not later confirm.
	feed(t, l, capture("p1", "n1"))

	lab, _, r := label(t, one(t, s))
	if lab != bus.LabelMisuse || r != bus.ReasonDispute {
		t.Fatalf("a dispute must override a pending step-up, got label=%v reason=%q", lab, r)
	}
}

func TestIdempotentRedelivery(t *testing.T) {
	s := &spyPub{}
	l := newLabeler(s)
	ev := dispute("d1")
	feed(t, l, ev, ev) // same settling event twice
	if n := len(s.snap()); n != 1 {
		t.Fatalf("a redelivered outcome must be labeled once, got %d", n)
	}
}

func TestLabelIDIsStableAcrossRedelivery(t *testing.T) {
	// Two labelers folding the same dispute must emit the same label id, so a
	// downstream trainer dedupes a redelivery rather than double-counting.
	a, b := &spyPub{}, &spyPub{}
	feed(t, newLabeler(a), dispute("d1"))
	feed(t, newLabeler(b), dispute("d1"))
	if one(t, a).EventID != one(t, b).EventID {
		t.Fatalf("label id must be a stable function of the settling event")
	}
}

func TestPublishErrorRetries(t *testing.T) {
	s := &spyPub{failNext: 1} // fail the first publish
	l := newLabeler(s)
	ev := dispute("d1")
	if err := l.Handle(context.Background(), ev); err == nil {
		t.Fatal("a publish failure must surface so the bus redelivers")
	}
	if err := l.Handle(context.Background(), ev); err != nil {
		t.Fatalf("redelivery must succeed: %v", err)
	}
	if n := len(s.snap()); n != 1 {
		t.Fatalf("exactly one label after a failed-then-retried publish, got %d", n)
	}
}

func TestOwnLabelEventsAreIgnored(t *testing.T) {
	s := &spyPub{}
	l := newLabeler(s)
	// The labeler's group also receives outcomes.v1; its own labels must not loop.
	feed(t, l, bus.OutcomeLabeledEvent("x", tok, at, bus.LabelMisuse, 1.0, bus.ReasonDispute))
	if n := len(s.snap()); n != 0 {
		t.Fatalf("an outcome label must not itself produce a label, got %d", n)
	}
}

func TestThroughBus(t *testing.T) {
	s := &spyPub{ch: make(chan domain.Event, 1)}
	l := newLabeler(s)
	b := busmem.New(3)
	defer b.Close()
	if _, err := l.Register(b); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := b.Publish(context.Background(), dispute("d1")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case ev := <-s.ch:
		if lab, _, r := label(t, ev); lab != bus.LabelMisuse || r != bus.ReasonDispute {
			t.Fatalf("unexpected label through the bus: label=%v reason=%q", lab, r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the labeler did not emit through the bus in time")
	}
}

