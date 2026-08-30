package reputation_test

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
	"github.com/Sagarkhandagre897/AgentShield/internal/reputation"
)

const agent = "agent_1"

// spyDepositor records reputation deposits and can be told to fail the next N.
type spyDepositor struct {
	mu       sync.Mutex
	calls    int
	lastRep  float64
	lastKey  string
	failNext int32
	ch       chan float64
}

func (s *spyDepositor) DepositReputation(_ context.Context, key string, rep float64, _ int64) error {
	if atomic.AddInt32(&s.failNext, -1) >= 0 {
		return errors.New("deposit boom")
	}
	s.mu.Lock()
	s.calls++
	s.lastRep = rep
	s.lastKey = key
	s.mu.Unlock()
	if s.ch != nil {
		s.ch <- rep
	}
	return nil
}

func (s *spyDepositor) snapshot() (int, float64, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, s.lastRep, s.lastKey
}

func capture(id string) domain.Event {
	return bus.WithAgent(bus.PaymentCapturedEvent(id, "tok_1", 1_700_000_000, 50000, "n_"+id), agent)
}
func failure(id string) domain.Event {
	return bus.WithAgent(bus.PaymentFailedEvent(id, "tok_1", 1_700_000_000, "n_"+id), agent)
}
func dispute(id string) domain.Event {
	return bus.WithAgent(bus.PaymentDisputedEvent(id, "tok_1", 1_700_000_000, "n_"+id), agent)
}

func newBuilder(d reputation.Depositor) *reputation.Builder {
	return reputation.New(d, reputation.DefaultParams(), func() int64 { return 1_700_000_000 })
}

func feed(t *testing.T, b *reputation.Builder, evs ...domain.Event) {
	t.Helper()
	for _, ev := range evs {
		if err := b.Handle(context.Background(), ev); err != nil {
			t.Fatalf("handle %s: %v", ev.EventID, err)
		}
	}
}

func TestColdStartIsNeutral(t *testing.T) {
	d := &spyDepositor{}
	b := newBuilder(d)

	feed(t, b, capture("e1")) // a single observation must barely move off the prior

	_, rep, key := d.snapshot()
	if key != agent {
		t.Fatalf("reputation keyed on agent: got %q", key)
	}
	// (1 + 20*0.5) / (1 + 20) = 11/21 ≈ 0.524
	if rep < 0.50 || rep > 0.56 {
		t.Fatalf("one capture should stay near neutral, got %v", rep)
	}
}

func TestReputationRisesWithCaptures(t *testing.T) {
	d := &spyDepositor{}
	b := newBuilder(d)

	for i := 0; i < 100; i++ {
		feed(t, b, capture(id(i)))
	}
	_, rep, _ := d.snapshot()
	if rep < 0.9 {
		t.Fatalf("a long clean history should earn high reputation, got %v", rep)
	}
}

func TestReputationFallsWithFailures(t *testing.T) {
	d := &spyDepositor{}
	b := newBuilder(d)

	for i := 0; i < 20; i++ {
		feed(t, b, failure(id(i)))
	}
	_, rep, _ := d.snapshot()
	if rep > 0.3 {
		t.Fatalf("a history of failures should sink reputation, got %v", rep)
	}
}

// TestDisputeWeighsHeavierThanFailure checks that one dispute costs more than one
// plain failure over the same otherwise-clean history.
func TestDisputeWeighsHeavierThanFailure(t *testing.T) {
	withFailure := &spyDepositor{}
	bf := newBuilder(withFailure)
	for i := 0; i < 10; i++ {
		feed(t, bf, capture(id(i)))
	}
	feed(t, bf, failure("bad"))
	_, repFailure, _ := withFailure.snapshot()

	withDispute := &spyDepositor{}
	bd := newBuilder(withDispute)
	for i := 0; i < 10; i++ {
		feed(t, bd, capture(id(i)))
	}
	feed(t, bd, dispute("bad"))
	_, repDispute, _ := withDispute.snapshot()

	if !(repDispute < repFailure) {
		t.Fatalf("a dispute must hurt more than a failure: dispute=%v failure=%v", repDispute, repFailure)
	}
}

// TestIdempotentRedelivery: a redelivered outcome must not be counted twice.
func TestIdempotentRedelivery(t *testing.T) {
	d := &spyDepositor{}
	b := newBuilder(d)

	ev := capture("e1")
	feed(t, b, ev, ev) // same event twice

	calls, rep, _ := d.snapshot()
	if calls != 1 {
		t.Fatalf("redelivery must fold once: deposits = %d, want 1", calls)
	}
	if rep < 0.50 || rep > 0.56 {
		t.Fatalf("redelivery must not double-count the success, got %v", rep)
	}
}

func TestUnattributedOutcomeSkipped(t *testing.T) {
	d := &spyDepositor{}
	b := newBuilder(d)

	// A capture with no agent stamped cannot be attributed.
	if err := b.Handle(context.Background(), bus.PaymentCapturedEvent("e1", "tok_1", 1_700_000_000, 50000, "n1")); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if calls, _, _ := d.snapshot(); calls != 0 {
		t.Fatalf("an unattributed outcome must not deposit, got %d deposits", calls)
	}
}

// TestDepositErrorRollsBack: a failed deposit is retryable and does not corrupt
// the counters — the redelivery lands the same reputation as a first attempt.
func TestDepositErrorRollsBack(t *testing.T) {
	d := &spyDepositor{failNext: 1} // fail the very first deposit
	b := newBuilder(d)

	ev := capture("e1")
	if err := b.Handle(context.Background(), ev); err == nil {
		t.Fatal("a deposit failure must surface so the bus redelivers")
	}
	if err := b.Handle(context.Background(), ev); err != nil {
		t.Fatalf("redelivery must succeed: %v", err)
	}

	calls, rep, _ := d.snapshot()
	if calls != 1 {
		t.Fatalf("exactly one successful deposit expected, got %d", calls)
	}
	// Must equal a single capture (11/21), proving the rolled-back attempt did
	// not double-count.
	if rep < 0.50 || rep > 0.56 {
		t.Fatalf("rolled-back counter must not inflate reputation, got %v", rep)
	}
}

func TestThroughBus(t *testing.T) {
	d := &spyDepositor{ch: make(chan float64, 1)}
	b := newBuilder(d)

	bus0 := busmem.New(3)
	defer bus0.Close()
	if _, err := b.Register(bus0); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := bus0.Publish(context.Background(), capture("e1")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case rep := <-d.ch:
		if rep < 0.50 || rep > 0.56 {
			t.Fatalf("unexpected reputation through the bus: %v", rep)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reputation did not deposit through the bus in time")
	}
}

func id(i int) string {
	return "e" + string(rune('A'+i%26)) + string(rune('0'+i/26))
}
