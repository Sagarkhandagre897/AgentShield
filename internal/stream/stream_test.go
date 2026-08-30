package stream_test

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
	"github.com/Sagarkhandagre897/AgentShield/internal/store"
	"github.com/Sagarkhandagre897/AgentShield/internal/store/memory"
	"github.com/Sagarkhandagre897/AgentShield/internal/stream"
)

const (
	tok    = "tok_1"
	day1TS = int64(1_000_000) // some second on day 11
	day2TS = day1TS + 86400   // same clock time, next day
)

func blockOf(t *testing.T, ts store.TokenStore, tokenID string) *domain.BlockState {
	t.Helper()
	bs, err := ts.GetBlockState(context.Background(), tokenID)
	if err != nil {
		t.Fatalf("get block-state: %v", err)
	}
	return bs
}

func hasNonce(bs *domain.BlockState, nonce string) bool {
	for _, n := range bs.SeenNonces {
		if n == nonce {
			return true
		}
	}
	return false
}

func TestCaptureAdvancesConsumptionAndSpendsNonce(t *testing.T) {
	ts := memory.NewTokenStore()
	p := stream.New(ts, nil)

	ev := bus.PaymentCapturedEvent("e1", tok, day1TS, 50000, "n1")
	if err := p.Handle(context.Background(), ev); err != nil {
		t.Fatalf("handle: %v", err)
	}

	bs := blockOf(t, ts, tok)
	if bs.ConsumedToday != 50000 || bs.ConsumedTotal != 50000 {
		t.Fatalf("consumption: today=%d total=%d, want 50000/50000", bs.ConsumedToday, bs.ConsumedTotal)
	}
	if !hasNonce(bs, "n1") {
		t.Fatalf("capture must spend its nonce: %v", bs.SeenNonces)
	}
	if bs.LastComputedAt != day1TS {
		t.Fatalf("last_computed_at = %d, want %d", bs.LastComputedAt, day1TS)
	}
}

func TestCapturesAccumulateWithinDay(t *testing.T) {
	ts := memory.NewTokenStore()
	p := stream.New(ts, nil)

	p.Handle(context.Background(), bus.PaymentCapturedEvent("e1", tok, day1TS, 30000, "n1"))
	p.Handle(context.Background(), bus.PaymentCapturedEvent("e2", tok, day1TS+10, 20000, "n2"))

	bs := blockOf(t, ts, tok)
	if bs.ConsumedToday != 50000 || bs.ConsumedTotal != 50000 {
		t.Fatalf("today=%d total=%d, want 50000/50000", bs.ConsumedToday, bs.ConsumedTotal)
	}
	if !hasNonce(bs, "n1") || !hasNonce(bs, "n2") {
		t.Fatalf("both nonces must be spent: %v", bs.SeenNonces)
	}
}

func TestDayRolloverResetsTodayNotTotal(t *testing.T) {
	ts := memory.NewTokenStore()
	p := stream.New(ts, nil)

	p.Handle(context.Background(), bus.PaymentCapturedEvent("e1", tok, day1TS, 40000, "n1"))
	p.Handle(context.Background(), bus.PaymentCapturedEvent("e2", tok, day2TS, 15000, "n2"))

	bs := blockOf(t, ts, tok)
	if bs.ConsumedToday != 15000 {
		t.Fatalf("consumed_today must reset for the new day: got %d, want 15000", bs.ConsumedToday)
	}
	if bs.ConsumedTotal != 55000 {
		t.Fatalf("consumed_total is lifetime and must not reset: got %d, want 55000", bs.ConsumedTotal)
	}
}

func TestDecisionAllowSpendsNonce(t *testing.T) {
	ts := memory.NewTokenStore()
	p := stream.New(ts, nil)

	ev := bus.DecisionMadeEvent("e1", tok, day1TS, bus.DecisionAllow, "n1", 50000)
	if err := p.Handle(context.Background(), ev); err != nil {
		t.Fatalf("handle: %v", err)
	}

	bs := blockOf(t, ts, tok)
	if !hasNonce(bs, "n1") {
		t.Fatalf("an ALLOW must spend its nonce: %v", bs.SeenNonces)
	}
	if bs.ConsumedTotal != 0 {
		t.Fatalf("a decision is not consumption: consumed_total=%d, want 0", bs.ConsumedTotal)
	}
}

func TestDecisionStepUpDoesNotSpendNonce(t *testing.T) {
	ts := memory.NewTokenStore()
	p := stream.New(ts, nil)

	ev := bus.DecisionMadeEvent("e1", tok, day1TS, bus.DecisionStepUp, "n1", 50000)
	if err := p.Handle(context.Background(), ev); err != nil {
		t.Fatalf("handle: %v", err)
	}

	// Nothing was written: a stepped-up request may legitimately return with the
	// same nonce, so it must not be pre-emptively burned.
	if _, err := ts.GetBlockState(context.Background(), tok); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("STEP_UP must not record a nonce; got err=%v", err)
	}
}

// TestIdempotentRedelivery is the at-least-once contract: folding the same
// captured event twice consumes once.
func TestIdempotentRedelivery(t *testing.T) {
	ts := memory.NewTokenStore()
	p := stream.New(ts, nil)

	ev := bus.PaymentCapturedEvent("e1", tok, day1TS, 50000, "n1")
	p.Handle(context.Background(), ev)
	p.Handle(context.Background(), ev) // redelivery of the identical event

	bs := blockOf(t, ts, tok)
	if bs.ConsumedTotal != 50000 {
		t.Fatalf("redelivery must not double-count: consumed_total=%d, want 50000", bs.ConsumedTotal)
	}
	if len(bs.SeenNonces) != 1 {
		t.Fatalf("nonce recorded once: %v", bs.SeenNonces)
	}
}

// flakyStore fails the first N PutBlockState calls, then behaves normally.
type flakyStore struct {
	*memory.TokenStore
	failsLeft int32
}

func (f *flakyStore) PutBlockState(ctx context.Context, s *domain.BlockState) error {
	if atomic.AddInt32(&f.failsLeft, -1) >= 0 {
		return errors.New("transient store error")
	}
	return f.TokenStore.PutBlockState(ctx, s)
}

// TestStoreErrorIsRetryable shows why the event is marked folded only after the
// write commits: a failed fold returns an error (the bus redelivers) and is not
// double-applied when it finally succeeds.
func TestStoreErrorIsRetryable(t *testing.T) {
	ts := &flakyStore{TokenStore: memory.NewTokenStore(), failsLeft: 1}
	p := stream.New(ts, nil)

	ev := bus.PaymentCapturedEvent("e1", tok, day1TS, 50000, "n1")
	if err := p.Handle(context.Background(), ev); err == nil {
		t.Fatal("first fold must surface the store error so the bus redelivers")
	}
	if err := p.Handle(context.Background(), ev); err != nil {
		t.Fatalf("redelivery must succeed once the store recovers: %v", err)
	}

	bs := blockOf(t, ts, tok)
	if bs.ConsumedTotal != 50000 {
		t.Fatalf("recovered fold must apply exactly once: consumed_total=%d, want 50000", bs.ConsumedTotal)
	}
}

// TestThroughBus wires the processor to the in-memory bus as a real subscriber
// and lets a published capture flow through Handle.
func TestThroughBus(t *testing.T) {
	ts := memory.NewTokenStore()
	p := stream.New(ts, nil)

	b := busmem.New(3)
	defer b.Close()
	if _, err := p.Register(b); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := b.Publish(context.Background(), bus.PaymentCapturedEvent("e1", tok, day1TS, 70000, "n1")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if bs, err := ts.GetBlockState(context.Background(), tok); err == nil && bs.ConsumedTotal == 70000 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("block-state did not converge through the bus in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// recordingSink captures the provenance records the processor appends, so tests
// can assert on the CHAIN the stream-processor now owns.
type recordingSink struct {
	mu   sync.Mutex
	recs []*domain.ProvenanceRecord
}

func (s *recordingSink) Emit(_ context.Context, r *domain.ProvenanceRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recs = append(s.recs, r)
}

func (s *recordingSink) all() []*domain.ProvenanceRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*domain.ProvenanceRecord, len(s.recs))
	copy(out, s.recs)
	return out
}

// TestEveryDecisionIsRecordedOnChain: the stream-processor is the CHAIN's writer,
// and it records every decision — ALLOW, STEP_UP and BLOCK — rebuilding the full
// record from the fields the decision.made event carried.
func TestEveryDecisionIsRecordedOnChain(t *testing.T) {
	ts := memory.NewTokenStore()
	sink := &recordingSink{}
	p := stream.New(ts, sink)

	for _, tc := range []struct {
		id, decision, code, pred string
	}{
		{"e_allow", bus.DecisionAllow, "OK_ALLOW", ""},
		{"e_stepup", bus.DecisionStepUp, "STEPUP_SCOPE", "P2"},
		{"e_block", bus.DecisionBlock, "BLOCKED_DUPLICATE", "P1"},
	} {
		rec := &domain.ProvenanceRecord{
			EvaluationID: tc.id, Decision: tc.decision, Code: tc.code,
			PredicateFailed: tc.pred, RequestDigest: "rd_" + tc.id, PolicyVersion: 3,
		}
		ev := bus.WithProvenance(
			bus.DecisionMadeEvent(tc.id, tok, day1TS, tc.decision, "n_"+tc.id, 50000),
			rec,
		)
		if err := p.Handle(context.Background(), ev); err != nil {
			t.Fatalf("handle %s: %v", tc.id, err)
		}
	}

	recs := sink.all()
	if len(recs) != 3 {
		t.Fatalf("every decision must be recorded on the CHAIN: got %d, want 3", len(recs))
	}
	// Spot-check the full reconstruction of the STEP_UP record from its event.
	got := recs[1]
	if got.EvaluationID != "e_stepup" || got.Decision != bus.DecisionStepUp ||
		got.Code != "STEPUP_SCOPE" || got.PredicateFailed != "P2" ||
		got.RequestDigest != "rd_e_stepup" || got.PolicyVersion != 3 || got.TS != day1TS {
		t.Fatalf("record rebuilt from event is wrong: %+v", got)
	}
}

// TestChainRecordsOncePerEvaluation: an at-least-once redelivery of the same
// decision.made must append exactly one CHAIN record — the seen-set that dedupes
// block-state folds protects the ledger write too.
func TestChainRecordsOncePerEvaluation(t *testing.T) {
	ts := memory.NewTokenStore()
	sink := &recordingSink{}
	p := stream.New(ts, sink)

	ev := bus.WithProvenance(
		bus.DecisionMadeEvent("e1", tok, day1TS, bus.DecisionAllow, "n1", 50000),
		&domain.ProvenanceRecord{EvaluationID: "e1", Decision: bus.DecisionAllow, Code: "OK_ALLOW"},
	)
	p.Handle(context.Background(), ev)
	p.Handle(context.Background(), ev) // redelivery of the identical event

	if recs := sink.all(); len(recs) != 1 {
		t.Fatalf("redelivery must record once on the CHAIN: got %d, want 1", len(recs))
	}
}
