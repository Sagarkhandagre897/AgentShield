package materialise_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Sagarkhandagre897/AgentShield/internal/bus"
	busmem "github.com/Sagarkhandagre897/AgentShield/internal/bus/memory"
	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
	"github.com/Sagarkhandagre897/AgentShield/internal/materialise"
	"github.com/Sagarkhandagre897/AgentShield/internal/store"
	"github.com/Sagarkhandagre897/AgentShield/internal/store/memory"
)

const (
	tok     = "tok_1"
	agent   = "agent_1"
	fixedAt = int64(1_700_000_000)
)

func seedToken(t *testing.T, ts store.TokenStore, ceiling int64) {
	t.Helper()
	err := ts.PutToken(context.Background(), &domain.Token{
		TokenID:           tok,
		CustomerID:        "cust_1",
		Type:              domain.TokenRecurring,
		MaxAmountPaise:    ceiling / 10,
		MaxPerDayPaise:    ceiling / 2,
		TokenCeilingPaise: ceiling,
		Status:            domain.TokenConfirmed,
	})
	if err != nil {
		t.Fatalf("seed token: %v", err)
	}
}

func seedBlock(t *testing.T, ts store.TokenStore, consumedTotal int64) {
	t.Helper()
	if err := ts.PutBlockState(context.Background(), &domain.BlockState{
		TokenID:       tok,
		ConsumedTotal: consumedTotal,
	}); err != nil {
		t.Fatalf("seed block: %v", err)
	}
}

func rowOf(t *testing.T, fs store.FeatureStore, key string) (*domain.FeatureRow, bool) {
	t.Helper()
	rows, err := fs.MultiGet(context.Background(), []string{key})
	if err != nil {
		t.Fatalf("multiget: %v", err)
	}
	r, ok := rows[key]
	return r, ok
}

func TestRefreshConsumptionComputesFraction(t *testing.T) {
	ts := memory.NewTokenStore()
	fs := memory.NewFeatureStore()
	seedToken(t, ts, 1_000_000)
	seedBlock(t, ts, 250000) // a quarter of the lifetime ceiling consumed

	m := materialise.New(ts, fs, func() int64 { return fixedAt })
	if err := m.Handle(context.Background(), bus.PaymentCapturedEvent("e1", tok, fixedAt, 250000, "n1")); err != nil {
		t.Fatalf("handle: %v", err)
	}

	r, ok := rowOf(t, fs, tok)
	if !ok {
		t.Fatal("token row must be materialised after a capture")
	}
	if r.ConsumptionFrac != 0.25 {
		t.Fatalf("consumption_frac = %v, want 0.25", r.ConsumptionFrac)
	}
	if r.ComputedAt != fixedAt {
		t.Fatalf("computed_at = %d, want %d", r.ComputedAt, fixedAt)
	}
}

func TestConsumptionClampsAtOne(t *testing.T) {
	ts := memory.NewTokenStore()
	fs := memory.NewFeatureStore()
	seedToken(t, ts, 1_000_000)
	seedBlock(t, ts, 3_000_000) // over the ceiling (P4 blocks this on the clock; the figure still clamps)

	m := materialise.New(ts, fs, func() int64 { return fixedAt })
	m.Handle(context.Background(), bus.PaymentCapturedEvent("e1", tok, fixedAt, 1, "n1"))

	r, _ := rowOf(t, fs, tok)
	if r == nil || r.ConsumptionFrac != 1.0 {
		t.Fatalf("consumption_frac must clamp to 1.0, got %v", r)
	}
}

func TestNoTokenNoRow(t *testing.T) {
	ts := memory.NewTokenStore()
	fs := memory.NewFeatureStore()
	// no token seeded — there is no ceiling to divide by

	m := materialise.New(ts, fs, func() int64 { return fixedAt })
	if err := m.Handle(context.Background(), bus.PaymentCapturedEvent("e1", tok, fixedAt, 250000, "n1")); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if _, ok := rowOf(t, fs, tok); ok {
		t.Fatal("no token means no ceiling means no row materialised")
	}
}

// TestDepositMergePreservesOtherFields is the merge-by-field contract: two
// engines writing the same agent key coexist rather than clobber.
func TestDepositMergePreservesOtherFields(t *testing.T) {
	fs := memory.NewFeatureStore()
	m := materialise.New(memory.NewTokenStore(), fs, func() int64 { return fixedAt })

	sigs := []domain.SignalDeviation{{Signal: "velocity", Deviation: 0.7, ObsCount: 42}}
	if err := m.DepositBehaviour(context.Background(), agent, 0.4, sigs, 100); err != nil {
		t.Fatalf("deposit behaviour: %v", err)
	}
	if err := m.DepositReputation(context.Background(), agent, 0.9, 200); err != nil {
		t.Fatalf("deposit reputation: %v", err)
	}

	r, ok := rowOf(t, fs, agent)
	if !ok {
		t.Fatal("agent row missing")
	}
	if r.BehaviourDeviation != 0.4 {
		t.Fatalf("behaviour deviation clobbered: %v", r.BehaviourDeviation)
	}
	if r.Reputation != 0.9 {
		t.Fatalf("reputation not merged: %v", r.Reputation)
	}
	if len(r.SignalDeviations) != 1 || r.SignalDeviations[0].Signal != "velocity" {
		t.Fatalf("signal deviations lost in merge: %v", r.SignalDeviations)
	}
	if r.ComputedAt != 200 {
		t.Fatalf("computed_at must be latest-write-wins: got %d, want 200", r.ComputedAt)
	}
}

func TestDepositsClamp(t *testing.T) {
	fs := memory.NewFeatureStore()
	m := materialise.New(memory.NewTokenStore(), fs, func() int64 { return fixedAt })

	m.DepositIntent(context.Background(), tok, 1.8, 10)   // over 1
	m.DepositNetwork(context.Background(), tok, -0.5, 20) // under 0

	r, _ := rowOf(t, fs, tok)
	if r.IntentDivergence != 1.0 {
		t.Fatalf("intent divergence must clamp to 1.0, got %v", r.IntentDivergence)
	}
	if r.NetworkRisk != 0.0 {
		t.Fatalf("network risk must clamp to 0.0, got %v", r.NetworkRisk)
	}
}

// countingFeatures counts Put calls so idempotency is observable.
type countingFeatures struct {
	*memory.FeatureStore
	puts int32
}

func (c *countingFeatures) Put(ctx context.Context, row *domain.FeatureRow) error {
	atomic.AddInt32(&c.puts, 1)
	return c.FeatureStore.Put(ctx, row)
}

func TestIdempotentHandle(t *testing.T) {
	ts := memory.NewTokenStore()
	fs := &countingFeatures{FeatureStore: memory.NewFeatureStore()}
	seedToken(t, ts, 1_000_000)
	seedBlock(t, ts, 250000)

	m := materialise.New(ts, fs, func() int64 { return fixedAt })
	ev := bus.PaymentCapturedEvent("e1", tok, fixedAt, 250000, "n1")
	m.Handle(context.Background(), ev)
	m.Handle(context.Background(), ev) // redelivery

	if n := atomic.LoadInt32(&fs.puts); n != 1 {
		t.Fatalf("redelivery must not re-materialise: puts = %d, want 1", n)
	}
}

func TestThroughBus(t *testing.T) {
	ts := memory.NewTokenStore()
	fs := memory.NewFeatureStore()
	seedToken(t, ts, 1_000_000)
	seedBlock(t, ts, 500000) // half consumed → frac 0.5, deterministic (only the materialiser runs)

	m := materialise.New(ts, fs, func() int64 { return fixedAt })
	b := busmem.New(3)
	defer b.Close()
	if _, err := m.Register(b); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := b.Publish(context.Background(), bus.PaymentCapturedEvent("e1", tok, fixedAt, 500000, "n1")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if r, ok := rowOf(t, fs, tok); ok && r.ConsumptionFrac == 0.5 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("consumption_frac did not materialise through the bus in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
