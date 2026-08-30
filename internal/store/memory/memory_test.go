package memory

import (
	"context"
	"errors"
	"testing"

	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
	"github.com/Sagarkhandagre897/AgentShield/internal/store"
)

func TestTokenStoreRoundTripAndNotFound(t *testing.T) {
	ctx := context.Background()
	s := NewTokenStore()

	if _, err := s.GetToken(ctx, "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetToken(missing) err = %v, want ErrNotFound", err)
	}

	tok := &domain.Token{
		TokenID:           "tok_1",
		CustomerID:        "cust_1",
		Type:              domain.TokenRecurring,
		MaxAmountPaise:    200000,
		MaxPerDayPaise:    500000,
		TokenCeilingPaise: 2000000,
		Status:            domain.TokenConfirmed,
	}
	if err := s.PutToken(ctx, tok); err != nil {
		t.Fatalf("PutToken valid: %v", err)
	}
	got, err := s.GetToken(ctx, "tok_1")
	if err != nil {
		t.Fatalf("GetToken: %v", err)
	}
	if got.MaxAmountPaise != 200000 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestPutTokenRejectsContainmentViolation(t *testing.T) {
	ctx := context.Background()
	s := NewTokenStore()
	bad := &domain.Token{
		TokenID:           "tok_bad",
		MaxAmountPaise:    600000, // above per-day
		MaxPerDayPaise:    500000,
		TokenCeilingPaise: 2000000,
	}
	if err := s.PutToken(ctx, bad); !errors.Is(err, domain.ErrContainment) {
		t.Fatalf("PutToken(bad) err = %v, want ErrContainment", err)
	}
	if _, err := s.GetToken(ctx, "tok_bad"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("rejected token must not be stored; err = %v", err)
	}
}

func TestBlockStateNoncesAreCopied(t *testing.T) {
	ctx := context.Background()
	s := NewTokenStore()
	in := &domain.BlockState{TokenID: "tok_1", SeenNonces: []string{"n1", "n2"}}
	if err := s.PutBlockState(ctx, in); err != nil {
		t.Fatalf("PutBlockState: %v", err)
	}
	// Mutating the caller's slice after Put must not affect stored state.
	in.SeenNonces[0] = "tampered"
	got, err := s.GetBlockState(ctx, "tok_1")
	if err != nil {
		t.Fatalf("GetBlockState: %v", err)
	}
	if got.SeenNonces[0] != "n1" {
		t.Fatalf("stored nonces were aliased to caller slice: %v", got.SeenNonces)
	}
	// Mutating the returned slice must not affect stored state either.
	got.SeenNonces[1] = "tampered"
	again, _ := s.GetBlockState(ctx, "tok_1")
	if again.SeenNonces[1] != "n2" {
		t.Fatalf("returned nonces were aliased to stored slice: %v", again.SeenNonces)
	}
}

func TestFeatureStoreMultiGetOmitsMissing(t *testing.T) {
	ctx := context.Background()
	s := NewFeatureStore()
	if err := s.Put(ctx, &domain.FeatureRow{Key: "cust_1", BehaviourDeviation: 0.4, ComputedAt: 100}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.MultiGet(ctx, []string{"cust_1", "agent_1"})
	if err != nil {
		t.Fatalf("MultiGet: %v", err)
	}
	if _, ok := got["cust_1"]; !ok {
		t.Fatalf("present key cust_1 missing from result")
	}
	if _, ok := got["agent_1"]; ok {
		t.Fatalf("absent key agent_1 must be omitted, never zero-filled")
	}
}

func TestFeatureRowSignalsAreCopied(t *testing.T) {
	ctx := context.Background()
	s := NewFeatureStore()
	row := &domain.FeatureRow{
		Key:              "cust_1",
		SignalDeviations: []domain.SignalDeviation{{Signal: "amount", Deviation: 1.0, ObsCount: 3}},
		ComputedAt:       100,
	}
	if err := s.Put(ctx, row); err != nil {
		t.Fatalf("Put: %v", err)
	}
	row.SignalDeviations[0].Deviation = 99 // mutate caller copy
	got, _ := s.MultiGet(ctx, []string{"cust_1"})
	if got["cust_1"].SignalDeviations[0].Deviation != 1.0 {
		t.Fatalf("stored signals were aliased to caller slice")
	}
}

func TestPolicyStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewPolicyStore()
	if _, err := s.GetOverlay(ctx, "tok_1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetOverlay(missing) err = %v, want ErrNotFound", err)
	}
	o := &domain.PolicyOverlay{TokenID: "tok_1", AllowedCategories: []string{"groceries"}, OverlayVersion: 2}
	if err := s.PutOverlay(ctx, o); err != nil {
		t.Fatalf("PutOverlay: %v", err)
	}
	got, err := s.GetOverlay(ctx, "tok_1")
	if err != nil {
		t.Fatalf("GetOverlay: %v", err)
	}
	if got.OverlayVersion != 2 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}
