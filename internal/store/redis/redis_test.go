package redis_test

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
	rediscstore "github.com/Sagarkhandagre897/AgentShield/internal/store"
	redisstore "github.com/Sagarkhandagre897/AgentShield/internal/store/redis"
)

// dial starts an in-process miniredis and returns a client wired to it. Using a
// real go-redis client against a fake server exercises the adapter's actual
// GET/SET/MGET/redis.Nil paths without needing a redis-server on the box.
func dial(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	return mr
}

func TestPolicyRoundTripAndMissing(t *testing.T) {
	mr := dial(t)
	ctx := context.Background()
	c, err := redisstore.Dial(ctx, mr.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ps := redisstore.NewPolicyStore(c)

	if _, err := ps.GetOverlay(ctx, "absent"); !errors.Is(err, rediscstore.ErrNotFound) {
		t.Fatalf("missing overlay must be ErrNotFound, got %v", err)
	}

	want := &domain.PolicyOverlay{
		TokenID:        "tok_1",
		MerchantRules:  map[string]string{"m1": "deny"},
		PerWindowCaps:  map[string]int64{"day": 50000},
		OverlayVersion: 3,
	}
	if err := ps.PutOverlay(ctx, want); err != nil {
		t.Fatalf("put overlay: %v", err)
	}
	got, err := ps.GetOverlay(ctx, "tok_1")
	if err != nil {
		t.Fatalf("get overlay: %v", err)
	}
	if got.OverlayVersion != 3 || got.MerchantRules["m1"] != "deny" || got.PerWindowCaps["day"] != 50000 {
		t.Fatalf("overlay round-trip lost data: %+v", got)
	}
}

func TestTokenRoundTripAndContainment(t *testing.T) {
	mr := dial(t)
	ctx := context.Background()
	c, err := redisstore.Dial(ctx, mr.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ts := redisstore.NewTokenStore(c)

	if _, err := ts.GetToken(ctx, "absent"); !errors.Is(err, rediscstore.ErrNotFound) {
		t.Fatalf("missing token must be ErrNotFound, got %v", err)
	}

	valid := &domain.Token{
		TokenID: "tok_1", CustomerID: "cust_1", Type: domain.TokenRecurring,
		MaxAmountPaise: 10000, MaxPerDayPaise: 50000, TokenCeilingPaise: 200000,
		Status: domain.TokenConfirmed,
	}
	if err := ts.PutToken(ctx, valid); err != nil {
		t.Fatalf("put valid token: %v", err)
	}
	got, err := ts.GetToken(ctx, "tok_1")
	if err != nil {
		t.Fatalf("get token: %v", err)
	}
	if got.TokenCeilingPaise != 200000 || got.Type != domain.TokenRecurring {
		t.Fatalf("token round-trip lost data: %+v", got)
	}

	// Containment must be enforced on every backend, not just in memory.
	bad := *valid
	bad.MaxAmountPaise = 999999 // per-debit > per-day > lifetime is violated
	if err := ts.PutToken(ctx, &bad); !errors.Is(err, domain.ErrContainment) {
		t.Fatalf("containment must be rejected by the redis backend, got %v", err)
	}
}

func TestBlockStateRoundTrip(t *testing.T) {
	mr := dial(t)
	ctx := context.Background()
	c, err := redisstore.Dial(ctx, mr.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ts := redisstore.NewTokenStore(c)

	if _, err := ts.GetBlockState(ctx, "absent"); !errors.Is(err, rediscstore.ErrNotFound) {
		t.Fatalf("missing block-state must be ErrNotFound, got %v", err)
	}

	bs := &domain.BlockState{
		TokenID: "tok_1", ConsumedToday: 100, ConsumedTotal: 500,
		SeenNonces: []string{"n1", "n2"}, LastComputedAt: 1_700_000_000,
	}
	if err := ts.PutBlockState(ctx, bs); err != nil {
		t.Fatalf("put block-state: %v", err)
	}
	got, err := ts.GetBlockState(ctx, "tok_1")
	if err != nil {
		t.Fatalf("get block-state: %v", err)
	}
	if got.ConsumedTotal != 500 || len(got.SeenNonces) != 2 || got.SeenNonces[1] != "n2" {
		t.Fatalf("block-state round-trip lost data: %+v", got)
	}
}

func TestFeatureMultiGetOmitsMissing(t *testing.T) {
	mr := dial(t)
	ctx := context.Background()
	c, err := redisstore.Dial(ctx, mr.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	fs := redisstore.NewFeatureStore(c)

	if err := fs.Put(ctx, &domain.FeatureRow{
		Key: "agent_1", BehaviourDeviation: 0.4, ComputedAt: 1_700_000_000,
		SignalDeviations: []domain.SignalDeviation{{Signal: "velocity", Deviation: 0.7, ObsCount: 42}},
	}); err != nil {
		t.Fatalf("put feature row: %v", err)
	}

	rows, err := fs.MultiGet(ctx, []string{"agent_1", "agent_absent"})
	if err != nil {
		t.Fatalf("multiget: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("absent key must be omitted, not zero-filled: got %d rows", len(rows))
	}
	r, ok := rows["agent_1"]
	if !ok || r.BehaviourDeviation != 0.4 || len(r.SignalDeviations) != 1 {
		t.Fatalf("feature row round-trip lost data: %+v", r)
	}
	if _, present := rows["agent_absent"]; present {
		t.Fatal("a missing figure must never be an optimistic zero row")
	}
}

func TestDialUnreachable(t *testing.T) {
	if _, err := redisstore.Dial(context.Background(), "127.0.0.1:1"); err == nil {
		t.Fatal("dial to an unreachable endpoint must fail at startup")
	}
}
