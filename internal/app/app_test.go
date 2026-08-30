package app_test

import (
	"context"
	"testing"
	"time"

	pb "github.com/Sagarkhandagre897/AgentShield/gen/go/agentshield/v1"
	"github.com/Sagarkhandagre897/AgentShield/internal/app"
	"github.com/Sagarkhandagre897/AgentShield/internal/bus"
	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
)

const nowT = int64(1_700_000_000)

func newApp(t *testing.T) *app.App {
	t.Helper()
	a, err := app.New(app.Config{
		Identify:   func(context.Context) string { return "caller_1" },
		Now:        func() int64 { return nowT },
		BusRetries: 3,
	})
	if err != nil {
		t.Fatalf("app.New: %v", err)
	}
	t.Cleanup(func() { a.Close() })
	return a
}

// seed populates the hot stores so that order() is ALLOWed: a confirmed token, an
// empty lien, an overlay, and fresh, quiet feature rows for every entity.
func seed(t *testing.T, a *app.App) {
	t.Helper()
	ctx := context.Background()

	if err := a.Tokens.PutToken(ctx, &domain.Token{
		TokenID: "tok_1", CustomerID: "cust_1", Type: domain.TokenRecurring,
		MaxAmountPaise: 200_000, MaxPerDayPaise: 500_000, TokenCeilingPaise: 2_000_000,
		ExpireAt: nowT + 3_600, Status: domain.TokenConfirmed,
	}); err != nil {
		t.Fatalf("seed token: %v", err)
	}
	if err := a.Tokens.PutBlockState(ctx, &domain.BlockState{TokenID: "tok_1"}); err != nil {
		t.Fatalf("seed block: %v", err)
	}
	if err := a.Policies.PutOverlay(ctx, &domain.PolicyOverlay{TokenID: "tok_1", OverlayVersion: 3}); err != nil {
		t.Fatalf("seed overlay: %v", err)
	}
	for _, r := range []*domain.FeatureRow{
		{Key: "cust_1", BehaviourDeviation: 0.05, NetworkRisk: 0.05, ComputedAt: nowT},
		{Key: "tok_1", IntentDivergence: 0.05, ComputedAt: nowT},
		{Key: "agent_1", Reputation: 0.95, ComputedAt: nowT},
		{Key: "merch_1", ComputedAt: nowT},
	} {
		if err := a.Features.Put(ctx, r); err != nil {
			t.Fatalf("seed feature %s: %v", r.Key, err)
		}
	}
}

func order() *pb.OrderContext {
	return &pb.OrderContext{
		EvaluationId: "eval_1", TokenId: "tok_1", CustomerId: "cust_1",
		AgentId: "agent_1", MerchantId: "merch_1", SessionId: "sess_1",
		AmountPaise: 50_000, CartHash: "cart_abc", EnvelopeDigest: "env_abc",
		ToolRisk: pb.ToolRisk_TOOL_RISK_LOW, Nonce: "nonce_new", Ts: nowT,
	}
}

// TestAllowSpendsNonceAcrossPlanes is the whole point of the composition root:
// an ALLOW on the clock announces decision.made on the bus, the off-clock
// stream-processor spends its nonce into the lien, and a replay of that nonce
// then blocks on the clock. It only passes if the two planes truly share state.
func TestAllowSpendsNonceAcrossPlanes(t *testing.T) {
	a := newApp(t)
	seed(t, a)
	ctx := context.Background()

	v, _ := a.Decision.Evaluate(ctx, order())
	if v.Decision != pb.Answer_ANSWER_ALLOW {
		t.Fatalf("baseline order must ALLOW, got %s/%s", v.Decision, v.Code)
	}

	// decision.made(ALLOW) travels the bus to the stream-processor, which spends
	// the nonce. Poll the lien until it lands.
	waitForNonce(t, a, "tok_1", "nonce_new")

	// The same nonce is now a replay — P1 must block it, which can only be true if
	// the async plane fed the spent nonce back into the sync plane's state.
	replay := order()
	replay.EvaluationId = "eval_2"
	v2, _ := a.Decision.Evaluate(ctx, replay)
	if v2.Decision != pb.Answer_ANSWER_BLOCK || v2.Code != pb.Code_BLOCKED_DUPLICATE {
		t.Fatalf("a spent nonce must replay-block, got %s/%s", v2.Decision, v2.Code)
	}

	// The decision landed on the tamper-evident CHAIN, and the chain verifies.
	if a.Chain.Len() < 1 {
		t.Fatalf("the CHAIN must record decisions, got len %d", a.Chain.Len())
	}
	if err := a.Chain.Verify(); err != nil {
		t.Fatalf("CHAIN must verify: %v", err)
	}
}

// TestCaptureBuildsReputationThroughMaterialiser exercises the feature-store-up
// meeting point: a settled capture attributed to an agent is folded by the
// reputation-builder and deposited THROUGH the materialiser (the single writer),
// so it lands on the agent's feature row.
func TestCaptureBuildsReputationThroughMaterialiser(t *testing.T) {
	a := newApp(t)
	ctx := context.Background()

	ev := bus.WithAgent(bus.PaymentCapturedEvent("cap_1", "tok_1", nowT, 50_000, "n1"), "agent_1")
	if err := a.Bus.Publish(ctx, ev); err != nil {
		t.Fatalf("publish: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		rows, _ := a.Features.MultiGet(ctx, []string{"agent_1"})
		if r := rows["agent_1"]; r != nil && r.Reputation > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("reputation did not reach the feature store through the materialiser in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitForNonce(t *testing.T, a *app.App, tokenID, nonce string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		b, err := a.Tokens.GetBlockState(context.Background(), tokenID)
		if err == nil && b != nil {
			for _, n := range b.SeenNonces {
				if n == nonce {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("nonce %q was not spent through the async plane in time", nonce)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
