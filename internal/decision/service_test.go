package decision

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/Sagarkhandagre897/AgentShield/gen/go/agentshield/v1"
	"github.com/Sagarkhandagre897/AgentShield/internal/bus"
	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
	"github.com/Sagarkhandagre897/AgentShield/internal/features"
	"github.com/Sagarkhandagre897/AgentShield/internal/score"
	"github.com/Sagarkhandagre897/AgentShield/internal/store"
	"github.com/Sagarkhandagre897/AgentShield/internal/store/memory"
)

const nowT int64 = 1_000_000

// order returns a baseline request that, against the seeded stores, is ALLOWed.
func order() *pb.OrderContext {
	return &pb.OrderContext{
		EvaluationId:   "eval_1",
		TokenId:        "tok_1",
		CustomerId:     "cust_1",
		AgentId:        "agent_1",
		MerchantId:     "merch_1",
		SessionId:      "sess_1",
		AmountPaise:    50_000, // below the value floor
		CartHash:       "cart_abc",
		EnvelopeDigest: "env_abc",
		ToolRisk:       pb.ToolRisk_TOOL_RISK_LOW,
		Nonce:          "nonce_new",
		Ts:             nowT,
	}
}

// seed builds the three hot stores populated so that order() is allowed:
// a confirmed token, an empty lien, an overlay (version 3), and fresh, quiet
// feature rows for every entity on the request.
func seed(t *testing.T) (*memory.TokenStore, *memory.PolicyStore, *memory.FeatureStore) {
	t.Helper()
	ctx := context.Background()

	ts := memory.NewTokenStore()
	if err := ts.PutToken(ctx, &domain.Token{
		TokenID: "tok_1", CustomerID: "cust_1", Type: domain.TokenRecurring,
		MaxAmountPaise: 200_000, MaxPerDayPaise: 500_000, TokenCeilingPaise: 2_000_000,
		ExpireAt: nowT + 3_600, Status: domain.TokenConfirmed,
	}); err != nil {
		t.Fatalf("seed token: %v", err)
	}
	if err := ts.PutBlockState(ctx, &domain.BlockState{TokenID: "tok_1", SeenNonces: []string{"nonce_old"}}); err != nil {
		t.Fatalf("seed block: %v", err)
	}

	ps := memory.NewPolicyStore()
	if err := ps.PutOverlay(ctx, &domain.PolicyOverlay{TokenID: "tok_1", OverlayVersion: 3}); err != nil {
		t.Fatalf("seed overlay: %v", err)
	}

	fs := memory.NewFeatureStore()
	for _, r := range []*domain.FeatureRow{
		{Key: "cust_1", BehaviourDeviation: 0.05, NetworkRisk: 0.05, ComputedAt: nowT},
		{Key: "tok_1", IntentDivergence: 0.05, ComputedAt: nowT},
		{Key: "agent_1", Reputation: 0.95, ComputedAt: nowT},
		{Key: "merch_1", ComputedAt: nowT},
	} {
		if err := fs.Put(ctx, r); err != nil {
			t.Fatalf("seed feature %s: %v", r.Key, err)
		}
	}
	return ts, ps, fs
}

func newService(ts store.TokenStore, ps store.PolicyStore, fs store.FeatureStore, identity string) (*Service, *chanPublisher) {
	pub := &chanPublisher{ch: make(chan domain.Event, 8)}
	svc := New(Config{
		Tokens:   ts,
		Policies: ps,
		Features: features.NewReader(fs, features.DefaultStalenessBudgetSeconds),
		Params:   score.Params{InterruptionCostPaise: score.DefaultInterruptionCostPaise},
		Events:   pub,
		Identify: func(context.Context) string { return identity },
		Now:      func() int64 { return nowT },
	})
	return svc, pub
}

func TestEvaluateAllow(t *testing.T) {
	ts, ps, fs := seed(t)
	svc, pub := newService(ts, ps, fs, "caller_1")

	v, err := svc.Evaluate(context.Background(), order())
	if err != nil {
		t.Fatalf("Evaluate returned a transport error: %v", err)
	}
	if v.Decision != pb.Answer_ANSWER_ALLOW || v.Code != pb.Code_OK_ALLOW {
		t.Fatalf("want ALLOW/OK_ALLOW, got %s/%s", v.Decision, v.Code)
	}
	if v.Retryable {
		t.Fatalf("an ALLOW is not retryable")
	}
	rec := pub.waitRecord(t)
	if rec.EvaluationID != "eval_1" || rec.Decision != bus.DecisionAllow || rec.PolicyVersion != 3 {
		t.Fatalf("provenance mismatch: %+v", rec)
	}
	if rec.RequestDigest == "" {
		t.Fatalf("the CHAIN record must carry a request digest")
	}
}

func TestEvaluateBlocksReplay(t *testing.T) {
	ts, ps, fs := seed(t)
	svc, pub := newService(ts, ps, fs, "caller_1")

	req := order()
	req.Nonce = "nonce_old" // already in the lien

	v, _ := svc.Evaluate(context.Background(), req)
	if v.Decision != pb.Answer_ANSWER_BLOCK || v.Code != pb.Code_BLOCKED_DUPLICATE {
		t.Fatalf("want BLOCK/BLOCKED_DUPLICATE, got %s/%s", v.Decision, v.Code)
	}
	if v.Retryable {
		t.Fatalf("a block is never retryable")
	}
	if rec := pub.waitRecord(t); rec.PredicateFailed != "P1" {
		t.Fatalf("provenance should record P1, got %q", rec.PredicateFailed)
	}
}

func TestEvaluateStepsUpOnScope(t *testing.T) {
	ts, ps, fs := seed(t)
	svc, pub := newService(ts, ps, fs, "caller_1")

	req := order()
	req.AmountPaise = 250_000 // over the 200,000 per-debit cap

	v, _ := svc.Evaluate(context.Background(), req)
	if v.Decision != pb.Answer_ANSWER_STEP_UP || v.Code != pb.Code_STEPUP_SCOPE {
		t.Fatalf("want STEP_UP/STEPUP_SCOPE, got %s/%s", v.Decision, v.Code)
	}
	if !v.Retryable {
		t.Fatalf("a step-up is retryable")
	}
	if rec := pub.waitRecord(t); rec.PredicateFailed != "P2" {
		t.Fatalf("provenance should record P2, got %q", rec.PredicateFailed)
	}
}

func TestEvaluateBlocksUnauthenticated(t *testing.T) {
	ts, ps, fs := seed(t)
	svc, pub := newService(ts, ps, fs, "") // no caller identity

	v, _ := svc.Evaluate(context.Background(), order())
	if v.Decision != pb.Answer_ANSWER_BLOCK || v.Code != pb.Code_BLOCKED_IDENTITY {
		t.Fatalf("want BLOCK/BLOCKED_IDENTITY, got %s/%s", v.Decision, v.Code)
	}
	if rec := pub.waitRecord(t); rec.PredicateFailed != "P5" {
		t.Fatalf("provenance should record P5, got %q", rec.PredicateFailed)
	}
}

func TestEvaluateFailsClosedOnMissingFeatures(t *testing.T) {
	ts, ps, _ := seed(t)
	// An empty feature store: the spine passes, but every figure is missing.
	svc, pub := newService(ts, ps, memory.NewFeatureStore(), "caller_1")

	v, _ := svc.Evaluate(context.Background(), order())
	if v.Decision != pb.Answer_ANSWER_STEP_UP || v.Code != pb.Code_STEPUP_FAILCLOSED {
		t.Fatalf("missing features must fail closed to STEP_UP/STEPUP_FAILCLOSED, got %s/%s", v.Decision, v.Code)
	}
	if rec := pub.waitRecord(t); rec.Decision != bus.DecisionStepUp {
		t.Fatalf("provenance mismatch: %+v", rec)
	}
}

// erroringTokens fails GetToken with a non-not-found error, to exercise the
// resolveToken fail-closed path.
type erroringTokens struct{}

func (erroringTokens) GetToken(context.Context, string) (*domain.Token, error) {
	return nil, errors.New("token store unreachable")
}
func (erroringTokens) PutToken(context.Context, *domain.Token) error { return nil }
func (erroringTokens) GetBlockState(context.Context, string) (*domain.BlockState, error) {
	return nil, store.ErrNotFound
}
func (erroringTokens) PutBlockState(context.Context, *domain.BlockState) error { return nil }

func TestEvaluateFailsClosedOnTokenStoreError(t *testing.T) {
	_, ps, fs := seed(t)
	svc, pub := newService(erroringTokens{}, ps, fs, "caller_1")

	v, _ := svc.Evaluate(context.Background(), order())
	if v.Decision != pb.Answer_ANSWER_STEP_UP || v.Code != pb.Code_STEPUP_FAILCLOSED {
		t.Fatalf("token store error must fail closed, got %s/%s", v.Decision, v.Code)
	}
	pub.waitRecord(t) // provenance is still announced for the fail-closed decision
}

func TestEvaluateCriticalToolFloor(t *testing.T) {
	ts, ps, fs := seed(t)
	svc, pub := newService(ts, ps, fs, "caller_1")

	req := order()
	req.ToolRisk = pb.ToolRisk_TOOL_RISK_CRITICAL // would otherwise ALLOW

	v, _ := svc.Evaluate(context.Background(), req)
	if v.Decision != pb.Answer_ANSWER_STEP_UP || v.Code != pb.Code_STEPUP_RISK {
		t.Fatalf("critical tool risk must floor an ALLOW to STEP_UP/STEPUP_RISK, got %s/%s", v.Decision, v.Code)
	}
	pub.waitRecord(t)
}

func TestConsumptionFrac(t *testing.T) {
	tok := &domain.Token{TokenCeilingPaise: 1_000_000}
	if f := consumptionFrac(tok, &domain.BlockState{ConsumedTotal: 250_000}); f != 0.25 {
		t.Fatalf("want 0.25, got %v", f)
	}
	if f := consumptionFrac(tok, &domain.BlockState{ConsumedTotal: 5_000_000}); f != 1 {
		t.Fatalf("over-consumption must clamp to 1, got %v", f)
	}
	if f := consumptionFrac(nil, nil); f != 0 {
		t.Fatalf("no mandate/lien must be 0, got %v", f)
	}
}

// chanPublisher captures the decision.made events respond() emits so tests can
// assert on them deterministically (the receive synchronises with the emit
// goroutine).
type chanPublisher struct{ ch chan domain.Event }

func (c *chanPublisher) Publish(_ context.Context, ev domain.Event) error {
	c.ch <- ev
	return nil
}

func (c *chanPublisher) wait(t *testing.T) domain.Event {
	t.Helper()
	select {
	case ev := <-c.ch:
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("no decision.made event published")
		return domain.Event{}
	}
}

// waitRecord reconstructs the provenance record the stream-processor would
// rebuild from the decision.made event — the CHAIN write now lives there, not in
// the decision service, so provenance is asserted off the published event.
func (c *chanPublisher) waitRecord(t *testing.T) *domain.ProvenanceRecord {
	t.Helper()
	return bus.ProvenanceFromEvent(c.wait(t))
}

// TestEvaluatePublishesDecisionMade: an ALLOW announces itself on the bus with
// the nonce and amount the stream-processor needs to spend the nonce, keyed by
// evaluation_id so a redelivery folds once and partitioned by token_id.
func TestEvaluatePublishesDecisionMade(t *testing.T) {
	ts, ps, fs := seed(t)
	svc, pub := newService(ts, ps, fs, "caller_1")

	v, _ := svc.Evaluate(context.Background(), order())
	if v.Decision != pb.Answer_ANSWER_ALLOW {
		t.Fatalf("want ALLOW, got %s", v.Decision)
	}

	ev := pub.wait(t)
	if ev.Type != bus.EventDecisionMade {
		t.Fatalf("want %s, got %s", bus.EventDecisionMade, ev.Type)
	}
	if ev.EventID != "eval_1" { // event_id == evaluation_id, so the async plane folds it once
		t.Fatalf("event_id must be the evaluation_id, got %q", ev.EventID)
	}
	if ev.TokenID != "tok_1" { // the partition key
		t.Fatalf("token_id mismatch, got %q", ev.TokenID)
	}
	if ev.OccurredAt != nowT {
		t.Fatalf("occurred_at must be the decision time, got %d", ev.OccurredAt)
	}
	if d, _ := bus.PayloadString(ev, bus.PayloadDecision); d != bus.DecisionAllow {
		t.Fatalf("decision payload must be ALLOW, got %q", d)
	}
	if n, _ := bus.PayloadString(ev, bus.PayloadNonce); n != "nonce_new" {
		t.Fatalf("nonce payload mismatch, got %q", n)
	}
	if amt, _ := bus.PayloadInt64(ev, bus.PayloadAmountPaise); amt != 50_000 {
		t.Fatalf("amount payload mismatch, got %d", amt)
	}
}

// TestEvaluatePublishesOnBlock: every decision is announced, not just ALLOWs — a
// blocked replay still emits decision.made carrying BLOCK, and it is the
// stream-processor's decision-gate (not the publisher) that declines to spend the
// nonce.
func TestEvaluatePublishesOnBlock(t *testing.T) {
	ts, ps, fs := seed(t)
	svc, pub := newService(ts, ps, fs, "caller_1")

	req := order()
	req.Nonce = "nonce_old" // already in the lien → P1 BLOCK

	v, _ := svc.Evaluate(context.Background(), req)
	if v.Decision != pb.Answer_ANSWER_BLOCK {
		t.Fatalf("want BLOCK, got %s", v.Decision)
	}
	ev := pub.wait(t)
	if d, _ := bus.PayloadString(ev, bus.PayloadDecision); d != bus.DecisionBlock {
		t.Fatalf("a block must still be announced as BLOCK, got %q", d)
	}
}
