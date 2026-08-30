package predicate

import (
	"testing"

	pb "github.com/Sagarkhandagre897/AgentShield/gen/go/agentshield/v1"
	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
)

const nowFixed int64 = 1_000_000

// validInput returns an Input that passes all six predicates. Each test case
// mutates exactly one thing off this baseline and asserts the outcome, so a
// failing assertion points at a single predicate.
func validInput() Input {
	return Input{
		Order: &pb.OrderContext{
			EvaluationId:   "eval_1",
			TokenId:        "tok_1",
			CustomerId:     "cust_1",
			AgentId:        "agent_1",
			MerchantId:     "merch_1",
			SessionId:      "sess_1",
			AmountPaise:    50_000, // ₹500, below the value floor
			CartHash:       "cart_abc",
			EnvelopeDigest: "env_abc",
			ToolRisk:       pb.ToolRisk_TOOL_RISK_LOW,
			Nonce:          "nonce_new",
			Ts:             nowFixed,
		},
		CallerID: "caller_1",
		Token: &domain.Token{
			TokenID:           "tok_1",
			CustomerID:        "cust_1",
			Type:              domain.TokenRecurring,
			MaxAmountPaise:    200_000,
			MaxPerDayPaise:    500_000,
			TokenCeilingPaise: 2_000_000,
			ExpireAt:          nowFixed + 3_600,
			Status:            domain.TokenConfirmed,
		},
		Block: &domain.BlockState{
			TokenID:       "tok_1",
			ConsumedToday: 0,
			ConsumedTotal: 0,
			SeenNonces:    []string{"nonce_old"},
		},
		Overlay:          nil,
		Now:              nowFixed,
		EnvelopeMaxPaise: 0,
		BoundAmountPaise: 0,
	}
}

func TestRunAllPass(t *testing.T) {
	if o := Run(validInput()); o.Terminal {
		t.Fatalf("baseline must pass all predicates, got terminal %s/%s via %s", o.Answer, o.Code, o.Predicate)
	}
}

func TestPredicates(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(in *Input)
		answer  pb.Answer
		code    pb.Code
		wantPre string // predicate expected to fire
	}{
		{
			name:    "P1 replay: nonce already seen",
			mutate:  func(in *Input) { in.Order.Nonce = "nonce_old" },
			answer:  pb.Answer_ANSWER_BLOCK,
			code:    pb.Code_BLOCKED_DUPLICATE,
			wantPre: "P1",
		},
		{
			name:    "P5 identity: unauthenticated caller",
			mutate:  func(in *Input) { in.CallerID = "" },
			answer:  pb.Answer_ANSWER_BLOCK,
			code:    pb.Code_BLOCKED_IDENTITY,
			wantPre: "P5",
		},
		{
			name:    "P5 identity: token resolves to nothing",
			mutate:  func(in *Input) { in.Token = nil },
			answer:  pb.Answer_ANSWER_BLOCK,
			code:    pb.Code_BLOCKED_IDENTITY,
			wantPre: "P5",
		},
		{
			name:    "P5 identity: slip belongs to another principal",
			mutate:  func(in *Input) { in.Token.CustomerID = "cust_other" },
			answer:  pb.Answer_ANSWER_BLOCK,
			code:    pb.Code_BLOCKED_IDENTITY,
			wantPre: "P5",
		},
		{
			name:    "P4 authority: token not confirmed",
			mutate:  func(in *Input) { in.Token.Status = domain.TokenPending },
			answer:  pb.Answer_ANSWER_BLOCK,
			code:    pb.Code_BLOCKED_AUTHORITY,
			wantPre: "P4",
		},
		{
			name:    "P4 authority: token cancelled",
			mutate:  func(in *Input) { in.Token.Status = domain.TokenCancelled },
			answer:  pb.Answer_ANSWER_BLOCK,
			code:    pb.Code_BLOCKED_AUTHORITY,
			wantPre: "P4",
		},
		{
			name:    "P4 authority: expired",
			mutate:  func(in *Input) { in.Now = in.Token.ExpireAt + 1 },
			answer:  pb.Answer_ANSWER_BLOCK,
			code:    pb.Code_BLOCKED_AUTHORITY,
			wantPre: "P4",
		},
		{
			name: "P4 authority: lifetime ceiling exhausted",
			mutate: func(in *Input) {
				in.Block.ConsumedTotal = 1_990_000 // + 50_000 amount > 2_000_000 ceiling
			},
			answer:  pb.Answer_ANSWER_BLOCK,
			code:    pb.Code_BLOCKED_AUTHORITY,
			wantPre: "P4",
		},
		{
			name:    "P6 binding: no cart_hash",
			mutate:  func(in *Input) { in.Order.CartHash = "" },
			answer:  pb.Answer_ANSWER_BLOCK,
			code:    pb.Code_BLOCKED_BINDING,
			wantPre: "P6",
		},
		{
			name: "P6 binding: committed amount disagrees",
			mutate: func(in *Input) {
				in.BoundAmountPaise = 60_000 // != 50_000 request amount
			},
			answer:  pb.Answer_ANSWER_BLOCK,
			code:    pb.Code_BLOCKED_BINDING,
			wantPre: "P6",
		},
		{
			name:    "P2 scope: over per-debit cap",
			mutate:  func(in *Input) { in.Order.AmountPaise = 250_000 }, // > 200_000 per-debit
			answer:  pb.Answer_ANSWER_STEP_UP,
			code:    pb.Code_STEPUP_SCOPE,
			wantPre: "P2",
		},
		{
			name: "P2 scope: over per-day window with today's consumption",
			mutate: func(in *Input) {
				in.Block.ConsumedToday = 480_000 // + 50_000 > 500_000 per-day
			},
			answer:  pb.Answer_ANSWER_STEP_UP,
			code:    pb.Code_STEPUP_SCOPE,
			wantPre: "P2",
		},
		{
			name: "P2 scope: overlay tightens per-debit below the amount",
			mutate: func(in *Input) {
				in.Overlay = &domain.PolicyOverlay{
					TokenID:       "tok_1",
					PerWindowCaps: map[string]int64{"per_debit": 40_000}, // < 50_000 amount
				}
			},
			answer:  pb.Answer_ANSWER_STEP_UP,
			code:    pb.Code_STEPUP_SCOPE,
			wantPre: "P2",
		},
		{
			name: "P2 scope: merchant denied by overlay",
			mutate: func(in *Input) {
				in.Overlay = &domain.PolicyOverlay{
					TokenID:       "tok_1",
					MerchantRules: map[string]string{"merch_1": "deny"},
				}
			},
			answer:  pb.Answer_ANSWER_STEP_UP,
			code:    pb.Code_STEPUP_SCOPE,
			wantPre: "P2",
		},
		{
			name: "P3 unbound: no envelope, above the value floor",
			mutate: func(in *Input) {
				in.Order.EnvelopeDigest = ""
				in.Order.AmountPaise = 150_000 // > ₹1,000 floor
			},
			answer:  pb.Answer_ANSWER_STEP_UP,
			code:    pb.Code_STEPUP_UNBOUND,
			wantPre: "P3",
		},
		{
			name: "P3 unbound: over the envelope's stated ceiling",
			mutate: func(in *Input) {
				in.EnvelopeMaxPaise = 100_000
				in.Order.AmountPaise = 150_000 // > envelope max and > floor, still < per-debit 200_000
			},
			answer:  pb.Answer_ANSWER_STEP_UP,
			code:    pb.Code_STEPUP_UNBOUND,
			wantPre: "P3",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := validInput()
			tc.mutate(&in)
			o := Run(in)
			if !o.Terminal {
				t.Fatalf("expected terminal via %s, got pass", tc.wantPre)
			}
			if o.Answer != tc.answer || o.Code != tc.code || o.Predicate != tc.wantPre {
				t.Fatalf("got %s/%s via %s, want %s/%s via %s",
					o.Answer, o.Code, o.Predicate, tc.answer, tc.code, tc.wantPre)
			}
		})
	}
}

// TestNoEnvelopeBelowFloorPasses verifies the value floor: a tiny unattested
// debit is not worth interrupting a real customer for, so P3 lets it through.
func TestNoEnvelopeBelowFloorPasses(t *testing.T) {
	in := validInput()
	in.Order.EnvelopeDigest = ""
	in.Order.AmountPaise = 50_000 // below the ₹1,000 floor
	if o := Run(in); o.Terminal {
		t.Fatalf("unattested debit below the floor must pass, got %s/%s via %s", o.Answer, o.Code, o.Predicate)
	}
}

// TestBlockBeatsStepUp is the ordering guarantee: when a request trips both a
// BLOCK predicate (expired authority) and a STEP-UP one (over per-debit), the
// BLOCK must win.
func TestBlockBeatsStepUp(t *testing.T) {
	in := validInput()
	in.Now = in.Token.ExpireAt + 1 // P4 would BLOCK
	in.Order.AmountPaise = 250_000 // P2 would STEP-UP
	o := Run(in)
	if o.Answer != pb.Answer_ANSWER_BLOCK || o.Code != pb.Code_BLOCKED_AUTHORITY || o.Predicate != "P4" {
		t.Fatalf("BLOCK must outrank STEP-UP: got %s/%s via %s", o.Answer, o.Code, o.Predicate)
	}
}
