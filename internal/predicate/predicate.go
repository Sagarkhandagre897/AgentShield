// Package predicate is the deterministic spine (System Design §5). The six
// predicates are pure integer and set comparisons over data already in hand.
// They run first and alone — before any feature is read, before any score is
// computed — and any one that refuses settles the request immediately with one
// of the fixed codes.
//
// This is the ONLY component in the whole system that can BLOCK. The ML engines
// added later sit on top of this spine: they can raise risk or ask for a
// step-up, but they can never reach past it to block or override a block.
//
// The logic here is pure: it takes everything it needs as an Input (including
// the current time and the authenticated caller identity) and returns an
// Outcome. It performs no I/O, so it is exhaustively table-testable.
package predicate

import (
	pb "github.com/Sagarkhandagre897/AgentShield/gen/go/agentshield/v1"
	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
)

// DefaultValueFloorPaise is the debit size below which an unattested or
// over-envelope debit is not worth interrupting a real customer for (§5, "P3
// STEP-UP above a value floor"). ₹1,000.
const DefaultValueFloorPaise int64 = 100000

// Input carries everything the six predicates compare. It is assembled on the
// clock from the request, the mTLS caller identity, and keyed reads of the
// token/block-state and policy stores. Now and the resolved figures are passed
// in (never read from the clock inside here) so the logic stays pure.
type Input struct {
	Order    *pb.OrderContext      // the request being evaluated
	CallerID string                // authenticated caller identity from mTLS; "" means unauthenticated
	Token    *domain.Token         // nil if the token_id resolved to nothing
	Block    *domain.BlockState    // nil if there is no reconstructed lien yet
	Overlay  *domain.PolicyOverlay // nil if the customer set no tightening overlay
	Now      int64                 // epoch seconds, injected

	// EnvelopeMaxPaise is the sealed intent envelope's stated ceiling, when it
	// is known on the clock. The VAULT is never read on the hot path, so this is
	// 0 (unknown) until the intent engine (Phase 4) materialises the session cap
	// into a hot store. P3 only compares against it when it is > 0.
	EnvelopeMaxPaise int64

	// BoundAmountPaise is the amount the order's cart_hash commits to, as known
	// to AgentShield from the signed order context. 0 means unknown; P6 only
	// compares against it when it is > 0.
	BoundAmountPaise int64
}

// Outcome is the result of running the spine. When Terminal is false, all six
// predicates passed and the request proceeds to readFeatures()/score. When
// Terminal is true, a predicate settled the request: Answer is BLOCK or
// STEP-UP, Code is the fixed caller-facing code, and Predicate names which of
// P1-P6 fired.
type Outcome struct {
	Terminal  bool
	Answer    pb.Answer
	Code      pb.Code
	Predicate string
}

func pass() Outcome { return Outcome{Answer: pb.Answer_ANSWER_ALLOW, Code: pb.Code_CODE_UNSPECIFIED} }

func block(code pb.Code, p string) Outcome {
	return Outcome{Terminal: true, Answer: pb.Answer_ANSWER_BLOCK, Code: code, Predicate: p}
}

func stepUp(code pb.Code, p string) Outcome {
	return Outcome{Terminal: true, Answer: pb.Answer_ANSWER_STEP_UP, Code: code, Predicate: p}
}

// Run evaluates the six predicates in a fixed order and returns the first
// terminal verdict, or a passing Outcome if all six pass.
//
// The four BLOCK predicates run before the two STEP-UP predicates on purpose: a
// genuine "no" (a replay, an unverifiable identity, a dead authority, an unbound
// amount) must win over a "re-confirm" when a request trips both. Within the
// blocks, the cheapest and most decisive checks come first. P5 (identity) runs
// before the checks that assume a token, so P4/P6/P2 can rely on Token being
// present.
func Run(in Input) Outcome {
	if o := p1Replay(in); o.Terminal {
		return o
	}
	if o := p5Identity(in); o.Terminal {
		return o
	}
	if o := p4Authority(in); o.Terminal {
		return o
	}
	if o := p6Binding(in); o.Terminal {
		return o
	}
	if o := p2Scope(in); o.Terminal {
		return o
	}
	if o := p3Unbound(in); o.Terminal {
		return o
	}
	return pass()
}

// P1 · Replay — have I already seen this exact request? A nonce already in the
// reconstructed block-state means a duplicate, which is never real money.
func p1Replay(in Input) Outcome {
	if in.Block == nil || in.Order.GetNonce() == "" {
		return pass()
	}
	for _, seen := range in.Block.SeenNonces {
		if seen == in.Order.GetNonce() {
			return block(pb.Code_BLOCKED_DUPLICATE, "P1")
		}
	}
	return pass()
}

// P5 · Unverifiable — can I prove who this is, and that the slip is theirs? An
// unauthenticated caller, a token that resolves to nothing, or a token whose
// principal does not match the request all mean identity cannot be proven.
func p5Identity(in Input) Outcome {
	if in.CallerID == "" {
		return block(pb.Code_BLOCKED_IDENTITY, "P5")
	}
	if in.Token == nil {
		return block(pb.Code_BLOCKED_IDENTITY, "P5")
	}
	if in.Token.CustomerID != in.Order.GetCustomerId() {
		return block(pb.Code_BLOCKED_IDENTITY, "P5")
	}
	return pass()
}

// P4 · Stale authority — has the permission expired, been cancelled, or run out?
// This owns the questions of whether the authority itself still exists: status,
// expiry, and lifetime exhaustion. Per-debit and per-window size are P2's, not
// this predicate's. Token is guaranteed non-nil here (P5 ran first).
func p4Authority(in Input) Outcome {
	t := in.Token
	if t.Status != domain.TokenConfirmed {
		return block(pb.Code_BLOCKED_AUTHORITY, "P4")
	}
	if t.ExpireAt > 0 && in.Now > t.ExpireAt {
		return block(pb.Code_BLOCKED_AUTHORITY, "P4")
	}
	consumedTotal := int64(0)
	if in.Block != nil {
		consumedTotal = in.Block.ConsumedTotal
	}
	if consumedTotal+in.Order.GetAmountPaise() > t.TokenCeilingPaise {
		return block(pb.Code_BLOCKED_AUTHORITY, "P4")
	}
	return pass()
}

// P6 · Binding — does the amount asked match the amount on this order? A missing
// cart_hash means nothing binds the amount to a real order; a known committed
// amount that disagrees with the request means the numbers do not agree.
func p6Binding(in Input) Outcome {
	if in.Order.GetCartHash() == "" {
		return block(pb.Code_BLOCKED_BINDING, "P6")
	}
	if in.BoundAmountPaise > 0 && in.BoundAmountPaise != in.Order.GetAmountPaise() {
		return block(pb.Code_BLOCKED_BINDING, "P6")
	}
	return pass()
}

// P2 · Scope — is it bigger than allowed, or to the wrong kind of place? Reads
// the effective (token ∩ overlay) bound: an overlay can only tighten, so the
// effective cap is the minimum of the two. Owns per-debit size, the per-day
// window against today's consumption, and merchant deny rules.
func p2Scope(in Input) Outcome {
	t := in.Token
	amt := in.Order.GetAmountPaise()

	perDebit := t.MaxAmountPaise
	perDay := t.MaxPerDayPaise
	if in.Overlay != nil {
		if c, ok := in.Overlay.PerWindowCaps["per_debit"]; ok && c < perDebit {
			perDebit = c
		}
		if c, ok := in.Overlay.PerWindowCaps["per_day"]; ok && c < perDay {
			perDay = c
		}
		if rule, ok := in.Overlay.MerchantRules[in.Order.GetMerchantId()]; ok && rule == "deny" {
			return stepUp(pb.Code_STEPUP_SCOPE, "P2")
		}
	}

	if amt > perDebit {
		return stepUp(pb.Code_STEPUP_SCOPE, "P2")
	}
	consumedToday := int64(0)
	if in.Block != nil {
		consumedToday = in.Block.ConsumedToday
	}
	if consumedToday+amt > perDay {
		return stepUp(pb.Code_STEPUP_SCOPE, "P2")
	}
	return pass()
}

// P3 · Unbound — is this debit backed by the instruction we were given? No
// envelope digest means the debit is unattested; above the value floor that is
// a step-up (silence is never read as consent). When the envelope's ceiling is
// known on the clock, a debit above it (and above the floor) is also a step-up.
func p3Unbound(in Input) Outcome {
	amt := in.Order.GetAmountPaise()
	if in.Order.GetEnvelopeDigest() == "" {
		if amt > DefaultValueFloorPaise {
			return stepUp(pb.Code_STEPUP_UNBOUND, "P3")
		}
		return pass()
	}
	if in.EnvelopeMaxPaise > 0 && amt > in.EnvelopeMaxPaise && amt > DefaultValueFloorPaise {
		return stepUp(pb.Code_STEPUP_UNBOUND, "P3")
	}
	return pass()
}
