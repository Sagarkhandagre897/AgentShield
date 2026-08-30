package score

import (
	"testing"

	pb "github.com/Sagarkhandagre897/AgentShield/gen/go/agentshield/v1"
)

func TestLinearScorerMonotonicInRisk(t *testing.T) {
	s := NewLinearScorer(DefaultWeights)
	low := s.Score(Evidence{BehaviourDeviation: 0.1})
	high := s.Score(Evidence{BehaviourDeviation: 0.9})
	if !(high > low) {
		t.Fatalf("higher behaviour deviation must raise p: low=%.4f high=%.4f", low, high)
	}
	if low <= 0 || high >= 1 {
		t.Fatalf("p must be bounded in (0,1): low=%.4f high=%.4f", low, high)
	}
}

func TestLinearScorerReputationLowersRisk(t *testing.T) {
	s := NewLinearScorer(DefaultWeights)
	noRep := s.Score(Evidence{IntentDivergence: 0.8})
	goodRep := s.Score(Evidence{IntentDivergence: 0.8, Reputation: 1.0})
	if !(goodRep < noRep) {
		t.Fatalf("reputation must lower p: none=%.4f good=%.4f", noRep, goodRep)
	}
}

func TestLinearScorerClampsInputs(t *testing.T) {
	s := NewLinearScorer(DefaultWeights)
	atOne := s.Score(Evidence{NetworkRisk: 1.0})
	beyond := s.Score(Evidence{NetworkRisk: 5.0}) // must clamp to 1.0
	if atOne != beyond {
		t.Fatalf("inputs beyond 1.0 must clamp: at1=%.6f beyond=%.6f", atOne, beyond)
	}
}

// fixedScorer returns a constant p, to test decide()'s expected-loss boundary
// independently of the combiner's shape.
type fixedScorer float64

func (f fixedScorer) Score(Evidence) float64 { return float64(f) }

func TestDecideDegradedFailsClosed(t *testing.T) {
	// Even a tiny amount and a benign scorer must step up when degraded.
	d := Decide(Evidence{Degraded: true}, 1, Params{InterruptionCostPaise: 1_000_000}, fixedScorer(0.0))
	if d.Answer != pb.Answer_ANSWER_STEP_UP || d.Code != pb.Code_STEPUP_FAILCLOSED || !d.FailClosed {
		t.Fatalf("degraded view must fail closed: %+v", d)
	}
}

func TestDecideExpectedLossBoundary(t *testing.T) {
	// p=0.10, amount=100000 -> expected loss = 10000 paise.
	ev := Evidence{}
	const amount = 100_000

	// Loss exactly equal to the cost is not "exceeds": ALLOW.
	eq := Decide(ev, amount, Params{InterruptionCostPaise: 10_000}, fixedScorer(0.10))
	if eq.Answer != pb.Answer_ANSWER_ALLOW || eq.Code != pb.Code_OK_ALLOW {
		t.Fatalf("loss == cost must ALLOW: %+v", eq)
	}
	if eq.ExpectedLoss != 10_000 {
		t.Fatalf("expected loss = 10000, got %d", eq.ExpectedLoss)
	}

	// One paise cheaper to interrupt: STEP-UP on risk.
	over := Decide(ev, amount, Params{InterruptionCostPaise: 9_999}, fixedScorer(0.10))
	if over.Answer != pb.Answer_ANSWER_STEP_UP || over.Code != pb.Code_STEPUP_RISK {
		t.Fatalf("loss > cost must STEP-UP on risk: %+v", over)
	}
}

func TestDecideNeverBlocks(t *testing.T) {
	// Across the full range of scores and the degraded case, the ensemble must
	// never produce a BLOCK — only the predicates can block.
	for _, p := range []float64{0.0, 0.25, 0.5, 0.75, 1.0} {
		d := Decide(Evidence{}, 1_000_000, Params{InterruptionCostPaise: DefaultInterruptionCostPaise}, fixedScorer(p))
		if d.Answer == pb.Answer_ANSWER_BLOCK {
			t.Fatalf("score %.2f produced a BLOCK; the ensemble must never block", p)
		}
	}
	d := Decide(Evidence{Degraded: true}, 1_000_000, Params{}, fixedScorer(1.0))
	if d.Answer == pb.Answer_ANSWER_BLOCK {
		t.Fatalf("degraded case produced a BLOCK; the ensemble must never block")
	}
}

func TestDecideLowRiskAllows(t *testing.T) {
	// A quiet request with good reputation and a small amount is allowed.
	ev := Evidence{Reputation: 1.0}
	d := Decide(ev, 50_000, Params{InterruptionCostPaise: DefaultInterruptionCostPaise}, NewLinearScorer(DefaultWeights))
	if d.Answer != pb.Answer_ANSWER_ALLOW {
		t.Fatalf("quiet, well-reputed, small request should ALLOW: %+v", d)
	}
}
