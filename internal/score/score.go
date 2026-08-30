// Package score is the scoreEnsemble() and decide() stages (System Design
// §6-§7). It runs only after the deterministic spine has passed, and it obeys
// two invariants that never bend:
//
//  1. It can only ALLOW or ask for a STEP-UP. It never BLOCKs — a block is the
//     predicates' alone. This is enforced by construction: Decide returns no
//     other answer.
//  2. When the feature view is degraded (any figure missing or stale) it fails
//     closed to a STEP-UP. It never allows on a guessed score.
//
// The Scorer is pluggable. The one here is a deterministic, model-free
// linear-logistic combiner — the "pre-model" placeholder. A calibrated model
// trained off the clock implements the same interface and drops in unchanged;
// the decide() maths above it does not move.
package score

import (
	"math"

	pb "github.com/Sagarkhandagre897/AgentShield/gen/go/agentshield/v1"
)

// DefaultInterruptionCostPaise is the modelled cost of a wrongful STEP-UP: the
// expected damage of interrupting a genuine customer (friction, abandonment).
// A step-up is worth it only when the expected loss of allowing exceeds it.
// Provisional and policy-tunable. ₹50.
const DefaultInterruptionCostPaise int64 = 5000

// Evidence is the flat set of figures the scorer combines for one request. The
// orchestrator maps the feature View onto it (each figure read from the row of
// the entity it belongs to); the scorer stays independent of the store shape.
// Every figure is oriented so that a larger value means more risk — except
// Reputation, where a larger value means more trust.
type Evidence struct {
	BehaviourDeviation float64 // behaviour engine: distance from the entity's own baseline
	IntentDivergence   float64 // intent engine: distance from the sealed intent envelope
	NetworkRisk        float64 // graph engine: proximity to known-bad structure
	Reputation         float64 // standing in [0,1]; higher lowers risk
	ConsumptionFrac    float64 // model-free day-one signal: fraction of authority already used
	Degraded           bool    // set by readFeatures() when any figure was missing or stale
}

// Scorer turns Evidence into a single calibrated probability that the debit is
// bad or misaligned, p in [0,1]. Pluggable: LinearScorer now, a trained
// calibrator later.
type Scorer interface {
	Score(ev Evidence) float64
}

// Weights parameterise the linear-logistic combiner.
type Weights struct {
	Behaviour   float64
	Intent      float64
	Network     float64
	Consumption float64
	Reputation  float64
	Bias        float64
}

// DefaultWeights is the provisional pre-model calibration. Deliberately simple
// and monotonic: higher deviation / divergence / network-risk / consumption
// raise p, higher reputation lowers it. These are placeholders for a calibrated
// model, not tuned values.
var DefaultWeights = Weights{
	Behaviour:   1.5,
	Intent:      1.5,
	Network:     1.2,
	Consumption: 0.8,
	Reputation:  1.0,
	Bias:        -2.2,
}

// LinearScorer is a logistic over a weighted sum of the clamped figures. Its
// output is bounded (0,1) and monotonic in every risk input, which is all
// decide() relies on.
type LinearScorer struct{ w Weights }

// NewLinearScorer builds a LinearScorer with the given weights.
func NewLinearScorer(w Weights) *LinearScorer { return &LinearScorer{w: w} }

func clamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}

// Score combines the evidence into a calibrated probability.
func (s *LinearScorer) Score(ev Evidence) float64 {
	z := s.w.Bias +
		s.w.Behaviour*clamp01(ev.BehaviourDeviation) +
		s.w.Intent*clamp01(ev.IntentDivergence) +
		s.w.Network*clamp01(ev.NetworkRisk) +
		s.w.Consumption*clamp01(ev.ConsumptionFrac) -
		s.w.Reputation*clamp01(ev.Reputation)
	return 1.0 / (1.0 + math.Exp(-z))
}

var _ Scorer = (*LinearScorer)(nil)

// Params carry the policy figures decide() weighs the score against.
type Params struct {
	// InterruptionCostPaise is the cost of a wrongful STEP-UP. Allowing is
	// preferred until the expected loss of doing so exceeds this.
	InterruptionCostPaise int64
}

// Decision is the internal outcome of scoring. Answer is ALLOW or STEP_UP only
// (never BLOCK). Score and ExpectedLoss are internal evidence: they are written
// to the CHAIN provenance record but are deliberately withheld from the caller's
// lean verdict, so the score surface cannot be probed.
type Decision struct {
	Answer       pb.Answer
	Code         pb.Code
	Score        float64 // calibrated p; internal only
	ExpectedLoss int64   // paise; internal only
	FailClosed   bool    // true when the STEP-UP was forced by a degraded view
}

// Decide applies the expected-loss rule. A degraded view fails closed to a
// STEP-UP first, without scoring. Otherwise the scorer produces p, the expected
// loss is p × rupees at risk, and the request is stepped up only when that loss
// exceeds the interruption cost. It never returns BLOCK.
func Decide(ev Evidence, amountPaise int64, p Params, scorer Scorer) Decision {
	if ev.Degraded {
		return Decision{
			Answer:     pb.Answer_ANSWER_STEP_UP,
			Code:       pb.Code_STEPUP_FAILCLOSED,
			FailClosed: true,
		}
	}
	sc := scorer.Score(ev)
	expectedLoss := int64(sc * float64(amountPaise))
	if expectedLoss > p.InterruptionCostPaise {
		return Decision{
			Answer:       pb.Answer_ANSWER_STEP_UP,
			Code:         pb.Code_STEPUP_RISK,
			Score:        sc,
			ExpectedLoss: expectedLoss,
		}
	}
	return Decision{
		Answer:       pb.Answer_ANSWER_ALLOW,
		Code:         pb.Code_OK_ALLOW,
		Score:        sc,
		ExpectedLoss: expectedLoss,
	}
}
