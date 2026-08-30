// Package domain holds the six objects that carry the whole system (System
// Design §10). They are small on purpose. Every amount is in paise (₹2,000 is
// 200000) so there is no floating-point money anywhere, and every identifier is
// one of the stable keys of §9 — never a VPA or UMN, which reassign on a
// payer-port.
//
// This package is pure data plus the invariants each object must hold. It
// imports nothing else in the tree, so both planes can depend on it.
package domain

import "errors"

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

// TokenType is the kind of mandate a token represents.
type TokenType string

const (
	TokenRecurring TokenType = "recurring"
	TokenOneTime   TokenType = "one_time"
	TokenTopUp     TokenType = "top_up"
)

// TokenStatus is the lifecycle state reconstructed from token events.
type TokenStatus string

const (
	TokenPending   TokenStatus = "pending"
	TokenConfirmed TokenStatus = "confirmed"
	TokenCancelled TokenStatus = "cancelled"
)

// ---------------------------------------------------------------------------
// Token — the permission slip. Primary key everywhere.
// ---------------------------------------------------------------------------

// Token is the mandate a customer granted an agent. The containment invariant
// (Validate) is enforced on every write: a violating write is rejected, not
// clamped.
type Token struct {
	TokenID           string      `json:"token_id"`
	CustomerID        string      `json:"customer_id"`
	Type              TokenType   `json:"type"`
	MaxAmountPaise    int64       `json:"max_amount_paise"`    // per-debit ceiling
	MaxPerDayPaise    int64       `json:"max_per_day_paise"`   // daily ceiling
	TokenCeilingPaise int64       `json:"token_ceiling_paise"` // lifetime ceiling
	Frequency         string      `json:"frequency"`           // daily | weekly | monthly
	ExpireAt          int64       `json:"expire_at"`           // epoch expiry; P4 compares against it
	Status            TokenStatus `json:"status"`
}

// ErrContainment is returned when a Token's ceilings are not ordered
// per-debit ≤ per-day ≤ lifetime.
var ErrContainment = errors.New("token containment violated: require max_amount_paise <= max_per_day_paise <= token_ceiling_paise")

// Validate enforces the containment invariant of §10.
func (t Token) Validate() error {
	if t.MaxAmountPaise <= 0 || t.MaxPerDayPaise <= 0 || t.TokenCeilingPaise <= 0 {
		return errors.New("token ceilings must be positive")
	}
	if !(t.MaxAmountPaise <= t.MaxPerDayPaise && t.MaxPerDayPaise <= t.TokenCeilingPaise) {
		return ErrContainment
	}
	return nil
}

// ---------------------------------------------------------------------------
// BlockState — event-sourced consumption, reached by token_id.
// ---------------------------------------------------------------------------

// BlockState is the lien state reconstructed off the clock from token events
// plus our own payment records — never assumed. The clock only reads it.
type BlockState struct {
	TokenID        string `json:"token_id"`
	ConsumedToday  int64  `json:"consumed_today"`  // paise consumed in the current day window
	ConsumedTotal  int64  `json:"consumed_total"`  // paise consumed over the token's life
	SeenNonces     []string `json:"seen_nonces"`   // replay evidence for P1
	LastComputedAt int64  `json:"last_computed_at"`
}

// ---------------------------------------------------------------------------
// PolicyOverlay — the customer's tightening. An overlay may only tighten.
// ---------------------------------------------------------------------------

// PolicyOverlay narrows a token. A write that would raise a cap or admit a
// category the token forbids is rejected (§10). P2 reads the effective
// (token ∩ overlay) bound.
type PolicyOverlay struct {
	TokenID           string            `json:"token_id"`
	AllowedCategories []string          `json:"allowed_categories"`
	MerchantRules     map[string]string `json:"merchant_rules"`   // merchant_id -> "allow" | "deny"
	PerWindowCaps     map[string]int64  `json:"per_window_caps"`  // window -> cap in paise, tighter than the token's
	OverlayVersion    int               `json:"overlay_version"`  // monotonic; recorded in every decision
}

// ---------------------------------------------------------------------------
// IntentEnvelope — sealed once per session. Raw text lives in the VAULT.
// ---------------------------------------------------------------------------

// Constraint is one explicit condition the user stated (e.g. once, before a date).
type Constraint struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

// IntentEnvelope is what the user asked for, sealed at session start by an LLM
// that runs exactly once, off the clock, and never decides. Only the digest
// travels on the request; the raw purpose text is erasable in the VAULT.
type IntentEnvelope struct {
	SessionID          string       `json:"session_id"`
	Purpose            string       `json:"purpose"`
	Category           string       `json:"category"`
	MaxAmountPaise     int64        `json:"max_amount_paise"`
	MerchantPreference string       `json:"merchant_preference"`
	Constraints        []Constraint `json:"constraints"`
	EnvelopeDigest     string       `json:"envelope_digest"` // sealed hash — written to order.notes
}

// ---------------------------------------------------------------------------
// FeatureRow — what the request reads. Always carries computed_at.
// ---------------------------------------------------------------------------

// SignalDeviation is one per-signal deviation with the number of observations
// it was computed from. Counts let the aggregator shrink a thin signal toward
// its prior rather than trusting it.
type SignalDeviation struct {
	Signal     string  `json:"signal"`
	Deviation  float64 `json:"deviation"`
	ObsCount   int64   `json:"obs_count"`
}

// FeatureRow is the precomputed row a worker deposited and the request reads by
// key. computed_at is ALWAYS present — it is what makes staleness a first-class
// fact (§8). A missing row is treated as missing, never as an optimistic zero.
type FeatureRow struct {
	Key                string            `json:"key"` // customer / token / agent / merchant / node id
	BehaviourDeviation float64           `json:"behaviour_deviation"`
	SignalDeviations   []SignalDeviation `json:"signal_deviations"`
	IntentDivergence   float64           `json:"intent_divergence"`
	NetworkRisk        float64           `json:"network_risk"`
	Reputation         float64           `json:"reputation"`
	ConsumptionFrac    float64           `json:"consumption_frac"` // model-free day-one signal
	ComputedAt         int64             `json:"computed_at"`      // freshness stamp — always present
}

// ---------------------------------------------------------------------------
// ProvenanceRecord — one row per decision on the CHAIN, written after the reply.
// ---------------------------------------------------------------------------

// ProvenanceRecord is the full, hash-linked audit record. It is what an
// operator and an auditor see, and it is written AFTER the reply — never on the
// caller's clock.
type ProvenanceRecord struct {
	EvaluationID   string `json:"evaluation_id"` // primary key; the id the caller received
	PrevHash       string `json:"prev_hash"`     // hash of the previous record — the chain link
	RequestDigest  string `json:"request_digest"`
	Decision       string `json:"decision"`
	Code           string `json:"code"`
	PredicateFailed string `json:"predicate_failed,omitempty"` // which of P1-P6 refused, if any
	EvidenceDigest string `json:"evidence_digest"`
	PolicyVersion  int    `json:"policy_version"`
	TS             int64  `json:"ts"` // when it was written — after the reply
}

// ---------------------------------------------------------------------------
// Event — every message on the bus. Consumers dedupe on event_id.
// ---------------------------------------------------------------------------

// Event is the envelope every bus message shares. token_id is the partition
// key, which is what guarantees per-token ordering. Delivery is at-least-once,
// so every consumer must be idempotent on event_id.
type Event struct {
	EventID    string            `json:"event_id"`
	Type       string            `json:"type"` // e.g. payment.captured, token.confirmed
	TokenID    string            `json:"token_id"`
	OccurredAt int64             `json:"occurred_at"`
	Payload    map[string]any    `json:"payload"`
	Source     string            `json:"source"`
	HMAC       string            `json:"hmac,omitempty"` // verified for webhooks
}
