// Package features is the readFeatures() stage (System Design §8). On the clock
// it does exactly one keyed multi-get across the stable entity ids on the
// request, then classifies each row's freshness against a staleness budget.
//
// It decides nothing. Its whole job is to report, honestly, which precomputed
// figures are present and fresh, which are stale, and which are missing — and
// to roll that up into a single Degraded flag. Absence is never filled with an
// optimistic zero: a blank is a blank, and a blank raises risk. When any figure
// the request asked for is missing or stale, Degraded is set, and the scorer /
// decide() must fail closed to a STEP-UP rather than trust a guessed score.
package features

import (
	"context"

	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
	"github.com/Sagarkhandagre897/AgentShield/internal/store"
)

// DefaultStalenessBudgetSeconds is how old a feature row may be before it is
// treated as stale. Figures are computed off the clock and lag by design; a row
// older than this can no longer be trusted for a live decision. Configurable per
// deployment; a single budget now, per-entity budgets later if needed.
const DefaultStalenessBudgetSeconds int64 = 300

// Freshness classifies one requested key against the staleness budget.
type Freshness int

const (
	// Missing means no row exists for the key. Treated as missing, never zero.
	Missing Freshness = iota
	// Stale means a row exists but its computed_at is older than the budget.
	Stale
	// Fresh means a row exists and is within the budget.
	Fresh
)

func (f Freshness) String() string {
	switch f {
	case Fresh:
		return "fresh"
	case Stale:
		return "stale"
	default:
		return "missing"
	}
}

// EntityKeys are the stable keys a single request reads features by (§9). Only
// non-empty keys are queried; Extra carries any additional keys (e.g. a network
// node or session) without churning the struct.
type EntityKeys struct {
	Customer string
	Token    string
	Agent    string
	Merchant string
	Extra    []string
}

// list returns the non-empty keys, de-duplicated, preserving first-seen order.
func (k EntityKeys) list() []string {
	seen := make(map[string]struct{}, 4+len(k.Extra))
	out := make([]string, 0, 4+len(k.Extra))
	add := func(key string) {
		if key == "" {
			return
		}
		if _, dup := seen[key]; dup {
			return
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	add(k.Customer)
	add(k.Token)
	add(k.Agent)
	add(k.Merchant)
	for _, e := range k.Extra {
		add(e)
	}
	return out
}

// View is the result of readFeatures(): the rows actually present (fresh or
// stale), a freshness verdict for every requested key, and the lists of missing
// and stale keys for provenance. Degraded rolls it up for decide().
type View struct {
	Rows      map[string]*domain.FeatureRow // present rows only (a stale row is kept, flagged Stale)
	Freshness map[string]Freshness          // one entry per requested key
	Missing   []string                      // keys with no row
	StaleKeys []string                      // keys whose row is older than the budget
	degraded  bool
}

// Degraded reports whether any requested figure was missing or stale. When it
// is true the ensemble score cannot be trusted and decide() fails closed to a
// STEP-UP — it never allows on a guessed figure.
func (v *View) Degraded() bool { return v.degraded }

// Reader performs the keyed feature read and freshness classification.
type Reader struct {
	store  store.FeatureStore
	budget int64 // staleness budget in seconds; <= 0 disables the staleness check
}

// NewReader builds a Reader over a feature store with the given staleness budget
// in seconds. Pass DefaultStalenessBudgetSeconds for the standard budget.
func NewReader(fs store.FeatureStore, budgetSeconds int64) *Reader {
	return &Reader{store: fs, budget: budgetSeconds}
}

// Read fetches the rows for the given keys in one multi-get and classifies each
// against the staleness budget at time now (epoch seconds).
//
// If the store itself is unreachable, every key is reported Missing, Degraded is
// set, and the store error is returned — the orchestrator fails closed. On a
// normal read, a key absent from the store's result is Missing; a present row
// older than the budget is Stale (the row is still returned, flagged, for
// provenance); anything else is Fresh.
func (r *Reader) Read(ctx context.Context, keys EntityKeys, now int64) (*View, error) {
	list := keys.list()
	v := &View{
		Rows:      make(map[string]*domain.FeatureRow, len(list)),
		Freshness: make(map[string]Freshness, len(list)),
	}

	rows, err := r.store.MultiGet(ctx, list)
	if err != nil {
		// Store unreachable: nothing can be trusted. Fail closed on every key.
		for _, k := range list {
			v.Freshness[k] = Missing
			v.Missing = append(v.Missing, k)
		}
		v.degraded = true
		return v, err
	}

	for _, k := range list {
		row, ok := rows[k]
		if !ok {
			v.Freshness[k] = Missing
			v.Missing = append(v.Missing, k)
			v.degraded = true
			continue
		}
		if r.budget > 0 && now-row.ComputedAt > r.budget {
			v.Freshness[k] = Stale
			v.StaleKeys = append(v.StaleKeys, k)
			v.Rows[k] = row
			v.degraded = true
			continue
		}
		v.Freshness[k] = Fresh
		v.Rows[k] = row
	}
	return v, nil
}
