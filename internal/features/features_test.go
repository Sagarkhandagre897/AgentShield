package features

import (
	"context"
	"errors"
	"testing"

	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
	"github.com/Sagarkhandagre897/AgentShield/internal/store/memory"
)

const now int64 = 1_000_000

func seed(t *testing.T, computedAt map[string]int64) *memory.FeatureStore {
	t.Helper()
	fs := memory.NewFeatureStore()
	for key, ts := range computedAt {
		if err := fs.Put(context.Background(), &domain.FeatureRow{Key: key, ComputedAt: ts}); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}
	return fs
}

func TestAllFresh(t *testing.T) {
	fs := seed(t, map[string]int64{
		"cust_1": now, "tok_1": now, "agent_1": now, "merch_1": now,
	})
	r := NewReader(fs, DefaultStalenessBudgetSeconds)
	v, err := r.Read(context.Background(), EntityKeys{
		Customer: "cust_1", Token: "tok_1", Agent: "agent_1", Merchant: "merch_1",
	}, now)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if v.Degraded() {
		t.Fatalf("all-fresh read must not be degraded: missing=%v stale=%v", v.Missing, v.StaleKeys)
	}
	for k, f := range v.Freshness {
		if f != Fresh {
			t.Fatalf("key %s: got %s, want fresh", k, f)
		}
	}
	if len(v.Rows) != 4 {
		t.Fatalf("want 4 present rows, got %d", len(v.Rows))
	}
}

func TestMissingKeyDegrades(t *testing.T) {
	fs := seed(t, map[string]int64{"cust_1": now, "tok_1": now}) // agent + merchant absent
	r := NewReader(fs, DefaultStalenessBudgetSeconds)
	v, err := r.Read(context.Background(), EntityKeys{
		Customer: "cust_1", Token: "tok_1", Agent: "agent_1", Merchant: "merch_1",
	}, now)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !v.Degraded() {
		t.Fatalf("a missing key must degrade the view")
	}
	if v.Freshness["agent_1"] != Missing || v.Freshness["merch_1"] != Missing {
		t.Fatalf("absent keys must be Missing: %v", v.Freshness)
	}
	if _, present := v.Rows["agent_1"]; present {
		t.Fatalf("a missing key must never appear in Rows (no optimistic zero)")
	}
	if len(v.Missing) != 2 {
		t.Fatalf("want 2 missing keys, got %v", v.Missing)
	}
}

func TestStaleRowDegradesButIsKept(t *testing.T) {
	fs := seed(t, map[string]int64{
		"cust_1": now,                                   // fresh
		"tok_1":  now - DefaultStalenessBudgetSeconds - 1, // one second past the budget
	})
	r := NewReader(fs, DefaultStalenessBudgetSeconds)
	v, err := r.Read(context.Background(), EntityKeys{Customer: "cust_1", Token: "tok_1"}, now)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !v.Degraded() {
		t.Fatalf("a stale row must degrade the view")
	}
	if v.Freshness["tok_1"] != Stale {
		t.Fatalf("tok_1 should be Stale, got %s", v.Freshness["tok_1"])
	}
	if v.Freshness["cust_1"] != Fresh {
		t.Fatalf("cust_1 should be Fresh, got %s", v.Freshness["cust_1"])
	}
	// A stale row is still surfaced (flagged), for provenance and observability.
	if _, ok := v.Rows["tok_1"]; !ok {
		t.Fatalf("a stale row should be kept in Rows, flagged Stale")
	}
	if len(v.StaleKeys) != 1 || v.StaleKeys[0] != "tok_1" {
		t.Fatalf("want tok_1 in StaleKeys, got %v", v.StaleKeys)
	}
}

func TestRowExactlyAtBudgetIsFresh(t *testing.T) {
	// age == budget is within budget (stale requires strictly older).
	fs := seed(t, map[string]int64{"cust_1": now - DefaultStalenessBudgetSeconds})
	r := NewReader(fs, DefaultStalenessBudgetSeconds)
	v, _ := r.Read(context.Background(), EntityKeys{Customer: "cust_1"}, now)
	if v.Freshness["cust_1"] != Fresh {
		t.Fatalf("row aged exactly to the budget must be Fresh, got %s", v.Freshness["cust_1"])
	}
}

func TestKeysAreDedupedAndEmptyDropped(t *testing.T) {
	fs := seed(t, map[string]int64{"cust_1": now})
	r := NewReader(fs, DefaultStalenessBudgetSeconds)
	// Customer and Token share a key; Merchant is empty; Extra repeats cust_1.
	v, _ := r.Read(context.Background(), EntityKeys{
		Customer: "cust_1", Token: "cust_1", Merchant: "", Extra: []string{"cust_1", "node_1"},
	}, now)
	if _, ok := v.Freshness[""]; ok {
		t.Fatalf("empty key must never be queried")
	}
	if len(v.Freshness) != 2 { // cust_1 (deduped) + node_1
		t.Fatalf("want 2 distinct keys, got %d: %v", len(v.Freshness), v.Freshness)
	}
	if v.Freshness["node_1"] != Missing {
		t.Fatalf("node_1 should be Missing, got %s", v.Freshness["node_1"])
	}
}

// erroringStore is a store.FeatureStore whose MultiGet always fails, to exercise
// the store-unreachable path.
type erroringStore struct{}

func (erroringStore) MultiGet(context.Context, []string) (map[string]*domain.FeatureRow, error) {
	return nil, errors.New("feature store unreachable")
}
func (erroringStore) Put(context.Context, *domain.FeatureRow) error { return nil }

func TestStoreErrorFailsClosed(t *testing.T) {
	r := NewReader(erroringStore{}, DefaultStalenessBudgetSeconds)
	v, err := r.Read(context.Background(), EntityKeys{Customer: "cust_1", Token: "tok_1"}, now)
	if err == nil {
		t.Fatalf("store error must be returned")
	}
	if !v.Degraded() {
		t.Fatalf("an unreachable store must degrade the view")
	}
	if len(v.Missing) != 2 {
		t.Fatalf("every key must be Missing when the store is unreachable, got %v", v.Missing)
	}
	if len(v.Rows) != 0 {
		t.Fatalf("no rows can be present when the store is unreachable")
	}
}
