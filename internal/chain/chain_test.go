package chain

import (
	"context"
	"strings"
	"testing"

	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
)

func rec(id, decision string) *domain.ProvenanceRecord {
	return &domain.ProvenanceRecord{
		EvaluationID:  id,
		RequestDigest: "req_" + id,
		Decision:      decision,
		Code:          "OK_ALLOW",
		EvidenceDigest: "ev_" + id,
		PolicyVersion: 3,
		TS:            1_000_000,
	}
}

func TestAppendLinksRecords(t *testing.T) {
	c := New()
	e1, _ := c.Append(rec("e1", "ALLOW"))
	e2, _ := c.Append(rec("e2", "STEP_UP"))
	e3, _ := c.Append(rec("e3", "ALLOW"))

	if e1.Record.PrevHash != "" {
		t.Fatalf("genesis prev_hash must be empty, got %q", e1.Record.PrevHash)
	}
	if e2.Record.PrevHash != e1.Hash {
		t.Fatalf("e2 must link to e1: prev=%q want %q", e2.Record.PrevHash, e1.Hash)
	}
	if e3.Record.PrevHash != e2.Hash {
		t.Fatalf("e3 must link to e2: prev=%q want %q", e3.Record.PrevHash, e2.Hash)
	}
	if c.Len() != 3 {
		t.Fatalf("len = %d, want 3", c.Len())
	}
	if c.Head() != e3.Hash {
		t.Fatalf("head = %q, want %q", c.Head(), e3.Hash)
	}
	if err := c.Verify(); err != nil {
		t.Fatalf("clean chain must verify: %v", err)
	}
}

func TestEmptyChainVerifies(t *testing.T) {
	if err := New().Verify(); err != nil {
		t.Fatalf("empty chain must verify: %v", err)
	}
}

func TestHashIsDeterministic(t *testing.T) {
	r := rec("e1", "ALLOW")
	if hashRecord(r) != hashRecord(r) {
		t.Fatal("hashRecord must be deterministic for identical input")
	}
	r2 := rec("e1", "BLOCK")
	if hashRecord(r) == hashRecord(r2) {
		t.Fatal("changing a field must change the hash")
	}
}

// TestTamperedContentIsDetected mutates a committed record's payload after the
// fact. Its stored hash no longer matches its content, so Verify must point at it.
func TestTamperedContentIsDetected(t *testing.T) {
	c := New()
	c.Append(rec("e1", "ALLOW"))
	c.Append(rec("e2", "ALLOW"))
	c.Append(rec("e3", "ALLOW"))

	// Someone edits the middle record in place, trying to rewrite history.
	c.entries[1].Record.Decision = "BLOCK"

	err := c.Verify()
	if err == nil {
		t.Fatal("tampered record must fail verification")
	}
	if !strings.Contains(err.Error(), "index 1") {
		t.Fatalf("error must point at the tampered index: %v", err)
	}
}

// TestBrokenLinkIsDetected removes a record from the middle. The following
// record's prev_hash now points at a hash that is no longer its predecessor.
func TestBrokenLinkIsDetected(t *testing.T) {
	c := New()
	c.Append(rec("e1", "ALLOW"))
	c.Append(rec("e2", "ALLOW"))
	c.Append(rec("e3", "ALLOW"))

	c.entries = append(c.entries[:1], c.entries[2:]...) // drop the middle entry

	if err := c.Verify(); err == nil {
		t.Fatal("removing a record must break the chain")
	}
}

func TestSinkAppends(t *testing.T) {
	c := New()
	s := NewSink(c)

	s.Emit(context.Background(), rec("e1", "ALLOW"))
	s.Emit(context.Background(), rec("e2", "STEP_UP"))

	if c.Len() != 2 {
		t.Fatalf("sink must append each emitted record: len = %d, want 2", c.Len())
	}
	if err := c.Verify(); err != nil {
		t.Fatalf("chain fed by sink must verify: %v", err)
	}
}

func TestAppendNilIsRejected(t *testing.T) {
	c := New()
	if _, err := c.Append(nil); err == nil {
		t.Fatal("appending a nil record must error")
	}
	if c.Len() != 0 {
		t.Fatalf("nil append must not grow the chain: len = %d", c.Len())
	}
}
