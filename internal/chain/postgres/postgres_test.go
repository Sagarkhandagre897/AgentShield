package pgchain

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
)

// These tests need a real PostgreSQL — set AGENTSHIELD_TEST_POSTGRES_DSN to a
// reachable instance (e.g. the deploy/docker-compose Postgres:
//   AGENTSHIELD_TEST_POSTGRES_DSN=postgres://agentshield:agentshield@localhost:5432/agentshield?sslmode=disable
// ). Absent it, they skip cleanly, the same way the ML-wheel tests skip — the
// hash definition itself is proven in internal/chain's pure tests.
const dsnEnv = "AGENTSHIELD_TEST_POSTGRES_DSN"

func rec(id, decision string) *domain.ProvenanceRecord {
	return &domain.ProvenanceRecord{
		EvaluationID:   id,
		RequestDigest:  "req_" + id,
		Decision:       decision,
		Code:           "OK_ALLOW",
		EvidenceDigest: "ev_" + id,
		PolicyVersion:  3,
		TS:             1_000_000,
	}
}

// open returns a fresh, empty ledger: it truncates the table so each test starts
// from genesis regardless of what ran before.
func open(t *testing.T) *Chain {
	t.Helper()
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("%s not set; skipping Postgres integration test", dsnEnv)
	}
	ctx := context.Background()
	c, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := c.pool.Exec(ctx, "TRUNCATE provenance RESTART IDENTITY"); err != nil {
		c.Close()
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestAppendLinksAndPersists(t *testing.T) {
	ctx := context.Background()
	c := open(t)

	e1, err := c.Append(ctx, rec("e1", "ALLOW"))
	if err != nil {
		t.Fatalf("append e1: %v", err)
	}
	e2, _ := c.Append(ctx, rec("e2", "STEP_UP"))
	e3, _ := c.Append(ctx, rec("e3", "ALLOW"))

	if e1.Record.PrevHash != "" {
		t.Fatalf("genesis prev_hash must be empty, got %q", e1.Record.PrevHash)
	}
	if e2.Record.PrevHash != e1.Hash || e3.Record.PrevHash != e2.Hash {
		t.Fatal("records must link head-to-tail")
	}
	if n, _ := c.Len(ctx); n != 3 {
		t.Fatalf("len = %d, want 3", n)
	}
	if h, _ := c.Head(ctx); h != e3.Hash {
		t.Fatalf("head = %q, want %q", h, e3.Hash)
	}
	if err := c.Verify(ctx); err != nil {
		t.Fatalf("clean chain must verify: %v", err)
	}
}

func TestEmptyChainVerifies(t *testing.T) {
	ctx := context.Background()
	if err := open(t).Verify(ctx); err != nil {
		t.Fatalf("empty chain must verify: %v", err)
	}
}

func TestTamperedContentIsDetected(t *testing.T) {
	ctx := context.Background()
	c := open(t)
	c.Append(ctx, rec("e1", "ALLOW"))
	c.Append(ctx, rec("e2", "ALLOW"))
	c.Append(ctx, rec("e3", "ALLOW"))

	// Rewrite the middle row's decision in place — its stored hash no longer
	// matches its content.
	if _, err := c.pool.Exec(ctx, "UPDATE provenance SET decision='BLOCK' WHERE seq=2"); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	err := c.Verify(ctx)
	if err == nil || !strings.Contains(err.Error(), "index 1") {
		t.Fatalf("tamper must be caught at index 1, got: %v", err)
	}
}

func TestBrokenLinkIsDetected(t *testing.T) {
	ctx := context.Background()
	c := open(t)
	c.Append(ctx, rec("e1", "ALLOW"))
	c.Append(ctx, rec("e2", "ALLOW"))
	c.Append(ctx, rec("e3", "ALLOW"))

	// Remove the middle row — e3's prev_hash now points at a hash that is no
	// longer its predecessor.
	if _, err := c.pool.Exec(ctx, "DELETE FROM provenance WHERE seq=2"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := c.Verify(ctx); err == nil {
		t.Fatal("removing a record must break the chain")
	}
}

func TestSinkAppendsAndReopenPersists(t *testing.T) {
	ctx := context.Background()
	c := open(t)
	s := NewSink(c, func(err error) { t.Errorf("sink emit failed: %v", err) })
	s.Emit(ctx, rec("e1", "ALLOW"))
	s.Emit(ctx, rec("e2", "STEP_UP"))

	if n, _ := c.Len(ctx); n != 2 {
		t.Fatalf("sink must append each record: len = %d, want 2", n)
	}
	// Re-open a second handle on the same table — the ledger is durable, so the
	// history and its verifiability are still there.
	c2, err := Open(ctx, os.Getenv(dsnEnv))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c2.Close()
	if n, _ := c2.Len(ctx); n != 2 {
		t.Fatalf("reopened ledger len = %d, want 2", n)
	}
	if err := c2.Verify(ctx); err != nil {
		t.Fatalf("reopened ledger must verify: %v", err)
	}
}

func TestAppendNilIsRejected(t *testing.T) {
	ctx := context.Background()
	c := open(t)
	if _, err := c.Append(ctx, nil); err == nil {
		t.Fatal("appending a nil record must error")
	}
	if n, _ := c.Len(ctx); n != 0 {
		t.Fatalf("nil append must not grow the chain: len = %d", n)
	}
}
