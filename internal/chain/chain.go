// Package chain is the CHAIN — the append-only, hash-linked provenance ledger
// the asynchronous plane keeps off the clock (System Design §9, §11). Every
// decision the synchronous service makes is emitted here after the reply, one
// record per evaluation.
//
// Each record carries the hash of the record before it (prev_hash), and its own
// hash is taken over its content including that link. Change any field of any
// past record and its hash no longer matches what the next record points at, so
// Verify walking the chain detects it. This is what lets an operator and an
// auditor trust the history: it is append-only and tamper-evident, not merely a
// log.
//
// The in-memory ledger here keeps the plane runnable; a PostgreSQL adapter with
// the same shape is the durable backing.
package chain

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
)

// Entry is one committed record together with its computed hash. Record.PrevHash
// is the hash of the entry before it; Hash is this record's own hash.
type Entry struct {
	Record domain.ProvenanceRecord
	Hash   string
}

// hashRecord takes the SHA-256 over the record's linked fields in a fixed order.
// PrevHash is included, which is what chains the entries together.
func hashRecord(r *domain.ProvenanceRecord) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x1f%s\x1f%s\x1f%s\x1f%s\x1f%s\x1f%s\x1f%d\x1f%d",
		r.PrevHash,
		r.EvaluationID,
		r.RequestDigest,
		r.Decision,
		r.Code,
		r.PredicateFailed,
		r.EvidenceDigest,
		r.PolicyVersion,
		r.TS,
	)
	return hex.EncodeToString(h.Sum(nil))
}

// HashRecord is the exported tamper-evidence definition: the SHA-256 over a
// record's linked fields (PrevHash included). A durable backing — the PostgreSQL
// adapter — reuses THIS function so its links and its Verify agree byte-for-byte
// with the in-memory ledger; there must only ever be one definition of the hash.
func HashRecord(r *domain.ProvenanceRecord) string { return hashRecord(r) }

// Chain is an in-memory append-only ledger.
type Chain struct {
	mu       sync.Mutex
	entries  []Entry
	lastHash string
}

// New returns an empty chain. The genesis record's prev_hash is the empty string.
func New() *Chain { return &Chain{} }

// Append seals a record onto the chain: it stamps prev_hash with the current
// head, computes the record's hash and commits it. It returns the committed
// entry. Append is the only mutator, so the chain only ever grows.
func (c *Chain) Append(rec *domain.ProvenanceRecord) (Entry, error) {
	if rec == nil {
		return Entry{}, fmt.Errorf("chain: nil record")
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	r := *rec
	r.PrevHash = c.lastHash
	e := Entry{Record: r, Hash: hashRecord(&r)}
	c.entries = append(c.entries, e)
	c.lastHash = e.Hash
	return e, nil
}

// Verify re-walks the chain and returns an error at the first entry whose stored
// hash no longer matches its content, or whose prev_hash does not match the
// entry before it. A clean walk means nothing has been altered or removed.
func (c *Chain) Verify() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	prev := ""
	for i := range c.entries {
		e := &c.entries[i]
		if e.Record.PrevHash != prev {
			return fmt.Errorf("chain: broken link at index %d (prev_hash mismatch)", i)
		}
		if hashRecord(&e.Record) != e.Hash {
			return fmt.Errorf("chain: tampered record at index %d (hash mismatch)", i)
		}
		prev = e.Hash
	}
	return nil
}

// Len returns the number of committed records.
func (c *Chain) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// Head returns the hash of the most recent record, or "" for an empty chain.
func (c *Chain) Head() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastHash
}

// Entries returns a copy of the ledger, oldest first.
func (c *Chain) Entries() []Entry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Entry, len(c.entries))
	copy(out, c.entries)
	return out
}

// Sink adapts a Chain to the decision service's provenance sink: Emit appends
// the record off the caller's clock. It satisfies decision.ProvenanceSink
// structurally (no import cycle).
type Sink struct{ chain *Chain }

// NewSink returns a Sink writing to the given chain.
func NewSink(c *Chain) *Sink { return &Sink{chain: c} }

// Emit appends the record. The sink is fire-and-forget by contract; an append
// failure (only a nil record here) is dropped, which a durable backing would
// surface via metrics instead.
func (s *Sink) Emit(_ context.Context, rec *domain.ProvenanceRecord) {
	_, _ = s.chain.Append(rec)
}
