// Package pgchain is the durable, PostgreSQL-backed CHAIN — the same append-only,
// hash-linked provenance ledger the in-memory internal/chain keeps, but persisted
// so the history survives a restart and an auditor can walk it long after the
// decision (System Design §9, §11).
//
// It reuses chain.HashRecord for the link and the verification, so a row written
// here hashes byte-for-byte identically to an in-memory entry — there is one
// definition of tamper-evidence, not two that could drift. Rows are ordered by a
// BIGSERIAL seq (insertion order); Append serializes on a transaction-scoped
// advisory lock so two concurrent appenders cannot read the same head and fork the
// chain. Verify re-walks by seq and points at the first row whose stored hash no
// longer matches its content or whose prev_hash no longer matches its predecessor.
package pgchain

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sagarkhandagre897/AgentShield/internal/chain"
	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
)

// appendLockKey is the constant advisory-lock key Append takes for its transaction.
// Any int64 works; it only has to be the same for every appender so they serialize
// against each other and nothing else.
const appendLockKey int64 = 0x41_53_43_48_41_49_4e // "ASCHAIN"

// schemaDDL creates the ledger table if it is absent. seq is the insertion order
// the walk relies on; the columns mirror domain.ProvenanceRecord one-to-one, plus
// the record's own hash.
const schemaDDL = `
CREATE TABLE IF NOT EXISTS provenance (
	seq              BIGSERIAL PRIMARY KEY,
	evaluation_id    TEXT    NOT NULL,
	prev_hash        TEXT    NOT NULL,
	request_digest   TEXT    NOT NULL,
	decision         TEXT    NOT NULL,
	code             TEXT    NOT NULL,
	predicate_failed TEXT    NOT NULL DEFAULT '',
	evidence_digest  TEXT    NOT NULL,
	policy_version   INTEGER NOT NULL,
	ts               BIGINT  NOT NULL,
	hash             TEXT    NOT NULL
);`

// Chain is a PostgreSQL-backed append-only ledger. It holds a connection pool and
// nothing else — the head lives in the table, read under lock at append time.
type Chain struct {
	pool *pgxpool.Pool
}

// Open dials the pool, verifies reachability, and ensures the schema exists so a
// misconfigured DSN fails at startup rather than on the first append.
func Open(ctx context.Context, dsn string) (*Chain, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgchain: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgchain: ping: %w", err)
	}
	if _, err := pool.Exec(ctx, schemaDDL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgchain: ensure schema: %w", err)
	}
	return &Chain{pool: pool}, nil
}

// Close releases the pool.
func (c *Chain) Close() { c.pool.Close() }

// Append seals a record onto the ledger. Inside one transaction it takes the
// append lock, reads the current head, stamps prev_hash, computes the record's
// hash (via the shared chain.HashRecord) and inserts the row — so the head a
// record links to is exactly the head at the moment it commits, never a stale one
// two appenders both saw. It returns the committed entry.
func (c *Chain) Append(ctx context.Context, rec *domain.ProvenanceRecord) (chain.Entry, error) {
	if rec == nil {
		return chain.Entry{}, fmt.Errorf("pgchain: nil record")
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return chain.Entry{}, fmt.Errorf("pgchain: begin: %w", err)
	}
	defer tx.Rollback(ctx) // no-op after a successful Commit

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", appendLockKey); err != nil {
		return chain.Entry{}, fmt.Errorf("pgchain: lock: %w", err)
	}

	head := ""
	err = tx.QueryRow(ctx, "SELECT hash FROM provenance ORDER BY seq DESC LIMIT 1").Scan(&head)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return chain.Entry{}, fmt.Errorf("pgchain: read head: %w", err)
	}

	r := *rec
	r.PrevHash = head
	h := chain.HashRecord(&r)

	_, err = tx.Exec(ctx,
		`INSERT INTO provenance
		 (evaluation_id, prev_hash, request_digest, decision, code,
		  predicate_failed, evidence_digest, policy_version, ts, hash)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		r.EvaluationID, r.PrevHash, r.RequestDigest, r.Decision, r.Code,
		r.PredicateFailed, r.EvidenceDigest, r.PolicyVersion, r.TS, h)
	if err != nil {
		return chain.Entry{}, fmt.Errorf("pgchain: insert: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return chain.Entry{}, fmt.Errorf("pgchain: commit: %w", err)
	}
	return chain.Entry{Record: r, Hash: h}, nil
}

// Entries returns the whole ledger oldest-first, each row rebuilt into an Entry
// (record + stored hash).
func (c *Chain) Entries(ctx context.Context) ([]chain.Entry, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT evaluation_id, prev_hash, request_digest, decision, code,
		        predicate_failed, evidence_digest, policy_version, ts, hash
		 FROM provenance ORDER BY seq ASC`)
	if err != nil {
		return nil, fmt.Errorf("pgchain: query entries: %w", err)
	}
	defer rows.Close()

	var out []chain.Entry
	for rows.Next() {
		var e chain.Entry
		r := &e.Record
		if err := rows.Scan(&r.EvaluationID, &r.PrevHash, &r.RequestDigest, &r.Decision,
			&r.Code, &r.PredicateFailed, &r.EvidenceDigest, &r.PolicyVersion, &r.TS, &e.Hash); err != nil {
			return nil, fmt.Errorf("pgchain: scan entry: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Verify re-walks the ledger by seq and returns an error at the first entry whose
// prev_hash does not match the entry before it, or whose content no longer hashes
// to its stored hash. A clean walk means nothing was altered or removed.
func (c *Chain) Verify(ctx context.Context) error {
	entries, err := c.Entries(ctx)
	if err != nil {
		return err
	}
	prev := ""
	for i := range entries {
		e := &entries[i]
		if e.Record.PrevHash != prev {
			return fmt.Errorf("pgchain: broken link at index %d (prev_hash mismatch)", i)
		}
		if chain.HashRecord(&e.Record) != e.Hash {
			return fmt.Errorf("pgchain: tampered record at index %d (hash mismatch)", i)
		}
		prev = e.Hash
	}
	return nil
}

// Len returns the number of committed records.
func (c *Chain) Len(ctx context.Context) (int, error) {
	var n int
	if err := c.pool.QueryRow(ctx, "SELECT count(*) FROM provenance").Scan(&n); err != nil {
		return 0, fmt.Errorf("pgchain: len: %w", err)
	}
	return n, nil
}

// Head returns the hash of the most recent record, or "" for an empty ledger.
func (c *Chain) Head(ctx context.Context) (string, error) {
	head := ""
	err := c.pool.QueryRow(ctx, "SELECT hash FROM provenance ORDER BY seq DESC LIMIT 1").Scan(&head)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("pgchain: head: %w", err)
	}
	return head, nil
}

// Sink adapts a durable Chain to the decision service's provenance sink. Emit is
// fire-and-forget by the sink contract, but a durable append CAN fail (the DB is
// down), so a failure is handed to onErr — metrics or a log — rather than dropped
// the way the in-memory sink can afford to. It satisfies decision.ProvenanceSink
// structurally (no import cycle).
type Sink struct {
	chain *Chain
	onErr func(error)
}

// NewSink returns a Sink writing to the given chain. onErr may be nil (failures
// are then silently dropped, matching the in-memory sink's posture).
func NewSink(c *Chain, onErr func(error)) *Sink { return &Sink{chain: c, onErr: onErr} }

// Emit appends the record off the caller's clock, reporting a durable failure to
// onErr instead of returning it (the sink is called after the reply is sent).
func (s *Sink) Emit(ctx context.Context, rec *domain.ProvenanceRecord) {
	if _, err := s.chain.Append(ctx, rec); err != nil && s.onErr != nil {
		s.onErr(err)
	}
}
