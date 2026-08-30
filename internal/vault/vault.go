// Package vault is the encrypted VAULT — the erasable home for the PII the plane
// must keep but must never read on the clock (System Design §9): a session's raw
// instruction text and the contact behind it. Only the intent envelope's DIGEST
// travels on a request; the raw purpose text lives here, sealed.
//
// Encryption is app-layer AES-256-GCM. Each session has its own random data key
// held in a KeyRing OUTSIDE this store; a row keeps only session_id, field, the
// key's handle (key_id), a per-record nonce and the ciphertext. session_id is the
// GCM associated data, so a row's ciphertext is bound to its session and cannot be
// replayed under another.
//
// Erasure is crypto-shredding, which is what makes a DPDP deletion real: Erase
// deletes the rows AND drops the session's data key from the ring. Because the key
// never lived in this store, a Postgres backup that still holds the ciphertext is
// unrecoverable once the key is gone — there is nothing left to decrypt it with.
// In production the ring is a KMS/HSM; the in-memory ring here keeps the plane
// runnable with identical semantics, behind the same interface (mirroring how the
// Redis/Kafka adapters sit behind the store/bus interfaces).
package vault

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Field names the PII columns the VAULT seals, one row per (session, field).
type Field string

const (
	// FieldInstruction is the raw instruction / purpose text the LLM saw at
	// session start; only its digest travels on the request.
	FieldInstruction Field = "raw_instruction_text"
	// FieldContact is the contact behind the session (phone/email/handle).
	FieldContact Field = "contact"
)

// keyLen is the AES-256 key size; nonceLen is the GCM standard nonce size.
const (
	keyLen   = 32
	nonceLen = 12
)

// Errors Reveal can return.
var (
	// ErrNotFound is returned when no row exists for the (session, field).
	ErrNotFound = errors.New("vault: no such record")
	// ErrErased is returned when the row exists but its key has been shredded —
	// the ciphertext is permanently unreadable (a DPDP erasure).
	ErrErased = errors.New("vault: record erased (key shredded)")
)

// KeyRing holds the per-session data keys the VAULT encrypts with, OUTSIDE the row
// store so a backup of the rows never carries the key. Crypto-shredding relies on
// this: Shred destroys a session's key, and no backup can undo it. Production plugs
// a KMS/HSM behind this interface; NewMemoryKeyRing is the runnable dev default.
type KeyRing interface {
	// KeyFor returns the session's data key, minting a fresh 256-bit key on first
	// use. id is the opaque handle stored beside the ciphertext so Reveal can find
	// the key again.
	KeyFor(sessionID string) (id string, key []byte, err error)
	// Open returns the key for a stored handle; ok is false once it is shredded.
	Open(id string) (key []byte, ok bool)
	// Shred destroys all key material for a session. Afterwards Open of its handle
	// fails and any ciphertext sealed under it is unrecoverable.
	Shred(sessionID string)
}

// MemoryKeyRing is the in-process KeyRing: random per-session keys held in memory,
// never persisted, so tearing the process down or shredding a session leaves any
// ciphertext backup undecryptable. Safe for concurrent use.
type MemoryKeyRing struct {
	mu     sync.Mutex
	bySess map[string]string // session_id -> key_id
	keys   map[string][]byte // key_id -> 32-byte key
	rand   io.Reader
}

// NewMemoryKeyRing returns an empty ring seeded from crypto/rand.
func NewMemoryKeyRing() *MemoryKeyRing {
	return &MemoryKeyRing{
		bySess: map[string]string{},
		keys:   map[string][]byte{},
		rand:   rand.Reader,
	}
}

// KeyFor mints one key per session and reuses it on later calls (fetch-or-create).
func (r *MemoryKeyRing) KeyFor(sessionID string) (string, []byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if id, ok := r.bySess[sessionID]; ok {
		return id, r.keys[id], nil
	}
	idb := make([]byte, 16)
	if _, err := io.ReadFull(r.rand, idb); err != nil {
		return "", nil, fmt.Errorf("vault: mint key id: %w", err)
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r.rand, key); err != nil {
		return "", nil, fmt.Errorf("vault: mint key: %w", err)
	}
	id := hex.EncodeToString(idb)
	r.bySess[sessionID] = id
	r.keys[id] = key
	return id, key, nil
}

// Open returns the key for a handle, or ok=false once shredded.
func (r *MemoryKeyRing) Open(id string) ([]byte, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k, ok := r.keys[id]
	return k, ok
}

// Shred drops the session's key and its handle mapping; the key is gone for good.
func (r *MemoryKeyRing) Shred(sessionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if id, ok := r.bySess[sessionID]; ok {
		delete(r.keys, id)
		delete(r.bySess, sessionID)
	}
}

// sealBytes encrypts plaintext with AES-256-GCM under key, binding it to aad and
// returning a fresh random nonce alongside the ciphertext.
func sealBytes(key, plaintext, aad []byte) (nonce, ciphertext []byte, err error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, err
	}
	nonce = make([]byte, nonceLen)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, err
	}
	return nonce, gcm.Seal(nil, nonce, plaintext, aad), nil
}

// openBytes decrypts and authenticates ciphertext; a tampered row or the wrong aad
// (a different session) fails the GCM tag check here.
func openBytes(key, nonce, ciphertext, aad []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return gcm.Open(nil, nonce, ciphertext, aad)
}

// schemaDDL creates the ledger's sibling table if absent. The row holds only what
// is safe at rest: no plaintext, and no key — just the handle to the key that
// lives in the ring.
const schemaDDL = `
CREATE TABLE IF NOT EXISTS vault (
	session_id TEXT  NOT NULL,
	field      TEXT  NOT NULL,
	key_id     TEXT  NOT NULL,
	nonce      BYTEA NOT NULL,
	ciphertext BYTEA NOT NULL,
	PRIMARY KEY (session_id, field)
);`

// Vault is the PostgreSQL-backed encrypted store. It holds a pool and the KeyRing
// the keys live in; the pool only ever sees ciphertext.
type Vault struct {
	pool *pgxpool.Pool
	ring KeyRing
}

// Open dials the pool, verifies reachability and ensures the schema, so a bad DSN
// fails at startup rather than on the first seal. ring is where the data keys live.
func Open(ctx context.Context, dsn string, ring KeyRing) (*Vault, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("vault: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("vault: ping: %w", err)
	}
	if _, err := pool.Exec(ctx, schemaDDL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("vault: ensure schema: %w", err)
	}
	return &Vault{pool: pool, ring: ring}, nil
}

// Close releases the pool. It does not touch the ring.
func (v *Vault) Close() { v.pool.Close() }

// Seal encrypts one PII field under the session's key and upserts the row. Re-
// sealing a field overwrites it in place under the same session key.
func (v *Vault) Seal(ctx context.Context, sessionID string, field Field, plaintext string) error {
	id, key, err := v.ring.KeyFor(sessionID)
	if err != nil {
		return err
	}
	nonce, ct, err := sealBytes(key, []byte(plaintext), []byte(sessionID))
	if err != nil {
		return fmt.Errorf("vault: seal: %w", err)
	}
	_, err = v.pool.Exec(ctx,
		`INSERT INTO vault (session_id, field, key_id, nonce, ciphertext)
		 VALUES ($1,$2,$3,$4,$5)
		 ON CONFLICT (session_id, field)
		 DO UPDATE SET key_id=EXCLUDED.key_id, nonce=EXCLUDED.nonce, ciphertext=EXCLUDED.ciphertext`,
		sessionID, string(field), id, nonce, ct)
	if err != nil {
		return fmt.Errorf("vault: store: %w", err)
	}
	return nil
}

// Reveal decrypts one field. It returns ErrNotFound when no row exists and
// ErrErased when the row survives but its key has been shredded.
func (v *Vault) Reveal(ctx context.Context, sessionID string, field Field) (string, error) {
	var (
		keyID string
		nonce []byte
		ct    []byte
	)
	err := v.pool.QueryRow(ctx,
		`SELECT key_id, nonce, ciphertext FROM vault WHERE session_id=$1 AND field=$2`,
		sessionID, string(field)).Scan(&keyID, &nonce, &ct)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("vault: read: %w", err)
	}
	key, ok := v.ring.Open(keyID)
	if !ok {
		return "", ErrErased
	}
	pt, err := openBytes(key, nonce, ct, []byte(sessionID))
	if err != nil {
		return "", fmt.Errorf("vault: decrypt: %w", err)
	}
	return string(pt), nil
}

// Erase performs a DPDP deletion: it removes every row for the session AND shreds
// the session's key. The key-drop is the irreversible act — even a backup that
// still holds the ciphertext cannot be decrypted once it is gone. It returns the
// number of rows deleted.
func (v *Vault) Erase(ctx context.Context, sessionID string) (int, error) {
	tag, err := v.pool.Exec(ctx, `DELETE FROM vault WHERE session_id=$1`, sessionID)
	if err != nil {
		return 0, fmt.Errorf("vault: erase rows: %w", err)
	}
	v.ring.Shred(sessionID)
	return int(tag.RowsAffected()), nil
}

// Sink adapts a Vault to the stream-processor's VAULT-writer seam: a plain-string
// Seal the stream package can depend on without importing this package's Field
// type. It is the VAULT counterpart of chain's Emit sink, with one deliberate
// difference — Seal RETURNS its error rather than reporting out of band, because
// sealing the PII is the whole purpose of the fold it runs in: a transient failure
// must redeliver so the raw text is never lost, not logged-and-dropped like the
// CHAIN append that rides alongside a block-state fold.
type Sink struct{ v *Vault }

// NewSink wraps a Vault as a stream VAULT sink.
func NewSink(v *Vault) *Sink { return &Sink{v: v} }

// Seal encrypts one field of a session's PII, converting the wire field name to a
// vault.Field. An empty plaintext is skipped (a session may seal an instruction
// with no contact on file), so an absent field never writes an empty row.
func (s *Sink) Seal(ctx context.Context, sessionID, field, plaintext string) error {
	if plaintext == "" {
		return nil
	}
	return s.v.Seal(ctx, sessionID, Field(field), plaintext)
}



