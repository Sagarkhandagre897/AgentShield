package vault

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Unit tests — no database. The KeyRing and the AES-256-GCM primitives are the
// heart of the crypto-shred guarantee, so they are proven here and always run.
// ---------------------------------------------------------------------------

func TestMemoryKeyRingMintsStablePerSession(t *testing.T) {
	r := NewMemoryKeyRing()
	id1, k1, err := r.KeyFor("s1")
	if err != nil {
		t.Fatalf("KeyFor: %v", err)
	}
	id1b, k1b, _ := r.KeyFor("s1")
	if id1 != id1b || !bytes.Equal(k1, k1b) {
		t.Fatal("KeyFor must return the same key for the same session")
	}
	if len(k1) != keyLen {
		t.Fatalf("key length = %d, want %d", len(k1), keyLen)
	}
	id2, k2, _ := r.KeyFor("s2")
	if id2 == id1 || bytes.Equal(k1, k2) {
		t.Fatal("different sessions must get different keys")
	}
}

func TestMemoryKeyRingShredDropsKey(t *testing.T) {
	r := NewMemoryKeyRing()
	id, _, _ := r.KeyFor("s1")
	if _, ok := r.Open(id); !ok {
		t.Fatal("minted key must be openable")
	}
	r.Shred("s1")
	if _, ok := r.Open(id); ok {
		t.Fatal("shredded key must not be openable")
	}
	// A fresh seal after erasure mints a brand-new, unrelated key.
	id2, _, _ := r.KeyFor("s1")
	if id2 == id {
		t.Fatal("re-minting after shred must not resurrect the old handle")
	}
}

func TestCryptoRoundTrip(t *testing.T) {
	_, key, _ := NewMemoryKeyRing().KeyFor("s1")
	nonce, ct, err := sealBytes(key, []byte("pay the rent"), []byte("s1"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	pt, err := openBytes(key, nonce, ct, []byte("s1"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if string(pt) != "pay the rent" {
		t.Fatalf("round-trip = %q", pt)
	}
}

func TestCryptoWrongAADFails(t *testing.T) {
	_, key, _ := NewMemoryKeyRing().KeyFor("s1")
	nonce, ct, _ := sealBytes(key, []byte("secret"), []byte("s1"))
	// Decrypting the same ciphertext under a different session (AAD) must fail:
	// this is what binds a row to its own session.
	if _, err := openBytes(key, nonce, ct, []byte("s2")); err == nil {
		t.Fatal("decryption under the wrong AAD must fail")
	}
}

func TestCryptoTamperFails(t *testing.T) {
	_, key, _ := NewMemoryKeyRing().KeyFor("s1")
	nonce, ct, _ := sealBytes(key, []byte("secret"), []byte("s1"))
	ct[0] ^= 0xff // flip a ciphertext byte
	if _, err := openBytes(key, nonce, ct, []byte("s1")); err == nil {
		t.Fatal("tampered ciphertext must fail the GCM tag check")
	}
}

// ---------------------------------------------------------------------------
// Integration tests — need a real PostgreSQL (same instance as the CHAIN). Set
// AGENTSHIELD_TEST_POSTGRES_DSN; absent it they skip cleanly, the crypto itself
// being proven by the unit tests above.
// ---------------------------------------------------------------------------

const dsnEnv = "AGENTSHIELD_TEST_POSTGRES_DSN"

// newVault returns a fresh, empty vault sharing one in-memory ring, truncating the
// table so each test starts clean.
func newVault(t *testing.T) *Vault {
	t.Helper()
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("%s not set; skipping Postgres integration test", dsnEnv)
	}
	ctx := context.Background()
	v, err := Open(ctx, dsn, NewMemoryKeyRing())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := v.pool.Exec(ctx, "TRUNCATE vault"); err != nil {
		v.Close()
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(v.Close)
	return v
}

func TestSealRevealRoundTrip(t *testing.T) {
	ctx := context.Background()
	v := newVault(t)
	if err := v.Seal(ctx, "sess1", FieldInstruction, "pay my rent to landlord"); err != nil {
		t.Fatalf("seal instruction: %v", err)
	}
	if err := v.Seal(ctx, "sess1", FieldContact, "+91-99999-00000"); err != nil {
		t.Fatalf("seal contact: %v", err)
	}
	if got, _ := v.Reveal(ctx, "sess1", FieldInstruction); got != "pay my rent to landlord" {
		t.Fatalf("instruction = %q", got)
	}
	if got, _ := v.Reveal(ctx, "sess1", FieldContact); got != "+91-99999-00000" {
		t.Fatalf("contact = %q", got)
	}
}

func TestRevealMissingIsNotFound(t *testing.T) {
	v := newVault(t)
	if _, err := v.Reveal(context.Background(), "nope", FieldContact); err != ErrNotFound {
		t.Fatalf("missing reveal err = %v, want ErrNotFound", err)
	}
}

func TestReSealOverwrites(t *testing.T) {
	ctx := context.Background()
	v := newVault(t)
	v.Seal(ctx, "sess1", FieldInstruction, "first")
	v.Seal(ctx, "sess1", FieldInstruction, "second")
	if got, _ := v.Reveal(ctx, "sess1", FieldInstruction); got != "second" {
		t.Fatalf("re-seal must overwrite: got %q", got)
	}
}

func TestEraseDeletesAndShreds(t *testing.T) {
	ctx := context.Background()
	v := newVault(t)
	v.Seal(ctx, "sess1", FieldInstruction, "pay rent")
	v.Seal(ctx, "sess1", FieldContact, "contact")
	n, err := v.Erase(ctx, "sess1")
	if err != nil {
		t.Fatalf("erase: %v", err)
	}
	if n != 2 {
		t.Fatalf("erase removed %d rows, want 2", n)
	}
	if _, err := v.Reveal(ctx, "sess1", FieldInstruction); err != ErrNotFound {
		t.Fatalf("after erase, reveal err = %v, want ErrNotFound", err)
	}
}

// TestCryptoShredBeatsBackup is the crux of DPDP erasure: a backup taken before
// the erasure still holds the ciphertext, yet it is unrecoverable because the key
// is gone. We capture a row, erase (which shreds the key AND deletes the row), then
// re-insert the captured row to simulate restoring it from that backup — and the
// value can no longer be decrypted.
func TestCryptoShredBeatsBackup(t *testing.T) {
	ctx := context.Background()
	v := newVault(t)
	v.Seal(ctx, "sess1", FieldInstruction, "pay rent")

	var keyID string
	var nonce, ct []byte
	if err := v.pool.QueryRow(ctx,
		`SELECT key_id, nonce, ciphertext FROM vault WHERE session_id='sess1' AND field=$1`,
		string(FieldInstruction)).Scan(&keyID, &nonce, &ct); err != nil {
		t.Fatalf("capture row: %v", err)
	}

	if _, err := v.Erase(ctx, "sess1"); err != nil {
		t.Fatalf("erase: %v", err)
	}

	// Restore the captured ciphertext from "backup".
	if _, err := v.pool.Exec(ctx,
		`INSERT INTO vault (session_id, field, key_id, nonce, ciphertext) VALUES ('sess1',$1,$2,$3,$4)`,
		string(FieldInstruction), keyID, nonce, ct); err != nil {
		t.Fatalf("restore row: %v", err)
	}

	if _, err := v.Reveal(ctx, "sess1", FieldInstruction); err != ErrErased {
		t.Fatalf("restored ciphertext must be unreadable: err = %v, want ErrErased", err)
	}
}

// TestAADBindsCiphertextToSession proves session_id-as-AAD stops a row being moved
// to another session: relabel a row's session_id (its key survives, unshredded) and
// the decrypt fails the tag check under the new session.
func TestAADBindsCiphertextToSession(t *testing.T) {
	ctx := context.Background()
	v := newVault(t)
	v.Seal(ctx, "sess1", FieldContact, "+91-99999-00000")
	if _, err := v.pool.Exec(ctx,
		`UPDATE vault SET session_id='sess2' WHERE session_id='sess1'`); err != nil {
		t.Fatalf("relabel: %v", err)
	}
	_, err := v.Reveal(ctx, "sess2", FieldContact)
	if err == nil || !strings.Contains(err.Error(), "decrypt") {
		t.Fatalf("relabelled row must fail to decrypt, got: %v", err)
	}
}

// TestSinkSealsThroughWireFields exercises the stream-facing seam the stream-processor
// actually holds: Sink.Seal takes the wire field name (equal to the Field constants,
// e.g. bus.PayloadRawInstruction == string(FieldInstruction)) and lands an encrypted,
// revealable row. An empty field is skipped, so an absent contact writes no row.
func TestSinkSealsThroughWireFields(t *testing.T) {
	ctx := context.Background()
	v := newVault(t)
	s := NewSink(v)

	if err := s.Seal(ctx, "sess1", string(FieldInstruction), "refund order 42"); err != nil {
		t.Fatalf("sink seal instruction: %v", err)
	}
	if err := s.Seal(ctx, "sess1", string(FieldContact), ""); err != nil {
		t.Fatalf("sink seal empty contact: %v", err)
	}

	if got, _ := v.Reveal(ctx, "sess1", FieldInstruction); got != "refund order 42" {
		t.Fatalf("instruction sealed via the sink = %q", got)
	}
	// The empty contact must not have written a row.
	if _, err := v.Reveal(ctx, "sess1", FieldContact); err != ErrNotFound {
		t.Fatalf("empty field must write no row: err = %v, want ErrNotFound", err)
	}
}

// TestSinkErasesThroughSeam exercises the other half of the stream-facing seam:
// Sink.Erase crypto-shreds a session (deletes its rows, shreds its key), discarding
// the row count the fold does not need, so a later Reveal reports the row gone. It is
// the DPDP-erasure path the stream-processor drives on an erasure.requested event.
func TestSinkErasesThroughSeam(t *testing.T) {
	ctx := context.Background()
	v := newVault(t)
	s := NewSink(v)

	if err := s.Seal(ctx, "sess1", string(FieldInstruction), "refund order 42"); err != nil {
		t.Fatalf("sink seal: %v", err)
	}
	if err := s.Erase(ctx, "sess1"); err != nil {
		t.Fatalf("sink erase: %v", err)
	}
	if _, err := v.Reveal(ctx, "sess1", FieldInstruction); err != ErrNotFound {
		t.Fatalf("after erase, reveal err = %v, want ErrNotFound", err)
	}
	// Erasing a session with nothing sealed is a harmless no-op.
	if err := s.Erase(ctx, "never-sealed"); err != nil {
		t.Fatalf("erasing an empty session must be a no-op, got: %v", err)
	}
}
