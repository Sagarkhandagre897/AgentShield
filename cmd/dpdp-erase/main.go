// Command dpdp-erase is the operator's request path for a DPDP right-to-erasure:
// the entrypoint a support tool (or a future self-service console) runs when a data
// principal asks us to forget a session's PII (System Design §9). It does not touch
// the VAULT itself — the decision plane never does, and no operator command should
// reach across into it. Instead it publishes ONE erasure.requested event to the bus
// (vault.v1), which the stream-processor — the single VAULT writer — folds off the
// clock: it deletes the session's rows AND shreds its data key (crypto-shredding),
// so even a backup that still holds the ciphertext becomes undecryptable.
//
// The request names only what to forget, never the data: a session_id (the VAULT
// key) and the token_id of the mandate it ran under (the bus partition key, so the
// erasure folds in order after that session's seals — a late seal can never
// resurrect erased PII behind it). Both are required; the stream-processor drops an
// event with no token_id, and there is nothing to erase without a session_id.
//
// It publishes and exits. Delivery is at-least-once and vault.Erase is idempotent,
// so re-running the same request (same or fresh event_id) is safe.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Sagarkhandagre897/AgentShield/internal/bus"
	kafkabus "github.com/Sagarkhandagre897/AgentShield/internal/bus/kafka"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// newEventID mints a random idempotency id when the operator does not supply one,
// so an accidental double-run of a distinct erasure still dedupes cleanly per event.
func newEventID() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("erase-%d", time.Now().UnixNano())
	}
	return "erase-" + hex.EncodeToString(b)
}

func main() {
	var (
		session    = flag.String("session", "", "session_id whose sealed PII to erase (the VAULT key) — required")
		token      = flag.String("token", "", "token_id of the mandate the session ran under (the bus partition key) — required")
		eventID    = flag.String("event-id", "", "idempotency id for the request (default: a generated one)")
		occurredAt = flag.Int64("occurred-at", 0, "event time in epoch seconds (default: now)")
		seedsFlag  = flag.String("seeds", env("KAFKA_SEEDS", "localhost:19092"), "comma-separated Kafka/Redpanda seed brokers")
	)
	flag.Parse()

	if *session == "" || *token == "" {
		flag.Usage()
		log.Fatal("dpdp-erase: both -session and -token are required")
	}
	if *eventID == "" {
		*eventID = newEventID()
	}
	if *occurredAt == 0 {
		*occurredAt = time.Now().Unix()
	}

	ctx := context.Background()
	seeds := strings.Split(*seedsFlag, ",")
	b, err := kafkabus.New(ctx, seeds, 3)
	if err != nil {
		log.Fatalf("dpdp-erase: kafka %v: %v", seeds, err)
	}
	defer b.Close()

	ev := bus.ErasureRequestedEvent(*eventID, *token, *session, *occurredAt)
	if err := b.Publish(ctx, ev); err != nil {
		log.Fatalf("dpdp-erase: publish erasure request: %v", err)
	}
	log.Printf("dpdp-erase: erasure requested — session=%s token=%s event_id=%s; the stream-processor will delete the rows and shred the key off the clock",
		*session, *token, *eventID)
}
