// Command worker is the off-clock host: the process that runs the asynchronous
// plane on its own. It builds the durable backends (Redis for the hot stores,
// Redpanda for the bus) and registers the three Go workers behind them — the
// stream-processor (reconstructs block-state), the feature-materialiser (the
// single writer to the feature store), and the reputation-builder (settled
// outcomes → trust) — then runs until a shutdown signal.
//
// This is the split app.go always anticipated: "when durable adapters replace
// the in-memory ones, each plane becomes its own deployment and this root splits
// along the same seams." The decision service is the other half; it never runs
// here. The two planes share only Redis and the bus, never a call.
//
// It requires REDIS_ADDR and KAFKA_SEEDS: a worker with no shared durable state
// has nothing to reconstruct into or read from, so a missing backend fails fast
// rather than starting a process that can do nothing.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Sagarkhandagre897/AgentShield/internal/bus"
	kafkabus "github.com/Sagarkhandagre897/AgentShield/internal/bus/kafka"
	pgchain "github.com/Sagarkhandagre897/AgentShield/internal/chain/postgres"
	"github.com/Sagarkhandagre897/AgentShield/internal/labeler"
	"github.com/Sagarkhandagre897/AgentShield/internal/materialise"
	"github.com/Sagarkhandagre897/AgentShield/internal/reputation"
	redisstore "github.com/Sagarkhandagre897/AgentShield/internal/store/redis"
	"github.com/Sagarkhandagre897/AgentShield/internal/stream"
)

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("worker: %s is required (the off-clock host needs shared durable backends)", key)
	}
	return v
}

func main() {
	ctx := context.Background()
	redisAddr := mustEnv("REDIS_ADDR")
	seeds := strings.Split(mustEnv("KAFKA_SEEDS"), ",")

	// Hot stores: Redis. The workers write token/block-state and feature rows;
	// the decision service reads the same keys from the same Redis.
	rc, err := redisstore.Dial(ctx, redisAddr)
	if err != nil {
		log.Fatalf("worker: redis: %v", err)
	}
	defer rc.Close()
	tokens := redisstore.NewTokenStore(rc)
	fstore := redisstore.NewFeatureStore(rc)

	// Bus: Redpanda. Publish is the decision service's job; here we only consume.
	b, err := kafkabus.New(ctx, seeds, 3)
	if err != nil {
		log.Fatalf("worker: kafka: %v", err)
	}
	defer b.Close()

	// Provenance CHAIN: the stream-processor is its single writer (architecture
	// diagram). When POSTGRES_DSN is set the ledger is durable — it survives a
	// restart and an auditor can walk it long after the decision; a durable append
	// that fails is logged rather than dropped silently. Absent the DSN the processor
	// runs without a ledger here (block-state and nonce-spending are unaffected).
	var chainSink stream.ProvenanceSink
	if dsn := os.Getenv("POSTGRES_DSN"); dsn != "" {
		pgc, err := pgchain.Open(ctx, dsn)
		if err != nil {
			log.Fatalf("worker: durable CHAIN setup failed: %v", err)
		}
		defer pgc.Close()
		chainSink = pgchain.NewSink(pgc, func(err error) {
			log.Printf("worker: durable CHAIN append failed: %v", err)
		})
		log.Printf("worker: durable PostgreSQL CHAIN enabled")
	}

	// The materialiser is the single writer to the feature store; the
	// reputation-builder deposits through it. The stream-processor writes
	// block-state and appends provenance to the CHAIN. The labeler turns settled
	// outcomes into labels on outcomes.v1 — it is the only worker that also
	// publishes. All fold from the bus, idempotent on event_id.
	mat := materialise.New(tokens, fstore, nil)
	rep := reputation.New(mat, reputation.DefaultParams(), nil)
	strm := stream.New(tokens, chainSink)
	lbl := labeler.New(b, labeler.DefaultParams())

	var cancels []func()
	for name, register := range map[string]func(bus.Bus) (func(), error){
		"stream-processor":     strm.Register,
		"feature-materialiser": mat.Register,
		"reputation-builder":   rep.Register,
		"labeler":              lbl.Register,
	} {
		cancel, err := register(b)
		if err != nil {
			log.Fatalf("worker: register %s: %v", name, err)
		}
		cancels = append(cancels, cancel)
	}
	log.Printf("worker: off-clock plane running (redis=%s, kafka=%v) — stream-processor, materialiser, reputation-builder, labeler", redisAddr, seeds)

	// Block until a shutdown signal, then stop the workers and release backends.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("worker: shutting down")
	for _, cancel := range cancels {
		cancel()
	}
}
