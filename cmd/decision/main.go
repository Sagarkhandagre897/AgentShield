// Command decision is the AgentShield entrypoint: it serves the synchronous
// plane's gRPC Evaluate in front of the composition root, which wires the three
// hot stores, the bus and the CHAIN behind it. Evaluate answers before money
// moves; the async plane folds outcomes back into the shared state behind it.
//
// It runs in one of two modes. With REDIS_ADDR and KAFKA_SEEDS set it is the
// split-process decision host: it reads Redis and publishes decision.made to
// Redpanda, and the workers run separately in cmd/worker. With neither set it
// falls back to the in-memory single-process root — stores, bus and the three
// workers all in this process — so the service is runnable locally with no infra.
//
// Transport is mutual TLS when AGENTSHIELD_TLS_CERT / _KEY / _CA are set — the
// caller identity P5 verifies comes from the client certificate. With no certs
// configured it starts in dev mode without TLS and takes a fixed identity from
// AGENTSHIELD_DEV_IDENTITY, so the service is runnable locally without a PKI.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb "github.com/Sagarkhandagre897/AgentShield/gen/go/agentshield/v1"
	"github.com/Sagarkhandagre897/AgentShield/internal/app"
	kafkabus "github.com/Sagarkhandagre897/AgentShield/internal/bus/kafka"
	pgchain "github.com/Sagarkhandagre897/AgentShield/internal/chain/postgres"
	"github.com/Sagarkhandagre897/AgentShield/internal/decision"
	redisstore "github.com/Sagarkhandagre897/AgentShield/internal/store/redis"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// serverTLS builds mutual-TLS credentials from the cert/key/CA env vars. If none
// are set it returns (nil, nil): the caller starts the server without TLS for
// local development. Client certificates are required and verified against the
// CA when TLS is enabled.
func serverTLS() (credentials.TransportCredentials, error) {
	certFile := os.Getenv("AGENTSHIELD_TLS_CERT")
	keyFile := os.Getenv("AGENTSHIELD_TLS_KEY")
	caFile := os.Getenv("AGENTSHIELD_TLS_CA")
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load key pair: %w", err)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("no CA certificates parsed from %s", caFile)
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS13,
	}), nil
}

// backends selects the durable backends from the environment. When REDIS_ADDR
// and KAFKA_SEEDS are both set it dials Redis and Redpanda and returns them for
// the split-process decision host; otherwise it returns a zero Config so app.New
// falls back to the in-memory single-process root. The bool reports split mode.
func backends(ctx context.Context) (app.Config, bool, error) {
	redisAddr := os.Getenv("REDIS_ADDR")
	seedsEnv := os.Getenv("KAFKA_SEEDS")
	if redisAddr == "" || seedsEnv == "" {
		return app.Config{}, false, nil
	}

	rc, err := redisstore.Dial(ctx, redisAddr)
	if err != nil {
		return app.Config{}, false, fmt.Errorf("redis: %w", err)
	}
	b, err := kafkabus.New(ctx, strings.Split(seedsEnv, ","), 3)
	if err != nil {
		_ = rc.Close()
		return app.Config{}, false, fmt.Errorf("kafka: %w", err)
	}
	return app.Config{
		Tokens:   redisstore.NewTokenStore(rc),
		Policies: redisstore.NewPolicyStore(rc),
		Features: redisstore.NewFeatureStore(rc),
		Bus:      b,
	}, true, nil
}

func main() {
	addr := env("AGENTSHIELD_ADDR", ":8443")

	creds, err := serverTLS()
	if err != nil {
		log.Fatalf("agentshield: TLS setup failed: %v", err)
	}

	// Caller identity: from the mTLS client cert in production, or a fixed dev
	// identity when running without a PKI.
	var identify func(context.Context) string
	var opts []grpc.ServerOption
	if creds != nil {
		opts = append(opts, grpc.Creds(creds))
		identify = decision.MTLSIdentity
		log.Printf("agentshield: mutual TLS enabled")
	} else {
		devID := env("AGENTSHIELD_DEV_IDENTITY", "dev-caller")
		identify = func(context.Context) string { return devID }
		log.Printf("agentshield: WARNING starting WITHOUT mTLS (dev mode); caller identity = %q", devID)
	}

	// Pick the backends from the environment: Redis + Redpanda (split-process, the
	// workers run in cmd/worker) or in-memory (single-process, workers in here).
	cfg, split, err := backends(context.Background())
	if err != nil {
		log.Fatalf("agentshield: backend setup failed: %v", err)
	}
	cfg.Identify = identify

	// Provenance ledger: the stream-processor is the CHAIN's single writer (the
	// architecture diagram), never this service. In single-process mode that
	// processor runs in-process, and POSTGRES_DSN makes its ledger durable — it
	// survives a restart and an auditor can walk it long after the reply. In
	// split-process mode the stream-processor runs in cmd/worker, which owns the
	// durable CHAIN, so the decision host does not open Postgres here.
	if dsn := os.Getenv("POSTGRES_DSN"); dsn != "" && !split {
		pgc, err := pgchain.Open(context.Background(), dsn)
		if err != nil {
			log.Fatalf("agentshield: durable CHAIN setup failed: %v", err)
		}
		defer pgc.Close()
		cfg.Sink = pgchain.NewSink(pgc, func(err error) {
			log.Printf("agentshield: durable CHAIN append failed: %v", err)
		})
		log.Printf("agentshield: durable PostgreSQL CHAIN enabled (single-process; stream-processor writes it)")
	}

	system, err := app.New(cfg)
	if err != nil {
		log.Fatalf("agentshield: wiring failed: %v", err)
	}
	defer system.Close()

	if split {
		log.Printf("agentshield: split-process mode — Redis stores + Redpanda bus (workers run in cmd/worker)")
	} else {
		log.Printf("agentshield: single-process mode — in-memory stores, bus and workers")
	}

	grpcServer := grpc.NewServer(opts...)
	pb.RegisterDecisionServer(grpcServer, system.Decision)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("agentshield: listen %s: %v", addr, err)
	}
	log.Printf("agentshield: listening on %s", addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("agentshield: serve: %v", err)
	}
}
