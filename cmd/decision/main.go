// Command decision is the AgentShield synchronous-plane entrypoint: one
// stateless gRPC service in front of the three hot stores. It answers Evaluate
// before money moves and nothing else.
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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb "github.com/Sagarkhandagre897/AgentShield/gen/go/agentshield/v1"
	"github.com/Sagarkhandagre897/AgentShield/internal/chain"
	"github.com/Sagarkhandagre897/AgentShield/internal/decision"
	"github.com/Sagarkhandagre897/AgentShield/internal/features"
	"github.com/Sagarkhandagre897/AgentShield/internal/score"
	"github.com/Sagarkhandagre897/AgentShield/internal/store/memory"
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

func main() {
	addr := env("AGENTSHIELD_ADDR", ":8443")

	// In-memory hot stores. Redis/Dragonfly adapters land with the async plane,
	// behind the same interfaces.
	tokens := memory.NewTokenStore()
	policies := memory.NewPolicyStore()
	fstore := memory.NewFeatureStore()

	// The CHAIN: every decision is recorded here after the reply, hash-linked so
	// the history is tamper-evident. In-process for the dev entrypoint; a
	// PostgreSQL backing lands behind the same sink.
	provenance := chain.New()

	creds, err := serverTLS()
	if err != nil {
		log.Fatalf("agentshield: TLS setup failed: %v", err)
	}

	cfg := decision.Config{
		Tokens:   tokens,
		Policies: policies,
		Features: features.NewReader(fstore, features.DefaultStalenessBudgetSeconds),
		Scorer:   score.NewLinearScorer(score.DefaultWeights),
		Params:   score.Params{InterruptionCostPaise: score.DefaultInterruptionCostPaise},
		Sink:     chain.NewSink(provenance),
	}

	var opts []grpc.ServerOption
	if creds != nil {
		opts = append(opts, grpc.Creds(creds))
		cfg.Identify = decision.MTLSIdentity
		log.Printf("agentshield: mutual TLS enabled")
	} else {
		devID := env("AGENTSHIELD_DEV_IDENTITY", "dev-caller")
		cfg.Identify = func(context.Context) string { return devID }
		log.Printf("agentshield: WARNING starting WITHOUT mTLS (dev mode); caller identity = %q", devID)
	}

	grpcServer := grpc.NewServer(opts...)
	pb.RegisterDecisionServer(grpcServer, decision.New(cfg))

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("agentshield: listen %s: %v", addr, err)
	}
	log.Printf("agentshield: decision service listening on %s", addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("agentshield: serve: %v", err)
	}
}
