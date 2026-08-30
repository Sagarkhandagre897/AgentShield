// Command decision is the AgentShield entrypoint: it serves the synchronous
// plane's gRPC Evaluate in front of the in-process composition root, which wires
// the three hot stores, the bus, the CHAIN and the three off-clock workers into
// one running system. Evaluate answers before money moves; the async plane folds
// outcomes back into the shared state behind it.
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
	"github.com/Sagarkhandagre897/AgentShield/internal/app"
	"github.com/Sagarkhandagre897/AgentShield/internal/decision"
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

	// The whole in-process system: the decision service in front, and the bus,
	// CHAIN and three off-clock workers behind it, all sharing one set of stores.
	// In-memory adapters keep it runnable without Kafka/Redis/PostgreSQL; each
	// lands behind the same interfaces.
	system, err := app.New(app.Config{Identify: identify})
	if err != nil {
		log.Fatalf("agentshield: wiring failed: %v", err)
	}
	defer system.Close()

	grpcServer := grpc.NewServer(opts...)
	pb.RegisterDecisionServer(grpcServer, system.Decision)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("agentshield: listen %s: %v", addr, err)
	}
	log.Printf("agentshield: listening on %s (decision service + async plane, in-process)", addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("agentshield: serve: %v", err)
	}
}
