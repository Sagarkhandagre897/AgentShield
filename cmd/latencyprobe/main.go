// Command latencyprobe measures the on-clock latency of the AgentShield decision
// service's one RPC — Evaluate — against an ALREADY-RUNNING decision host. It is
// the timing counterpart of cmd/driverkit: where driverkit drives correctness,
// this drives latency. It dials the live gRPC, fires a warmup burst (to absorb the
// dial, the HTTP/2 handshake and cold caches), then times N sequential Evaluate
// calls, one in flight at a time, and reports the latency distribution.
//
// It replays ONE representative legit request (a known-ALLOW OrderContext, dumped
// by demo/latency_test.py after it primes the live world) and stamps a fresh
// evaluation_id + nonce on every call, so each stays on the full ALLOW path: P1
// replay never trips, and no call short-circuits early the way a predicate BLOCK
// would. It asserts every verdict is ALLOW and reports any that are not — a
// fail-closed STEP-UP would mean the world was not primed, so the number would not
// be the full seven-stage latency the design budgets against.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/Sagarkhandagre897/AgentShield/gen/go/agentshield/v1"
)

// orderJSON is the wire form of the 12 OrderContext fields, tags matching the
// generator's order_context() keys — the same shape driverkit's evaluate op reads.
type orderJSON struct {
	EvaluationID   string `json:"evaluation_id"`
	TokenID        string `json:"token_id"`
	CustomerID     string `json:"customer_id"`
	AgentID        string `json:"agent_id"`
	MerchantID     string `json:"merchant_id"`
	SessionID      string `json:"session_id"`
	AmountPaise    int64  `json:"amount_paise"`
	CartHash       string `json:"cart_hash"`
	EnvelopeDigest string `json:"envelope_digest"`
	ToolRisk       int32  `json:"tool_risk"`
	Nonce          string `json:"nonce"`
	TS             int64  `json:"ts"`
}

// loadOrder reads the base OrderContext the probe replays from a JSON file. It
// returns the plain struct (not a *pb.OrderContext) so each call can build a fresh
// proto message — copying a protobuf value would copy its internal mutex.
func loadOrder(path string) (*orderJSON, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var o orderJSON
	if err := json.Unmarshal(raw, &o); err != nil {
		return nil, fmt.Errorf("decode order %s: %w", path, err)
	}
	return &o, nil
}

// percentile returns the p-th percentile (p in [0,1]) of an ascending slice by the
// nearest-rank method. sorted must be non-empty.
func percentile(sorted []float64, p float64) float64 {
	rank := int(math.Ceil(p*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	addr := flag.String("decision", env("DECISION_ADDR", "localhost:8443"), "decision gRPC address")
	orderPath := flag.String("order", "", "path to a JSON OrderContext to replay (a known-ALLOW request)")
	n := flag.Int("n", 2000, "measured Evaluate calls")
	warmup := flag.Int("warmup", 200, "warmup calls, discarded — absorbs dial + cold caches")
	flag.Parse()

	if *orderPath == "" {
		fmt.Fprintln(os.Stderr, "latencyprobe: --order is required")
		os.Exit(2)
	}
	base, err := loadOrder(*orderPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "latencyprobe: %v\n", err)
		os.Exit(1)
	}

	cc, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "latencyprobe: grpc: %v\n", err)
		os.Exit(1)
	}
	defer cc.Close()
	client := pb.NewDecisionClient(cc)
	ctx := context.Background()

	// fire stamps a fresh evaluation_id + nonce so every call is a first-seen ALLOW
	// (P1 never trips), and returns the RPC's wall-clock duration and its verdict.
	// A new proto is built per call — a protobuf value must not be copied.
	seq := 0
	fire := func() (time.Duration, pb.Answer, pb.Code, error) {
		seq++
		stamp := time.Now().UnixNano()
		req := &pb.OrderContext{
			EvaluationId:   fmt.Sprintf("lat-%d-%d", stamp, seq),
			TokenId:        base.TokenID,
			CustomerId:     base.CustomerID,
			AgentId:        base.AgentID,
			MerchantId:     base.MerchantID,
			SessionId:      base.SessionID,
			AmountPaise:    base.AmountPaise,
			CartHash:       base.CartHash,
			EnvelopeDigest: base.EnvelopeDigest,
			ToolRisk:       pb.ToolRisk(base.ToolRisk),
			Nonce:          fmt.Sprintf("lnonce-%d-%d", stamp, seq),
			Ts:             base.TS,
		}
		t0 := time.Now()
		v, err := client.Evaluate(ctx, req)
		dt := time.Since(t0)
		if err != nil {
			return dt, 0, 0, err
		}
		return dt, v.GetDecision(), v.GetCode(), nil
	}

	// Warmup — discard timings, but fail fast if the cold verdict is not ALLOW.
	for i := 0; i < *warmup; i++ {
		_, ans, code, err := fire()
		if err != nil {
			fmt.Fprintf(os.Stderr, "latencyprobe: warmup rpc failed: %v\n", err)
			os.Exit(1)
		}
		if ans != pb.Answer_ANSWER_ALLOW {
			fmt.Fprintf(os.Stderr, "latencyprobe: warmup verdict %s/%s (want ALLOW) — world not primed?\n",
				pb.Answer_name[int32(ans)], pb.Code_name[int32(code)])
			os.Exit(1)
		}
	}

	// Measured — N sequential calls, one in flight at a time: a clean per-request
	// latency, not a throughput-under-concurrency number.
	ms := make([]float64, 0, *n)
	allow := 0
	wall0 := time.Now()
	for i := 0; i < *n; i++ {
		dt, ans, _, err := fire()
		if err != nil {
			fmt.Fprintf(os.Stderr, "latencyprobe: rpc %d failed: %v\n", i, err)
			os.Exit(1)
		}
		if ans == pb.Answer_ANSWER_ALLOW {
			allow++
		}
		ms = append(ms, float64(dt.Microseconds())/1000.0)
	}
	wall := time.Since(wall0)
	sort.Float64s(ms)

	var sum float64
	for _, v := range ms {
		sum += v
	}
	mean := sum / float64(len(ms))
	anomalies := *n - allow

	fmt.Printf("on-clock Evaluate latency — %d measured calls (warmup %d), concurrency 1\n", *n, *warmup)
	fmt.Printf("  target %s   verdict spread: ALLOW=%d  (non-ALLOW anomalies: %d)\n", *addr, allow, anomalies)
	fmt.Printf("  min   %7.3f ms\n", ms[0])
	fmt.Printf("  mean  %7.3f ms\n", mean)
	fmt.Printf("  p50   %7.3f ms\n", percentile(ms, 0.50))
	fmt.Printf("  p90   %7.3f ms\n", percentile(ms, 0.90))
	fmt.Printf("  p95   %7.3f ms\n", percentile(ms, 0.95))
	fmt.Printf("  p99   %7.3f ms\n", percentile(ms, 0.99))
	fmt.Printf("  max   %7.3f ms\n", ms[len(ms)-1])
	fmt.Printf("  throughput ~%.0f calls/s over %s wall\n", float64(*n)/wall.Seconds(), wall.Round(time.Millisecond))
	if anomalies > 0 {
		os.Exit(3)
	}
}
