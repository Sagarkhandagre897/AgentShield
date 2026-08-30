// Command driverkit is the Phase-7 live-driver's I/O arm: the contract-bound Go
// half of the two-part driver (System Design §14, the eval harness). The Python
// orchestrator (services/driver) owns the scenario, the timeline order, the
// barrier logic and the oracle scoring; it shells out to THIS process for every
// action that must touch a real AgentShield contract, so those actions reuse the
// exact same generated bindings, store schemas and bus builders the product runs
// with — zero stub drift from the on-clock gRPC contract or the off-clock event
// envelope.
//
// It speaks newline-delimited JSON on stdin/stdout: one request object per line
// in, one response object per line out, in order. It is long-lived — dialled once
// (Redis, Redpanda, the decision gRPC), then driven op-by-op — so a whole replay
// pays the connection cost once. A labels collector subscribes to outcomes.v1 at
// startup, before any traffic, so every settled label the run produces is captured
// for the eval regardless of when the driver drains it.
//
// It NEVER decides or labels anything: it seeds, seals, evaluates, emits webhooks,
// deposits engine-stand-in figures, reads back block/feature state, and reports
// what the real system said. Every verdict it returns is the live decision service's.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/Sagarkhandagre897/AgentShield/gen/go/agentshield/v1"
	"github.com/Sagarkhandagre897/AgentShield/internal/bus"
	kafkabus "github.com/Sagarkhandagre897/AgentShield/internal/bus/kafka"
	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
	"github.com/Sagarkhandagre897/AgentShield/internal/store"
	redisstore "github.com/Sagarkhandagre897/AgentShield/internal/store/redis"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// request is one line of stdin: an op plus the union of every op's fields. Only
// the fields an op needs are read; the rest stay zero. Kept flat and permissive
// so the Python side can build a request as a plain dict.
type request struct {
	Op string `json:"op"`

	// seed_token / seed_overlay carry a whole domain object as raw JSON, so the
	// driverkit unmarshals it straight into the store's own type (schema parity).
	Token   json.RawMessage `json:"token,omitempty"`
	Overlay json.RawMessage `json:"overlay,omitempty"`

	// evaluate carries exactly the 12 OrderContext fields.
	Order *orderContext `json:"order,omitempty"`

	// The bus-event ops (seal/capture/dispute/cancel/deposit_feature) and the
	// read ops (get_block/get_feature) share these scalar fields.
	EventID        string  `json:"event_id,omitempty"`
	TokenID        string  `json:"token_id,omitempty"`
	SessionID      string  `json:"session_id,omitempty"`
	AgentID        string  `json:"agent_id,omitempty"`
	OccurredAt     int64   `json:"occurred_at,omitempty"`
	AmountPaise    int64   `json:"amount_paise,omitempty"`
	Nonce          string  `json:"nonce,omitempty"`
	RawInstruction string  `json:"raw_instruction,omitempty"`
	Contact        string  `json:"contact,omitempty"`
	Kind           string  `json:"kind,omitempty"`        // deposit_feature: behaviour|intent|network
	FeatureKey     string  `json:"feature_key,omitempty"` // deposit_feature / get_feature key
	Value          float64 `json:"value,omitempty"`       // deposit_feature figure

	// collect_labels tuning.
	TimeoutMs int `json:"timeout_ms,omitempty"`
	Expect    int `json:"expect,omitempty"`
}

// orderContext is the wire form of the 12 proto OrderContext fields, with json
// tags matching the generator's Debit.order_context() keys exactly.
type orderContext struct {
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

func (o *orderContext) toProto() *pb.OrderContext {
	return &pb.OrderContext{
		EvaluationId:   o.EvaluationID,
		TokenId:        o.TokenID,
		CustomerId:     o.CustomerID,
		AgentId:        o.AgentID,
		MerchantId:     o.MerchantID,
		SessionId:      o.SessionID,
		AmountPaise:    o.AmountPaise,
		CartHash:       o.CartHash,
		EnvelopeDigest: o.EnvelopeDigest,
		ToolRisk:       pb.ToolRisk(o.ToolRisk),
		Nonce:          o.Nonce,
		Ts:             o.TS,
	}
}

// response is one line of stdout: the op echoed, ok/error, and whatever the op
// produced. Absent fields (a nil block, no labels) marshal away, so the Python
// side reads only what an op returns.
type response struct {
	Op        string             `json:"op"`
	OK        bool               `json:"ok"`
	Error     string             `json:"error,omitempty"`
	Decision  string             `json:"decision,omitempty"`  // evaluate: the Answer enum name
	Code      string             `json:"code,omitempty"`      // evaluate: the Code enum name
	EvalID    string             `json:"eval_id,omitempty"`   // evaluate: the id the caller received
	Retryable bool               `json:"retryable,omitempty"` // evaluate: verdict retry hint
	Block     *domain.BlockState `json:"block,omitempty"`     // get_block (nil = absent)
	Feature   *domain.FeatureRow `json:"feature,omitempty"`   // get_feature (nil = absent)
	Labels    []labelRecord      `json:"labels,omitempty"`    // collect_labels
}

// labelRecord is one settled training label the run produced on outcomes.v1, as
// the collector captured it. The Python eval joins these back to the scenario by
// token_id and reason to score the labeler against the oracle.
type labelRecord struct {
	EventID    string  `json:"event_id"`
	TokenID    string  `json:"token_id"`
	OccurredAt int64   `json:"occurred_at"`
	Label      float64 `json:"label"`
	Weight     float64 `json:"weight"`
	Reason     string  `json:"reason"`
}

// fail builds an error response for an op.
func fail(op string, err error) response {
	return response{Op: op, OK: false, Error: err.Error()}
}

// kit holds the one-time-dialled backends the ops act through: the three Redis
// stores (seed/read), the Redpanda bus (webhooks + feature deposits + the labels
// collector) and the gRPC decision client (evaluate). Every field reuses the
// exact adapter the product runs with, so an op touches the real contract.
type kit struct {
	ctx      context.Context
	tokens   *redisstore.TokenStore
	policies *redisstore.PolicyStore
	features *redisstore.FeatureStore
	events   bus.Bus
	decision pb.DecisionClient

	mu        sync.Mutex
	labels    []labelRecord
	labelSeen map[string]bool // dedupe outcomes.v1 redelivery on event_id
}

// onEvent is the labels collector's handler. It sees every event on the bus
// (the subscription spans all topics) and keeps only outcome.labeled, deduping
// on event_id because delivery is at-least-once.
func (k *kit) onEvent(_ context.Context, ev domain.Event) error {
	if ev.Type != bus.EventOutcomeLabeled {
		return nil
	}
	label, _ := bus.PayloadFloat64(ev, bus.PayloadLabel)
	weight, _ := bus.PayloadFloat64(ev, bus.PayloadWeight)
	reason, _ := bus.PayloadString(ev, bus.PayloadReason)
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.labelSeen[ev.EventID] {
		return nil
	}
	k.labelSeen[ev.EventID] = true
	k.labels = append(k.labels, labelRecord{
		EventID: ev.EventID, TokenID: ev.TokenID, OccurredAt: ev.OccurredAt,
		Label: label, Weight: weight, Reason: reason,
	})
	return nil
}

// collectLabels waits until at least expect labels have arrived or timeoutMs
// elapses, then returns a snapshot of everything collected so far. It does not
// clear the buffer — the driver drains once at the end, and a snapshot keeps the
// op idempotent if it is called more than once.
func (k *kit) collectLabels(expect, timeoutMs int) []labelRecord {
	if timeoutMs <= 0 {
		timeoutMs = 5000
	}
	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	for {
		k.mu.Lock()
		n := len(k.labels)
		k.mu.Unlock()
		if (expect > 0 && n >= expect) || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	out := make([]labelRecord, len(k.labels))
	copy(out, k.labels)
	return out
}

// handle dispatches one request to the op it names and returns the response.
// It NEVER decides or labels: seed/seal/deposit publish or store; evaluate asks
// the live decision service and reports its verdict verbatim; get_* read back
// real state; collect_labels drains what the real labeler produced.
func (k *kit) handle(req request) response {
	resp := response{Op: req.Op}
	switch req.Op {
	case "ping":
		resp.OK = true

	case "seed_token":
		// Unmarshal straight into the store's own domain.Token so the scenario's
		// JSON is validated by the same containment invariant a real write is.
		var t domain.Token
		if err := json.Unmarshal(req.Token, &t); err != nil {
			return fail(req.Op, fmt.Errorf("seed_token: decode: %w", err))
		}
		if err := k.tokens.PutToken(k.ctx, &t); err != nil {
			return fail(req.Op, err)
		}
		resp.OK = true

	case "seed_overlay":
		var o domain.PolicyOverlay
		if err := json.Unmarshal(req.Overlay, &o); err != nil {
			return fail(req.Op, fmt.Errorf("seed_overlay: decode: %w", err))
		}
		if err := k.policies.PutOverlay(k.ctx, &o); err != nil {
			return fail(req.Op, err)
		}
		resp.OK = true

	case "seal_envelope":
		// The one PII-bearing event → vault.v1. token_id is required or the
		// stream-processor drops it; session_id is the VAULT key.
		ev := bus.EnvelopeSealedEvent(req.EventID, req.TokenID, req.SessionID, req.OccurredAt, req.RawInstruction, req.Contact)
		if err := k.events.Publish(k.ctx, ev); err != nil {
			return fail(req.Op, err)
		}
		resp.OK = true

	case "evaluate":
		if req.Order == nil {
			return fail(req.Op, errors.New("evaluate: missing order"))
		}
		v, err := k.decision.Evaluate(k.ctx, req.Order.toProto())
		if err != nil {
			return fail(req.Op, err)
		}
		resp.OK = true
		resp.Decision = pb.Answer_name[int32(v.GetDecision())]
		resp.Code = pb.Code_name[int32(v.GetCode())]
		resp.EvalID = v.GetEvaluationId()
		resp.Retryable = v.GetRetryable()

	case "capture":
		// Money moved. Reuse the debit's nonce so the stream-processor spends it,
		// and stamp the agent so the outcome attributes to reputation.
		ev := bus.PaymentCapturedEvent(req.EventID, req.TokenID, req.OccurredAt, req.AmountPaise, req.Nonce)
		if req.AgentID != "" {
			ev = bus.WithAgent(ev, req.AgentID)
		}
		if err := k.events.Publish(k.ctx, ev); err != nil {
			return fail(req.Op, err)
		}
		resp.OK = true

	case "dispute":
		ev := bus.PaymentDisputedEvent(req.EventID, req.TokenID, req.OccurredAt, req.Nonce)
		if req.AgentID != "" {
			ev = bus.WithAgent(ev, req.AgentID)
		}
		if err := k.events.Publish(k.ctx, ev); err != nil {
			return fail(req.Op, err)
		}
		resp.OK = true

	case "cancel":
		// token.cancelled has no builder in the bus package — the mandate lifecycle
		// event is constructed inline (the labeler reads it as a light MISUSE).
		ev := domain.Event{
			EventID:    req.EventID,
			Type:       bus.EventTokenCancelled,
			TokenID:    req.TokenID,
			OccurredAt: req.OccurredAt,
			Source:     "webhook",
		}
		if err := k.events.Publish(k.ctx, ev); err != nil {
			return fail(req.Op, err)
		}
		resp.OK = true

	case "deposit_feature":
		// The engine stand-in: publish a real feature-deposit event so the live
		// materialiser (the single store writer) folds it into the keyed row the
		// clock reads. kind picks which engine's figure is deposited.
		var ev domain.Event
		switch req.Kind {
		case "behaviour":
			ev = bus.FeatureBehaviourDepositEvent(req.EventID, req.TokenID, req.FeatureKey, req.OccurredAt, req.Value, nil)
		case "intent":
			ev = bus.FeatureIntentDepositEvent(req.EventID, req.TokenID, req.FeatureKey, req.OccurredAt, req.Value)
		case "network":
			ev = bus.FeatureNetworkDepositEvent(req.EventID, req.TokenID, req.FeatureKey, req.OccurredAt, req.Value)
		default:
			return fail(req.Op, fmt.Errorf("deposit_feature: unknown kind %q", req.Kind))
		}
		if err := k.events.Publish(k.ctx, ev); err != nil {
			return fail(req.Op, err)
		}
		resp.OK = true

	case "get_block":
		// Absent block-state is a valid answer (nothing consumed yet), not an
		// error — the driver polls this to know a prior fold has landed.
		bs, err := k.tokens.GetBlockState(k.ctx, req.TokenID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				resp.OK = true
				break
			}
			return fail(req.Op, err)
		}
		resp.OK = true
		resp.Block = bs

	case "get_feature":
		rows, err := k.features.MultiGet(k.ctx, []string{req.FeatureKey})
		if err != nil {
			return fail(req.Op, err)
		}
		resp.OK = true
		if r, ok := rows[req.FeatureKey]; ok {
			resp.Feature = r
		}

	case "collect_labels":
		resp.OK = true
		resp.Labels = k.collectLabels(req.Expect, req.TimeoutMs)

	default:
		return fail(req.Op, fmt.Errorf("unknown op %q", req.Op))
	}
	return resp
}

// fatal writes to stderr (stdout is the NDJSON channel, kept clean) and exits.
func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "driverkit: "+format+"\n", args...)
	os.Exit(1)
}

func main() {
	ctx := context.Background()
	redisAddr := env("REDIS_ADDR", "localhost:6379")
	seeds := strings.Split(env("KAFKA_SEEDS", "localhost:19092"), ",")
	decisionAddr := env("DECISION_ADDR", "localhost:8443")

	// Dial the three backends once. The whole replay reuses these connections.
	rc, err := redisstore.Dial(ctx, redisAddr)
	if err != nil {
		fatal("redis: %v", err)
	}
	defer rc.Close()

	b, err := kafkabus.New(ctx, seeds, 3)
	if err != nil {
		fatal("kafka: %v", err)
	}
	defer b.Close()

	// The decision service runs in dev mode (no mTLS) under the harness, so the
	// probe dials insecure. grpc.NewClient connects lazily on the first Evaluate.
	cc, err := grpc.NewClient(decisionAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal("grpc: %v", err)
	}
	defer cc.Close()

	k := &kit{
		ctx:       ctx,
		tokens:    redisstore.NewTokenStore(rc),
		policies:  redisstore.NewPolicyStore(rc),
		features:  redisstore.NewFeatureStore(rc),
		events:    b,
		decision:  pb.NewDecisionClient(cc),
		labelSeen: map[string]bool{},
	}

	// Start the labels collector BEFORE any traffic so every settled label the run
	// produces on outcomes.v1 is captured, whenever the driver drains it. A unique
	// group per process keeps this run's offsets independent of any prior run.
	group := fmt.Sprintf("driverkit-labels-%d", time.Now().UnixNano())
	cancel, err := b.Subscribe(group, k.onEvent)
	if err != nil {
		fatal("subscribe outcomes: %v", err)
	}
	defer cancel()

	// Readiness on stderr; stdout carries only NDJSON responses.
	fmt.Fprintf(os.Stderr, "driverkit: ready — redis=%s kafka=%v decision=%s\n", redisAddr, seeds, decisionAddr)

	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64*1024), 4*1024*1024) // a seeded token/overlay line can be large
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	enc := json.NewEncoder(out)

	for in.Scan() {
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}
		var req request
		var resp response
		if uerr := json.Unmarshal([]byte(line), &req); uerr != nil {
			resp = response{OK: false, Error: fmt.Sprintf("bad request json: %v", uerr)}
		} else {
			resp = k.handle(req)
		}
		if eerr := enc.Encode(&resp); eerr != nil {
			fatal("encode response: %v", eerr)
		}
		out.Flush() // one response per line, promptly — the driver blocks on it
	}
	if serr := in.Err(); serr != nil {
		fatal("stdin: %v", serr)
	}
}
