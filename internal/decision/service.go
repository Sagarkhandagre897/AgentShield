// Package decision is the synchronous plane's orchestrator (System Design §4).
// One stateless gRPC service runs the seven stages in order for every request
// and always returns a verdict — it never returns a transport error in place of
// a decision, because "answer, and fail closed" is the whole contract.
//
//  1. ingress      — authenticate the caller (mTLS) and take the request
//  2. resolveToken — keyed reads of token, block-state and overlay
//  3. runPredicates— the deterministic spine P1-P6 (before any feature read)
//  4. readFeatures — one keyed multi-get, with staleness
//  5. scoreEnsemble— fold the figures into one calibrated probability
//  6. decide       — expected loss vs interruption cost
//  7. respond      — reply to the caller, THEN announce the decision off the clock
//
// A refusing predicate settles the request at stage 3 and stages 4-6 are
// skipped. Any backend error that means we cannot know what we must know fails
// closed to a STEP-UP. The caller's verdict is lean by design — no score, band
// or threshold — so the score surface cannot be probed; the full record travels
// on decision.made and the stream-processor appends it to the CHAIN after the reply.
package decision

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	pb "github.com/Sagarkhandagre897/AgentShield/gen/go/agentshield/v1"
	"github.com/Sagarkhandagre897/AgentShield/internal/bus"
	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
	"github.com/Sagarkhandagre897/AgentShield/internal/features"
	"github.com/Sagarkhandagre897/AgentShield/internal/predicate"
	"github.com/Sagarkhandagre897/AgentShield/internal/score"
	"github.com/Sagarkhandagre897/AgentShield/internal/store"
)

// EventPublisher publishes one decision.made event per evaluation to the
// asynchronous plane, after the reply. bus.Bus satisfies it; the in-memory build
// hands a NopPublisher. This is the bus-down meeting point of the two planes: the
// clock announces the decision — carrying the full provenance record's fields —
// and never reads back. The stream-processor is the one that appends it to the
// CHAIN off the clock; this service never touches the ledger (per the architecture
// diagram).
type EventPublisher interface {
	Publish(ctx context.Context, ev domain.Event) error
}

// NopPublisher drops events. Default when no publisher is configured.
type NopPublisher struct{}

// Publish does nothing.
func (NopPublisher) Publish(context.Context, domain.Event) error { return nil }

// Config assembles a Service. Only the stores and feature reader are required;
// the rest default to the standard scorer, a no-op publisher, mTLS identity and
// the wall clock.
type Config struct {
	Tokens   store.TokenStore
	Policies store.PolicyStore
	Features *features.Reader
	Scorer   score.Scorer
	Params   score.Params
	Events   EventPublisher                   // decision.made (carrying provenance) goes here, after the reply
	Identify func(ctx context.Context) string // caller identity from the transport
	Now      func() int64                     // epoch seconds
}

// Service implements the generated DecisionServer.
type Service struct {
	pb.UnimplementedDecisionServer
	tokens   store.TokenStore
	policies store.PolicyStore
	features *features.Reader
	scorer   score.Scorer
	params   score.Params
	events   EventPublisher
	identify func(ctx context.Context) string
	now      func() int64
}

// New builds a Service from a Config, filling defaults.
func New(cfg Config) *Service {
	if cfg.Scorer == nil {
		cfg.Scorer = score.NewLinearScorer(score.DefaultWeights)
	}
	if cfg.Params.InterruptionCostPaise == 0 {
		cfg.Params.InterruptionCostPaise = score.DefaultInterruptionCostPaise
	}
	if cfg.Events == nil {
		cfg.Events = NopPublisher{}
	}
	if cfg.Identify == nil {
		cfg.Identify = MTLSIdentity
	}
	if cfg.Now == nil {
		cfg.Now = func() int64 { return time.Now().Unix() }
	}
	return &Service{
		tokens:   cfg.Tokens,
		policies: cfg.Policies,
		features: cfg.Features,
		scorer:   cfg.Scorer,
		params:   cfg.Params,
		events:   cfg.Events,
		identify: cfg.Identify,
		now:      cfg.Now,
	}
}

// Evaluate runs the seven stages and returns a verdict. It always returns a
// verdict and a nil error — a failure to read a backend becomes a fail-closed
// STEP-UP, not a gRPC error.
func (s *Service) Evaluate(ctx context.Context, in *pb.OrderContext) (*pb.Verdict, error) {
	now := s.now()
	caller := s.identify(ctx)

	// Stage 2 — resolveToken. A not-found token/overlay/block is a normal case
	// (nil, handled by the predicates); any other store error means we cannot
	// know the limits, and §9 is explicit that this is exactly when we must not
	// proceed — so we fail closed.
	tok, err := lookupToken(ctx, s.tokens, in.GetTokenId())
	if err != nil {
		return s.respond(ctx, in, now, pb.Answer_ANSWER_STEP_UP, pb.Code_STEPUP_FAILCLOSED, "", 0), nil
	}
	block, err := lookupBlock(ctx, s.tokens, in.GetTokenId())
	if err != nil {
		return s.respond(ctx, in, now, pb.Answer_ANSWER_STEP_UP, pb.Code_STEPUP_FAILCLOSED, "", 0), nil
	}
	overlay, err := lookupOverlay(ctx, s.policies, in.GetTokenId())
	if err != nil {
		return s.respond(ctx, in, now, pb.Answer_ANSWER_STEP_UP, pb.Code_STEPUP_FAILCLOSED, "", 0), nil
	}
	policyVersion := 0
	if overlay != nil {
		policyVersion = overlay.OverlayVersion
	}

	// Stage 3 — runPredicates. Runs before any feature read; a refusing
	// predicate is terminal and stages 4-6 are skipped.
	if o := predicate.Run(predicate.Input{
		Order:    in,
		CallerID: caller,
		Token:    tok,
		Block:    block,
		Overlay:  overlay,
		Now:      now,
	}); o.Terminal {
		return s.respond(ctx, in, now, o.Answer, o.Code, o.Predicate, policyVersion), nil
	}

	// Stage 4 — readFeatures. A store error or any missing/stale figure sets the
	// view degraded; decide() turns that into a fail-closed STEP-UP.
	view, _ := s.features.Read(ctx, features.EntityKeys{
		Customer: in.GetCustomerId(),
		Token:    in.GetTokenId(),
		Agent:    in.GetAgentId(),
		Merchant: in.GetMerchantId(),
	}, now)

	// Stages 5-6 — scoreEnsemble + decide.
	ev := evidence(view, in, tok, block)
	d := score.Decide(ev, in.GetAmountPaise(), s.params, s.scorer)

	// Policy floor: a critical-risk tool is never quietly allowed — it always
	// warrants at least a STEP-UP. It can never be turned into a BLOCK here;
	// blocking stays the predicates' alone.
	if in.GetToolRisk() == pb.ToolRisk_TOOL_RISK_CRITICAL && d.Answer == pb.Answer_ANSWER_ALLOW {
		d.Answer = pb.Answer_ANSWER_STEP_UP
		d.Code = pb.Code_STEPUP_RISK
	}

	// Stage 7 — respond (then emit).
	return s.respond(ctx, in, now, d.Answer, d.Code, "", policyVersion), nil
}

// respond builds the lean caller verdict and, off the caller's clock, announces
// the decision on the bus carrying the full provenance record's fields. The event
// is derived from the verdict being returned, so what the async plane records
// always reflects what the caller saw. This is the reply-then-emit seam and the
// two planes' bus-down meeting point: we answer first, then tell the async plane,
// and never read back. The stream-processor — not this service — appends the record
// to the CHAIN (the architecture diagram assigns the ledger write there).
func (s *Service) respond(ctx context.Context, in *pb.OrderContext, now int64, ans pb.Answer, code pb.Code, predicateFailed string, policyVersion int) *pb.Verdict {
	v := &pb.Verdict{
		EvaluationId: in.GetEvaluationId(),
		Decision:     ans,
		Code:         code,
		Retryable:    ans == pb.Answer_ANSWER_STEP_UP, // a step-up can be re-confirmed and retried; a block cannot
	}

	// The record's fields ride the event: evaluation_id / decision / ts on the
	// envelope, the rest via WithProvenance. request_digest is a fingerprint of the
	// order, so the raw request never travels the bus — only enough to bind the
	// audit record to what was asked. The stream-processor rebuilds and appends it.
	rec := &domain.ProvenanceRecord{
		EvaluationID:    in.GetEvaluationId(),
		RequestDigest:   requestDigest(in),
		Decision:        answerString(ans),
		Code:            code.String(),
		PredicateFailed: predicateFailed,
		PolicyVersion:   policyVersion,
		TS:              now,
	}
	// The event_id is the evaluation_id: exactly one decision.made per evaluation,
	// so a redelivery folds once. The stream-processor spends the nonce only when
	// the decision was an ALLOW, and records every decision on the CHAIN.
	ev := bus.WithProvenance(
		bus.DecisionMadeEvent(in.GetEvaluationId(), in.GetTokenId(), now, answerString(ans), in.GetNonce(), in.GetAmountPaise()),
		rec,
	)

	// Reply-then-emit: the publish outlives the request, so detach cancellation
	// (keep any values) and run off the critical path.
	go func() {
		dctx := context.WithoutCancel(ctx)
		_ = s.events.Publish(dctx, ev)
	}()

	return v
}

// requestDigest is the SHA-256 fingerprint of an order's identifying content, in a
// fixed field order. It binds a CHAIN record to exactly what was asked without ever
// putting the raw request on the bus — only this digest travels.
func requestDigest(in *pb.OrderContext) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x1f%s\x1f%s\x1f%s\x1f%s\x1f%s\x1f%d\x1f%s\x1f%s\x1f%d\x1f%s\x1f%d",
		in.GetEvaluationId(),
		in.GetTokenId(),
		in.GetCustomerId(),
		in.GetAgentId(),
		in.GetMerchantId(),
		in.GetSessionId(),
		in.GetAmountPaise(),
		in.GetCartHash(),
		in.GetEnvelopeDigest(),
		int32(in.GetToolRisk()),
		in.GetNonce(),
		in.GetTs(),
	)
	return hex.EncodeToString(h.Sum(nil))
}

// answerString maps a verdict answer onto the decision string the bus carries,
// keeping the bus contract free of the generated protobuf enum.
func answerString(a pb.Answer) string {
	switch a {
	case pb.Answer_ANSWER_ALLOW:
		return bus.DecisionAllow
	case pb.Answer_ANSWER_STEP_UP:
		return bus.DecisionStepUp
	case pb.Answer_ANSWER_BLOCK:
		return bus.DecisionBlock
	default:
		return ""
	}
}

// evidence maps the feature view onto the flat Evidence the scorer reads: each
// figure is taken from the row of the entity it belongs to. ConsumptionFrac is
// computed directly from the mandate and lien — it is the model-free day-one
// signal, available even when no feature row exists.
func evidence(view *features.View, in *pb.OrderContext, tok *domain.Token, block *domain.BlockState) score.Evidence {
	ev := score.Evidence{
		Degraded:        view.Degraded(),
		ConsumptionFrac: consumptionFrac(tok, block),
	}
	if r := view.Rows[in.GetCustomerId()]; r != nil {
		ev.BehaviourDeviation = r.BehaviourDeviation
		ev.NetworkRisk = r.NetworkRisk
	}
	if r := view.Rows[in.GetTokenId()]; r != nil {
		ev.IntentDivergence = r.IntentDivergence
	}
	if r := view.Rows[in.GetAgentId()]; r != nil {
		ev.Reputation = r.Reputation
	}
	return ev
}

func consumptionFrac(tok *domain.Token, block *domain.BlockState) float64 {
	if tok == nil || tok.TokenCeilingPaise <= 0 || block == nil {
		return 0
	}
	f := float64(block.ConsumedTotal) / float64(tok.TokenCeilingPaise)
	if f < 0 {
		return 0
	}
	if f > 1 {
		return 1
	}
	return f
}

// lookupToken returns the token, or (nil, nil) when absent, or (nil, err) on a
// real store failure. Absence is the predicates' business; failure fails closed.
func lookupToken(ctx context.Context, ts store.TokenStore, id string) (*domain.Token, error) {
	t, err := ts.GetToken(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	return t, err
}

func lookupBlock(ctx context.Context, ts store.TokenStore, id string) (*domain.BlockState, error) {
	b, err := ts.GetBlockState(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	return b, err
}

func lookupOverlay(ctx context.Context, ps store.PolicyStore, id string) (*domain.PolicyOverlay, error) {
	o, err := ps.GetOverlay(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	return o, err
}

// MTLSIdentity extracts the authenticated caller identity from the request's
// mTLS peer certificate: a URI SAN (SPIFFE id) if present, else the subject
// common name. It returns "" when the connection is not mutually authenticated,
// which P5 treats as unverifiable and blocks.
func MTLSIdentity(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return ""
	}
	certs := tlsInfo.State.PeerCertificates
	if len(certs) == 0 {
		return ""
	}
	if len(certs[0].URIs) > 0 {
		return certs[0].URIs[0].String()
	}
	return certs[0].Subject.CommonName
}
