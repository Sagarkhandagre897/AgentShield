// Package kafka is a bus.Bus over a Kafka-class broker (Redpanda), behind the
// same interface the in-memory broker satisfies. It is the durable spine the two
// planes meet at going down (§3, §11): the synchronous plane publishes, the
// off-clock workers subscribe, and nothing above the interface changes.
//
// The event Type selects the topic; token_id is the record key, which is what
// gives per-token ordering (a partition holds one token's events in order).
// Delivery is at-least-once: a consumer group commits an offset only after its
// handler has folded the record, so a crash or rebalance redelivers uncommitted
// events — safe because every handler is idempotent on event_id. Each Subscribe
// is an independent consumer group, so every group sees every event (fan-out)
// while sharing the work within a group.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Sagarkhandagre897/AgentShield/internal/bus"
	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
)

// Topic names — the live analytic topics, outcomes.v1 for the learning loop's
// settled-outcome labels, and vault.v1 for the one PII-bearing channel. token_id
// keys every one of them (envelope.sealed keys on the mandate the session runs
// under, with the session_id in the payload).
const (
	TopicEvaluations = "evaluations.v1"
	TopicPayments    = "payments.v1"
	TopicTokens      = "tokens.v1"
	TopicFeatures    = "features.v1"
	TopicOutcomes    = "outcomes.v1"
	TopicVault       = "vault.v1"
)

// allTopics is what every consumer group subscribes to: a group sees every event
// and its handler no-ops on the types it does not care about (§ the handlers'
// default branch). It is also the bootstrap set the deploy init creates.
var allTopics = []string{TopicEvaluations, TopicPayments, TopicTokens, TopicFeatures, TopicOutcomes, TopicVault}

// topicFor maps an event Type to its topic. An unknown type has no home and is
// rejected on Publish rather than silently dropped.
func topicFor(eventType string) (string, bool) {
	switch eventType {
	case bus.EventDecisionMade:
		return TopicEvaluations, true
	case bus.EventPaymentCaptured, bus.EventPaymentFailed, bus.EventPaymentDisputed:
		return TopicPayments, true
	case bus.EventTokenConfirmed, bus.EventTokenCancelled:
		return TopicTokens, true
	case bus.EventFeatureBehaviour, bus.EventFeatureIntent, bus.EventFeatureNetwork:
		return TopicFeatures, true
	case bus.EventOutcomeLabeled:
		return TopicOutcomes, true
	case bus.EventEnvelopeSealed, bus.EventErasureRequested:
		return TopicVault, true
	default:
		return "", false
	}
}

// Bus is a bus.Bus over Redpanda. One shared producer client fans out to the
// topics; each Subscribe opens its own consumer-group client.
type Bus struct {
	seeds      []string
	producer   *kgo.Client
	maxRetries int

	mu      sync.Mutex
	cancels []func() // stops each subscription's poll loop
	wg      sync.WaitGroup
	closed  bool
}

// New opens the producer, verifies broker reachability with Ping, and returns a
// bus seeded with the given brokers. maxRetries bounds inline handler retries
// before an event is left uncommitted for broker-level redelivery.
func New(ctx context.Context, seeds []string, maxRetries int) (*Bus, error) {
	p, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.AllowAutoTopicCreation(), // dev convenience; deploy bootstraps topics explicitly
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new producer: %w", err)
	}
	if err := p.Ping(ctx); err != nil {
		p.Close()
		return nil, fmt.Errorf("kafka: ping %v: %w", seeds, err)
	}
	return &Bus{seeds: seeds, producer: p, maxRetries: maxRetries}, nil
}

// Publish JSON-encodes the event and produces it to the topic for its Type,
// keyed by token_id so a token's events land on one partition in order. It waits
// for the broker ack so a produce error surfaces to the caller.
func (b *Bus) Publish(ctx context.Context, ev domain.Event) error {
	topic, ok := topicFor(ev.Type)
	if !ok {
		return fmt.Errorf("kafka: no topic for event type %q", ev.Type)
	}
	val, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("kafka: encode event %s: %w", ev.EventID, err)
	}
	rec := &kgo.Record{Topic: topic, Key: []byte(ev.TokenID), Value: val}
	return b.producer.ProduceSync(ctx, rec).FirstErr()
}

// Subscribe registers a consumer group by name across all topics and starts
// draining it in a goroutine. The returned cancel func stops that group.
func (b *Bus) Subscribe(group string, h bus.Handler) (func(), error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return func() {}, context.Canceled
	}
	b.mu.Unlock()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(b.seeds...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(allTopics...),
		kgo.DisableAutoCommit(), // commit only after the handler folds — at-least-once
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new consumer %q: %w", group, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.wg.Add(1)
	go b.run(ctx, cl, h)

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			cl.Close() // unblocks any in-flight PollFetches
		})
	}

	b.mu.Lock()
	b.cancels = append(b.cancels, stop)
	b.mu.Unlock()
	return stop, nil
}

// run polls the group and folds each record, preserving per-partition order and
// committing only the leading run of successfully-handled records — so a failed
// fold leaves its offset (and everything after it on that partition) for
// redelivery.
func (b *Bus) run(ctx context.Context, cl *kgo.Client, h bus.Handler) {
	defer b.wg.Done()
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			// A poll error (e.g. transient broker blip) — nothing committed,
			// so the records return on the next poll. Bail on shutdown.
			if errors.Is(errs[0].Err, context.Canceled) {
				return
			}
			continue
		}
		fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
			var commit []*kgo.Record
			for _, rec := range ftp.Records {
				if !b.fold(ctx, h, rec) {
					break // stop this partition's commit prefix at the first failure
				}
				commit = append(commit, rec)
			}
			if len(commit) > 0 {
				_ = cl.CommitRecords(ctx, commit...)
			}
		})
	}
}

// fold decodes a record into an event and hands it to the handler, retrying up
// to maxRetries. It returns true once the handler succeeds. A record that fails
// to decode is skipped as handled (true): it is poison, and blocking the
// partition on it forever would stall every later event.
func (b *Bus) fold(ctx context.Context, h bus.Handler, rec *kgo.Record) bool {
	var ev domain.Event
	if json.Unmarshal(rec.Value, &ev) != nil {
		return true // undecodable; do not wedge the partition
	}
	for attempt := 0; attempt <= b.maxRetries; attempt++ {
		if ctx.Err() != nil {
			return false
		}
		if h(ctx, ev) == nil {
			return true
		}
	}
	return false // leave uncommitted for broker-level redelivery
}

// Close stops every consumer group, closes the producer, and waits for the poll
// loops to exit.
func (b *Bus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	cancels := b.cancels
	b.cancels = nil
	b.mu.Unlock()

	for _, stop := range cancels {
		stop()
	}
	b.wg.Wait()
	b.producer.Close()
	return nil
}

var _ bus.Bus = (*Bus)(nil)
