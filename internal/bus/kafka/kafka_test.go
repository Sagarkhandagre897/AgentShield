package kafka

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Sagarkhandagre897/AgentShield/internal/bus"
	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
)

func TestTopicForRouting(t *testing.T) {
	cases := map[string]string{
		bus.EventDecisionMade:     TopicEvaluations,
		bus.EventPaymentCaptured:  TopicPayments,
		bus.EventPaymentFailed:    TopicPayments,
		bus.EventPaymentDisputed:  TopicPayments,
		bus.EventTokenConfirmed:   TopicTokens,
		bus.EventTokenCancelled:   TopicTokens,
		bus.EventFeatureBehaviour: TopicFeatures,
		bus.EventFeatureIntent:    TopicFeatures,
		bus.EventFeatureNetwork:   TopicFeatures,
	}
	for typ, want := range cases {
		got, ok := topicFor(typ)
		if !ok || got != want {
			t.Errorf("topicFor(%q) = %q,%v; want %q,true", typ, got, ok, want)
		}
	}
	if _, ok := topicFor("something.unknown"); ok {
		t.Error("an unknown event type must have no topic")
	}
}

// TestEventWireRoundTrip pins the wire format: the adapter JSON-encodes a
// domain.Event, and a JSON-decoded copy must still yield the deposited figure
// through the same payload readers a Python engine's message would exercise.
func TestEventWireRoundTrip(t *testing.T) {
	sigs := []domain.SignalDeviation{{Signal: "velocity", Deviation: 0.7, ObsCount: 42}}
	ev := bus.FeatureBehaviourDepositEvent("b1", "tok_1", "agent_1", 111, 0.4, sigs)

	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back domain.Event
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if topic, ok := topicFor(back.Type); !ok || topic != TopicFeatures {
		t.Fatalf("decoded event routes to %q (ok=%v), want features.v1", topic, ok)
	}
	dev, ok := bus.PayloadFloat64(back, bus.PayloadDeviation)
	if !ok || dev != 0.4 {
		t.Fatalf("deviation did not survive the wire: %v (ok=%v)", dev, ok)
	}
	got := bus.PayloadSignals(back)
	if len(got) != 1 || got[0].Signal != "velocity" || got[0].ObsCount != 42 {
		t.Fatalf("signal breakdown did not survive the wire: %v", got)
	}
}

// TestIntegrationPublishSubscribe is the real-broker proof: it runs only when
// KAFKA_SEEDS points at a reachable Redpanda (e.g. `docker compose up`), and is
// skipped otherwise so the suite stays green without infra.
func TestIntegrationPublishSubscribe(t *testing.T) {
	seedsEnv := os.Getenv("KAFKA_SEEDS")
	if seedsEnv == "" {
		t.Skip("set KAFKA_SEEDS=host:port to run the Redpanda integration test")
	}
	seeds := strings.Split(seedsEnv, ",")
	ctx := context.Background()

	b, err := New(ctx, seeds, 3)
	if err != nil {
		t.Fatalf("new bus: %v", err)
	}
	defer b.Close()

	var mu sync.Mutex
	got := map[string]domain.Event{}
	group := "test-group-" + time.Now().Format("150405.000")
	cancel, err := b.Subscribe(group, func(_ context.Context, ev domain.Event) error {
		mu.Lock()
		got[ev.EventID] = ev
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()

	ev := bus.PaymentCapturedEvent("e1", "tok_1", 1_700_000_000, 250000, "n1")
	if err := b.Publish(ctx, ev); err != nil {
		t.Fatalf("publish: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		mu.Lock()
		_, ok := got["e1"]
		mu.Unlock()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("published event did not arrive through the consumer group in time")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
