package memory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
)

func recv(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delivery")
		return ""
	}
}

func TestDeliversInOrder(t *testing.T) {
	b := New(0)
	defer b.Close()

	got := make(chan string, 8)
	if _, err := b.Subscribe("g1", func(_ context.Context, ev domain.Event) error {
		got <- ev.EventID
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	ctx := context.Background()
	for _, id := range []string{"e1", "e2", "e3"} {
		if err := b.Publish(ctx, domain.Event{EventID: id, TokenID: "tok_1"}); err != nil {
			t.Fatalf("publish %s: %v", id, err)
		}
	}
	for _, want := range []string{"e1", "e2", "e3"} {
		if got := recv(t, got); got != want {
			t.Fatalf("out of order: got %s want %s", got, want)
		}
	}
}

func TestFanOutToEveryGroup(t *testing.T) {
	b := New(0)
	defer b.Close()

	a := make(chan string, 1)
	c := make(chan string, 1)
	b.Subscribe("stream-processor", func(_ context.Context, ev domain.Event) error { a <- ev.EventID; return nil })
	b.Subscribe("materialiser", func(_ context.Context, ev domain.Event) error { c <- ev.EventID; return nil })

	if err := b.Publish(context.Background(), domain.Event{EventID: "e1", TokenID: "tok_1"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if recv(t, a) != "e1" || recv(t, c) != "e1" {
		t.Fatalf("both consumer groups must receive the event")
	}
}

func TestAtLeastOnceRetry(t *testing.T) {
	b := New(3) // up to 3 retries
	defer b.Close()

	var calls int32
	done := make(chan struct{})
	b.Subscribe("g", func(_ context.Context, ev domain.Event) error {
		if atomic.AddInt32(&calls, 1) < 3 {
			return errors.New("transient")
		}
		close(done)
		return nil
	})

	b.Publish(context.Background(), domain.Event{EventID: "e1"})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was never retried to success")
	}
	if n := atomic.LoadInt32(&calls); n != 3 {
		t.Fatalf("want 3 attempts, got %d", n)
	}
}

func TestNoRetryWhenMaxIsZero(t *testing.T) {
	b := New(0)
	defer b.Close()

	var calls int32
	fired := make(chan struct{})
	b.Subscribe("g", func(_ context.Context, ev domain.Event) error {
		atomic.AddInt32(&calls, 1)
		close(fired)
		return errors.New("boom")
	})

	b.Publish(context.Background(), domain.Event{EventID: "e1"})
	<-fired
	time.Sleep(50 * time.Millisecond) // allow any (wrong) retry to happen
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("maxRetries=0 must mean a single attempt, got %d", n)
	}
}

// TestConsumerIdempotencyOnRedelivery shows the contract the workers rely on: a
// redelivered event (same event_id) must fold in only once. The bus delivers
// at-least-once; the consumer dedupes.
func TestConsumerIdempotencyOnRedelivery(t *testing.T) {
	b := New(0)
	defer b.Close()

	var mu sync.Mutex
	seen := map[string]struct{}{}
	applied := 0
	processed := make(chan struct{}, 4)
	b.Subscribe("g", func(_ context.Context, ev domain.Event) error {
		mu.Lock()
		if _, dup := seen[ev.EventID]; !dup {
			seen[ev.EventID] = struct{}{}
			applied++
		}
		mu.Unlock()
		processed <- struct{}{}
		return nil
	})

	ctx := context.Background()
	b.Publish(ctx, domain.Event{EventID: "e1", TokenID: "tok_1"})
	b.Publish(ctx, domain.Event{EventID: "e1", TokenID: "tok_1"}) // redelivery
	<-processed
	<-processed

	mu.Lock()
	defer mu.Unlock()
	if applied != 1 {
		t.Fatalf("idempotent consumer must apply a duplicate once, applied %d", applied)
	}
}

func TestCancelStopsDelivery(t *testing.T) {
	b := New(0)
	defer b.Close()

	got := make(chan string, 1)
	cancel, _ := b.Subscribe("g", func(_ context.Context, ev domain.Event) error {
		got <- ev.EventID
		return nil
	})
	cancel()

	// After cancel, a publish reaches no one; it must not block or panic.
	if err := b.Publish(context.Background(), domain.Event{EventID: "e1"}); err != nil {
		t.Fatalf("publish after cancel: %v", err)
	}
	select {
	case <-got:
		t.Fatal("cancelled subscriber must not receive events")
	case <-time.After(100 * time.Millisecond):
	}
}
