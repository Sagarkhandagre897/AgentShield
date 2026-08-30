// Package memory is an in-memory bus.Bus that keeps the asynchronous plane
// runnable and testable without a broker. It fans each published event out to
// every subscriber, preserves publish order per subscriber (so per-token order
// holds, since all events flow through one Publish path), and redelivers on
// handler error up to a retry bound — the at-least-once contract the workers are
// written against. A Kafka/Redpanda adapter lands behind the same interface.
package memory

import (
	"context"
	"sync"

	"github.com/Sagarkhandagre897/AgentShield/internal/bus"
	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
)

// subscription is one consumer group: an ordered queue drained by a single
// goroutine, so a group processes events strictly in the order they were
// published.
type subscription struct {
	group string
	h     bus.Handler
	queue chan domain.Event
	done  chan struct{}
}

// Bus is an in-memory bus.Bus.
type Bus struct {
	mu         sync.RWMutex
	subs       []*subscription
	maxRetries int
	buffer     int
	wg         sync.WaitGroup
	closed     bool
}

// New returns an in-memory bus. maxRetries is how many extra times a failing
// handler is retried before the event is dropped (dead-lettered); 0 means one
// attempt. The per-subscriber queue is fixed at a reasonable buffer.
func New(maxRetries int) *Bus {
	return &Bus{maxRetries: maxRetries, buffer: 1024}
}

// Publish sends the event to every current subscriber's queue in registration
// order, blocking for backpressure until each accepts it (or ctx is done).
func (b *Bus) Publish(ctx context.Context, ev domain.Event) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return context.Canceled
	}
	for _, s := range b.subs {
		select {
		case s.queue <- ev:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			// subscriber cancelled mid-publish; skip it
		}
	}
	return nil
}

// Subscribe registers a consumer group and starts draining it. The returned
// cancel func stops the group.
func (b *Bus) Subscribe(group string, h bus.Handler) (func(), error) {
	s := &subscription{
		group: group,
		h:     h,
		queue: make(chan domain.Event, b.buffer),
		done:  make(chan struct{}),
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return func() {}, context.Canceled
	}
	b.subs = append(b.subs, s)
	b.mu.Unlock()

	b.wg.Add(1)
	go b.run(s)

	var once sync.Once
	return func() { once.Do(func() { b.remove(s) }) }, nil
}

func (b *Bus) run(s *subscription) {
	defer b.wg.Done()
	for {
		select {
		case ev := <-s.queue:
			b.deliver(s, ev)
		case <-s.done:
			return
		}
	}
}

// deliver calls the handler, retrying on error up to maxRetries. Downstream
// idempotency (on event_id) is what makes the retry safe.
func (b *Bus) deliver(s *subscription, ev domain.Event) {
	for attempt := 0; attempt <= b.maxRetries; attempt++ {
		if err := s.h(context.Background(), ev); err == nil {
			return
		}
		select {
		case <-s.done:
			return
		default:
		}
	}
	// Retries exhausted: the event is dropped (dead-lettered). A real broker
	// routes it to a dead-letter topic; the in-memory broker drops it.
}

func (b *Bus) remove(s *subscription) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, cur := range b.subs {
		if cur == s {
			b.subs = append(b.subs[:i], b.subs[i+1:]...)
			close(s.done)
			return
		}
	}
}

// Close stops all delivery and waits for in-flight handlers to finish.
func (b *Bus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	for _, s := range b.subs {
		close(s.done)
	}
	b.subs = nil
	b.mu.Unlock()

	b.wg.Wait()
	return nil
}

var _ bus.Bus = (*Bus)(nil)
