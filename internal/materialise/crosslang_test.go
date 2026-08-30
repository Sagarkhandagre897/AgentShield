package materialise_test

import (
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/Sagarkhandagre897/AgentShield/internal/bus"
	"github.com/Sagarkhandagre897/AgentShield/internal/domain"
	"github.com/Sagarkhandagre897/AgentShield/internal/materialise"
	"github.com/Sagarkhandagre897/AgentShield/internal/store/memory"
)

// emitScript is the Python helper that prints one behaviour-deposit event as the
// exact JSON the off-clock engine publishes. Path is relative to this package.
var emitScript = filepath.Join("..", "..", "services", "shared", "tools", "emit_deposit.py")

// TestCrossLanguageDepositLands is the seam proof across the language boundary,
// with no broker: the Python side (agentshield_shared) serialises a
// feature.behaviour.deposited event; encoding/json decodes those very bytes into
// a Go domain.Event; the real Go materialiser folds it onto the feature store;
// and a keyed read — the on-clock MP2 read — picks the figure back up. If the
// Python schema and the Go domain/bus contracts ever drift a field, this fails.
//
// Skips (does not fail) where python3 is absent, so `go test ./...` stays green
// on a machine without the interpreter.
func TestCrossLanguageDepositLands(t *testing.T) {
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not on PATH; skipping cross-language seam proof")
	}

	out, err := exec.Command(py, emitScript).Output()
	if err != nil {
		t.Fatalf("emit_deposit.py failed: %v", err)
	}

	// The bytes crossed the boundary as JSON, exactly as a broker would carry them.
	var ev domain.Event
	if err := json.Unmarshal(out, &ev); err != nil {
		t.Fatalf("Python deposit bytes did not decode into domain.Event: %v\nbytes: %s", err, out)
	}

	// The envelope the Python side stamped must match the Go bus contract.
	if ev.Type != bus.EventFeatureBehaviour {
		t.Fatalf("type = %q, want %q", ev.Type, bus.EventFeatureBehaviour)
	}
	if ev.Source != "behaviour-engine" {
		t.Fatalf("source = %q, want behaviour-engine", ev.Source)
	}
	if ev.EventID == "" {
		t.Fatal("deposit carried no event_id; redelivery could not dedupe")
	}

	// Fold it through the single writer, then read it back by key (the MP2 read).
	fs := memory.NewFeatureStore()
	m := materialise.New(memory.NewTokenStore(), fs, func() int64 { return fixedAt })
	ctx := context.Background()
	if err := m.Handle(ctx, ev); err != nil {
		t.Fatalf("materialiser rejected the Python deposit: %v", err)
	}

	r, ok := rowOf(t, fs, agent)
	if !ok {
		t.Fatalf("Python deposit did not land on feature row %q", agent)
	}
	if r.BehaviourDeviation != 0.55 {
		t.Fatalf("behaviour_deviation = %v, want 0.55 (as emitted by Python)", r.BehaviourDeviation)
	}
	if len(r.SignalDeviations) != 1 || r.SignalDeviations[0].Signal != "velocity" || r.SignalDeviations[0].ObsCount != 42 {
		t.Fatalf("signal breakdown did not survive the boundary: %v", r.SignalDeviations)
	}
	if r.ComputedAt != 444 {
		t.Fatalf("computed_at = %d, want 444 (the engine's occurred_at)", r.ComputedAt)
	}

	// Idempotency across the boundary: the stable Python event_id folds once.
	if err := m.Handle(ctx, ev); err != nil {
		t.Fatalf("redelivery of the Python deposit errored: %v", err)
	}
}
