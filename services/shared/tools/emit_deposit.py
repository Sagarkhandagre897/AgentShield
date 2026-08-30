"""emit_deposit — print one behaviour-deposit event as JSON on stdout.

Used by the Go cross-language test (internal/materialise/crosslang_test.go) to
prove that the bytes the Python engine publishes decode into domain.Event and
land on the feature row through the real Go materialiser — the on/off-clock seam
verified across the language boundary, with no broker.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agentshield_shared import bus  # noqa: E402
from agentshield_shared.schema import SignalDeviation  # noqa: E402

ev = bus.build_behaviour_event(
    feature_key="agent_1",
    deviation=0.55,
    occurred_at=444,
    signals=[SignalDeviation(signal="velocity", deviation=0.7, obs_count=42)],
    token_id="tok_1",
)
sys.stdout.write(ev.to_json().decode("utf-8"))
