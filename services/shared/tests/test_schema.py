"""Contract tests for the wire schema — pure stdlib, no broker needed.

They pin the JSON the Python side emits to exactly the keys the Go domain/bus
structs expect, so a deposit an engine publishes decodes on the Go side without
translation. Runnable with pytest or directly: `python3 tests/test_schema.py`.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agentshield_shared import bus, hmac_util, schema  # noqa: E402
from agentshield_shared.schema import Event, FeatureRow, SignalDeviation  # noqa: E402


def test_event_json_keys_match_go_tags():
    ev = Event(event_id="e1", type=schema.EVENT_PAYMENT_CAPTURED, token_id="tok_1", occurred_at=42)
    d = ev.to_dict()
    assert set(d) == {"event_id", "type", "token_id", "occurred_at", "payload", "source"}
    # hmac is omitempty on the Go side — absent unless set.
    assert "hmac" not in d
    back = Event.from_json(ev.to_json())
    assert back == ev


def test_feature_row_keys_match_go_tags():
    r = FeatureRow(key="agent_1", behaviour_deviation=0.4, computed_at=7)
    d = r.to_dict()
    assert set(d) == {
        "key",
        "behaviour_deviation",
        "signal_deviations",
        "intent_divergence",
        "network_risk",
        "reputation",
        "consumption_frac",
        "computed_at",
    }
    assert FeatureRow.from_dict(d) == r


def test_behaviour_deposit_shape():
    sigs = [SignalDeviation(signal="velocity", deviation=0.7, obs_count=42)]
    ev = bus.build_behaviour_event("agent_1", 0.55, 444, sigs, token_id="tok_1")
    assert ev.type == schema.EVENT_FEATURE_BEHAVIOUR
    assert ev.source == "behaviour-engine"
    assert schema.topic_for(ev.type) == schema.TOPIC_FEATURES
    assert ev.payload[schema.PAYLOAD_FEATURE_KEY] == "agent_1"
    assert ev.payload[schema.PAYLOAD_DEVIATION] == 0.55
    assert ev.payload[schema.PAYLOAD_SIGNAL_DEVIATIONS][0]["obs_count"] == 42
    # A stable id: the same computation folds once on redelivery.
    assert ev.event_id == bus.build_behaviour_event("agent_1", 0.55, 444, sigs, "tok_1").event_id


def test_intent_and_network_deposit_shape():
    iv = bus.build_intent_event("tok_1", 0.6, 222)
    assert iv.type == schema.EVENT_FEATURE_INTENT and iv.source == "intent-engine"
    assert iv.payload == {schema.PAYLOAD_FEATURE_KEY: "tok_1", schema.PAYLOAD_DIVERGENCE: 0.6}

    nw = bus.build_network_event("node_1", 0.8, 333)
    assert nw.type == schema.EVENT_FEATURE_NETWORK and nw.source == "graph-engine"
    assert nw.payload == {schema.PAYLOAD_FEATURE_KEY: "node_1", schema.PAYLOAD_RISK: 0.8}


def test_hmac_sign_verify():
    msg = b'{"event_id":"e1"}'
    sig = hmac_util.sign("secret", msg)
    assert hmac_util.verify("secret", msg, sig)
    assert not hmac_util.verify("secret", msg, "deadbeef")
    assert not hmac_util.verify("wrong", msg, sig)


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
