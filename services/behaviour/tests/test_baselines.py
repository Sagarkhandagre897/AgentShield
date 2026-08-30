"""Layer-1 baseline tests (System Design §11) — pure stdlib.

They pin the behaviour that makes the deviation trustworthy: a debit is judged
against the principal's own past, a well-supported spike reads high, and a spike
off a barely-seen principal is shrunk toward the no-anomaly prior rather than
trusted thin. Runnable with pytest or directly.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agentshield_behaviour.baselines import (  # noqa: E402
    FEATURE_NAMES,
    BaselineBank,
    PrincipalState,
    baseline_deviation,
)


def _calm(bank, key="agent_1", n=60, amount=10000):
    ts = 1_000_000
    sig = fv = None
    for _ in range(n):
        sig, fv = bank.observe(key, amount, "merchant_A", ts)
        ts += 3600
    return sig, fv, ts


def test_calm_baseline_reads_low():
    bank = BaselineBank()
    _calm(bank)
    sig, _ = bank.observe("agent_1", 10000, "merchant_A", 1_000_000 + 61 * 3600)
    assert baseline_deviation(sig) < 0.1


def test_amount_spike_reads_high():
    bank = BaselineBank()
    _, _, ts = _calm(bank)
    sig, _ = bank.observe("agent_1", 500000, "merchant_NEW", ts)
    assert baseline_deviation(sig) > 0.7
    by = {s.signal: s for s in sig}
    assert by["amount_z"].deviation > 0.5
    assert by["merchant_novelty"].deviation > 0.5


def test_cold_start_is_shrunk():
    bank = BaselineBank()
    bank.observe("newbie", 10000, "m1", 1)
    sig, _ = bank.observe("newbie", 999999, "m2", 2)  # huge, but off 1 prior obs
    assert baseline_deviation(sig) < 0.3, "a thin signal must not flag on its own"


def test_feature_vector_width_is_stable():
    bank = BaselineBank()
    sig, fv = bank.observe("k", 100, "m", 10)
    assert len(fv) == len(FEATURE_NAMES)
    # A merchant-less observation still yields a full-width vector.
    sig2, fv2 = bank.observe("k2", 100, "", 10)
    assert len(fv2) == len(FEATURE_NAMES)


def test_measured_against_prior_not_self():
    # The first observation cannot be anomalous — there is no baseline yet.
    st = PrincipalState(key="k")
    sig = st.observe(10_000_000, "m", 100)
    assert all(s.deviation == 0.0 for s in sig if s.signal == "amount_z")


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
