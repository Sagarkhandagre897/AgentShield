"""Streaming-sketch tests (System Design §11, Layer 1) — pure stdlib.

They pin the bounded-memory primitives the behaviour engine leans on: a distinct
count within HyperLogLog's error budget, a Count-Min frequency that never
under-counts, and a P-square quantile that converges to the true tail. Runnable
with pytest or directly: `python3 tests/test_sketches.py`.
"""

import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agentshield_behaviour.sketches import CountMinSketch, HyperLogLog, P2Quantile  # noqa: E402


def test_hyperloglog_within_error_budget():
    hll = HyperLogLog(p=10)  # 1024 registers → ~3.25% standard error
    for i in range(5000):
        hll.add(f"merchant_{i}")
    est = hll.count()
    assert abs(est - 5000) / 5000 < 0.10, f"HLL estimate {est} too far from 5000"


def test_hyperloglog_small_range_is_exact_ish():
    hll = HyperLogLog(p=8)
    for m in ["a", "b", "c", "a", "b"]:  # 3 distinct
        hll.add(m)
    assert 2.0 <= hll.count() <= 4.0


def test_countmin_never_undercounts():
    cms = CountMinSketch()
    for _ in range(300):
        cms.add("hot")
    for i in range(2000):
        cms.add(f"cold_{i}")
    est = cms.estimate("hot")
    assert est >= 300, "Count-Min must never under-count"
    assert est < 300 * 1.5, f"Count-Min over-count {est} implausibly high"
    assert cms.estimate("never_seen") < 50


def test_p2_quantile_converges():
    random.seed(1)
    p2 = P2Quantile(0.95)
    data = [random.gauss(0.0, 1.0) for _ in range(20000)]
    for x in data:
        p2.add(x)
    true_p95 = sorted(data)[int(0.95 * len(data))]
    assert abs(p2.value() - true_p95) < 0.1, f"P² p95 {p2.value():.3f} vs true {true_p95:.3f}"


def test_p2_before_five_observations():
    p2 = P2Quantile(0.5)
    p2.add(10.0)
    p2.add(20.0)
    assert 10.0 <= p2.value() <= 20.0  # empirical fallback, no crash


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
