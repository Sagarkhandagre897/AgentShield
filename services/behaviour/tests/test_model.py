"""Model-layer tests (System Design §11, Layers 2-3) — need the ML wheels.

They pin what the clock actually depends on: the scorer emits a *probability* (in
[0, 1], monotone in misuse) because the aggregator multiplies it by rupees, not a
rank; the isolation-forest floor lifts an outlier above a typical row without any
labels; and a persisted model predicts identically once reloaded. Skipped whole
when numpy/lightgbm/scikit-learn are absent — Layer 1 stands alone there.

Runnable with pytest or directly (`python3 tests/test_model.py`), and a clean
no-op skip in either mode when the wheels are missing.
"""

import math
import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agentshield_behaviour.model import (  # noqa: E402
    BehaviourScorer,
    IsolationFloor,
    available,
)

_HAVE = available()

try:
    import pytest  # noqa: E402

    pytestmark = pytest.mark.skipif(not _HAVE, reason="ML wheels (numpy/lightgbm/scikit-learn) not installed")
except ImportError:  # pragma: no cover - direct run without pytest
    pytest = None

# A feature vector is 6-wide (see baselines.FEATURE_NAMES): four shrunk signal
# deviations then two volume terms. Clean rows sit near zero; misuse rows carry
# high, well-supported deviations.
_CLEAN = [0.03, 0.02, 0.01, 0.04, math.log1p(60), math.log1p(4)]
_MISUSE = [0.92, 0.88, 0.80, 0.90, math.log1p(6), math.log1p(1)]


def _training_set(n=120):
    rng = random.Random(7)
    X, y = [], []
    for _ in range(n):
        X.append([max(0.0, v + rng.gauss(0.0, 0.02)) for v in _CLEAN])
        y.append(0)
        X.append([max(0.0, v + rng.gauss(0.0, 0.02)) for v in _MISUSE])
        y.append(1)
    return X, y


def test_scorer_is_none_until_fitted():
    s = BehaviourScorer()
    assert not s.fitted
    assert s.predict(_CLEAN) is None  # engine leans on the floor + Layer 1 instead


def test_scorer_emits_a_monotone_probability():
    X, y = _training_set()
    s = BehaviourScorer()
    s.train(X, y)
    assert s.fitted
    clean, misuse = s.predict(_CLEAN), s.predict(_MISUSE)
    assert 0.0 <= clean <= 1.0 and 0.0 <= misuse <= 1.0  # a probability, not a rank
    assert misuse > clean, "misuse must score above clean"


def test_scorer_rejects_single_class():
    # Isotonic calibration is meaningless without both outcomes to calibrate against.
    s = BehaviourScorer()
    try:
        s.train([_CLEAN, _CLEAN], [0, 0])
        raised = False
    except ValueError:
        raised = True
    assert raised, "single-class training must be rejected"


def test_floor_is_zero_until_fitted():
    assert IsolationFloor().score(_MISUSE) == 0.0  # no floor, never a spurious alarm


def test_floor_lifts_outliers_without_labels():
    X, y = _training_set()
    typical = [row for row, label in zip(X, y) if label == 0]
    f = IsolationFloor()
    f.train(typical)  # unsupervised: only the shape of "normal"
    outlier = [8.0, 8.0, 8.0, 8.0, 0.0, 0.0]  # nothing like a typical row
    assert f.score(outlier) > f.score(_CLEAN)


def test_persisted_model_predicts_identically():
    import tempfile

    X, y = _training_set()
    s, f = BehaviourScorer(), IsolationFloor()
    s.train(X, y)
    f.train([row for row, label in zip(X, y) if label == 0])
    s_before, f_before = s.predict(_MISUSE), f.score(_MISUSE)
    with tempfile.TemporaryDirectory() as d:
        sp, fp = os.path.join(d, "gbdt.pkl"), os.path.join(d, "floor.pkl")
        s.save(sp)
        f.save(fp)
        s2, f2 = BehaviourScorer(), IsolationFloor()
        s2.load(sp)
        f2.load(fp)
        assert abs(s2.predict(_MISUSE) - s_before) < 1e-9
        assert abs(f2.score(_MISUSE) - f_before) < 1e-9


if __name__ == "__main__":
    if not _HAVE:
        print("skipped (ML wheels not installed)")
        raise SystemExit(0)
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
