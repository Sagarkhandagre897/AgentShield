"""model — Layers 2 and 3 of the behaviour engine (System Design §11).

    Layer 2 — the calibrated scorer: "A gradient-boosted-tree model (LightGBM /
    XGBoost) turns the baseline deviations into a score, and isotonic calibration
    turns that score into a probability. Calibration is not optional here: the
    aggregator on the clock multiplies this figure by rupees to get an expected
    loss, so it must be a real probability, not a rank."

    Layer 3 — the label-free floor: "Beneath the supervised model sits an
    unsupervised floor — an isolation forest now, an autoencoder later — so that a
    pattern never seen in the labels still registers as anomalous."

The floor is a *lower bound* on the figure, which is why the engine takes the max
of the two: a labelled model can raise the number, never suppress an anomaly the
labels have not yet caught up to. Labels come only from SETTLED outcomes (a
dispute is misuse, a clean capture is not) — never from our own past decisions.

numpy / lightgbm / scikit-learn are imported lazily so Layer 1 and its tests run
without the ML wheels installed. `available()` reports whether they are present.
"""

from __future__ import annotations

import math
import pickle
from typing import List, Optional, Sequence

try:
    import numpy as np
    from lightgbm import LGBMClassifier
    from sklearn.ensemble import IsolationForest
    from sklearn.isotonic import IsotonicRegression

    _HAVE_ML = True
except Exception:  # pragma: no cover - exercised only where the wheels are absent
    _HAVE_ML = False


def available() -> bool:
    """True when the ML stack is importable — the calibrated scorer and the floor
    are live. When False the engine runs on Layer 1 alone (a defensible cold-start
    mode), never crashing for want of a wheel."""
    return _HAVE_ML


def _require_ml() -> None:
    if not _HAVE_ML:
        raise RuntimeError("numpy/lightgbm/scikit-learn not installed; `pip install -e services/behaviour`")


class BehaviourScorer:
    """Layer 2: a LightGBM classifier whose margin is turned into a calibrated
    probability by isotonic regression. Unfitted, predict() returns None — the
    engine then leans on the floor and the Layer-1 aggregate rather than inventing
    a number."""

    def __init__(self) -> None:
        self._gbdt = None
        self._iso = None

    @property
    def fitted(self) -> bool:
        return self._gbdt is not None and self._iso is not None

    def train(self, X: Sequence[Sequence[float]], y: Sequence[int]) -> None:
        """Fit the trees on the deviation vectors, then fit isotonic calibration on
        the trees' own scores so the output is a probability, not a rank. Labels
        are settled outcomes: 1 = misuse (a dispute), 0 = a clean settlement."""
        _require_ml()
        Xa, ya = np.asarray(X, dtype=float), np.asarray(y, dtype=int)
        if len(set(ya.tolist())) < 2:
            raise ValueError("need both classes (misuse and clean) to calibrate")
        self._gbdt = LGBMClassifier(n_estimators=200, num_leaves=31, learning_rate=0.05, verbosity=-1)
        self._gbdt.fit(Xa, ya)
        raw = self._gbdt.predict_proba(Xa)[:, 1]
        self._iso = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
        self._iso.fit(raw, ya)

    def predict(self, x: Sequence[float]) -> Optional[float]:
        if not self.fitted:
            return None
        raw = self._gbdt.predict_proba(np.asarray([x], dtype=float))[:, 1]
        return float(self._iso.predict(raw)[0])

    def save(self, path: str) -> None:
        with open(path, "wb") as f:
            pickle.dump({"gbdt": self._gbdt, "iso": self._iso}, f)

    def load(self, path: str) -> None:
        _require_ml()
        with open(path, "rb") as f:
            blob = pickle.load(f)
        self._gbdt, self._iso = blob["gbdt"], blob["iso"]


class IsolationFloor:
    """Layer 3: an isolation forest fitted on the deviation vectors of everyone we
    have seen — no labels. Its decision_function is positive for typical rows and
    negative for outliers; a logistic squashes that into a [0, 1] anomaly floor.
    Unfitted, it contributes 0 (no floor), never a spurious alarm."""

    def __init__(self, scale: float = 0.1) -> None:
        self._forest = None
        self._scale = scale

    @property
    def fitted(self) -> bool:
        return self._forest is not None

    def train(self, X: Sequence[Sequence[float]]) -> None:
        _require_ml()
        self._forest = IsolationForest(n_estimators=200, contamination="auto", random_state=0)
        self._forest.fit(np.asarray(X, dtype=float))

    def score(self, x: Sequence[float]) -> float:
        if not self.fitted:
            return 0.0
        df = float(self._forest.decision_function(np.asarray([x], dtype=float))[0])
        return 1.0 / (1.0 + math.exp(df / self._scale))  # df<0 (outlier) -> ->1

    def save(self, path: str) -> None:
        with open(path, "wb") as f:
            pickle.dump({"forest": self._forest, "scale": self._scale}, f)

    def load(self, path: str) -> None:
        _require_ml()
        with open(path, "rb") as f:
            blob = pickle.load(f)
        self._forest, self._scale = blob["forest"], blob["scale"]


def combine(calibrated: Optional[float], floor: float, baseline: float) -> float:
    """The one figure the engine deposits. The floor is a lower bound on whatever
    the labelled model says; before the model has labels, the Layer-1 aggregate
    stands in for it. Clamped to [0, 1] — it is a probability the clock multiplies
    by rupees."""
    upper = calibrated if calibrated is not None else baseline
    return max(0.0, min(1.0, max(upper, floor)))


