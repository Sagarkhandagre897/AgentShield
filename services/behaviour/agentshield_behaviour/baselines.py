"""baselines — Layer 1 of the behaviour engine (System Design §11).

For each principal the engine keeps a running sense of normal and, on every new
observation, reports how far that observation departs from it — one deviation per
signal, each carried with the number of observations it was computed from. §11:

    "EWMA and robust z-scores on amount, rolling quantiles, velocity, and
     sketch-based distinct counts ... On cold start ... the baseline is shrunk
     toward a peer or population prior rather than trusted thin."

The shrink is why a big z-score off three observations does not, by itself, flag a
principal: a signal's deviation is pulled toward the no-anomaly prior in
proportion to how little we have seen (n / (n + kappa)). The raw deviation and its
count are both reported, so a downstream aggregator can shrink differently if it
wants — the counts travel for exactly that reason.

Pure stdlib; the ML layers sit above this.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, List

from .sketches import CountMinSketch, HyperLogLog, P2Quantile

# Signal names — also the keys the deposited per-signal breakdown carries.
SIG_AMOUNT_Z = "amount_z"
SIG_AMOUNT_P95 = "amount_p95"
SIG_VELOCITY = "velocity"
SIG_MERCHANT_NOVELTY = "merchant_novelty"

_EWMA_ALPHA = 0.1        # baseline responsiveness
_MAD_TO_SIGMA = 1.2533   # E[|X-mu|] = sigma * sqrt(2/pi); invert for a robust sigma
_SHRINK_KAPPA = 20.0     # observations at which a signal is half-trusted
_EPS = 1e-9


def _squash(raw: float, scale: float) -> float:
    """Map a non-negative magnitude into [0, 1), monotone and saturating, so a
    z of 2 and a z of 20 are both 'anomalous' without one dominating the sum."""
    if raw <= 0:
        return 0.0
    return 1.0 - math.exp(-raw / scale)


def _shrink(deviation: float, n: int) -> float:
    """Pull a deviation toward the no-anomaly prior (0) by how little we've seen."""
    return deviation * (n / (n + _SHRINK_KAPPA))


@dataclass
class Signal:
    signal: str
    deviation: float  # squashed to [0, 1]
    obs_count: int


@dataclass
class PrincipalState:
    """The running baseline for one principal (an agent / token / customer id).
    Fixed memory: an EWMA pair, two P-square quantiles, and the two sketches."""

    key: str
    n_amount: int = 0
    ewma_mean: float = 0.0
    ewma_mad: float = 0.0
    p50: P2Quantile = field(default_factory=lambda: P2Quantile(0.50))
    p95: P2Quantile = field(default_factory=lambda: P2Quantile(0.95))
    last_ts: int = 0
    n_gap: int = 0
    ewma_gap: float = 0.0
    hll: HyperLogLog = field(default_factory=HyperLogLog)
    cms: CountMinSketch = field(default_factory=CountMinSketch)
    n_merchant: int = 0

    def _amount_z(self, amount: float) -> float:
        """Robust z of the amount against the prior baseline (measured before the
        update, so an observation is judged against the past, not itself)."""
        if self.n_amount == 0:
            return 0.0
        sigma = _MAD_TO_SIGMA * self.ewma_mad
        return abs(amount - self.ewma_mean) / (sigma + _EPS)

    def _update_amount(self, amount: float) -> None:
        if self.n_amount == 0:
            self.ewma_mean = amount
        else:
            diff = amount - self.ewma_mean
            self.ewma_mean += _EWMA_ALPHA * diff
            self.ewma_mad += _EWMA_ALPHA * (abs(diff) - self.ewma_mad)
        self.n_amount += 1
        self.p50.add(amount)
        self.p95.add(amount)

    def _velocity_dev(self, ts: int) -> float:
        """A burst reads as an inter-arrival gap far below the principal's usual
        gap. Only meaningful once a baseline gap exists."""
        if self.last_ts == 0 or self.n_gap == 0 or self.ewma_gap <= 0:
            return 0.0
        gap = max(0, ts - self.last_ts)
        return _squash(max(0.0, (self.ewma_gap - gap) / (self.ewma_gap + _EPS)), 0.5)

    def observe(self, amount: float, merchant: str, ts: int) -> List[Signal]:
        """Fold one observation and return the current per-signal deviations. The
        deviations are computed against the *prior* baseline, then the baseline is
        advanced — so a debit is always measured against the principal's history,
        never against itself."""
        signals: List[Signal] = []

        if amount is not None and amount > 0:
            z = self._amount_z(amount)
            over_p95 = max(0.0, (amount - self.p95.value()) / (abs(self.p95.value()) + _EPS))
            signals.append(Signal(SIG_AMOUNT_Z, _squash(z, 2.0), self.n_amount))
            signals.append(Signal(SIG_AMOUNT_P95, _squash(over_p95, 1.0), self.n_amount))
            self._update_amount(amount)

        if ts:
            signals.append(Signal(SIG_VELOCITY, self._velocity_dev(ts), self.n_gap))
            if self.last_ts:
                gap = max(0, ts - self.last_ts)
                self.ewma_gap = gap if self.n_gap == 0 else self.ewma_gap + _EWMA_ALPHA * (gap - self.ewma_gap)
                self.n_gap += 1
            self.last_ts = ts

        if merchant:
            freq = self.cms.estimate(merchant)
            novelty = 1.0 - freq / (self.n_merchant + 1)
            signals.append(Signal(SIG_MERCHANT_NOVELTY, max(0.0, novelty), self.n_merchant))
            self.hll.add(merchant)
            self.cms.add(merchant)
            self.n_merchant += 1

        return signals

    def feature_vector(self, signals: List[Signal]) -> List[float]:
        """The shrunk deviations plus volume context — the row the ML layers score.
        Missing signals read as 0 so the vector width is stable."""
        by_name = {s.signal: _shrink(s.deviation, s.obs_count) for s in signals}
        return [
            by_name.get(SIG_AMOUNT_Z, 0.0),
            by_name.get(SIG_AMOUNT_P95, 0.0),
            by_name.get(SIG_VELOCITY, 0.0),
            by_name.get(SIG_MERCHANT_NOVELTY, 0.0),
            math.log1p(self.n_amount),
            math.log1p(self.hll.count()),
        ]


FEATURE_NAMES = [SIG_AMOUNT_Z, SIG_AMOUNT_P95, SIG_VELOCITY, SIG_MERCHANT_NOVELTY, "log_n", "log_distinct_merchants"]


def baseline_deviation(signals: List[Signal]) -> float:
    """The cold-start aggregate: a noisy-OR of the shrunk per-signal deviations —
    any one strong, well-supported signal lifts the figure, and thin signals
    barely move it. This is what stands in until the calibrated model has labels."""
    prod = 1.0
    for s in signals:
        prod *= 1.0 - _shrink(s.deviation, s.obs_count)
    return 1.0 - prod


class BaselineBank:
    """All principals' Layer-1 state. One dict; each state is fixed-size, so the
    bank grows linearly in principals seen, not in their volume."""

    def __init__(self) -> None:
        self._states: Dict[str, PrincipalState] = {}

    def observe(self, key: str, amount: float, merchant: str, ts: int):
        st = self._states.get(key)
        if st is None:
            st = self._states[key] = PrincipalState(key=key)
        signals = st.observe(amount, merchant, ts)
        return signals, st.feature_vector(signals)

    def __len__(self) -> int:
        return len(self._states)


