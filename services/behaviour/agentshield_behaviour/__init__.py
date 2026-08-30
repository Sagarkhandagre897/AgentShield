"""agentshield_behaviour — the behaviour engine (System Design §11).

A three-layer streaming pipeline on the off-clock plane that answers one question
— is this principal acting unlike itself? — and leaves one calibrated
behaviour_deviation on the shelf for the request to read by key:

    Layer 1  baselines  — per-principal EWMA/robust-z, rolling quantiles (P²),
                          velocity, and sketch-based distinct counts (HLL /
                          Count-Min), shrunk toward a prior on cold start.
    Layer 2  model      — a LightGBM scorer + isotonic calibration → a probability.
    Layer 3  model      — an isolation-forest floor beneath the labels; the figure
                          is max(calibrated, floor).

Layer 1 is pure stdlib; Layers 2-3 import the ML stack lazily, so the baselines
and their tests run without the wheels installed.
"""

from .baselines import BaselineBank, PrincipalState, Signal, baseline_deviation
from .engine import BehaviourEngine, Deposit
from .model import BehaviourScorer, IsolationFloor, available, combine

__all__ = [
    "BaselineBank",
    "PrincipalState",
    "Signal",
    "baseline_deviation",
    "BehaviourEngine",
    "Deposit",
    "BehaviourScorer",
    "IsolationFloor",
    "available",
    "combine",
]
