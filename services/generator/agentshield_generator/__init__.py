"""agentshield_generator — the synthetic scenario generator (Phase 7, component 1).

A generator run is pure, deterministic data: ``build_scenario(Config(seed=...))``
returns a :class:`Scenario` — mandates, sealed intent envelopes, a timeline of debit
OrderContexts, and settled-outcome recipes — describing a world the live driver
(component 2) will replay against the real decision service and bus.

The generator decides nothing and labels nothing. It tags each debit with the
family it belongs to and the verdict that family is *meant* to earn, and it records
what the world does in reaction (a capture, a chargeback, a pulled mandate). The
verdict is the live system's to reach; the training label is the Go labeler's to
distil from those settled outcomes (§6). The tags here are ground truth for scoring
the run, never an input the system trains on.

Public surface:
    build_scenario, Config, Generator — synthesis
    Scenario, Token, Overlay, SealedEnvelope, Debit, Settlement — the data model
    families — the family / verdict / label vocabulary and the LEGEND
    envelope_digest, normalise_purpose — the intent-parity digest helpers
"""

from __future__ import annotations

from . import families
from .families import LEGEND, ALL_FAMILIES, MISUSE_FAMILIES, FamilyInfo
from .generate import Config, Generator, build_scenario
from .scenario import (
    Debit,
    Overlay,
    Scenario,
    SealedEnvelope,
    Settlement,
    Token,
    envelope_digest,
    normalise_purpose,
)

__all__ = [
    "build_scenario",
    "Config",
    "Generator",
    "Scenario",
    "Token",
    "Overlay",
    "SealedEnvelope",
    "Debit",
    "Settlement",
    "envelope_digest",
    "normalise_purpose",
    "families",
    "LEGEND",
    "ALL_FAMILIES",
    "MISUSE_FAMILIES",
    "FamilyInfo",
]

__version__ = "0.1.0"
