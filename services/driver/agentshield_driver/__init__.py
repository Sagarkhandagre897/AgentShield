"""agentshield_driver — the Python orchestrator half of the Phase-7 live driver.

The driver replays a generated Scenario against the *live* AgentShield system and
scores what the real system did against the generator's ground truth. It never
decides or labels anything itself: it shells out to the contract-bound Go driverkit
(``cmd/driverkit``) for every action that touches a real contract (seed, seal,
evaluate, capture, dispute, cancel, deposit a feature, read block/feature state,
drain labels), so those actions reuse the exact generated bindings, store schemas
and bus builders the product runs with — zero stub drift.

Two halves, one contract (see the driverkit package doc):

  * the Go driverkit owns the I/O — it speaks newline-delimited JSON on stdin/stdout
    and turns each op into a real gRPC call, store write or bus publish;
  * this Python orchestrator owns the *choreography* — the timeline order, the
    barrier logic (let an off-clock fold land before a debit that depends on it),
    the settlement phasing (defer step-up confirmations past the decision.made that
    arms the labeler), and the oracle scoring.

Public surface:

  * :class:`~agentshield_driver.kit.Kit` — the live NDJSON client over the driverkit
    subprocess, and :class:`~agentshield_driver.kit.BaseKit`, the typed-op base both
    the live kit and the test :class:`~agentshield_driver.fakekit.FakeKit` share.
  * :func:`~agentshield_driver.orchestrator.run_scenario` — the phase machine that
    drives a scenario dict through a Kit and returns a results dict.
  * :mod:`~agentshield_driver.oracle` — verdict/label scoring against ground truth.
  * :mod:`~agentshield_driver.run` — the top-level wiring that brings up the stack,
    launches the split-process binaries + the driverkit, runs the orchestrator and
    writes the results JSON.
"""

from __future__ import annotations

__all__ = [
    "BaseKit",
    "Kit",
    "KitError",
    "FakeKit",
    "Timings",
    "run_scenario",
    "score_run",
    "Stack",
    "build_stack",
    "run",
]

from .kit import BaseKit, Kit, KitError
from .fakekit import FakeKit
from .oracle import score_run
from .orchestrator import Timings, run_scenario
from .stack import Stack, build_stack
from .run import run
