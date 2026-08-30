"""test_orchestrator — the phase machine end-to-end against the in-memory FakeKit.

No Redis, Redpanda or gRPC: the :class:`FakeKit` replays the real decision path and
labeler semantics in memory, so these tests exercise the actual choreography — the
seed/seal/pre-warm ordering, the timeline barriers, the deferred step-up captures,
the disputes and cancellations — and score it with the real oracle. A faithful fake
plus a correct pre-warm should reproduce every family's intended verdict, so the run
scores 100%; anything less is an orchestration bug, not a model miss.
"""

from __future__ import annotations

import pytest

from agentshield_driver.fakekit import FakeKit
from agentshield_driver.orchestrator import Timings, run_scenario

# Silence the settle sleeps and shorten the poll windows — the fake settles
# synchronously, so barriers resolve on the first poll and no real waiting is needed.
FAST = Timings(
    feature_poll_timeout=2.0, barrier_poll_timeout=2.0, poll_interval=0.0,
    settle_after_evaluate=0.0, settle_after_capture=0.0, settle_after_settlement=0.0,
    labels_timeout_ms=0,
)


@pytest.fixture(scope="module")
def scenario():
    from agentshield_generator.generate import build_scenario
    return build_scenario().to_dict()


def _quiet(_msg):  # swallow the phase narration in tests
    pass


def test_full_run_scores_perfectly(scenario):
    """A faithful fake + the standard pre-warm reproduces every family's intended
    verdict AND code, so the whole run is exact."""
    res = run_scenario(FakeKit(), scenario, timings=FAST, log=_quiet)

    assert res["warnings"] == [], f"unexpected barrier warnings: {res['warnings']}"
    assert res["overall"]["evaluated"] == res["overall"]["debits"]
    assert res["overall"]["decision_accuracy"] == 1.0, res["by_family"]
    assert res["overall"]["code_accuracy"] == 1.0, res["by_family"]


def test_every_family_is_perfect(scenario):
    """Each family — deterministic-spine, soft-learned and graph-structural — lands
    its expected decision, so no single pattern is silently missed."""
    res = run_scenario(FakeKit(), scenario, timings=FAST, log=_quiet)
    for family, stats in res["by_family"].items():
        assert stats["decision_accuracy"] == 1.0, (family, stats)
        assert stats["code_accuracy"] == 1.0, (family, stats)


def test_labels_cover_the_three_settled_reasons(scenario):
    """The run produces all three settled-label reasons: confirmed step-ups (LEGIT),
    disputes (MISUSE) and cancellations (MISUSE) — the outcomes the labeler distils."""
    res = run_scenario(FakeKit(), scenario, timings=FAST, log=_quiet)
    by_reason = res["labels"]["observed_by_reason"]
    assert by_reason.get("confirmed_step_up", 0) > 0
    assert by_reason.get("dispute", 0) > 0
    assert by_reason.get("cancellation", 0) > 0
    # Every cancelled mandate settles a MISUSE label.
    assert by_reason["cancellation"] == len(scenario["cancellations"])


def test_replay_barrier_blocks_the_duplicate():
    """A focused replay: the first (allowed) debit's capture spends the nonce; the
    barrier makes the driver wait for that spend to fold before the replay, which the
    predicate spine then blocks as a duplicate."""
    from agentshield_generator.generate import Config, Generator

    g = Generator(Config())
    g.add_replay()
    scn = g.build().to_dict()

    kit = FakeKit()
    res = run_scenario(kit, scn, timings=FAST, log=_quiet)

    rows = {r["family"]: r for r in res["per_debit"]}
    assert rows["legit"]["actual_decision"] == "ALLOW"
    assert rows["replay"]["actual_decision"] == "BLOCK"
    assert rows["replay"]["actual_code"] == "BLOCKED_DUPLICATE"
    assert res["warnings"] == []


def test_bustout_barrier_folds_consumption():
    """A focused bust-out: the early legs allow-and-capture, draining the day's room;
    the crossing legs carry a barrier and, once the prior captures have folded into
    consumption, step up on the per-day cap."""
    from agentshield_generator.generate import Config, Generator

    g = Generator(Config())
    g.add_velocity_bustout()
    scn = g.build().to_dict()

    res = run_scenario(FakeKit(), scn, timings=FAST, log=_quiet)
    decisions = [r["actual_decision"] for r in res["per_debit"]]
    # First legs allow, later legs step up once the day's room is drained.
    assert decisions[0] == "ALLOW"
    assert decisions[-1] == "STEP_UP"
    assert res["overall"]["decision_accuracy"] == 1.0
    assert res["warnings"] == []


def test_seal_requires_a_token_binding(scenario):
    """seal_envelope needs a non-empty token_id (the stream-processor drops a seal
    keyed on empty); the driver binds each session to its first debit's token, so
    every sealed envelope has one."""
    kit = FakeKit()
    run_scenario(kit, scenario, timings=FAST, log=_quiet)
    assert kit.seals, "no envelopes were sealed"
    for rec in kit.seals.values():
        assert rec["token_id"], "an envelope was sealed with an empty token_id"
