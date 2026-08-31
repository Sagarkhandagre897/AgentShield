"""latency_test — measure the on-clock Evaluate latency of an ALREADY-RUNNING
AgentShield decision service, on real transport.

Like ``demo/live_test.py``, this assumes the split-process stack is already up and
LEFT up (docker infra + cmd/worker + cmd/decision) — see the "Run it live" section
of the top-level README. It does the smallest amount of work needed to get one
legit request onto the full ALLOW path, then hands the timing loop to the Go
``cmd/latencyprobe`` binary:

  1. build the deterministic seed-7 world and PRIME the live system with it —
     seed the mandates, seal the envelopes, and deposit + await the engine
     stand-in features (reusing the exact orchestrator phases live_test drives),
     so a legit debit reads a full, non-degraded feature row and ALLOWs;
  2. dump one representative legit-ALLOW OrderContext to a JSON file;
  3. run cmd/latencyprobe, which dials the live gRPC and times N sequential
     Evaluate calls — each with a fresh evaluation_id + nonce so it stays a
     first-seen ALLOW — and prints the min / mean / p50 / p90 / p95 / p99 / max.

The measured number is the pure on-clock RPC round-trip over loopback: caller ->
gRPC -> resolve token/block/overlay (Redis) -> P1–P6 -> read features (Redis) ->
score -> decide -> reply. The decision.made publish is fire-and-forget off the
critical path, so it is not on the clock this measures. Nothing is torn down.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO / "services" / "driver"),
                str(_REPO / "services" / "generator"),
                str(_REPO / "services" / "shared")]

from agentshield_driver.kit import Kit                                    # noqa: E402
from agentshield_driver.orchestrator import (                            # noqa: E402
    RunLog, Timings, _order_context, _prewarm, _seal, _seed,
)
from agentshield_generator.generate import Config, build_scenario         # noqa: E402


def hr(title: str) -> None:
    print(f"\n{'=' * 74}\n{title}\n{'=' * 74}")


def ensure_bins(repo: Path) -> tuple[Path, Path]:
    """Build cmd/driverkit and cmd/latencyprobe into ./bin if absent (no-op once built)."""
    bindir = repo / "bin"
    bindir.mkdir(exist_ok=True)
    out = []
    for name in ("driverkit", "latencyprobe"):
        p = bindir / name
        if not p.exists():
            subprocess.run(["go", "build", "-o", str(p), f"./cmd/{name}"],
                           cwd=str(repo), check=True)
        out.append(p)
    return out[0], out[1]


def pick_legit_allow(scenario: dict) -> dict:
    """The first legit debit the generator expects to ALLOW — the representative
    happy-path request whose latency we measure."""
    for d in scenario["timeline"]:
        if d["family"] == "legit" and d["expected_decision"] == "ALLOW":
            return d
    raise SystemExit("no legit ALLOW debit in the scenario")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="latency_test", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed", type=int, default=7, help="generator seed (default 7)")
    ap.add_argument("--n", type=int, default=2000, help="measured Evaluate calls")
    ap.add_argument("--warmup", type=int, default=200, help="warmup calls (discarded)")
    ap.add_argument("--redis", default="localhost:6379")
    ap.add_argument("--kafka", default="localhost:19092")
    ap.add_argument("--decision", default="localhost:8443")
    args = ap.parse_args(argv)

    scenario = build_scenario(Config(seed=args.seed)).to_dict()
    print(f"scenario seed={args.seed} totals={scenario['meta'].get('totals')}")

    driverkit, probe = ensure_bins(_REPO)

    # Prime the live world so a legit request reads a full, non-degraded feature row
    # and ALLOWs — reusing the exact seed / seal / pre-warm phases live_test drives.
    env = dict(os.environ, REDIS_ADDR=args.redis, KAFKA_SEEDS=args.kafka, DECISION_ADDR=args.decision)
    proc = subprocess.Popen([str(driverkit)], cwd=str(_REPO), env=env,
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, text=True, bufsize=1)
    kit = Kit(proc)
    try:
        hr("PRIME — seeding + sealing + pre-warming the live world (so a legit request ALLOWs)")
        kit.ping()
        _seed(kit, scenario, log=lambda m: print(f"  {m}"))
        _seal(kit, scenario, log=lambda m: print(f"  {m}"))
        _prewarm(kit, scenario, Timings(), RunLog(), log=lambda m: print(f"  {m}"))
    finally:
        kit.close()  # priming done; the probe dials the decision gRPC directly

    debit = pick_legit_allow(scenario)
    order = _order_context(debit)
    with tempfile.NamedTemporaryFile("w", suffix=".json", prefix="ashield-order-",
                                     delete=False) as f:
        json.dump(order, f)
        order_path = f.name
    print(f"  representative request: {debit['evaluation_id']} "
          f"(family={debit['family']}, {order['amount_paise']} paise) -> {order_path}")

    hr("MEASURE — timing N sequential Evaluate calls over the live gRPC")
    r = subprocess.run(
        [str(probe), "--decision", args.decision, "--order", order_path,
         "--n", str(args.n), "--warmup", str(args.warmup)],
        cwd=str(_REPO))

    hr("DONE — stack + hosts LEFT RUNNING (tear down when you're ready)")
    print("  docker infra + cmd/decision + cmd/worker are still up.")
    print(f"  (the dumped request lingers at {order_path} — re-run the probe against it anytime)")
    return r.returncode


if __name__ == "__main__":
    sys.exit(main())
