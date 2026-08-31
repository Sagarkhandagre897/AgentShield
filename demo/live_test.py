"""live_test — drive real traffic through an ALREADY-RUNNING AgentShield and watch
the verdicts land.

Unlike ``python -m agentshield_driver`` (which owns the whole lifecycle — it brings
the docker backends and the two Go hosts up, runs, and tears everything back down),
this script assumes the split-process stack is ALREADY UP and LEFT UP:

    docker infra (Redpanda + Redis + Postgres) + cmd/worker + cmd/decision

so you can watch the containers in Docker Desktop while it runs and poke at the
stores afterwards. It only spawns the contract-bound ``cmd/driverkit`` (the same
NDJSON arm the eval harness uses), replays a generated scenario against the live
system through the tested orchestrator, and prints:

  * the on-clock verdict spread (ALLOW / STEP_UP / BLOCK) as it happens,
  * a marquee table of representative verdicts read back from the durable CHAIN
    (Postgres ``provenance``) — one per verdict class, incl. a P1 replay BLOCK,
  * the settled training labels the off-clock labeler produced.

See the "Run it live" section of the top-level README for the three commands that
bring the stack up first. Nothing here is torn down — teardown stays your call.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

# The three sibling Python packages (pure-stdlib, no install) — the driver harness,
# the scenario generator, and the shared wire contract — resolved off the repo root.
_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO / "services" / "driver"),
                str(_REPO / "services" / "generator"),
                str(_REPO / "services" / "shared")]

from agentshield_driver.kit import Kit                       # noqa: E402
from agentshield_driver.orchestrator import run_scenario     # noqa: E402
from agentshield_driver.run import summarise                 # noqa: E402
from agentshield_generator.generate import Config, build_scenario  # noqa: E402

def hr(title: str) -> None:
    print(f"\n{'=' * 74}\n{title}\n{'=' * 74}")


def ensure_driverkit(repo: Path) -> Path:
    """Build cmd/driverkit into ./bin if it isn't there yet (a no-op once built)."""
    binpath = repo / "bin" / "driverkit"
    if not binpath.exists():
        binpath.parent.mkdir(exist_ok=True)
        subprocess.run(["go", "build", "-o", str(binpath), "./cmd/driverkit"],
                       cwd=str(repo), check=True)
    return binpath


def chain_verdict(container: str, evaluation_id: str) -> str:
    """Read one decision back from the durable CHAIN (Postgres provenance) via the
    compose container. Returns 'decision|code' or '' if unavailable (e.g. the worker
    was started without POSTGRES_DSN, or a non-standard container name)."""
    q = f"SELECT decision || '|' || code FROM provenance WHERE evaluation_id='{evaluation_id}'"
    try:
        r = subprocess.run(["docker", "exec", container, "psql", "-U", "agentshield",
                            "-d", "agentshield", "-tAc", q], capture_output=True, text=True)
        return r.stdout.strip() if r.returncode == 0 else ""
    except Exception:
        return ""


def marquee(scenario: dict, container: str) -> None:
    """Pick one representative debit per verdict class straight from the scenario's
    ground truth, then read what the live system actually decided back off the CHAIN
    — so a P1 replay BLOCK and a graph STEP_UP are legible on their own, not just as
    an aggregate accuracy number."""
    tl = scenario["timeline"]

    def pick(pred):
        return next((d for d in tl if pred(d)), None)

    picks = [
        ("legit ALLOW", pick(lambda d: d["family"] == "legit" and d["expected_decision"] == "ALLOW")),
        ("intent-drift STEP_UP", pick(lambda d: d["family"] == "intent_drift")),
        ("scope-overrun STEP_UP", pick(lambda d: d["family"] == "scope_overrun")),
        ("graph-ring STEP_UP", pick(lambda d: d["family"] in ("mule_fan_in", "shared_device_ring", "synchronised_fleet"))),
        ("replay BLOCK (P1)", pick(lambda d: d["family"] == "replay")),
        ("revoked-token BLOCK", pick(lambda d: d["family"] == "stale_revoked_token")),
    ]
    hr("MARQUEE — representative verdicts, read back from the durable CHAIN")
    print(f"  {'what':<24}{'eval':<12}{'expected':<26}{'live (CHAIN)':<26}{'ok'}")
    for label, d in picks:
        if d is None:
            continue
        exp = f"{d['expected_decision']}/{d['expected_code']}"
        got = chain_verdict(container, d["evaluation_id"]) or "<not on chain>"
        ok = "✓" if got.replace("|", "/") == exp else ("·" if got == "<not on chain>" else "✗")
        print(f"  {label:<24}{d['evaluation_id']:<12}{exp:<26}{got.replace('|', '/'):<26}{ok}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="live_test", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed", type=int, default=7, help="generator seed (default 7)")
    ap.add_argument("--redis", default="localhost:6379")
    ap.add_argument("--kafka", default="localhost:19092")
    ap.add_argument("--decision", default="localhost:8443")
    ap.add_argument("--pg-container", default="agentshield-postgres",
                    help="compose container to read the CHAIN back from for the marquee table")
    args = ap.parse_args(argv)

    scenario = build_scenario(Config(seed=args.seed)).to_dict()
    print(f"scenario seed={args.seed} totals={scenario['meta'].get('totals')}")

    binpath = ensure_driverkit(_REPO)
    import os
    env = dict(os.environ, REDIS_ADDR=args.redis, KAFKA_SEEDS=args.kafka, DECISION_ADDR=args.decision)
    proc = subprocess.Popen([str(binpath)], cwd=str(_REPO), env=env,
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, text=True, bufsize=1)
    kit = Kit(proc)
    try:
        hr("REPLAY — driving the generated world through the live gRPC + bus")
        results = run_scenario(kit, scenario, log=lambda m: print(f"  {m}"))
        hr("SCORE — live verdicts + settled labels vs the generator's ground truth")
        print(summarise(results))
        marquee(scenario, args.pg_container)
        hr("DONE — stack + hosts LEFT RUNNING (tear down when you're ready)")
        print("  docker infra + cmd/decision + cmd/worker are still up.")
        return 0 if results["overall"]["evaluated"] == results["overall"]["debits"] else 2
    finally:
        kit.close()  # closes only the driverkit subprocess; the stack stays up


if __name__ == "__main__":
    sys.exit(main())
