"""run — the top-level wiring: scenario in, live run, results out.

This is the one module that ties the three halves together for a real run: it
sources a Scenario (either a pre-built JSON or one freshly minted from the
generator), brings up the live split-process stack, drives the scenario through the
orchestrator against the live Kit, writes the results JSON, and tears the stack
down. It is deliberately thin — every hard part lives in a tested module (the
generator, the orchestrator, the oracle, the stack); this just sequences them and
owns the CLI surface and the file I/O.

Usage (module entrypoint)::

    python -m agentshield_driver --out results.json           # generate + run + score
    python -m agentshield_driver --scenario scn.json --out r.json
    python -m agentshield_driver --seed 12 --keep-stack        # a different world, stack left up

The generator is imported from its sibling services/generator at runtime (no
install), matching the driver's pure-stdlib, zero-wheels contract.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .orchestrator import Timings, run_scenario
from .stack import build_stack


def _load_generator() -> Any:
    """Put the sibling generator package on sys.path and return its build_scenario.
    Kept lazy so a --scenario run needs neither the generator nor its directory."""
    here = Path(__file__).resolve()
    repo_root = here.parents[3]  # services/driver/agentshield_driver/run.py -> repo root
    gen_root = repo_root / "services" / "generator"
    if str(gen_root) not in sys.path:
        sys.path.insert(0, str(gen_root))
    from agentshield_generator.generate import Config, build_scenario  # noqa: WPS433
    return Config, build_scenario


def load_scenario(*, path: Optional[str], seed: Optional[int]) -> Dict[str, Any]:
    """Source the scenario dict: a pre-built JSON if ``path`` is given, else a fresh
    one minted from the generator (optionally reseeded)."""
    if path:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    Config, build_scenario = _load_generator()
    cfg = Config(seed=seed) if seed is not None else Config()
    return build_scenario(cfg).to_dict()


def summarise(results: Dict[str, Any]) -> str:
    """A compact human summary of a results dict — the one thing printed to stdout so
    a run's outcome is legible without opening the JSON."""
    o = results["overall"]
    lines = [
        f"debits={o['debits']} evaluated={o['evaluated']} "
        f"decision_acc={o['decision_accuracy']} code_acc={o['code_accuracy']}",
    ]
    for fam in sorted(results.get("by_family", {})):
        s = results["by_family"][fam]
        lines.append(
            f"  {fam:<22} n={s['count']:<3} decision={s['decision_accuracy']} code={s['code_accuracy']}"
        )
    labs = results.get("labels", {})
    lines.append(f"labels: {labs.get('observed_count', 0)} observed {labs.get('observed_by_reason', {})}")
    warnings: List[str] = results.get("warnings", [])
    if warnings:
        lines.append(f"WARNINGS ({len(warnings)}):")
        lines.extend(f"  - {w}" for w in warnings)
    return "\n".join(lines)


def run(
    *,
    scenario_path: Optional[str] = None,
    seed: Optional[int] = None,
    out_path: Optional[str] = None,
    keep_stack: bool = False,
    timings: Optional[Timings] = None,
    log: Callable[[str], None] = print,
) -> Dict[str, Any]:
    """Source a scenario, bring the stack up, replay + score it, write the results,
    and tear the stack down. Returns the results dict."""
    scenario = load_scenario(path=scenario_path, seed=seed)
    meta = scenario.get("meta", {})
    log(f"scenario: {meta.get('totals', {})} seed={meta.get('seed')}")

    stack = build_stack(log=log, keep_stack=keep_stack)
    started = time.time()
    with stack:
        results = run_scenario(stack.kit, scenario, timings=timings, log=log)
    results["run_seconds"] = round(time.time() - started, 2)

    if out_path:
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(results, fh, indent=2, sort_keys=False)
        log(f"results written to {out_path}")
    log(summarise(results))
    return results


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="agentshield_driver",
        description="Replay a generated Scenario against the live AgentShield and score it.",
    )
    src = parser.add_mutually_exclusive_group()
    src.add_argument("--scenario", help="path to a pre-built scenario JSON (else one is generated)")
    src.add_argument("--seed", type=int, help="generator seed for a fresh scenario")
    parser.add_argument("--out", default="results.json", help="where to write the results JSON")
    parser.add_argument("--keep-stack", action="store_true", help="leave the docker backends up after the run")
    args = parser.parse_args(argv)

    try:
        results = run(
            scenario_path=args.scenario, seed=args.seed,
            out_path=args.out, keep_stack=args.keep_stack,
        )
    except Exception as exc:  # a run failure is a non-zero exit, with the reason
        print(f"driver: run failed: {exc}", file=sys.stderr)
        return 1
    # A perfect-decision run exits 0; a degraded one still writes results but flags it.
    return 0 if results["overall"]["evaluated"] == results["overall"]["debits"] else 2
