"""python -m agentshield_generator — dump a synthetic scenario as JSON.

The scenario is written to stdout (or ``--out FILE``), ready for the Phase-7 live
driver to seed and replay. ``--summary`` prints just the meta block (seed, totals,
per-family counts, legend) so you can eyeball coverage without paging the whole
timeline.

    python -m agentshield_generator --seed 7 --out scenario.json
    python -m agentshield_generator --summary
"""

from __future__ import annotations

import json
from typing import List, Optional

from .generate import Config, build_scenario


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(
        prog="python -m agentshield_generator",
        description="AgentShield synthetic scenario generator (Phase 7) — legit + labelled misuse + graph structures.",
    )
    ap.add_argument("--seed", type=int, default=Config.seed, help="PRNG seed (reproducible; default %(default)s)")
    ap.add_argument("--base-ts", type=int, default=Config.base_ts, help="scenario logical t0, epoch seconds")
    ap.add_argument("--out", default="-", help="output file, or '-' for stdout (default)")
    ap.add_argument("--indent", type=int, default=2, help="JSON indent (default %(default)s; 0 for compact)")
    ap.add_argument("--summary", action="store_true", help="print only the meta block, not the full scenario")
    args = ap.parse_args(argv)

    scenario = build_scenario(Config(seed=args.seed, base_ts=args.base_ts))
    indent = args.indent or None

    if args.summary:
        text = json.dumps(scenario.meta, indent=indent, sort_keys=False)
    else:
        text = scenario.to_json(indent=indent)

    if args.out == "-":
        print(text, flush=True)
    else:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.write("\n")
        # A one-line receipt to stderr-free stdout is noisy for piping; keep it terse.
        print(f"wrote {args.out}: {scenario.meta['totals']['debits']} debits, "
              f"{scenario.meta['totals']['tokens']} tokens", flush=True)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
