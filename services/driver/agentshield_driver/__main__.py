"""Module entrypoint: ``python -m agentshield_driver`` runs a live scenario.

Thin shim over :func:`agentshield_driver.run.main` so the package is runnable
directly — bring up the stack, replay a generated (or loaded) scenario against the
live system, score it, and write the results JSON."""

from __future__ import annotations

import sys

from .run import main

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
