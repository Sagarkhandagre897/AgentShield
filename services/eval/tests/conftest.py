"""Test wiring: put the eval package (and, for the integration test, the driver +
generator siblings) on sys.path so the suite runs from anywhere without an install."""

from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve()
_EVAL = _HERE.parents[1]                    # services/eval
_SERVICES = _EVAL.parent                    # services/
for p in (_EVAL, _SERVICES / "driver", _SERVICES / "generator"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))
