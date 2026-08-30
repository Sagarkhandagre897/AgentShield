"""conftest — put the driver package and its sibling generator on the import path.

The driver is pure stdlib and shells out to the Go driverkit at runtime, so nothing
is pip-installed. The tests here need two things importable without installation:
the driver package itself (``agentshield_driver``) and the generator that mints the
scenarios it replays (``agentshield_generator``, in the sibling services/generator).
Both are added to ``sys.path`` here so ``pytest`` finds them from a clean checkout.
"""

from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve()
_DRIVER_ROOT = _HERE.parents[1]          # services/driver
_GENERATOR_ROOT = _HERE.parents[2] / "generator"  # services/generator

for p in (_DRIVER_ROOT, _GENERATOR_ROOT):
    s = str(p)
    if s not in sys.path:
        sys.path.insert(0, s)
