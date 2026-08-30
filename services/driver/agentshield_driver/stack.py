"""stack — bring up the live split-process AgentShield and hand back a live Kit.

This is the infra half of the driver's runtime. It owns the processes the
orchestrator drives against but never itself touches: the docker-compose backends
(Redis, Redpanda, Postgres), the two split-process Go binaries (the off-clock
worker host and the on-clock decision service), and the Go driverkit subprocess the
:class:`~agentshield_driver.kit.Kit` speaks NDJSON to.

Why a clean slate each run: the driverkit's labels collector subscribes with a
unique consumer group and franz-go defaults to the earliest offset, so a run over
dirty topics would re-read prior runs' settled labels. :meth:`Stack.start` tears the
volumes down and brings them back up with ``--wait`` (healthchecks green) so every
run replays against empty topics and empty stores — reproducible from the seed alone.

The split wiring mirrors the product exactly: the decision service runs in dev mode
(no TLS -> a fixed caller identity, which P5 accepts) with the interruption cost and
staleness budget the scenario's numerics assume; the worker host runs the four
off-clock consumers (stream-processor, materialiser, reputation-builder, labeler).
Both share only Redis and the bus — never a call — which is the whole two-plane point.
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional

from .kit import Kit

# Host-facing endpoints the docker-compose stack publishes (see deploy/docker-compose.yml).
REDIS_ADDR = "localhost:6379"
KAFKA_SEEDS = "localhost:19092"
POSTGRES_DSN = "postgres://agentshield:agentshield@localhost:5432/agentshield?sslmode=disable"
DECISION_ADDR = "localhost:8443"

# The numerics the generator's families assume (see the orchestrator pre-warm): the
# interruption cost the soft/graph step-ups must clear, and a disabled staleness
# budget so a pre-warmed row is trusted however old its logical timestamp.
INTERRUPTION_COST_PAISE = 100000
STALENESS_BUDGET_SECONDS = 0

# Readiness markers the two hosts log once wired (Go's log package writes to stderr).
_WORKER_READY = "off-clock plane running"
_DECISION_READY = "listening on"


def _find_repo_root(start: Path) -> Path:
    """Walk up from this file until the Go module root (the dir holding go.mod)."""
    for p in [start, *start.parents]:
        if (p / "go.mod").is_file():
            return p
    raise RuntimeError("could not locate the Go module root (go.mod) above " + str(start))


class _LogPump(threading.Thread):
    """Drain a process's stderr forever, keep the last lines, and flag a marker.

    A Go host logs continuously; if nothing reads its stderr pipe it eventually
    blocks. This daemon thread reads every line (so the pipe never fills), keeps a
    bounded tail for diagnostics, and sets an event the first time ``marker`` is
    seen, so :meth:`Stack._await` can block on readiness without racing the reader."""

    def __init__(self, name: str, stream, marker: str, echo: Callable[[str], None]):
        super().__init__(name=f"logpump-{name}", daemon=True)
        self._stream = stream
        self._marker = marker
        self._echo = echo
        self._name = name
        self.ready = threading.Event()
        self.tail: List[str] = []

    def run(self) -> None:
        for raw in iter(self._stream.readline, ""):
            line = raw.rstrip("\n")
            self.tail.append(line)
            if len(self.tail) > 200:
                self.tail.pop(0)
            self._echo(f"[{self._name}] {line}")
            if self._marker and self._marker in line:
                self.ready.set()
        self._stream.close()


@dataclass
class Stack:
    """The live infra a run needs, and its lifecycle. Build one, :meth:`start` it,
    drive :attr:`kit` through the orchestrator, then :meth:`stop`. Also a context
    manager, so ``with Stack(...) as s:`` guarantees teardown even on failure."""

    repo_root: Path
    compose_file: Path
    log: Callable[[str], None] = print
    keep_stack: bool = False           # leave the docker volumes up after stop (for debugging)
    startup_timeout: float = 90.0

    kit: Optional[Kit] = None
    _procs: Dict[str, subprocess.Popen] = field(default_factory=dict)
    _pumps: Dict[str, _LogPump] = field(default_factory=dict)

    # --- lifecycle ---------------------------------------------------------
    def __enter__(self) -> "Stack":
        self.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop()

    def start(self) -> Kit:
        """Clean-slate the backends, build the binaries, launch the two hosts, and
        spawn the driverkit. Returns the live Kit (also stored on :attr:`kit`)."""
        self._compose("down", "-v")
        self.log("stack: bringing up Redis + Redpanda + Postgres (clean volumes)")
        # --wait only the long-running, healthchecked backends: it treats any
        # waited-on container that *exits* as a failure, and topic-init is a
        # one-shot that exits 0. So bring the durable trio up and block on their
        # healthchecks, then run the one-shot separately and wait for it to finish.
        self._compose("up", "-d", "--wait", "redpanda", "redis", "postgres")
        self.log("stack: creating topics (topic-init)")
        self._compose("up", "-d", "topic-init")
        self._compose("wait", "topic-init")  # block until topics exist (exits 0)

        bindir = self._build_binaries()

        env = os.environ.copy()
        env.update(REDIS_ADDR=REDIS_ADDR, KAFKA_SEEDS=KAFKA_SEEDS)

        # Off-clock host: the four workers, durable CHAIN + VAULT on Postgres.
        worker_env = dict(env, POSTGRES_DSN=POSTGRES_DSN)
        self._spawn("worker", [str(bindir / "worker")], worker_env, _WORKER_READY)

        # On-clock host: dev mode (no TLS -> fixed identity), the scenario's numerics.
        decision_env = dict(
            env,
            AGENTSHIELD_INTERRUPTION_COST_PAISE=str(INTERRUPTION_COST_PAISE),
            AGENTSHIELD_STALENESS_BUDGET_SECONDS=str(STALENESS_BUDGET_SECONDS),
            AGENTSHIELD_ADDR=":8443",
        )
        self._spawn("decision", [str(bindir / "decision")], decision_env, _DECISION_READY)

        # The driverkit talks to all three: gRPC decision, Redis stores, the bus.
        dk_env = dict(env, DECISION_ADDR=DECISION_ADDR)
        proc = subprocess.Popen(
            [str(bindir / "driverkit")], cwd=str(self.repo_root), env=dk_env,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1,
        )
        self._procs["driverkit"] = proc
        # Drain the driverkit's stderr (its stdout is the Kit's NDJSON channel).
        pump = _LogPump("driverkit", proc.stderr, "", self.log)
        pump.start()
        self._pumps["driverkit"] = pump

        self.kit = Kit(proc)
        self.kit.ping()  # blocks until the driverkit has dialled every backend
        self.log("stack: driverkit up — all backends dialled")
        return self.kit

    def stop(self) -> None:
        """Tear everything down in reverse: close the driverkit's stdin (it exits its
        read loop), terminate the two hosts, then drop the docker stack unless the
        caller asked to keep it."""
        if self.kit is not None:
            try:
                self.kit.close()
            except Exception:
                pass
        for name in ("driverkit", "decision", "worker"):
            self._terminate(name)
        if not self.keep_stack:
            self._compose("down", "-v")
        else:
            self.log("stack: leaving docker backends up (keep_stack=True)")

    # --- internals ---------------------------------------------------------
    def _compose(self, *args: str) -> None:
        """Run one docker compose subcommand against the stack's compose file."""
        cmd = ["docker", "compose", "-f", str(self.compose_file), *args]
        subprocess.run(cmd, cwd=str(self.repo_root), check=True)

    def _build_binaries(self) -> Path:
        """Compile the three Go binaries into a per-run bin dir. Building through the
        product's own packages is what keeps the driver contract-bound — the exact
        gRPC client, store adapters and bus builders the product ships."""
        bindir = self.repo_root / "bin"
        bindir.mkdir(exist_ok=True)
        for name in ("worker", "decision", "driverkit"):
            self.log(f"stack: building {name}")
            subprocess.run(
                ["go", "build", "-o", str(bindir / name), f"./cmd/{name}"],
                cwd=str(self.repo_root), check=True,
            )
        return bindir

    def _spawn(self, name: str, argv: List[str], env: Dict[str, str], marker: str) -> None:
        """Launch a host process, pump its stderr, and block until its readiness
        marker is logged (or raise on timeout / early exit)."""
        proc = subprocess.Popen(
            argv, cwd=str(self.repo_root), env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, bufsize=1,
        )
        self._procs[name] = proc
        pump = _LogPump(name, proc.stderr, marker, self.log)
        pump.start()
        self._pumps[name] = pump
        self._await(name, proc, pump)

    def _await(self, name: str, proc: subprocess.Popen, pump: _LogPump) -> None:
        deadline = time.time() + self.startup_timeout
        while time.time() < deadline:
            if pump.ready.wait(timeout=0.2):
                self.log(f"stack: {name} ready")
                return
            if proc.poll() is not None:
                tail = "\n".join(pump.tail[-20:])
                raise RuntimeError(f"{name} exited early (code {proc.returncode}):\n{tail}")
        tail = "\n".join(pump.tail[-20:])
        raise TimeoutError(f"{name} not ready within {self.startup_timeout}s:\n{tail}")

    def _terminate(self, name: str) -> None:
        proc = self._procs.get(name)
        if proc is None or proc.poll() is not None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def build_stack(*, log: Callable[[str], None] = print, keep_stack: bool = False) -> Stack:
    """Construct a :class:`Stack` rooted at the repo that contains this package,
    pointed at the standard deploy/docker-compose.yml."""
    repo_root = _find_repo_root(Path(__file__).resolve())
    compose_file = repo_root / "deploy" / "docker-compose.yml"
    return Stack(repo_root=repo_root, compose_file=compose_file, log=log, keep_stack=keep_stack)
