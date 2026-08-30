"""kit — the typed op surface and the live NDJSON transport to the Go driverkit.

:class:`BaseKit` defines every AgentShield op as a typed method that builds the
flat request the driverkit reads and returns the parsed response dict. It is
transport-agnostic: subclasses implement ``_send`` (one request in, one response
out). Two implement it — :class:`Kit`, which speaks newline-delimited JSON over the
long-lived driverkit subprocess, and the test ``FakeKit``, which simulates the
backends in-memory so the orchestration logic is unit-testable with no infra.

The request field names match the Go ``request`` struct's json tags exactly, and
the response field names match the Go ``response`` struct's — so a request built
here decodes straight into the op the driverkit runs, and its reply decodes back
here without translation. The driverkit is long-lived and answers one response
line per request line, in order; :class:`Kit` serialises access so that
request/response pairing holds under concurrent callers.
"""

from __future__ import annotations

import json
import threading
from typing import Any, Dict, Optional


class KitError(RuntimeError):
    """An op returned ``ok=false`` (carrying the driverkit's error string), or the
    transport itself failed (the subprocess exited, or closed its stdout)."""


class BaseKit:
    """Typed AgentShield ops over an abstract one-line-in/one-object-out transport.

    Subclasses implement :meth:`_send`. Every helper here builds the request dict
    the Go driverkit expects and hands it to :meth:`op`, which raises
    :class:`KitError` on a non-ok response so a caller can drive the happy path and
    let failures surface as exceptions."""

    def _send(self, request: Dict[str, Any]) -> Dict[str, Any]:  # pragma: no cover
        raise NotImplementedError

    def op(self, **request: Any) -> Dict[str, Any]:
        """Send one op and return its response dict, raising on ``ok=false``."""
        resp = self._send(request)
        if not resp.get("ok", False):
            raise KitError(f"{request.get('op')}: {resp.get('error', 'unknown error')}")
        return resp

    # --- setup ops ---------------------------------------------------------
    def ping(self) -> Dict[str, Any]:
        return self.op(op="ping")

    def seed_token(self, token: Dict[str, Any]) -> Dict[str, Any]:
        """Seed one mandate. ``token`` is a whole domain.Token dict — the driverkit
        unmarshals it into the store's own type, so the same containment invariant a
        real write is validated by applies here."""
        return self.op(op="seed_token", token=token)

    def seed_overlay(self, overlay: Dict[str, Any]) -> Dict[str, Any]:
        return self.op(op="seed_overlay", overlay=overlay)

    def seal_envelope(
        self, *, event_id: str, token_id: str, session_id: str,
        occurred_at: int, raw_instruction: str, contact: str,
    ) -> Dict[str, Any]:
        """Seal the one PII-bearing event into the VAULT (vault.v1). token_id is the
        partition key and is required — the stream-processor drops a seal with an
        empty token_id — so the caller maps the session onto the mandate it runs
        under; session_id is the VAULT key."""
        return self.op(
            op="seal_envelope", event_id=event_id, token_id=token_id, session_id=session_id,
            occurred_at=occurred_at, raw_instruction=raw_instruction, contact=contact,
        )

    def deposit_feature(
        self, *, event_id: str, token_id: str, kind: str, feature_key: str,
        occurred_at: int, value: float,
    ) -> Dict[str, Any]:
        """Deposit an engine-stand-in figure through the live materialiser. ``kind``
        picks the field (behaviour|intent|network) and ``feature_key`` the entity
        row it lands on; ``token_id`` is only the partition key."""
        return self.op(
            op="deposit_feature", event_id=event_id, token_id=token_id, kind=kind,
            feature_key=feature_key, occurred_at=occurred_at, value=value,
        )

    # --- on/off-clock ops --------------------------------------------------
    def evaluate(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """Ask the live decision service to evaluate one OrderContext and report its
        verdict verbatim (decision/code/eval_id/retryable)."""
        return self.op(op="evaluate", order=order)

    def capture(
        self, *, event_id: str, token_id: str, occurred_at: int,
        amount_paise: int, nonce: str, agent_id: str = "",
    ) -> Dict[str, Any]:
        """Emit a payment.captured — money moved. Reuses the debit's nonce so the
        stream-processor spends it, and stamps the agent for reputation."""
        return self.op(
            op="capture", event_id=event_id, token_id=token_id, occurred_at=occurred_at,
            amount_paise=amount_paise, nonce=nonce, agent_id=agent_id,
        )

    def dispute(
        self, *, event_id: str, token_id: str, occurred_at: int,
        nonce: str, agent_id: str = "",
    ) -> Dict[str, Any]:
        """Emit a payment.disputed — a chargeback, the strongest settled negative."""
        return self.op(
            op="dispute", event_id=event_id, token_id=token_id, occurred_at=occurred_at,
            nonce=nonce, agent_id=agent_id,
        )

    def cancel(self, *, event_id: str, token_id: str, occurred_at: int) -> Dict[str, Any]:
        """Emit a token.cancelled — a pulled mandate, a light MISUSE."""
        return self.op(op="cancel", event_id=event_id, token_id=token_id, occurred_at=occurred_at)

    # --- read-back / drain ops --------------------------------------------
    def get_block(self, token_id: str) -> Dict[str, Any]:
        """Read a token's reconstructed block-state (nil block == absent, not an
        error) — the driver polls this to know a prior off-clock fold has landed."""
        return self.op(op="get_block", token_id=token_id)

    def get_feature(self, feature_key: str) -> Dict[str, Any]:
        return self.op(op="get_feature", feature_key=feature_key)

    def collect_labels(self, *, expect: int = 0, timeout_ms: int = 5000) -> Dict[str, Any]:
        """Drain the settled labels the real labeler produced on outcomes.v1, waiting
        until at least ``expect`` have arrived or ``timeout_ms`` elapses."""
        return self.op(op="collect_labels", expect=expect, timeout_ms=timeout_ms)


class Kit(BaseKit):
    """Live NDJSON client over the long-lived driverkit subprocess.

    Writes one compact JSON request line to the process's stdin and reads exactly
    one response line back from its stdout, under a lock so concurrent callers keep
    their request/response pairing. The subprocess is dialled once (Redis, Redpanda,
    the decision gRPC) and driven op-by-op, so a whole replay pays the connect cost
    once. Ownership of the process lifecycle stays with the caller (see
    :mod:`agentshield_driver.stack`)."""

    def __init__(self, proc: Any):
        self._proc = proc
        self._lock = threading.Lock()

    def _send(self, request: Dict[str, Any]) -> Dict[str, Any]:
        line = json.dumps(request, separators=(",", ":"))
        with self._lock:
            if self._proc.poll() is not None:
                raise KitError(f"driverkit exited with code {self._proc.returncode}")
            self._proc.stdin.write(line + "\n")
            self._proc.stdin.flush()
            resp_line = self._proc.stdout.readline()
        if not resp_line:
            raise KitError("driverkit closed stdout without a response")
        try:
            return json.loads(resp_line)
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive
            raise KitError(f"driverkit returned non-JSON line: {resp_line!r}") from exc

    def close(self) -> None:
        """Close the driverkit's stdin so its read loop ends and it exits cleanly."""
        try:
            self._proc.stdin.close()
        except Exception:  # pragma: no cover - best-effort teardown
            pass
