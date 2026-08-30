"""fakekit — an in-memory AgentShield the orchestration logic can be tested against.

:class:`FakeKit` implements the same typed op surface as the live
:class:`~agentshield_driver.kit.Kit`, but instead of shelling out to the Go
driverkit it simulates the backends in a few dicts. It replays the exact decision
path the product runs — the P1-P6 predicate spine, the linear-logistic scorer, the
tool floor, the fail-closed feature read — and the exact labeler semantics — a
step-up arms a pending confirmation, a matching capture confirms it (LEGIT), a
dispute chargebacks (MISUSE) and clears the token's pending, a cancellation settles
a light MISUSE. That lets the whole phase machine (seed→seal→pre-warm→timeline→
settle→collect) be exercised, and the oracle scored, with no Redis/Redpanda/gRPC.

It is deliberately faithful, not a stub: the verdict a debit gets here is computed
by the same maths the Go service uses (weights and codes mirrored from
``internal/score`` and ``internal/predicate``), and it returns the driverkit's own
wire shapes — the proto ``ANSWER_*`` decision name, the ``Code`` enum name, and the
``block``/``feature``/``labels`` dicts keyed exactly as ``internal/domain`` tags
them — so a test that passes here is testing the real choreography, not a mock of it.
"""

from __future__ import annotations

import math
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from .kit import BaseKit

# The interruption cost the live run configures (AGENTSHIELD_INTERRUPTION_COST_PAISE);
# a step-up is earned only when the expected loss of allowing exceeds it.
_DEFAULT_INTERRUPTION_COST = 100000

# Linear-logistic weights, mirrored byte-for-byte from score.DefaultWeights so a
# scenario that separates ALLOW from STEP_UP against the live scorer does so here too.
_W_BEHAVIOUR = 1.5
_W_INTENT = 1.5
_W_NETWORK = 1.2
_W_CONSUMPTION = 0.8
_W_REPUTATION = 1.0
_W_BIAS = -2.2

# proto Answer enum names (pb.Answer_name), the wire form the driverkit returns.
_ANSWER = {"ALLOW": "ANSWER_ALLOW", "STEP_UP": "ANSWER_STEP_UP", "BLOCK": "ANSWER_BLOCK"}

# The label vocabulary the Go labeler emits (mirror internal/bus).
_LABEL_MISUSE = 1.0
_LABEL_LEGIT = 0.0


def _clamp01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x


def _sigmoid(z: float) -> float:
    return 1.0 / (1.0 + math.exp(-z))


class FakeKit(BaseKit):
    """An in-memory AgentShield with the real decision + labeler semantics.

    State mirrors the four things the live backends hold: the token/overlay stores,
    the per-token reconstructed block-state (spent nonces + consumption), the
    feature store, and the labeler's pending step-ups + settled labels. Every op the
    live kit exposes is implemented against that state; ``_send`` dispatches on the
    op name exactly as the driverkit's stdin loop does, so :class:`FakeKit` is a
    drop-in for :class:`~agentshield_driver.kit.Kit` in a test."""

    def __init__(self, *, interruption_cost_paise: int = _DEFAULT_INTERRUPTION_COST,
                 staleness_budget_seconds: int = 0):
        self.interruption_cost = interruption_cost_paise
        self.budget = staleness_budget_seconds  # <= 0 disables the staleness check
        self.tokens: Dict[str, Dict[str, Any]] = {}
        self.overlays: Dict[str, Dict[str, Any]] = {}
        self.features: Dict[str, Dict[str, Any]] = {}
        self.blocks: Dict[str, Dict[str, Any]] = {}
        self.seals: Dict[str, Dict[str, Any]] = {}
        self.pending: Set[Tuple[str, str]] = set()      # armed (token_id, nonce) step-ups
        self.labels: List[Dict[str, Any]] = []          # settled outcome.labeled records
        self.captures: List[Dict[str, Any]] = []        # audit of money that moved

    # --- transport dispatch (mirrors the driverkit's op switch) ------------
    def _send(self, request: Dict[str, Any]) -> Dict[str, Any]:
        op = request.get("op", "")
        handler = getattr(self, f"_op_{op}", None)
        if handler is None:
            return {"op": op, "ok": False, "error": f"unknown op: {op}"}
        try:
            resp = handler(request)
        except Exception as exc:  # surface as the driverkit would: ok=false + message
            return {"op": op, "ok": False, "error": str(exc)}
        resp.setdefault("op", op)
        resp.setdefault("ok", True)
        return resp

    # --- setup ops ---------------------------------------------------------
    def _op_ping(self, _req: Dict[str, Any]) -> Dict[str, Any]:
        return {}

    def _op_seed_token(self, req: Dict[str, Any]) -> Dict[str, Any]:
        t = req["token"]
        self.tokens[t["token_id"]] = t
        return {}

    def _op_seed_overlay(self, req: Dict[str, Any]) -> Dict[str, Any]:
        o = req["overlay"]
        self.overlays[o["token_id"]] = o
        return {}

    def _op_seal_envelope(self, req: Dict[str, Any]) -> Dict[str, Any]:
        if not req.get("token_id"):
            return {"ok": False, "error": "seal_envelope: empty token_id"}
        self.seals[req["session_id"]] = dict(req)
        return {}

    def _op_deposit_feature(self, req: Dict[str, Any]) -> Dict[str, Any]:
        key = req["feature_key"]
        row = self.features.setdefault(key, _blank_row(key))
        field = {
            "behaviour": "behaviour_deviation",
            "intent": "intent_divergence",
            "network": "network_risk",
        }.get(req["kind"])
        if field is None:
            return {"ok": False, "error": f"deposit_feature: unknown kind {req['kind']}"}
        row[field] = float(req["value"])
        row["computed_at"] = int(req.get("occurred_at", 0))
        return {}

    # --- on-clock: evaluate (the seven stages) -----------------------------
    def _op_evaluate(self, req: Dict[str, Any]) -> Dict[str, Any]:
        order = req["order"]
        now = int(time.time())
        token = self.tokens.get(order["token_id"])
        block = self.blocks.get(order["token_id"])
        overlay = self.overlays.get(order["token_id"])

        answer, code = self._predicates(order, token, block, overlay, now)
        if answer is None:
            answer, code = self._score(order, token, block)
            # Tool floor: a CRITICAL tool is never quietly allowed (never a BLOCK).
            if int(order.get("tool_risk", 0)) == 4 and answer == "ALLOW":
                answer, code = "STEP_UP", "STEPUP_RISK"

        if answer == "STEP_UP" and order.get("nonce"):
            # decision.made arms the labeler's pending confirmation for this slip.
            self.pending.add((order["token_id"], order["nonce"]))

        return {
            "decision": _ANSWER[answer],
            "code": code,
            "eval_id": order["evaluation_id"],
            "retryable": answer == "STEP_UP",
        }

    def _predicates(self, order, token, block, overlay, now):
        """Run P1→P5→P4→P6→P2→P3 and return (answer, code) for the first refusal,
        or (None, None) if all six pass. Mirrors predicate.Run's fixed order."""
        amt = int(order.get("amount_paise", 0))
        seen = set((block or {}).get("seen_nonces") or [])
        # P1 · replay
        if block is not None and order.get("nonce") and order["nonce"] in seen:
            return "BLOCK", "BLOCKED_DUPLICATE"
        # P5 · identity (the fake caller is always authenticated, like dev mode)
        if token is None or token.get("customer_id") != order.get("customer_id"):
            return "BLOCK", "BLOCKED_IDENTITY"
        # P4 · authority
        if token.get("status") != "confirmed":
            return "BLOCK", "BLOCKED_AUTHORITY"
        expire_at = int(token.get("expire_at", 0))
        if expire_at > 0 and now > expire_at:
            return "BLOCK", "BLOCKED_AUTHORITY"
        consumed_total = int((block or {}).get("consumed_total", 0))
        if consumed_total + amt > int(token.get("token_ceiling_paise", 0)):
            return "BLOCK", "BLOCKED_AUTHORITY"
        # P6 · binding
        if not order.get("cart_hash"):
            return "BLOCK", "BLOCKED_BINDING"
        # P2 · scope (overlay may only tighten)
        per_debit = int(token.get("max_amount_paise", 0))
        per_day = int(token.get("max_per_day_paise", 0))
        if overlay is not None:
            caps = overlay.get("per_window_caps") or {}
            if "per_debit" in caps and caps["per_debit"] < per_debit:
                per_debit = caps["per_debit"]
            if "per_day" in caps and caps["per_day"] < per_day:
                per_day = caps["per_day"]
            rules = overlay.get("merchant_rules") or {}
            if rules.get(order.get("merchant_id")) == "deny":
                return "STEP_UP", "STEPUP_SCOPE"
        if amt > per_debit:
            return "STEP_UP", "STEPUP_SCOPE"
        consumed_today = int((block or {}).get("consumed_today", 0))
        if consumed_today + amt > per_day:
            return "STEP_UP", "STEPUP_SCOPE"
        # P3 · unbound
        if not order.get("envelope_digest"):
            if amt > 100000:
                return "STEP_UP", "STEPUP_UNBOUND"
        return None, None

    def _score(self, order, token, block):
        """readFeatures → scoreEnsemble → decide. A missing entity row fails the
        read closed (STEPUP_FAILCLOSED); otherwise the linear score's expected loss
        is weighed against the interruption cost."""
        now = int(time.time())
        keys = _dedup(order.get("customer_id"), order.get("token_id"),
                      order.get("agent_id"), order.get("merchant_id"))
        rows: Dict[str, Dict[str, Any]] = {}
        degraded = False
        for k in keys:
            row = self.features.get(k)
            if row is None:
                degraded = True
                continue
            if self.budget > 0 and now - int(row.get("computed_at", 0)) > self.budget:
                degraded = True
            rows[k] = row
        if degraded:
            return "STEP_UP", "STEPUP_FAILCLOSED"

        cust = rows.get(order.get("customer_id"), {})
        tokrow = rows.get(order.get("token_id"), {})
        agent = rows.get(order.get("agent_id"), {})
        cons = _consumption_frac(token, block)
        z = (_W_BIAS
             + _W_BEHAVIOUR * _clamp01(cust.get("behaviour_deviation", 0.0))
             + _W_INTENT * _clamp01(tokrow.get("intent_divergence", 0.0))
             + _W_NETWORK * _clamp01(cust.get("network_risk", 0.0))
             + _W_CONSUMPTION * _clamp01(cons)
             - _W_REPUTATION * _clamp01(agent.get("reputation", 0.0)))
        expected_loss = int(_sigmoid(z) * float(order.get("amount_paise", 0)))
        if expected_loss > self.interruption_cost:
            return "STEP_UP", "STEPUP_RISK"
        return "ALLOW", "OK_ALLOW"

    # --- settlement ops (money moves, chargebacks, pulled mandates) --------
    def _op_capture(self, req: Dict[str, Any]) -> Dict[str, Any]:
        token_id = req["token_id"]
        nonce = req.get("nonce", "")
        amount = int(req.get("amount_paise", 0))
        occurred_at = int(req.get("occurred_at", 0))
        blk = self._block(token_id)
        if nonce and nonce not in blk["seen_nonces"]:
            blk["seen_nonces"].append(nonce)
        blk["consumed_today"] += amount
        blk["consumed_total"] += amount
        blk["last_computed_at"] = occurred_at
        self.captures.append(dict(req))
        # A capture that confirms an armed step-up is a LEGIT label; a bare capture
        # (nothing pending) teaches the labeler nothing.
        if (token_id, nonce) in self.pending:
            self.pending.discard((token_id, nonce))
            self._emit_label(req["event_id"], token_id, occurred_at,
                             _LABEL_LEGIT, 1.0, "confirmed_step_up")
        return {}

    def _op_dispute(self, req: Dict[str, Any]) -> Dict[str, Any]:
        token_id = req["token_id"]
        occurred_at = int(req.get("occurred_at", 0))
        # A chargeback is the strongest settled negative and voids the token's
        # pending step-ups (they can no longer be trusted as confirmed).
        self.pending = {(t, n) for (t, n) in self.pending if t != token_id}
        self._emit_label(req["event_id"], token_id, occurred_at,
                         _LABEL_MISUSE, 1.0, "dispute")
        return {}

    def _op_cancel(self, req: Dict[str, Any]) -> Dict[str, Any]:
        # A pulled mandate is a light MISUSE (half weight).
        self._emit_label(req["event_id"], req["token_id"],
                         int(req.get("occurred_at", 0)), _LABEL_MISUSE, 0.5, "cancellation")
        return {}

    # --- read-back / drain ops --------------------------------------------
    def _op_get_block(self, req: Dict[str, Any]) -> Dict[str, Any]:
        blk = self.blocks.get(req["token_id"])
        return {"block": blk} if blk is not None else {}

    def _op_get_feature(self, req: Dict[str, Any]) -> Dict[str, Any]:
        row = self.features.get(req["feature_key"])
        return {"feature": row} if row is not None else {}

    def _op_collect_labels(self, _req: Dict[str, Any]) -> Dict[str, Any]:
        # The fake settles synchronously, so everything is already in hand; return a
        # snapshot (the op is idempotent, matching the driverkit's collector).
        return {"labels": [dict(l) for l in self.labels]}

    # --- internal helpers --------------------------------------------------
    def _block(self, token_id: str) -> Dict[str, Any]:
        return self.blocks.setdefault(token_id, {
            "token_id": token_id, "consumed_today": 0, "consumed_total": 0,
            "seen_nonces": [], "last_computed_at": 0,
        })

    def _emit_label(self, event_id, token_id, occurred_at, label, weight, reason):
        self.labels.append({
            "event_id": event_id, "token_id": token_id, "occurred_at": occurred_at,
            "label": label, "weight": weight, "reason": reason,
        })


def _blank_row(key: str) -> Dict[str, Any]:
    """A feature row with every figure at zero — what a fresh deposit lands on. The
    field names mirror domain.FeatureRow's json tags so get_feature returns the same
    shape the driverkit does."""
    return {
        "key": key, "behaviour_deviation": 0.0, "signal_deviations": None,
        "intent_divergence": 0.0, "network_risk": 0.0, "reputation": 0.0,
        "consumption_frac": 0.0, "computed_at": 0,
    }


def _dedup(*keys: Optional[str]) -> List[str]:
    """Non-empty keys, de-duplicated, first-seen order — features.EntityKeys.list()."""
    seen: Set[str] = set()
    out: List[str] = []
    for k in keys:
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


def _consumption_frac(token: Optional[Dict[str, Any]], block: Optional[Dict[str, Any]]) -> float:
    """The model-free day-one signal: fraction of the lifetime ceiling consumed."""
    if not token or int(token.get("token_ceiling_paise", 0)) <= 0 or not block:
        return 0.0
    return _clamp01(int(block.get("consumed_total", 0)) / float(token["token_ceiling_paise"]))
