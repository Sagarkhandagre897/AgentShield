"""families — the vocabulary of the synthetic world.

Every debit the generator emits is tagged with one *family*: either a legitimate
session or one of the misuse patterns the System Design calls out (§5, §11–§13).
This module holds those tags, the caller-facing decision/code vocabulary the eval
oracle compares against (mirrors the proto ``Answer``/``Code`` enums), the declared
``tool_risk`` levels, and the settled-outcome label vocabulary (mirrors
``internal/bus`` — a test asserts the two never drift).

    THE LABEL LINE (§6) — nothing here is a label the system trains on. These are
    the generator's OWN ground truth, used to *drive* which settled outcomes get
    emitted (a fraud ring's captures are later disputed; a mandate is pulled) and to
    score the live run in the eval notebook. The training labels themselves are
    produced downstream by the Go labeler, only from those settled outcomes — never
    from these tags and never from the system's own verdicts.
"""

from __future__ import annotations

from typing import Dict, NamedTuple

# --- Families ---------------------------------------------------------------
# Legitimate traffic. The bulk of the world; the false-positive denominator.
FAMILY_LEGIT = "legit"

# Deterministic-spine misuse (§5). Each trips exactly one predicate, so its
# expected verdict is not a matter of ML confidence — it is table-decidable.
FAMILY_REPLAY = "replay"               # P1 — a seen nonce, replayed
FAMILY_STALE_REVOKED = "stale_revoked_token"  # P4 — authority expired or pulled
FAMILY_SCOPE_OVERRUN = "scope_overrun"        # P2 — outside the agreed scope

# Soft, learned misuse. The spine passes; a risk figure has to earn the step-up.
FAMILY_INTENT_DRIFT = "intent_drift"          # §12 — debit drifts from the sealed intent
FAMILY_VELOCITY_BUSTOUT = "velocity_bustout"  # §11 — a burst that drains the day's room

# Graph-structural misuse (§13). No single debit looks wrong; the shape does.
FAMILY_MULE_FAN_IN = "mule_fan_in"                # many payers → one collector
FAMILY_SHARED_DEVICE_RING = "shared_device_ring"  # a dense shared-entity clique
FAMILY_SYNCHRONISED_FLEET = "synchronised_fleet"  # agents acting in lockstep

MISUSE_FAMILIES = (
    FAMILY_REPLAY,
    FAMILY_STALE_REVOKED,
    FAMILY_SCOPE_OVERRUN,
    FAMILY_INTENT_DRIFT,
    FAMILY_VELOCITY_BUSTOUT,
    FAMILY_MULE_FAN_IN,
    FAMILY_SHARED_DEVICE_RING,
    FAMILY_SYNCHRONISED_FLEET,
)
ALL_FAMILIES = (FAMILY_LEGIT,) + MISUSE_FAMILIES

# --- Caller-facing verdict vocabulary (mirror proto Answer / Code) ----------
# The oracle records the *ideal* verdict; the eval notebook compares the live
# verdict against it. The strings match the bus decision values and the pb.Code
# enum names exactly, so the driver can compare without translation.
DECISION_ALLOW = "ALLOW"
DECISION_STEP_UP = "STEP_UP"
DECISION_BLOCK = "BLOCK"

CODE_OK_ALLOW = "OK_ALLOW"
CODE_STEPUP_SCOPE = "STEPUP_SCOPE"
CODE_STEPUP_UNBOUND = "STEPUP_UNBOUND"
CODE_STEPUP_RISK = "STEPUP_RISK"
CODE_STEPUP_FAILCLOSED = "STEPUP_FAILCLOSED"
CODE_BLOCKED_DUPLICATE = "BLOCKED_DUPLICATE"
CODE_BLOCKED_AUTHORITY = "BLOCKED_AUTHORITY"
CODE_BLOCKED_IDENTITY = "BLOCKED_IDENTITY"
CODE_BLOCKED_BINDING = "BLOCKED_BINDING"

# --- Declared tool risk (mirror proto ToolRisk) ----------------------------
# A DECLARED field on the request, never something we verified. It can heighten
# caution (a CRITICAL tool is never quietly allowed — the decision service floors
# it to a step-up) but must never BLOCK on its own (§5).
TOOL_RISK_UNSPECIFIED = 0
TOOL_RISK_LOW = 1
TOOL_RISK_MEDIUM = 2
TOOL_RISK_HIGH = 3
TOOL_RISK_CRITICAL = 4

# --- Settled-outcome label vocabulary (mirror internal/bus) -----------------
# What a settled outcome resolves to once the Go labeler distils it. Recorded on a
# debit's settlement so the eval notebook knows what label the run *should* have
# produced — never injected as a label itself.
LABEL_MISUSE = 1.0
LABEL_LEGIT = 0.0
REASON_DISPUTE = "dispute"                  # a chargeback — strongest settled negative (full weight)
REASON_CANCELLATION = "cancellation"        # a pulled mandate — soft negative (light weight)
REASON_CONFIRMED_STEP_UP = "confirmed_step_up"  # a step-up a human passed, money then moved — legitimate


class FamilyInfo(NamedTuple):
    """A family's ideal verdict and a one-line note on the mechanism that earns
    it — the reference an operator (or the eval notebook) reads to understand why
    a pattern is expected to be caught, and where."""

    ideal_decision: str
    ideal_code: str
    mechanism: str


# The legend: how each family is *meant* to be decided, and by what. Soft families
# name STEPUP_RISK because the spine passes and a learned figure must raise the
# expected loss above the interruption cost; deterministic families name the exact
# predicate code they trip. These are intentions, not guarantees — the live run is
# what the eval measures.
LEGEND: Dict[str, FamilyInfo] = {
    FAMILY_LEGIT: FamilyInfo(
        DECISION_ALLOW, CODE_OK_ALLOW,
        "Passes the spine with a low risk figure; expected loss stays below the interruption cost.",
    ),
    FAMILY_REPLAY: FamilyInfo(
        DECISION_BLOCK, CODE_BLOCKED_DUPLICATE,
        "P1 — the nonce was already spent by the first (allowed) debit, so the replay is a duplicate.",
    ),
    FAMILY_STALE_REVOKED: FamilyInfo(
        DECISION_BLOCK, CODE_BLOCKED_AUTHORITY,
        "P4 — the mandate is expired or cancelled, so the authority no longer exists.",
    ),
    FAMILY_SCOPE_OVERRUN: FamilyInfo(
        DECISION_STEP_UP, CODE_STEPUP_SCOPE,
        "P2 — the debit is larger than the agreed cap or to a merchant the overlay denies.",
    ),
    FAMILY_INTENT_DRIFT: FamilyInfo(
        DECISION_STEP_UP, CODE_STEPUP_RISK,
        "§12 — the spine passes (bound, in-cap) but the debit's category/merchant drifts from the sealed intent.",
    ),
    FAMILY_VELOCITY_BUSTOUT: FamilyInfo(
        DECISION_STEP_UP, CODE_STEPUP_SCOPE,
        "§11/P2 — a burst whose captures drain the day's room; the crossing debit exceeds the per-day cap.",
    ),
    FAMILY_MULE_FAN_IN: FamilyInfo(
        DECISION_STEP_UP, CODE_STEPUP_RISK,
        "§13 — many independent payers funnel into one collector; the network risk on the payers rises.",
    ),
    FAMILY_SHARED_DEVICE_RING: FamilyInfo(
        DECISION_STEP_UP, CODE_STEPUP_RISK,
        "§13 — a dense clique sharing agents and a merchant; component structure raises the network risk.",
    ),
    FAMILY_SYNCHRONISED_FLEET: FamilyInfo(
        DECISION_STEP_UP, CODE_STEPUP_RISK,
        "§13/§11 — a fleet of agents debiting the same merchant, same amount, same instant.",
    ),
}
