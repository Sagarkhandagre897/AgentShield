"""generate — the deterministic synthesis of a Scenario.

Given a seed and a set of counts, ``build_scenario`` produces a reproducible world:
legitimate sessions, one bucket per misuse family, and the three graph structures.
It is deterministic (a seeded PRNG, no clock) so the same seed always yields the
same JSON — a run is reproducible from the seed alone.

Each family is engineered to trip exactly the mechanism the oracle names (see
``families.LEGEND``):

  * the deterministic families (replay, stale/revoked, scope overrun) are built so a
    specific predicate P1/P4/P2 must fire — their expected verdict is table-decidable
    and does not depend on any model;
  * the soft families (intent drift, velocity bust-out) pass the spine and leave a
    single risk signal (a divergence, a drained day) for a learned figure to catch;
  * the graph families (fan-in, shared-device ring, synchronised fleet) are just
    id-topology and timing — every debit passes the spine, and only the *shape* is
    wrong, so the network-risk figure is what must earn the step-up.

Two design rules the money side obeys (§3, §9): a token's ceilings are always
contained (per-debit ≤ per-day ≤ lifetime), and expiry is stamped in absolute time
(a live token never expires; a stale one always has), because the decision service
checks expiry against the wall clock while consumption windows track each event's
own timestamp.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import List, Optional

from . import families as F
from .scenario import Debit, Overlay, Scenario, SealedEnvelope, Settlement, Token

# --- Money constants (paise) -----------------------------------------------
# The predicate value floor (§5): below ₹1,000 an unattested/over-envelope debit
# is not worth an interruption. Legit debits sit above it so they are "real".
FLOOR_PAISE = 100_000

# A standard confirmed mandate: per-debit ₹10k ≤ per-day ₹30k ≤ lifetime ₹3L.
STD_PER_DEBIT = 1_000_000
STD_PER_DAY = 3_000_000
STD_CEILING = 30_000_000

# A comfortably-legit debit: above the floor, well under the per-debit cap, and
# small enough that a few in a day stay under the per-day cap.
LEGIT_AMOUNT = 600_000

# --- Time constants (epoch seconds) ----------------------------------------
# Expiry is absolute, NOT relative to the scenario clock: the decision service
# checks it against the wall clock, so a live token must outlive any run and a
# stale one must predate any run.
EXPIRE_FAR_FUTURE = 4_102_444_800  # 2100-01-01 — a live mandate never expires
EXPIRE_IN_PAST = 1_600_000_000     # 2020-09-13 — a stale mandate always has

# --- The merchant world -----------------------------------------------------
# In-envelope merchants a legit session shops at, each with the category and the
# purpose a matching intent envelope would seal.
HOME_MERCHANTS = [
    ("m_bigbasket", "groceries", "purchase", "bigbasket"),
    ("m_croma", "electronics", "purchase", "croma"),
    ("m_netflix", "subscription", "subscription", "netflix"),
    ("m_makemytrip", "travel", "purchase", "makemytrip"),
    ("m_1mg", "pharmacy", "purchase", "1mg"),
]
# Off-envelope merchants an intent-drift debit diverts to — a different category
# and the kind of liquid, resaleable target a drifting agent reaches for.
DRIFT_MERCHANTS = [
    ("m_giftcards", "gift-cards"),
    ("m_crypto", "crypto"),
    ("m_wallet_topup", "wallet-topup"),
]
# The collector a mule fan-in funnels into.
MULE_MERCHANT = ("m_collector", "wallet-topup")
# The shared storefront a device ring cycles through.
RING_MERCHANT = ("m_ring_store", "electronics")
# The single merchant a synchronised fleet all hit at once.
FLEET_MERCHANT = ("m_fleet_target", "gift-cards")


@dataclass
class Config:
    """The knobs. Defaults give a compact but fully-covered scenario: legit traffic
    dominates, every misuse family and every graph structure is present, and there
    are enough settled outcomes for the labeler to have something to distil."""

    seed: int = 7
    base_ts: int = 1_700_000_000  # 2023-11-14; the scenario's logical t0

    n_legit: int = 12             # ordinary allowed sessions (the FP denominator)
    n_cautious_legit: int = 4     # CRITICAL-tool sessions → step-up a human confirms (LEGIT labels)
    n_intent_drift: int = 4
    n_scope_overrun: int = 4      # split across the over-cap and merchant-deny flavours
    n_velocity_bustout: int = 2   # each is a burst of `bustout_len` debits
    bustout_len: int = 6
    n_replay: int = 3             # each contributes a legit seed debit + its replay
    n_stale_revoked: int = 4      # split across expired and revoked flavours

    mule_fan_in_rings: int = 1
    fan_in_size: int = 6
    shared_device_rings: int = 1
    ring_size: int = 5
    synchronised_fleets: int = 1
    fleet_size: int = 6


class Generator:
    """Holds the reproducible state — the PRNG and the monotonic id/time minters —
    and builds a Scenario one family at a time. Kept as a class so the minters and
    the growing Scenario are threaded implicitly rather than passed everywhere."""

    def __init__(self, config: Config):
        self.cfg = config
        self.rng = random.Random(config.seed)
        self.scn = Scenario()
        self._ts = config.base_ts
        # Monotonic counters behind the human-readable ids. Distinct spaces so a
        # customer id never collides with a token id, etc.
        self._n = {"cust": 0, "agent": 0, "token": 0, "sess": 0, "eval": 0, "nonce": 0, "cart": 0, "contact": 0}

    # --- minters ------------------------------------------------------------
    def _id(self, kind: str, prefix: str) -> str:
        self._n[kind] += 1
        return f"{prefix}_{self._n[kind]:05d}"

    def _customer(self) -> str:
        return self._id("cust", "cust")

    def _agent(self) -> str:
        return self._id("agent", "agent")

    def _token_id(self) -> str:
        return self._id("token", "tok")

    def _session(self) -> str:
        return self._id("sess", "sess")

    def _eval(self) -> str:
        return self._id("eval", "eval")

    def _nonce(self) -> str:
        return self._id("nonce", "nonce")

    def _cart(self) -> str:
        return self._id("cart", "cart")

    def _contact(self) -> str:
        self._n["contact"] += 1
        digits = str(9000000000 + self._n["contact"])
        return f"+91-{digits[:5]}-{digits[5:]}"

    def _advance(self, gap: int) -> int:
        """Return the current logical time, then move it forward by `gap` seconds."""
        now = self._ts
        self._ts += gap
        return now

    # --- token / envelope factories ---------------------------------------
    def _token(
        self,
        customer_id: str,
        *,
        status: str = "confirmed",
        expire_at: int = EXPIRE_FAR_FUTURE,
        per_debit: int = STD_PER_DEBIT,
        per_day: int = STD_PER_DAY,
        ceiling: int = STD_CEILING,
        ttype: str = "recurring",
        frequency: str = "monthly",
    ) -> Token:
        t = Token(
            token_id=self._token_id(),
            customer_id=customer_id,
            type=ttype,
            max_amount_paise=per_debit,
            max_per_day_paise=per_day,
            token_ceiling_paise=ceiling,
            frequency=frequency,
            expire_at=expire_at,
            status=status,
        )
        t.validate()
        self.scn.tokens.append(t)
        return t

    def _envelope(self, purpose: str, category: str, merchant_pref: str, max_amount: int) -> SealedEnvelope:
        rupees = max_amount // 100
        instruction = (
            f"Please {purpose} {category} from {merchant_pref}; "
            f"keep each payment under Rs {rupees}."
        )
        env = SealedEnvelope(
            session_id=self._session(),
            purpose=purpose,
            category=category,
            max_amount_paise=max_amount,
            merchant_preference=merchant_pref,
            raw_instruction=instruction,
            contact=self._contact(),
            constraints={},
        )
        self.scn.envelopes.append(env)
        return env

    # --- the low-level debit factory ---------------------------------------
    def _debit(
        self,
        *,
        token: Token,
        agent_id: str,
        merchant_id: str,
        env: Optional[SealedEnvelope],
        amount: int,
        family: str,
        expected_decision: str,
        expected_code: str,
        is_misuse: bool,
        tool_risk: int = F.TOOL_RISK_LOW,
        nonce: Optional[str] = None,
        ts: Optional[int] = None,
        settlement: Optional[Settlement] = None,
        barrier: bool = False,
    ) -> Debit:
        d = Debit(
            evaluation_id=self._eval(),
            token_id=token.token_id,
            customer_id=token.customer_id,
            agent_id=agent_id,
            merchant_id=merchant_id,
            session_id=env.session_id if env else "",
            amount_paise=amount,
            cart_hash=self._cart(),
            envelope_digest=env.digest() if env else "",
            tool_risk=tool_risk,
            nonce=nonce if nonce is not None else self._nonce(),
            ts=ts if ts is not None else self._advance(3600),
            is_misuse=is_misuse,
            family=family,
            expected_decision=expected_decision,
            expected_code=expected_code,
            settlement=settlement or Settlement(),
            barrier=barrier,
        )
        self.scn.timeline.append(d)
        return d

    # =======================================================================
    # Family builders
    # =======================================================================

    def add_legit_session(self, *, cautious: bool = False) -> None:
        """A real customer, a real mandate, a sealed intent, and a debit that
        belongs. Non-cautious: it should ALLOW and its capture teaches NOTHING (a
        bare, undisputed capture is not a label, §6). Cautious: a CRITICAL-tool
        debit the decision service floors to a step-up, which the human then passes
        — a capture on the SAME nonce, which the labeler reads as a confirmed
        step-up: the one way a legitimate outcome earns a LEGIT label."""
        customer = self._customer()
        agent = self._agent()
        merchant_id, category, purpose, pref = self.rng.choice(HOME_MERCHANTS)
        token = self._token(customer)
        env = self._envelope(purpose, category, pref, token.max_amount_paise)

        if cautious:
            self._debit(
                token=token, agent_id=agent, merchant_id=merchant_id, env=env,
                amount=LEGIT_AMOUNT, family=F.FAMILY_LEGIT,
                expected_decision=F.DECISION_STEP_UP, expected_code=F.CODE_STEPUP_RISK,
                is_misuse=False, tool_risk=F.TOOL_RISK_CRITICAL,
                settlement=Settlement(
                    capture_when="allow_or_stepup", capture_amount_paise=LEGIT_AMOUNT,
                    expected_label=F.LABEL_LEGIT, expected_reason=F.REASON_CONFIRMED_STEP_UP,
                ),
            )
            return

        for _ in range(self.rng.randint(1, 3)):
            self._debit(
                token=token, agent_id=agent, merchant_id=merchant_id, env=env,
                amount=LEGIT_AMOUNT, family=F.FAMILY_LEGIT,
                expected_decision=F.DECISION_ALLOW, expected_code=F.CODE_OK_ALLOW,
                is_misuse=False, tool_risk=F.TOOL_RISK_LOW,
                settlement=Settlement(capture_when="allow", capture_amount_paise=LEGIT_AMOUNT),
            )

    def add_intent_drift(self) -> None:
        """The spine is satisfied — the debit is bound to a sealed envelope and sits
        under every cap — but it diverts to a different category (groceries sealed,
        gift-cards charged). Nothing deterministic can object; only the intent
        engine's divergence can raise the risk to a step-up (§12)."""
        customer = self._customer()
        agent = self._agent()
        _, category, purpose, pref = self.rng.choice(HOME_MERCHANTS)
        drift_merchant, _drift_category = self.rng.choice(DRIFT_MERCHANTS)
        token = self._token(customer)
        env = self._envelope(purpose, category, pref, token.max_amount_paise)
        self._debit(
            token=token, agent_id=agent, merchant_id=drift_merchant, env=env,
            amount=LEGIT_AMOUNT, family=F.FAMILY_INTENT_DRIFT,
            expected_decision=F.DECISION_STEP_UP, expected_code=F.CODE_STEPUP_RISK,
            is_misuse=True, tool_risk=F.TOOL_RISK_MEDIUM,
            settlement=Settlement(
                capture_when="allow_or_stepup", capture_amount_paise=LEGIT_AMOUNT,
                then_dispute=True, expected_label=F.LABEL_MISUSE, expected_reason=F.REASON_DISPUTE,
            ),
        )

    def add_scope_overrun(self, *, deny_flavour: bool) -> None:
        """Outside the agreed scope — P2. Two flavours: a debit larger than the
        per-debit cap, or a debit to a merchant the customer's overlay denies. Either
        is a step-up (STEPUP_SCOPE), never a block: scope is a "re-confirm", not a
        "no"."""
        customer = self._customer()
        agent = self._agent()
        merchant_id, category, purpose, pref = self.rng.choice(HOME_MERCHANTS)
        token = self._token(customer)
        env = self._envelope(purpose, category, pref, token.max_amount_paise)

        if deny_flavour:
            # An overlay that denies this merchant — a tightening, never a widening.
            self.scn.overlays.append(
                Overlay(token_id=token.token_id, merchant_rules={merchant_id: "deny"}, overlay_version=1)
            )
            amount = LEGIT_AMOUNT  # under every cap; the deny is what fires
        else:
            amount = token.max_amount_paise + 500_000  # over the per-debit cap

        self._debit(
            token=token, agent_id=agent, merchant_id=merchant_id, env=env,
            amount=amount, family=F.FAMILY_SCOPE_OVERRUN,
            expected_decision=F.DECISION_STEP_UP, expected_code=F.CODE_STEPUP_SCOPE,
            is_misuse=True, tool_risk=F.TOOL_RISK_MEDIUM,
            settlement=Settlement(
                capture_when="allow_or_stepup", capture_amount_paise=amount,
                then_dispute=True, expected_label=F.LABEL_MISUSE, expected_reason=F.REASON_DISPUTE,
            ),
        )

    def add_velocity_bustout(self) -> None:
        """A burst on one mandate: each debit is under the per-debit cap, but their
        captures drain the day's room, so the debit that crosses the per-day cap
        steps up (P2), and the behaviour engine's velocity signal has been climbing
        all along (§11). Early debits allow-and-capture; the crossing debit and
        after carry a barrier — the driver must let the prior captures fold into
        consumption before sending them, or the race would hide the pattern. The
        whole burst is fraud, so every captured leg is later disputed."""
        customer = self._customer()
        agent = self._agent()
        token = self._token(customer)
        env = self._envelope("purchase", "electronics", "croma", token.max_amount_paise)

        leg = 800_000  # under per-debit (₹10k); three fit the per-day ₹30k, the fourth busts it
        consumed = 0
        for _i in range(self.cfg.bustout_len):
            crosses = consumed + leg > token.max_per_day_paise
            merchant_id, _c, _p, _pref = self.rng.choice(HOME_MERCHANTS)
            if crosses:
                decision, code = F.DECISION_STEP_UP, F.CODE_STEPUP_SCOPE
            else:
                decision, code = F.DECISION_ALLOW, F.CODE_OK_ALLOW
            self._debit(
                token=token, agent_id=agent, merchant_id=merchant_id, env=env,
                amount=leg, family=F.FAMILY_VELOCITY_BUSTOUT,
                expected_decision=decision, expected_code=code,
                is_misuse=True, tool_risk=F.TOOL_RISK_MEDIUM,
                ts=self._advance(120),  # tight spacing, same day
                barrier=crosses,
                settlement=Settlement(
                    capture_when="allow_or_stepup", capture_amount_paise=leg,
                    then_dispute=True, expected_label=F.LABEL_MISUSE, expected_reason=F.REASON_DISPUTE,
                ),
            )
            consumed += leg  # the captured legs the driver will have folded

    def add_replay(self) -> None:
        """A seen request, sent twice. The first debit is legitimate and, once
        allowed, spends its nonce off the clock; the second reuses that exact nonce
        (a fresh evaluation_id — P1 keys on the nonce, not the id) and must be
        blocked as a duplicate. The replay carries a barrier: the driver waits for
        the first debit's nonce-spend to fold before sending it."""
        customer = self._customer()
        agent = self._agent()
        merchant_id, category, purpose, pref = self.rng.choice(HOME_MERCHANTS)
        token = self._token(customer)
        env = self._envelope(purpose, category, pref, token.max_amount_paise)
        shared_nonce = self._nonce()

        self._debit(
            token=token, agent_id=agent, merchant_id=merchant_id, env=env,
            amount=LEGIT_AMOUNT, family=F.FAMILY_LEGIT,
            expected_decision=F.DECISION_ALLOW, expected_code=F.CODE_OK_ALLOW,
            is_misuse=False, tool_risk=F.TOOL_RISK_LOW, nonce=shared_nonce,
            settlement=Settlement(capture_when="allow", capture_amount_paise=LEGIT_AMOUNT),
        )
        self._debit(
            token=token, agent_id=agent, merchant_id=merchant_id, env=env,
            amount=LEGIT_AMOUNT, family=F.FAMILY_REPLAY,
            expected_decision=F.DECISION_BLOCK, expected_code=F.CODE_BLOCKED_DUPLICATE,
            is_misuse=True, tool_risk=F.TOOL_RISK_LOW, nonce=shared_nonce, barrier=True,
            settlement=Settlement(capture_when="never"),  # a block moves no money
        )

    def add_stale_revoked(self, *, revoked_flavour: bool) -> None:
        """The authority itself is gone — P4. Revoked: the mandate was cancelled, so
        the driver emits a token.cancelled the labeler reads as a light MISUSE
        (cancellation), and the debit against it is blocked. Expired: the mandate's
        expiry is in the past, so the debit is blocked. Either way the block moves no
        money — the label, when there is one, comes from the cancellation, not the
        debit."""
        customer = self._customer()
        agent = self._agent()
        merchant_id, category, purpose, pref = self.rng.choice(HOME_MERCHANTS)
        if revoked_flavour:
            token = self._token(customer, status="cancelled")
            self.scn.cancellations.append(token.token_id)
        else:
            token = self._token(customer, expire_at=EXPIRE_IN_PAST)
        env = self._envelope(purpose, category, pref, token.max_amount_paise)
        self._debit(
            token=token, agent_id=agent, merchant_id=merchant_id, env=env,
            amount=LEGIT_AMOUNT, family=F.FAMILY_STALE_REVOKED,
            expected_decision=F.DECISION_BLOCK, expected_code=F.CODE_BLOCKED_AUTHORITY,
            is_misuse=True, tool_risk=F.TOOL_RISK_LOW,
            settlement=Settlement(capture_when="never"),
        )

    def add_mule_fan_in(self) -> None:
        """Many independent payers funnel into one collector merchant in a tight
        window. Each debit is individually unremarkable — bound, in-cap, to an
        allowed merchant — so the spine passes; the fan-in *shape* is what the graph
        engine must catch, lifting the network risk on the payers (§13). The
        collector's charges are later repudiated, so each is a settled MISUSE."""
        mule_merchant, _cat = MULE_MERCHANT
        for _ in range(self.cfg.fan_in_size):
            customer = self._customer()
            agent = self._agent()
            token = self._token(customer)
            env = self._envelope("purchase", "wallet-topup", "collector", token.max_amount_paise)
            self._debit(
                token=token, agent_id=agent, merchant_id=mule_merchant, env=env,
                amount=LEGIT_AMOUNT, family=F.FAMILY_MULE_FAN_IN,
                expected_decision=F.DECISION_STEP_UP, expected_code=F.CODE_STEPUP_RISK,
                is_misuse=True, tool_risk=F.TOOL_RISK_LOW,
                ts=self._advance(300),  # a burst-window fan-in, not spread over days
                settlement=Settlement(
                    capture_when="allow_or_stepup", capture_amount_paise=LEGIT_AMOUNT,
                    then_dispute=True, expected_label=F.LABEL_MISUSE, expected_reason=F.REASON_DISPUTE,
                ),
            )

    def add_shared_device_ring(self) -> None:
        """A dense clique: several customers cycling through a small shared pool of
        agents and one storefront. No debit is out of scope, but the customers are
        knit together far more tightly than real strangers ever are, so the
        component structure raises the network risk on all of them (§13)."""
        ring_merchant, _cat = RING_MERCHANT
        shared_agents = [self._agent(), self._agent()]  # the pool the ring shares
        for _ in range(self.cfg.ring_size):
            customer = self._customer()
            token = self._token(customer)
            env = self._envelope("purchase", "electronics", "ring-store", token.max_amount_paise)
            for agent in shared_agents:  # each ring member uses BOTH shared agents — the density
                self._debit(
                    token=token, agent_id=agent, merchant_id=ring_merchant, env=env,
                    amount=LEGIT_AMOUNT, family=F.FAMILY_SHARED_DEVICE_RING,
                    expected_decision=F.DECISION_STEP_UP, expected_code=F.CODE_STEPUP_RISK,
                    is_misuse=True, tool_risk=F.TOOL_RISK_LOW,
                    ts=self._advance(600),
                    settlement=Settlement(
                        capture_when="allow_or_stepup", capture_amount_paise=LEGIT_AMOUNT,
                        then_dispute=True, expected_label=F.LABEL_MISUSE, expected_reason=F.REASON_DISPUTE,
                    ),
                )

    def add_synchronised_fleet(self) -> None:
        """A fleet of agents debiting the same merchant, the same amount, at the same
        instant — the lockstep no organic population shows. Every debit shares one
        timestamp (the synchrony), which the behaviour and graph engines together are
        meant to flag (§11, §13)."""
        fleet_merchant, _cat = FLEET_MERCHANT
        instant = self._advance(3600)  # ONE shared instant for the whole fleet
        amount = 750_000
        for _ in range(self.cfg.fleet_size):
            customer = self._customer()
            agent = self._agent()
            token = self._token(customer)
            env = self._envelope("purchase", "gift-cards", "fleet-target", token.max_amount_paise)
            self._debit(
                token=token, agent_id=agent, merchant_id=fleet_merchant, env=env,
                amount=amount, family=F.FAMILY_SYNCHRONISED_FLEET,
                expected_decision=F.DECISION_STEP_UP, expected_code=F.CODE_STEPUP_RISK,
                is_misuse=True, tool_risk=F.TOOL_RISK_LOW, ts=instant,
                settlement=Settlement(
                    capture_when="allow_or_stepup", capture_amount_paise=amount,
                    then_dispute=True, expected_label=F.LABEL_MISUSE, expected_reason=F.REASON_DISPUTE,
                ),
            )

    # =======================================================================
    def build(self) -> Scenario:
        c = self.cfg
        for _ in range(c.n_legit):
            self.add_legit_session()
        for _ in range(c.n_cautious_legit):
            self.add_legit_session(cautious=True)
        for _ in range(c.n_intent_drift):
            self.add_intent_drift()
        for i in range(c.n_scope_overrun):
            self.add_scope_overrun(deny_flavour=(i % 2 == 0))
        for _ in range(c.n_velocity_bustout):
            self.add_velocity_bustout()
        for _ in range(c.n_replay):
            self.add_replay()
        for i in range(c.n_stale_revoked):
            self.add_stale_revoked(revoked_flavour=(i % 2 == 0))
        for _ in range(c.mule_fan_in_rings):
            self.add_mule_fan_in()
        for _ in range(c.shared_device_rings):
            self.add_shared_device_ring()
        for _ in range(c.synchronised_fleets):
            self.add_synchronised_fleet()

        self.scn.meta = {
            "seed": c.seed,
            "base_ts": c.base_ts,
            "generator_version": "0.1.0",
            "counts_by_family": self.scn.counts_by_family(),
            "totals": {
                "tokens": len(self.scn.tokens),
                "overlays": len(self.scn.overlays),
                "envelopes": len(self.scn.envelopes),
                "debits": len(self.scn.timeline),
                "cancellations": len(self.scn.cancellations),
                "misuse_debits": sum(1 for d in self.scn.timeline if d.is_misuse),
            },
            "legend": {fam: info._asdict() for fam, info in F.LEGEND.items()},
            "contract_notes": (
                "order_context() == proto OrderContext; tokens/overlays mirror internal/domain; "
                "labels come only from settled outcomes (the Go labeler), never from these tags."
            ),
        }
        self.scn.validate()
        return self.scn


def build_scenario(config: Optional[Config] = None) -> Scenario:
    """Build a reproducible Scenario from a Config (or the defaults). This is the
    package's front door — the live driver calls it (or loads its JSON) to get the
    world it will replay."""
    return Generator(config or Config()).build()
