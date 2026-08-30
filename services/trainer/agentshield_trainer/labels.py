"""labels — the settled outcomes, read back as training labels (System Design §6).

The labeler is the only producer of outcomes.v1, and its rule is strict: a label
is emitted only for a settled outcome — a dispute, a cancellation, a confirmed
step-up — never for silence and never for our own verdict. This module simply
reads those label events back; it invents nothing, so the "where labels may come
from" rule is enforced upstream and honoured here by construction.

A token can settle more than once (a confirmed step-up that is later disputed),
so folding is deterministic: misuse dominates a legitimate label regardless of
order (a repudiated charge was misuse even if the human first waved it through),
and within one class the later outcome wins. Pure stdlib — no wheels needed to
assemble the label set.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, Optional, Tuple

try:  # the shared wire contract; bootstrap onto sys.path when run in-tree
    from agentshield_shared.schema import (
        EVENT_OUTCOME_LABELED,
        LABEL_MISUSE,
        PAYLOAD_LABEL,
        PAYLOAD_REASON,
        PAYLOAD_WEIGHT,
        Event,
    )
except ModuleNotFoundError:  # pragma: no cover
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))
    from agentshield_shared.schema import (
        EVENT_OUTCOME_LABELED,
        LABEL_MISUSE,
        PAYLOAD_LABEL,
        PAYLOAD_REASON,
        PAYLOAD_WEIGHT,
        Event,
    )


@dataclass(frozen=True)
class Label:
    """One settled label for a token: the target (1.0 misuse, 0.0 legit), how much
    to trust it, why it was assigned, and when it settled (for point-in-time
    alignment against the features)."""

    value: float
    weight: float
    reason: str
    occurred_at: int

    @property
    def is_misuse(self) -> bool:
        return self.value >= LABEL_MISUSE


def _dominates(incoming: Label, current: Label) -> bool:
    """Whether a newly-seen label should replace the one held for a token. Misuse
    dominates a legitimate label whichever arrived first; within a class, the later
    outcome wins."""
    if incoming.value != current.value:
        return incoming.value > current.value  # misuse (1.0) beats legit (0.0)
    return incoming.occurred_at >= current.occurred_at


class LabelSet:
    """The settled labels, keyed by token_id — the join key the events carry."""

    def __init__(self) -> None:
        self._by_token: Dict[str, Label] = {}

    def add(self, token_id: str, label: Label) -> None:
        if not token_id:
            return
        cur = self._by_token.get(token_id)
        if cur is None or _dominates(label, cur):
            self._by_token[token_id] = label

    def get(self, token_id: str) -> Optional[Label]:
        return self._by_token.get(token_id)

    def items(self) -> Iterator[Tuple[str, Label]]:
        return iter(self._by_token.items())

    def __len__(self) -> int:
        return len(self._by_token)

    def __contains__(self, token_id: str) -> bool:
        return token_id in self._by_token

    @classmethod
    def from_events(cls, events: Iterable[Event]) -> "LabelSet":
        """Fold every outcome.labeled event into the set. Non-label events are
        ignored, so a full event stream can be passed straight in."""
        ls = cls()
        for ev in events:
            if ev.type != EVENT_OUTCOME_LABELED:
                continue
            value = float(ev.payload.get(PAYLOAD_LABEL, 0.0))
            weight = float(ev.payload.get(PAYLOAD_WEIGHT, 1.0))
            reason = str(ev.payload.get(PAYLOAD_REASON, ""))
            ls.add(ev.token_id, Label(value, weight, reason, ev.occurred_at))
        return ls
