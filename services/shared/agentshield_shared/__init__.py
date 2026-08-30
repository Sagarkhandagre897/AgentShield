"""agentshield_shared — the Python side of the AgentShield cross-plane seam.

Off-clock ML engines import this to consume bus events and deposit their one
calibrated figure back through the feature-materialiser (the single writer). The
schemas here mirror the Go ``domain`` and ``bus`` contracts field-for-field.
"""

from . import hmac_util, schema
from .schema import Event, FeatureRow, SignalDeviation, topic_for

__all__ = [
    "schema",
    "hmac_util",
    "Event",
    "FeatureRow",
    "SignalDeviation",
    "topic_for",
]

# bus imports confluent_kafka lazily; re-export its symbols only if importable so
# `import agentshield_shared` works for contract tests without the client.
try:
    from . import bus  # noqa: F401
    from .bus import DepositPublisher, EventConsumer  # noqa: F401

    __all__ += ["bus", "DepositPublisher", "EventConsumer"]
except Exception:  # pragma: no cover
    pass
