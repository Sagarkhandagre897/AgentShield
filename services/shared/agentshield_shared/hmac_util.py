"""hmac_util — webhook signature helper.

Events that enter the bus from an outside webhook carry an HMAC the ingester
verifies before trusting them (domain.Event.hmac, "verified for webhooks"). This
is HMAC-SHA256 over the message bytes, hex-encoded, with a constant-time compare
so verification does not leak timing. The engines are inside the trust boundary
and publish without a signature; this helper is for any producer that sits on
the webhook edge.
"""

from __future__ import annotations

import hashlib
import hmac


def sign(secret: bytes | str, message: bytes) -> str:
    """Return the hex HMAC-SHA256 of message under secret."""
    if isinstance(secret, str):
        secret = secret.encode("utf-8")
    return hmac.new(secret, message, hashlib.sha256).hexdigest()


def verify(secret: bytes | str, message: bytes, signature: str) -> bool:
    """Constant-time check that signature is a valid HMAC-SHA256 of message."""
    return hmac.compare_digest(sign(secret, message), signature)
