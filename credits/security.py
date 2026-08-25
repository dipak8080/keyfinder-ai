"""Signing and hashing. Stdlib only — no new crypto dependency."""

from __future__ import annotations

import base64
import hashlib
import hmac
import ipaddress
import secrets
import uuid

from .config import get_settings


def new_id(prefix: str = "") -> str:
    raw = uuid.uuid4().hex
    return f"{prefix}{raw}" if prefix else raw


def new_token(nbytes: int = 32) -> str:
    return secrets.token_urlsafe(nbytes)


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def sign(value: str, *, purpose: str) -> str:
    """Return 'value.signature'. The purpose is mixed into the MAC, so a
    session cookie can never be replayed as an identity cookie."""
    key = get_settings().secret_key.encode()
    sig = hmac.new(key, f"{purpose}:{value}".encode(), hashlib.sha256).digest()
    return f"{value}.{_b64(sig)}"


def unsign(signed: str | None, *, purpose: str) -> str | None:
    """Return the value if the signature is valid, else None."""
    if not signed or "." not in signed:
        return None
    value, _, sig = signed.rpartition(".")
    if not value:
        return None
    expected = sign(value, purpose=purpose).rpartition(".")[2]
    return value if hmac.compare_digest(sig, expected) else None


def hash_token(token: str) -> str:
    """Magic-link tokens are stored only as this hash."""
    key = get_settings().secret_key.encode()
    return hmac.new(key, f"token:{token}".encode(), hashlib.sha256).hexdigest()


def hash_ip(ip: str | None) -> str:
    """Salted, truncated hash — raw IPs are never stored.

    IPv6 collapses to its /64 so one household can't farm free jobs by
    rotating addresses inside its own prefix.
    """
    if not ip:
        return "unknown"
    try:
        addr = ipaddress.ip_address(ip.strip())
        if addr.version == 6:
            normalised = str(ipaddress.ip_network(f"{addr}/64", strict=False).network_address)
        else:
            normalised = str(addr)
    except ValueError:
        normalised = ip.strip()
    salt = get_settings().ip_hash_salt.encode()
    return hmac.new(salt, f"ip:{normalised}".encode(), hashlib.sha256).hexdigest()[:32]


# ---------------------------------------------------------------------------
# Webhook authentication
# ---------------------------------------------------------------------------
#
# Provider-specific verification lives in credits/providers/<name>.py,
# NOT here. Ko-fi authenticates with a token inside the body; a provider
# added later might sign the raw body with an HMAC header instead, and
# that difference belongs next to the code that parses that provider's
# payload rather than in a shared dispatch function that has to know
# about all of them.
#
# An earlier version of this file carried verify_lemonsqueezy_signature()
# and verify_paddle_signature() alongside Ko-fi's, plus a
# verify_webhook() that switched on PAYMENTS_PROVIDER. Both were removed:
# they were dead code for providers that were never wired up, and
# untested crypto that nothing calls is worse than no crypto at all -
# it reads as "this is handled" to whoever comes next.