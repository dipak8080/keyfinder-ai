"""
credits/providers/ - Payment provider adapters.

ONE provider is implemented: Ko-fi. This package exists anyway, with a
registry and a shared event type, for a specific reason worth writing
down rather than assuming.

WHY A REGISTRY FOR A SINGLE PROVIDER
------------------------------------
Ko-fi pays out through PayPal or Stripe only. That is fine today - an
AU PayPal account is connected and already receiving - but it is a
dependency on someone else's country list, and it is the single thing
most likely to force a provider change later. If PayPal ever restricts
the account, or Ko-fi's 5% shop fee stops being worth it at volume, the
migration should be one new file in this directory, not a rewrite of
the webhook, the ledger and the receipt path.

The cost of the seam now is one dataclass and one dict. The cost of
retrofitting it after a provider change becomes urgent is a day of work
under pressure, with live orders in flight.

WHAT AN ADAPTER IS RESPONSIBLE FOR
----------------------------------
Exactly three things, all provider-specific:

  read_payload(request) -> dict     decode the transport (Ko-fi posts
                                    form-encoded with a JSON string in a
                                    'data' field; a JSON provider would
                                    just parse the body)
  verify(...)           -> bool     authenticate the request
  to_event(payload)     -> Event    map provider fields onto PaymentEvent

Everything downstream of to_event() - accounts, the ledger, pending
claims, receipts, idempotency, the orders table - never learns which
provider the money came from. That is what makes the seam real rather
than decorative: if adding a provider required touching the ledger,
the abstraction would be a lie.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class WebhookRejected(Exception):
    """Authentication failed. Surfaces as 401 - the provider should NOT
    retry, because a bad secret will still be bad in five minutes."""


class WebhookUnprocessable(Exception):
    """Authenticated, but the payload can't be turned into an event -
    a malformed body, or a shop item whose code matches no configured
    pack. Surfaces as 400: retrying won't help, and a 500 would make
    the provider redeliver it forever."""


@dataclass
class PaymentEvent:
    """What every provider is reduced to before anything is credited.

    provider_txid is the idempotency anchor: it becomes the ledger's
    idempotency_key and the orders table's provider_order_id, so the
    same payment delivered twice credits once. It must be the
    provider's own immutable payment id - NOT a delivery/message id,
    which can differ between retries of the same payment.
    """
    provider: str
    provider_txid: str
    email: str
    credits: int
    pack_keys: list[str]
    amount_usd: float
    currency: str
    # Delivery id, distinct from provider_txid. Used only for the
    # webhook_events replay log, so a retried delivery is visible as a
    # retry rather than silently collapsing into the payment row.
    delivery_id: str
    raw: dict[str, Any] = field(default_factory=dict)


# Populated at import. Keep this the ONLY place a provider name string
# is mapped to code - config validates against these keys, so a typo in
# PAYMENTS_PROVIDER fails at boot with the valid list, not at the first
# payment with a 404.
from . import kofi as _kofi  # noqa: E402  (circular-free: kofi imports nothing from here)

ADAPTERS = {
    _kofi.NAME: _kofi,
}

SUPPORTED_PROVIDERS = tuple(ADAPTERS)


def get_adapter(name: str):
    try:
        return ADAPTERS[name]
    except KeyError:
        raise RuntimeError(
            f"PAYMENTS_PROVIDER={name!r} has no adapter. Available: {SUPPORTED_PROVIDERS}"
        )