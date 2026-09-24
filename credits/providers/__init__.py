"""
credits/providers/ - Payment provider adapters.

Dodo Payments is the only provider. The registry stays so a future
provider is one new file here, not a rewrite of the webhook, the ledger
and the receipt path.

An adapter provides:

  read_payload(request) -> dict     decode the webhook body
  verify(...)           -> bool     authenticate the request
  to_event(payload)     -> Event    map provider fields onto PaymentEvent

Everything downstream of to_event() never learns which provider the
money came from.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class WebhookRejected(Exception):
    """Authentication failed. Surfaces as 401."""


class WebhookUnprocessable(Exception):
    """Authenticated, but the payload can't be turned into an event.
    Surfaces as 400 so the provider stops redelivering it."""


@dataclass
class PaymentEvent:
    """What every provider is reduced to before anything is credited.

    provider_txid is the idempotency anchor: the ledger's idempotency_key
    and the orders table's provider_order_id, so the same payment
    delivered twice credits once.
    """
    provider: str
    provider_txid: str
    email: str
    credits: int
    pack_keys: list[str]
    amount_usd: float
    currency: str
    # Delivery id, used only for the webhook_events replay log.
    delivery_id: str
    # Our own checkout reference (order_sources key), when it differs
    # from the payment id.
    order_ref: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


from . import dodo as _dodo  # noqa: E402

ADAPTERS = {
    _dodo.NAME: _dodo,
}

SUPPORTED_PROVIDERS = tuple(ADAPTERS)
DEFAULT_PROVIDER = _dodo.NAME


def get_adapter(name: str):
    try:
        return ADAPTERS[name]
    except KeyError:
        raise RuntimeError(
            f"payment provider {name!r} has no adapter. Available: {SUPPORTED_PROVIDERS}"
        )