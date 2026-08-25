"""
credits/providers/kofi.py - Ko-fi adapter.

TRANSPORT (from Ko-fi's own API docs page, verified against the live
examples it renders there):

    POST <webhook url>
    Content-Type: application/x-www-form-urlencoded
    body: data=<url-encoded JSON string>

    {
      "verification_token": "...",     # shared secret, IN THE BODY
      "message_id": "...",             # DELIVERY id - changes on retry
      "type": "Shop Order",            # or Donation / Subscription / Commission
      "email": "buyer@example.com",
      "amount": "8.00",                # STRING, and it's the ORDER TOTAL
      "currency": "USD",
      "kofi_transaction_id": "...",    # PAYMENT id - stable across retries
      "shop_items": [
        {"direct_link_code": "d436971309", "variation_name": null, "quantity": 1}
      ]
    }

THREE THINGS THAT SHAPE THIS FILE
---------------------------------
1. NO REQUEST SIGNATURE. Authentication is a shared token sent as plain
   text inside the body. It is compared in constant time and never
   logged - and unlike an HMAC it does not cover the body at all, so a
   leaked token lets anyone forge orders. That is Ko-fi's design, not a
   choice available here; the mitigations are HTTPS-only, and recording
   every order with its transaction id so forgeries are auditable and
   reversible.

2. NO CUSTOM DATA. Nothing in the payload identifies the browser that
   paid - only the buyer's email. That single gap is the entire reason
   credits/claims.py exists.

3. message_id IS NOT THE PAYMENT ID. Ko-fi retries "a reasonable number
   of times" until it gets a 200, and its docs describe the retry as
   carrying the same message_id - but the field that is guaranteed to
   identify the PAYMENT is kofi_transaction_id. The ledger keys on the
   latter and the replay log on the former, so a retry is recorded as a
   retry while still being unable to double-credit.
"""

from __future__ import annotations

import json
import logging

from ..config import get_settings

log = logging.getLogger("credits.providers.kofi")

NAME = "kofi"

# Only this type grants credits. Donations and subscriptions are real
# Ko-fi events that will arrive at this webhook and must be accepted
# (200) and ignored - returning an error would make Ko-fi retry a tip
# forever.
CREDIT_GRANTING_TYPES = ("Shop Order",)


async def read_payload(request) -> dict:
    """Decode Ko-fi's form-encoded envelope into the inner JSON."""
    form = await request.form()
    data = form.get("data")
    if not data:
        raise ValueError("no 'data' field in form body")
    return json.loads(data)


def verify(payload: dict, *, raw_body: bytes = b"", headers=None) -> bool:
    """Constant-time compare of the body's token against the configured
    secret. raw_body/headers are accepted and unused - they're part of
    the adapter interface for providers that sign the request instead.
    """
    import hmac

    expected = get_settings().webhook_secret
    received = payload.get("verification_token")
    if not expected or not received:
        return False
    return hmac.compare_digest(str(expected).strip(), str(received).strip())


def _resolve_pack(item: dict, order_total_usd: float):
    """Which pack is this line item?

    Primary: the shop item's direct_link_code, matched against
    PACK_*_PRICE_REF. Exact, and correct even for a multi-item order.

    Fallback: the order total against a pack price. This exists for one
    real case - someone tips the exact price of a pack instead of using
    the shop link, which happens more than you'd expect on Ko-fi. It's
    only safe because config refuses to boot with two packs at the same
    price, so an amount can never be ambiguous.
    """
    settings = get_settings()
    code = str(item.get("direct_link_code") or "").strip()
    pack = settings.pack_by_price_ref(code) if code else None
    if pack is None:
        pack = settings.pack_by_amount(order_total_usd)
    return pack


def to_event(payload: dict):
    """Map a Ko-fi payload onto a PaymentEvent, or None if it isn't a
    credit-granting event (a tip, a membership payment)."""
    from . import PaymentEvent, WebhookUnprocessable

    event_type = str(payload.get("type") or "")
    delivery_id = str(payload.get("message_id") or "")

    if event_type not in CREDIT_GRANTING_TYPES:
        log.info("ko-fi %s event ignored (not a shop order)", event_type or "unknown")
        return None

    email = str(payload.get("email") or "").strip().lower()
    txid = str(payload.get("kofi_transaction_id") or "")
    if not email or not txid:
        raise WebhookUnprocessable("shop order missing email or kofi_transaction_id")

    try:
        total = float(payload.get("amount") or 0)
    except (TypeError, ValueError):
        total = 0.0

    items = payload.get("shop_items") or [{"direct_link_code": "", "quantity": 1}]

    credits = 0
    packs: list[str] = []
    for item in items:
        try:
            qty = max(1, int(item.get("quantity") or 1))
        except (TypeError, ValueError):
            qty = 1
        pack = _resolve_pack(item, total)
        if pack is None:
            # Logged loudly and skipped rather than failing the whole
            # order: a mixed order containing one unknown item should
            # still credit the items that DID match, and the operator
            # can top up the difference from the admin endpoint.
            log.error(
                "ko-fi order %s: item %r (order total %.2f) matches no configured pack - "
                "check PACK_*_PRICE_REF against the shop item codes",
                txid, item.get("direct_link_code"), total,
            )
            continue
        credits += pack.credits * qty
        packs.append(pack.key)

    if credits <= 0:
        raise WebhookUnprocessable(
            f"order {txid} matched no known pack (total={total} {payload.get('currency')})"
        )

    return PaymentEvent(
        provider=NAME,
        provider_txid=txid,
        email=email,
        credits=credits,
        pack_keys=packs,
        amount_usd=total,
        currency=str(payload.get("currency") or "USD"),
        delivery_id=delivery_id or txid,
        raw=payload,
    )