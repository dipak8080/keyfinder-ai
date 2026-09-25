"""
credits/providers/dodo.py - Dodo Payments adapter.

Dodo is the merchant of record. A checkout session is created server-side
for one pack, the buyer pays on Dodo's hosted page, and credits are granted
from either the confirm route (fast path, after the return redirect) or the
payment.succeeded webhook (backstop). Both key the ledger on the payment id.

Credits come from the PRODUCT the buyer paid for, mapped through
DODO_PRODUCT_<PACK>. The pack in metadata is only a fallback, and metadata
is written by this server at session creation, never by the buyer.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import os
import time

import requests

from ..config import get_settings

log = logging.getLogger("credits.providers.dodo")

NAME = "dodo"

LIVE_BASE = "https://live.dodopayments.com"
TEST_BASE = "https://test.dodopayments.com"

_TIMEOUT = 20
SIGNATURE_TOLERANCE_SECONDS = 300
PAID_STATUSES = ("succeeded",)
CREDIT_GRANTING_EVENTS = ("payment.succeeded",)
LOGGED_EVENTS = ("payment.failed",)
SUBSCRIPTION_EVENT_PREFIX = "subscription."


class DodoError(RuntimeError):
    pass


def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or "").strip()


def is_test() -> bool:
    return _env("DODO_ENVIRONMENT", "test").lower() != "live"


def api_base() -> str:
    return TEST_BASE if is_test() else LIVE_BASE


def _api_key() -> str:
    return _env("DODO_API_KEY")


def _webhook_secret() -> str:
    return _env("DODO_WEBHOOK_SECRET")


def return_url() -> str:
    return _env("DODO_RETURN_URL", "https://audioforges.com/checkout/success")


def product_id_for(pack_key: str) -> str:
    return _env(f"DODO_PRODUCT_{pack_key.upper()}")


def pass_product_id() -> str:
    return product_id_for("pass")


def pack_for_product(product_id: str):
    target = (product_id or "").strip()
    if not target:
        return None
    for pack in get_settings().packs_sorted():
        if product_id_for(pack.key) == target:
            return pack
    if pass_product_id() and pass_product_id() == target:
        return get_settings().pass_pack()
    return None


def _pack_by_key(key: str):
    s = get_settings()
    if key == "pass":
        return s.pass_pack()
    return s.pack(key)


def configured() -> bool:
    if not _api_key():
        return False
    return all(product_id_for(p.key) for p in get_settings().packs_sorted())


def _call(method: str, path: str, *, json_body=None) -> dict:
    if not _api_key():
        raise DodoError("DODO_API_KEY is not set")
    resp = requests.request(
        method,
        f"{api_base()}{path}",
        headers={
            "Authorization": f"Bearer {_api_key()}",
            "Content-Type": "application/json",
        },
        json=json_body,
        timeout=_TIMEOUT,
    )
    if resp.status_code >= 400:
        log.error("dodo %s %s -> %s: %s", method, path, resp.status_code, resp.text[:800])
        raise DodoError(f"Dodo rejected the request ({resp.status_code}).")
    if not resp.content:
        return {}
    return resp.json() or {}


def create_checkout(pack, email: str, ref: str) -> dict:
    product_id = product_id_for(pack.key)
    if not product_id:
        raise DodoError(f"DODO_PRODUCT_{pack.key.upper()} is not set")
    clean = (email or "").strip().lower()
    return _call(
        "POST",
        "/checkouts",
        json_body={
            "product_cart": [{"product_id": product_id, "quantity": 1}],
            "customer": {"email": clean},
            "return_url": return_url(),
            "metadata": {"af_ref": ref, "pack": pack.key, "email": clean},
            "feature_flags": {"allow_discount_code": False},
        },
    )


def get_payment(payment_id: str) -> dict:
    return _call("GET", f"/payments/{payment_id}")


def get_subscription(subscription_id: str) -> dict:
    return _call("GET", f"/subscriptions/{subscription_id}")


def list_subscription_payments(subscription_id: str) -> list[dict]:
    from urllib.parse import urlencode
    query = urlencode({"subscription_id": subscription_id, "status": "succeeded", "page_size": 100})
    return list((_call("GET", f"/payments?{query}") or {}).get("items") or [])


def create_portal_link(customer_id: str, return_url: str) -> str:
    from urllib.parse import quote, urlencode
    query = urlencode({"return_url": return_url})
    return str(_call("POST", f"/customers/{quote(customer_id)}/customer-portal/session?{query}").get("link") or "")


def set_cancel_at_period_end(subscription_id: str, cancel: bool) -> dict:
    return _call("PATCH", f"/subscriptions/{subscription_id}",
                 json_body={"cancel_at_next_billing_date": bool(cancel)})


def event_from_payment(payment: dict, *, delivery_id: str = "", raw: dict | None = None):
    """PaymentEvent for a succeeded payment, or None if it has not succeeded yet."""
    from . import PaymentEvent, WebhookUnprocessable

    payment_id = str(payment.get("payment_id") or "")
    if not payment_id:
        raise WebhookUnprocessable("payment carries no payment_id")

    if str(payment.get("status") or "").lower() not in PAID_STATUSES:
        return None

    metadata = payment.get("metadata") or {}

    credits = 0
    amount_usd = 0.0
    packs: list[str] = []
    for item in payment.get("product_cart") or []:
        pack = pack_for_product(str(item.get("product_id") or ""))
        if pack is None:
            log.error("dodo payment %s: product %r matches no DODO_PRODUCT_* pack",
                      payment_id, item.get("product_id"))
            continue
        try:
            qty = max(1, int(item.get("quantity") or 1))
        except (TypeError, ValueError):
            qty = 1
        credits += pack.credits * qty
        amount_usd += pack.price_usd * qty
        packs.append(pack.key)

    if credits <= 0:
        pack = _pack_by_key(str(metadata.get("pack") or ""))
        if pack is not None:
            credits, amount_usd, packs = pack.credits, pack.price_usd, [pack.key]

    subscription_id = str(payment.get("subscription_id") or "")
    if credits <= 0 and subscription_id:
        pack = pack_for_product(str(get_subscription(subscription_id).get("product_id") or ""))
        if pack is not None:
            credits, amount_usd, packs = pack.credits, pack.price_usd, [pack.key]

    if credits <= 0:
        raise WebhookUnprocessable(f"payment {payment_id} matched no configured pack")

    email = str(metadata.get("email") or "").strip().lower()
    if not email:
        email = str((payment.get("customer") or {}).get("email") or "").strip().lower()
    if not email:
        raise WebhookUnprocessable(f"payment {payment_id} carries no buyer email")

    return PaymentEvent(
        provider=NAME,
        provider_txid=payment_id,
        email=email,
        credits=credits,
        pack_keys=packs,
        amount_usd=round(amount_usd, 2),
        currency="USD",
        delivery_id=delivery_id or payment_id,
        order_ref=str(metadata.get("af_ref") or ""),
        raw=raw if raw is not None else payment,
    )


async def read_payload(request) -> dict:
    return await request.json()


def _secret_bytes(secret: str) -> bytes:
    if secret.startswith("whsec_"):
        return base64.b64decode(secret[len("whsec_"):])
    return secret.encode()


def verify(payload: dict, *, raw_body: bytes = b"", headers=None) -> bool:
    """Standard Webhooks: base64 HMAC-SHA256 over "<id>.<timestamp>.<raw body>"."""
    secret = _webhook_secret()
    if not secret or headers is None or not raw_body:
        log.warning("dodo webhook verification skipped: secret, headers or body missing")
        return False

    msg_id = headers.get("webhook-id") or ""
    ts = headers.get("webhook-timestamp") or ""
    sig_header = headers.get("webhook-signature") or ""
    if not (msg_id and ts and sig_header):
        return False

    try:
        if abs(time.time() - int(ts)) > SIGNATURE_TOLERANCE_SECONDS:
            log.warning("dodo webhook signature outside tolerance (ts=%s)", ts)
            return False
    except ValueError:
        return False

    try:
        key = _secret_bytes(secret)
    except (ValueError, TypeError):
        log.error("DODO_WEBHOOK_SECRET is not valid base64 after whsec_")
        return False

    signed = f"{msg_id}.{ts}.".encode() + raw_body
    expected = base64.b64encode(hmac.new(key, signed, hashlib.sha256).digest()).decode()

    for part in sig_header.split():
        _, _, sig = part.partition(",")
        if sig and hmac.compare_digest(expected, sig):
            return True
    return False


def to_event(payload: dict):
    event_type = str(payload.get("type") or "")
    data = payload.get("data") or {}

    if event_type not in CREDIT_GRANTING_EVENTS:
        if event_type.startswith(SUBSCRIPTION_EVENT_PREFIX):
            return None
        if event_type in LOGGED_EVENTS:
            log.warning("dodo %s for payment %s - review in the dashboard",
                        event_type, data.get("payment_id"))
        else:
            log.info("dodo %s event ignored", event_type or "unknown")
        return None

    return event_from_payment(data, delivery_id=str(data.get("payment_id") or ""), raw=payload)