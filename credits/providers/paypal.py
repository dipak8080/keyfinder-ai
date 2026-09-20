"""
credits/providers/paypal.py - PayPal adapter.

Unlike Ko-fi there is no hosted shop link. An order is created
server-side, the buyer approves it in the PayPal popup, and the capture
is confirmed server-side. Credits are granted from the CAPTURE RESULT,
not from the webhook, because PayPal sandbox webhooks are routinely
delayed or dropped. The webhook remains registered as a backup and is
idempotent against the same capture id.

custom_id carries "<pack_key>|<email>" through the whole flow, so both
the capture path and the webhook path know what was bought without a
lookup. PayPal caps it at 127 characters.
"""

from __future__ import annotations

import base64
import logging
import os
import threading
import time

import requests

from ..config import get_settings

log = logging.getLogger("credits.providers.paypal")

NAME = "paypal"

LIVE_BASE = "https://api-m.paypal.com"
SANDBOX_BASE = "https://api-m.sandbox.paypal.com"

_TIMEOUT = 20
_token_lock = threading.Lock()
_token_cache: dict[str, tuple[str, float]] = {}


class PayPalError(RuntimeError):
    pass


def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or "").strip()


def is_sandbox() -> bool:
    return _env("PAYPAL_ENV", "sandbox").lower() != "live"


def api_base() -> str:
    return SANDBOX_BASE if is_sandbox() else LIVE_BASE


def client_id() -> str:
    return _env("PAYPAL_CLIENT_ID")


def _client_secret() -> str:
    return _env("PAYPAL_CLIENT_SECRET")


def webhook_id() -> str:
    return _env("PAYPAL_WEBHOOK_ID")


def brand_name() -> str:
    return _env("PAYPAL_BRAND_NAME", "AudioForges")


def soft_descriptor() -> str:
    return _env("PAYPAL_SOFT_DESCRIPTOR", "AUDIOFORGES")[:22]


def currency() -> str:
    return _env("PAYPAL_CURRENCY", "USD").upper()


def configured() -> bool:
    return bool(client_id() and _client_secret())


def _access_token() -> str:
    key = f"{api_base()}:{client_id()}"
    with _token_lock:
        cached = _token_cache.get(key)
        if cached and cached[1] > time.time() + 60:
            return cached[0]

    if not configured():
        raise PayPalError("PAYPAL_CLIENT_ID / PAYPAL_CLIENT_SECRET are not set")

    basic = base64.b64encode(f"{client_id()}:{_client_secret()}".encode()).decode()
    resp = requests.post(
        f"{api_base()}/v1/oauth2/token",
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={"grant_type": "client_credentials"},
        timeout=_TIMEOUT,
    )
    if resp.status_code != 200:
        log.error("paypal token failed %s: %s", resp.status_code, resp.text[:500])
        raise PayPalError("Could not authenticate with PayPal.")

    body = resp.json()
    token = str(body.get("access_token") or "")
    if not token:
        raise PayPalError("PayPal returned no access token.")

    with _token_lock:
        _token_cache[key] = (token, time.time() + float(body.get("expires_in") or 300))
    return token


def _call(method: str, path: str, *, json_body=None, request_id: str | None = None) -> dict:
    headers = {
        "Authorization": f"Bearer {_access_token()}",
        "Content-Type": "application/json",
    }
    if request_id:
        headers["PayPal-Request-Id"] = request_id

    resp = requests.request(
        method, f"{api_base()}{path}", headers=headers, json=json_body, timeout=_TIMEOUT
    )
    if resp.status_code >= 400:
        log.error("paypal %s %s -> %s: %s", method, path, resp.status_code, resp.text[:800])
        raise PayPalError(f"PayPal rejected the request ({resp.status_code}).")
    if not resp.content:
        return {}
    return resp.json()


def build_custom_id(pack_key: str, email: str) -> str:
    return f"{pack_key}|{(email or '').strip().lower()}"[:127]


def parse_custom_id(custom_id: str) -> tuple[str, str]:
    raw = str(custom_id or "")
    pack_key, _, email = raw.partition("|")
    return pack_key.strip(), email.strip().lower()


def create_order(pack, email: str) -> dict:
    """Creates a CAPTURE-intent order for one pack. Returns the raw order."""
    body = {
        "intent": "CAPTURE",
        "purchase_units": [
            {
                "custom_id": build_custom_id(pack.key, email),
                "description": f"{pack.credits} AudioForges credits"[:127],
                "soft_descriptor": soft_descriptor(),
                "amount": {
                    "currency_code": currency(),
                    "value": f"{pack.price_usd:.2f}",
                },
            }
        ],
        "payment_source": {
            "paypal": {
                "experience_context": {
                    "brand_name": brand_name(),
                    "shipping_preference": "NO_SHIPPING",
                    "user_action": "PAY_NOW",
                }
            }
        },
    }
    return _call("POST", "/v2/checkout/orders", json_body=body)


def capture_order(order_id: str) -> dict:
    """Captures an approved order. PayPal-Request-Id keyed on the order id
    so a retried capture returns the original result instead of a second
    charge."""
    return _call(
        "POST",
        f"/v2/checkout/orders/{order_id}/capture",
        json_body={},
        request_id=f"af-capture-{order_id}",
    )


def get_order(order_id: str) -> dict:
    return _call("GET", f"/v2/checkout/orders/{order_id}")


def _capture_node(order: dict) -> dict:
    for unit in order.get("purchase_units") or []:
        for capture in (unit.get("payments") or {}).get("captures") or []:
            return capture
    return {}


def event_from_order(order: dict):
    """Maps a captured order onto a PaymentEvent, or None if it did not
    complete. Used by the capture route; the webhook path uses to_event."""
    from . import PaymentEvent, WebhookUnprocessable

    if str(order.get("status") or "").upper() != "COMPLETED":
        return None

    capture = _capture_node(order)
    capture_id = str(capture.get("id") or "")
    if not capture_id:
        raise WebhookUnprocessable("captured order carries no capture id")

    unit = (order.get("purchase_units") or [{}])[0]
    custom_id = capture.get("custom_id") or unit.get("custom_id") or ""
    pack_key, claimed_email = parse_custom_id(custom_id)

    # The typed email wins. It is what the pending claim is keyed on and
    # where the receipt goes; the PayPal account's address is incidental
    # and is often a different one. Matches the webhook path below.
    payer_email = str(((order.get("payer") or {}).get("email_address")) or "").strip().lower()
    email = claimed_email or payer_email
    if not email:
        raise WebhookUnprocessable(f"capture {capture_id} carries no buyer email")

    return _build_event(capture_id, pack_key, email, capture.get("amount") or unit.get("amount"), order)


def _build_event(capture_id: str, pack_key: str, email: str, amount: dict | None, raw: dict):
    from . import PaymentEvent, WebhookUnprocessable

    settings = get_settings()
    amount = amount or {}
    try:
        paid = float(amount.get("value") or 0)
    except (TypeError, ValueError):
        paid = 0.0

    # The amount guard below compares numbers, so the currency must be
    # ours first: 20.00 IDR would otherwise pass for the $20 pack. An
    # order forged with the public client id controls its own currency.
    paid_currency = str(amount.get("currency_code") or "").upper()
    if paid_currency != currency():
        raise WebhookUnprocessable(
            f"capture {capture_id} paid in {paid_currency or 'unknown currency'}, expected {currency()}"
        )

    pack = settings.pack(pack_key) if pack_key else None
    if pack is None:
        pack = settings.pack_by_amount(paid)
    if pack is None:
        raise WebhookUnprocessable(
            f"capture {capture_id} matches no configured pack (custom_id={pack_key!r}, paid={paid})"
        )

    # Guard against a tampered order: the amount actually captured must
    # match the pack's price. An order created outside our own create
    # route could otherwise claim a 100-credit pack for a dollar.
    if paid + 0.01 < pack.price_usd:
        raise WebhookUnprocessable(
            f"capture {capture_id} paid {paid} for pack {pack.key} priced {pack.price_usd}"
        )

    return PaymentEvent(
        provider=NAME,
        provider_txid=capture_id,
        email=email,
        credits=pack.credits,
        pack_keys=[pack.key],
        amount_usd=paid,
        currency=str(amount.get("currency_code") or currency()),
        delivery_id=capture_id,
        raw=raw,
    )


async def read_payload(request) -> dict:
    return await request.json()


def verify(payload: dict, *, raw_body: bytes = b"", headers=None) -> bool:
    """Asks PayPal whether the signature on this delivery is genuine.

    There is no local HMAC to check: PayPal signs with a rotating cert
    and exposes verification as an API call. A missing PAYPAL_WEBHOOK_ID
    fails closed rather than accepting everything.
    """
    wid = webhook_id()
    if not wid or headers is None:
        log.warning("paypal webhook verification skipped: PAYPAL_WEBHOOK_ID not set")
        return False

    required = {
        "transmission_id": headers.get("paypal-transmission-id"),
        "transmission_time": headers.get("paypal-transmission-time"),
        "cert_url": headers.get("paypal-cert-url"),
        "auth_algo": headers.get("paypal-auth-algo"),
        "transmission_sig": headers.get("paypal-transmission-sig"),
    }
    if not all(required.values()):
        log.warning("paypal webhook missing signature headers")
        return False

    try:
        result = _call(
            "POST",
            "/v1/notifications/verify-webhook-signature",
            json_body={**required, "webhook_id": wid, "webhook_event": payload},
        )
    except PayPalError:
        return False

    return str(result.get("verification_status") or "").upper() == "SUCCESS"


CREDIT_GRANTING_EVENTS = ("PAYMENT.CAPTURE.COMPLETED",)


def to_event(payload: dict):
    from . import WebhookUnprocessable

    event_type = str(payload.get("event_type") or "")
    if event_type not in CREDIT_GRANTING_EVENTS:
        log.info("paypal %s event ignored", event_type or "unknown")
        return None

    resource = payload.get("resource") or {}
    capture_id = str(resource.get("id") or "")
    if not capture_id:
        raise WebhookUnprocessable("capture event carries no resource id")

    pack_key, claimed_email = parse_custom_id(resource.get("custom_id") or "")
    email = claimed_email
    if not email:
        payer = (resource.get("payer") or {}).get("email_address")
        email = str(payer or "").strip().lower()
    if not email:
        raise WebhookUnprocessable(f"capture {capture_id} carries no buyer email")

    return _build_event(capture_id, pack_key, email, resource.get("amount"), payload)