"""
credits/providers/paddle.py - Paddle Billing adapter.

Paddle is the merchant of record. A transaction is created server-side
for one pack, Paddle.js opens its overlay against that transaction id,
and credits are granted from either the confirm route (fast path) or the
transaction.completed webhook (backstop). Both key the ledger on the
transaction id, so a payment credits once.

Credits come from the PRICE the buyer actually paid for, mapped through
PADDLE_PRICE_<PACK>. Transaction custom_data only carries the email and
is never trusted for the amount.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import time

import requests

from ..config import get_settings

log = logging.getLogger("credits.providers.paddle")

NAME = "paddle"

LIVE_BASE = "https://api.paddle.com"
SANDBOX_BASE = "https://sandbox-api.paddle.com"

_TIMEOUT = 20
SIGNATURE_TOLERANCE_SECONDS = 300
PAID_STATUSES = ("paid", "completed")
CREDIT_GRANTING_EVENTS = ("transaction.completed",)


class PaddleError(RuntimeError):
    pass


def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or "").strip()


def is_sandbox() -> bool:
    return _env("PADDLE_ENV", "sandbox").lower() != "live"


def api_base() -> str:
    return SANDBOX_BASE if is_sandbox() else LIVE_BASE


def client_token() -> str:
    return _env("PADDLE_CLIENT_TOKEN")


def _api_key() -> str:
    return _env("PADDLE_API_KEY")


def _webhook_secret() -> str:
    return _env("PADDLE_WEBHOOK_SECRET")


def price_id_for(pack_key: str) -> str:
    return _env(f"PADDLE_PRICE_{pack_key.upper()}")


def pack_for_price(price_id: str):
    target = (price_id or "").strip()
    if not target:
        return None
    for pack in get_settings().packs_sorted():
        if price_id_for(pack.key) == target:
            return pack
    return None


def configured() -> bool:
    if not (_api_key() and client_token()):
        return False
    return all(price_id_for(p.key) for p in get_settings().packs_sorted())


def _call(method: str, path: str, *, json_body=None) -> dict:
    if not _api_key():
        raise PaddleError("PADDLE_API_KEY is not set")
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
        log.error("paddle %s %s -> %s: %s", method, path, resp.status_code, resp.text[:800])
        raise PaddleError(f"Paddle rejected the request ({resp.status_code}).")
    if not resp.content:
        return {}
    return (resp.json() or {}).get("data") or {}


def create_transaction(pack, email: str) -> dict:
    price_id = price_id_for(pack.key)
    if not price_id:
        raise PaddleError(f"PADDLE_PRICE_{pack.key.upper()} is not set")
    return _call(
        "POST",
        "/transactions",
        json_body={
            "items": [{"price_id": price_id, "quantity": 1}],
            "custom_data": {"pack": pack.key, "email": (email or "").strip().lower()},
        },
    )


def get_transaction(transaction_id: str) -> dict:
    return _call("GET", f"/transactions/{transaction_id}")


def _customer_email(customer_id: str) -> str:
    if not customer_id:
        return ""
    try:
        customer = _call("GET", f"/customers/{customer_id}")
    except PaddleError:
        return ""
    return str(customer.get("email") or "").strip().lower()


def event_from_transaction(txn: dict, *, delivery_id: str = "", raw: dict | None = None):
    """PaymentEvent for a paid transaction, or None if it has not been paid yet."""
    from . import PaymentEvent, WebhookUnprocessable

    txn_id = str(txn.get("id") or "")
    if not txn_id:
        raise WebhookUnprocessable("transaction carries no id")

    if str(txn.get("status") or "").lower() not in PAID_STATUSES:
        return None

    credits = 0
    amount_usd = 0.0
    packs: list[str] = []
    for item in txn.get("items") or []:
        price_id = str((item.get("price") or {}).get("id") or item.get("price_id") or "")
        pack = pack_for_price(price_id)
        if pack is None:
            log.error("paddle txn %s: price %r matches no PADDLE_PRICE_* pack", txn_id, price_id)
            continue
        try:
            qty = max(1, int(item.get("quantity") or 1))
        except (TypeError, ValueError):
            qty = 1
        credits += pack.credits * qty
        amount_usd += pack.price_usd * qty
        packs.append(pack.key)

    if credits <= 0:
        raise WebhookUnprocessable(f"transaction {txn_id} matched no configured pack")

    custom = txn.get("custom_data") or {}
    email = str(custom.get("email") or "").strip().lower()
    if not email:
        email = _customer_email(str(txn.get("customer_id") or ""))
    if not email:
        raise WebhookUnprocessable(f"transaction {txn_id} carries no buyer email")

    return PaymentEvent(
        provider=NAME,
        provider_txid=txn_id,
        email=email,
        credits=credits,
        pack_keys=packs,
        amount_usd=round(amount_usd, 2),
        currency="USD",
        delivery_id=delivery_id or txn_id,
        order_ref=txn_id,
        raw=raw if raw is not None else txn,
    )


async def read_payload(request) -> dict:
    return await request.json()


def _parse_signature(header: str) -> tuple[str, list[str]]:
    ts = ""
    sigs: list[str] = []
    for part in (header or "").split(";"):
        key, _, value = part.strip().partition("=")
        if key == "ts":
            ts = value
        elif key == "h1" and value:
            sigs.append(value)
    return ts, sigs


def verify(payload: dict, *, raw_body: bytes = b"", headers=None) -> bool:
    """HMAC-SHA256 over "<ts>:<raw body>" with the destination's secret."""
    secret = _webhook_secret()
    if not secret or headers is None or not raw_body:
        log.warning("paddle webhook verification skipped: secret, headers or body missing")
        return False

    ts, sigs = _parse_signature(headers.get("paddle-signature") or "")
    if not ts or not sigs:
        return False

    try:
        if abs(time.time() - int(ts)) > SIGNATURE_TOLERANCE_SECONDS:
            log.warning("paddle webhook signature outside tolerance (ts=%s)", ts)
            return False
    except ValueError:
        return False

    expected = hmac.new(
        secret.encode(), f"{ts}:".encode() + raw_body, hashlib.sha256
    ).hexdigest()
    return any(hmac.compare_digest(expected, sig) for sig in sigs)


def to_event(payload: dict):
    event_type = str(payload.get("event_type") or "")
    if event_type not in CREDIT_GRANTING_EVENTS:
        log.info("paddle %s event ignored", event_type or "unknown")
        return None

    delivery = str(payload.get("event_id") or payload.get("notification_id") or "")
    return event_from_transaction(payload.get("data") or {}, delivery_id=delivery, raw=payload)