"""
credits/paypal_routes.py - The two endpoints the PayPal buttons call.

    POST /credits/paypal/order     create an order for one pack
    POST /credits/paypal/capture   capture it and grant the credits

Credits are granted here, from the capture response, rather than waiting
on PAYMENT.CAPTURE.COMPLETED. PayPal's sandbox drops and delays webhooks
often enough that a buyer would sit staring at an unchanged balance. The
webhook stays registered as the backup path and cannot double-credit:
both routes key the ledger on the same capture id.

Sync handlers for the same reason as credits/routes.py - the SQLite work
blocks, so FastAPI's threadpool is where it belongs.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr, Field

from . import claims, fulfil, paywall
from .config import get_settings
from .identity import Identity
from .providers import WebhookUnprocessable
from .providers import paypal as pp

log = logging.getLogger("credits.paypal")
router = APIRouter(prefix="/credits/paypal", tags=["credits"])


class OrderRequest(BaseModel):
    pack: str = Field(..., max_length=32)
    email: EmailStr


class CaptureRequest(BaseModel):
    order_id: str = Field(..., max_length=64)


def _require_configured() -> None:
    if not pp.configured():
        raise HTTPException(status_code=503, detail={"error": "paypal_not_configured"})


@router.get("/config")
def paypal_config() -> dict:
    """Client id and currency for the JS SDK. No secret leaves the server."""
    return {
        "enabled": pp.configured(),
        "client_id": pp.client_id(),
        "currency": pp.currency(),
        "sandbox": pp.is_sandbox(),
    }


@router.post("/order")
def create_order(
    body: OrderRequest,
    identity: Identity = Depends(paywall.get_identity),
) -> dict:
    _require_configured()

    pack = get_settings().pack(body.pack)
    if pack is None:
        raise HTTPException(status_code=400, detail={"error": "unknown_pack"})

    email = str(body.email).strip().lower()

    # Same claim the Ko-fi flow records before leaving the site, so the
    # credits land in the tab that bought them even if the buyer pays
    # with a different PayPal email.
    try:
        claims.record_claim(
            email=email, subject_id=identity.subject_id,
            pack=pack.key, ip_hash=identity.ip_hash,
        )
    except Exception:  # noqa: BLE001
        log.exception("could not record claim for %s", email)

    try:
        order = pp.create_order(pack, email)
    except pp.PayPalError as exc:
        log.error("paypal order creation failed for %s: %s", pack.key, exc)
        raise HTTPException(status_code=502, detail={"error": "paypal_unavailable"})

    order_id = str(order.get("id") or "")
    if not order_id:
        raise HTTPException(status_code=502, detail={"error": "paypal_no_order_id"})

    log.info("paypal order %s created: pack=%s %.2f %s", order_id, pack.key,
             pack.price_usd, pp.currency())
    return {"order_id": order_id}


@router.post("/capture")
def capture_order(body: CaptureRequest) -> dict:
    _require_configured()

    try:
        order = pp.capture_order(body.order_id)
    except pp.PayPalError as exc:
        log.error("paypal capture failed for %s: %s", body.order_id, exc)
        raise HTTPException(status_code=502, detail={"error": "capture_failed"})

    try:
        event = pp.event_from_order(order)
    except WebhookUnprocessable as exc:
        log.error("paypal capture %s unprocessable: %s", body.order_id, exc)
        raise HTTPException(status_code=400, detail={"error": "unprocessable", "message": str(exc)})

    if event is None:
        status = str(order.get("status") or "unknown")
        log.warning("paypal order %s captured with status %s", body.order_id, status)
        raise HTTPException(status_code=402, detail={"error": "not_completed", "status": status})

    try:
        granted, balance = fulfil.apply_payment(event)
    except Exception:  # noqa: BLE001
        # The money moved. Do not tell the browser it failed in a way that
        # invites a second purchase - the webhook will retry fulfilment.
        log.exception("paypal capture %s: payment taken but fulfilment failed", event.provider_txid)
        raise HTTPException(status_code=500, detail={"error": "fulfilment_failed"})

    if granted:
        try:
            asyncio.run(fulfil.send_receipt(event, balance))
        except Exception:  # noqa: BLE001
            log.exception("receipt failed for %s - credits WERE granted", event.email)

    return {
        "ok": True,
        "credits": event.credits,
        "balance": balance,
        "already_applied": not granted,
    }