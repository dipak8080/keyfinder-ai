"""
credits/dodo_routes.py - Endpoints the Dodo checkout calls.

    GET  /credits/dodo/config     is Dodo checkout available
    POST /credits/dodo/checkout   create a hosted checkout session for one pack
    POST /credits/dodo/confirm    grant credits once Dodo reports the payment succeeded
    POST /credits/dodo/pass/cancel   Studio Pass: stop at the end of the paid month
    POST /credits/dodo/pass/resume   Studio Pass: undo a scheduled cancel
    POST /credits/dodo/pass/portal   Studio Pass: Dodo billing portal link (card, invoices)

The payment.succeeded webhook is the backstop. Both paths key the ledger
on the payment id.
"""

from __future__ import annotations

import asyncio
import logging
import re

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, EmailStr, Field

from rate_limit import check_rate_limit

from . import claims, fulfil, paywall, subscriptions
from .config import get_settings
from .db import connect, now_iso, tx
from .identity import Identity
from .providers import WebhookUnprocessable
from .providers import dodo as dd
from .security import new_id
from .webhook import _enabled_providers

log = logging.getLogger("credits.dodo")
router = APIRouter(prefix="/credits/dodo", tags=["credits"])


def _rate_limited(max_requests: int, window_seconds: int):
    def dep(request: Request) -> None:
        check_rate_limit(request, max_requests=max_requests, window_seconds=window_seconds)
    return dep


class CheckoutRequest(BaseModel):
    pack: str = Field(..., max_length=32)
    email: EmailStr
    source: str | None = Field(default=None, max_length=64)
    tool: str | None = Field(default=None, max_length=64)
    page: str | None = Field(default=None, max_length=128)


class ConfirmRequest(BaseModel):
    payment_id: str = Field(..., max_length=64, pattern=r"^pay_[A-Za-z0-9]+$")


_TAG_RE = re.compile(r"[^a-z0-9_\-/:.]")


def _tag(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = _TAG_RE.sub("", value.strip().lower())[:64]
    return cleaned or None


def _enabled() -> bool:
    return dd.NAME in _enabled_providers(get_settings()) and dd.configured()


def _require_enabled() -> None:
    if not _enabled():
        raise HTTPException(status_code=503, detail={"error": "dodo_not_configured"})


def _record_source(ref: str, body: CheckoutRequest, identity: Identity) -> None:
    try:
        with connect() as conn, tx(conn):
            conn.execute(
                """INSERT OR REPLACE INTO order_sources
                   (provider, provider_order_id, source, tool, page, subject_id, created_at)
                   VALUES ('dodo', ?, ?, ?, ?, ?, ?)""",
                (ref, _tag(body.source), _tag(body.tool),
                 (body.page or "").strip()[:128] or None, identity.subject_id, now_iso()),
            )
    except Exception:  # noqa: BLE001
        log.exception("could not record order source for %s", ref)


@router.get("/config")
def dodo_config() -> dict:
    return {
        "enabled": _enabled(),
        "environment": "test" if dd.is_test() else "live",
    }


@router.post(
    "/checkout",
    dependencies=[Depends(_rate_limited(max_requests=15, window_seconds=3600))],
)
def create_checkout(
    body: CheckoutRequest,
    identity: Identity = Depends(paywall.get_identity),
) -> dict:
    _require_enabled()

    settings = get_settings()
    if body.pack == "pass":
        if not settings.studio_pass_enabled or not dd.pass_product_id():
            raise HTTPException(status_code=400, detail={"error": "pass_unavailable"})
        if subscriptions.has_active_pass(identity):
            raise HTTPException(status_code=409, detail={"error": "already_subscribed",
                                                         "message": "You already have an active Studio Pass."})
        pack = settings.pass_pack()
    else:
        pack = settings.pack(body.pack)
    if pack is None:
        raise HTTPException(status_code=400, detail={"error": "unknown_pack"})

    email = str(body.email).strip().lower()

    try:
        claims.record_claim(
            email=email, subject_id=identity.subject_id,
            pack=pack.key, ip_hash=identity.ip_hash,
        )
    except Exception:  # noqa: BLE001
        log.exception("could not record claim for %s", email)

    ref = new_id("dco_")
    _record_source(ref, body, identity)

    try:
        session = dd.create_checkout(pack, email, ref)
    except dd.DodoError as exc:
        log.error("dodo checkout creation failed for %s: %s", pack.key, exc)
        raise HTTPException(status_code=502, detail={"error": "dodo_unavailable"})

    checkout_url = str(session.get("checkout_url") or "")
    if not checkout_url:
        raise HTTPException(status_code=502, detail={"error": "dodo_no_checkout_url"})

    log.info("dodo checkout %s created: ref=%s pack=%s source=%s tool=%s",
             session.get("session_id"), ref, pack.key, _tag(body.source), _tag(body.tool))
    return {"checkout_url": checkout_url, "session_id": session.get("session_id")}


@router.post(
    "/confirm",
    dependencies=[Depends(_rate_limited(max_requests=60, window_seconds=3600))],
)
def confirm_payment(body: ConfirmRequest) -> dict:
    _require_enabled()

    try:
        payment = dd.get_payment(body.payment_id)
    except dd.DodoError:
        raise HTTPException(status_code=502, detail={"error": "dodo_unavailable"})

    try:
        event = dd.event_from_payment(payment)
    except WebhookUnprocessable as exc:
        log.error("dodo payment %s unprocessable: %s", body.payment_id, exc)
        raise HTTPException(status_code=400, detail={"error": "unprocessable", "message": str(exc)})

    if event is None:
        return {"ok": False, "pending": True, "status": str(payment.get("status") or "unknown")}

    try:
        granted, balance = fulfil.apply_payment(event)
    except Exception:  # noqa: BLE001
        log.exception("dodo payment %s: paid but fulfilment failed", event.provider_txid)
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

def _own_active_subscription(identity: Identity):
    row = subscriptions.current_for(identity)
    if row is None or row["status"] not in subscriptions.ACTIVE_STATUSES:
        raise HTTPException(status_code=404, detail={"error": "no_active_pass"})
    return row


def _set_pass_cancel(identity: Identity, cancel: bool) -> dict:
    _require_enabled()
    row = _own_active_subscription(identity)
    try:
        dd.set_cancel_at_period_end(row["subscription_id"], cancel)
    except dd.DodoError:
        raise HTTPException(status_code=502, detail={"error": "dodo_unavailable"})
    transition = subscriptions.set_cancel_flag(row["subscription_id"], cancel)
    if transition:
        try:
            fresh = subscriptions.current_for(identity)
            asyncio.run(subscriptions.notify(transition, dict(fresh) if fresh else None))
        except Exception:  # noqa: BLE001
            log.exception("pass %s email failed", transition)
    log.info("studio pass %s %s by %s", row["subscription_id"],
             "set to cancel at period end" if cancel else "resumed", identity.account_id)
    return {"ok": True, "studio_pass": subscriptions.summary(identity)}


@router.post("/pass/cancel", dependencies=[Depends(_rate_limited(max_requests=10, window_seconds=3600))])
def cancel_pass(identity: Identity = Depends(paywall.get_identity)) -> dict:
    return _set_pass_cancel(identity, True)


@router.post("/pass/resume", dependencies=[Depends(_rate_limited(max_requests=10, window_seconds=3600))])
def resume_pass(identity: Identity = Depends(paywall.get_identity)) -> dict:
    return _set_pass_cancel(identity, False)


@router.post("/pass/portal", dependencies=[Depends(_rate_limited(max_requests=20, window_seconds=3600))])
def pass_portal(identity: Identity = Depends(paywall.get_identity)) -> dict:
    """Dodo's hosted billing page: update card, see invoices, cancel.
    Works for active and on-hold passes, which is how a failed renewal
    gets fixed."""
    _require_enabled()
    row = subscriptions.current_for(identity)
    if row is None or not row["customer_id"]:
        raise HTTPException(status_code=404, detail={"error": "no_pass"})
    try:
        link = dd.create_portal_link(row["customer_id"], f"{get_settings().frontend_url}{subscriptions.MANAGE_PATH}")
    except dd.DodoError:
        raise HTTPException(status_code=502, detail={"error": "dodo_unavailable"})
    if not link:
        raise HTTPException(status_code=502, detail={"error": "dodo_no_portal_link"})
    return {"url": link}