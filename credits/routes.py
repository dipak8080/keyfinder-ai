"""
credits/routes.py - The three endpoints the frontend talks to.

    GET  /credits/me        balance, free allowance, paywall state, packs
    POST /credits/preview   will this job cost a credit? (UX only)
    POST /credits/claim     record intent to buy, before leaving for Ko-fi

None of these can charge anything. Charging happens exactly once, inside
the job-creation request, via paywall.guard() - see credits/paywall.py.
That separation is the point: an endpoint the browser can call freely
must never be able to move the ledger.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, EmailStr, Field

from . import claims, ledger, paywall
from .config import get_settings
from .identity import Identity

log = logging.getLogger("credits.routes")
router = APIRouter(prefix="/credits", tags=["credits"])


class PreviewRequest(BaseModel):
    tool: str = Field(..., max_length=64)
    input_seconds: float | None = Field(default=None, ge=0, le=60 * 60 * 24)


class ClaimRequest(BaseModel):
    email: EmailStr
    pack: str = Field(..., max_length=32)


@router.get("/me")
async def me(
    response: Response,
    identity: Identity = Depends(paywall.get_identity),
) -> dict:
    """Everything the UI needs in one call: balance, free ops remaining,
    which tools are metered, and the pack list with live buy links.

    Safe to call on every page load. It is also what mints the identity
    cookie for a first-time visitor, which is why it returns the same
    shape whether the paywall is on or off - the frontend reads
    paywall.enabled and renders nothing when it's false.

    NO-STORE IS LOAD-BEARING, not hygiene. This response contains a
    per-user balance behind a domain that sits on Cloudflare. A cached
    copy served to the wrong visitor would show them someone else's
    credits - and because the identity cookie is set on the same
    response, a cached Set-Cookie would hand two people the same subject
    id. Neither is recoverable by a later request.
    """
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, private"
    response.headers["Pragma"] = "no-cache"
    return ledger.summary(identity)


@router.post("/preview")
async def preview(
    body: PreviewRequest,
    identity: Identity = Depends(paywall.get_identity),
) -> dict:
    """What pressing Start will consume: nothing, a free op, or a credit.

    ADVISORY ONLY. The duration here comes from the browser, and the job
    endpoint re-probes the real file with ffprobe before charging. If the
    two disagree, the server's number is the one that counts - this
    exists so the button can say "uses 1 credit" before the click, not to
    decide anything.
    """
    return paywall.preview(identity, body.tool, body.input_seconds)


@router.post("/claim")
async def claim(
    body: ClaimRequest,
    identity: Identity = Depends(paywall.get_identity),
) -> dict:
    """Record which browser is about to buy which pack, keyed by email.

    THE WHOLE REASON THIS ENDPOINT EXISTS: Ko-fi's webhook carries no
    custom data. When the payment lands, the only thing identifying the
    buyer is the email they typed at Ko-fi's checkout - there is nothing
    tying it back to the tab they started from. Without this, every
    purchase would require clicking a magic link in an email before the
    credits appeared, which is a miserable first experience for someone
    who just paid three dollars.

    So the frontend asks for the email BEFORE redirecting to Ko-fi and
    posts it here. The webhook matches on it and links the account to
    this subject, and the credits show up in the tab they bought from.

    BEST-EFFORT BY DESIGN. If they pay with a different email, or take
    longer than CLAIM_TTL_MINUTES, the match simply misses - and the
    receipt email's magic link still works. Nothing is ever blocked by
    this table, and a miss costs a click, not a payment.

    Returns the buy URL so the caller does not need a second round trip.
    """
    settings = get_settings()
    pack = settings.pack(body.pack)
    if pack is None:
        raise HTTPException(status_code=404, detail={"error": "unknown_pack"})

    buy_url = pack.resolved_buy_url(settings.payments_provider, settings.provider_store_slug)
    if not buy_url:
        # Misconfiguration, not user error - the pack exists in code but
        # has no checkout link. get_settings() refuses to boot in this
        # state when the paywall is on, so reaching here means the
        # paywall is off and someone called this anyway.
        log.error("pack %r has no buy URL - PACK_%s_PRICE_REF is unset",
                  pack.key, pack.key.upper())
        raise HTTPException(status_code=503, detail={"error": "checkout_unavailable"})

    claims.record_claim(
        email=str(body.email),
        subject_id=identity.subject_id,
        pack=pack.key,
        ip_hash=identity.ip_hash,
    )

    return {
        "ok": True,
        "buy_url": buy_url,
        "pack": pack.key,
        "credits": pack.credits,
        "price_usd": pack.price_usd,
        "claim_expires_minutes": settings.claim_ttl_minutes,
    }