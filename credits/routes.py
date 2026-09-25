"""
credits/routes.py - The endpoints the frontend talks to.

    GET  /credits/me                balance, free allowance, paywall state, packs
    POST /credits/preview           will this job cost a credit? (UX only)
    POST /credits/turnstile/verify  bot check before free GPU runs

Checkout lives in credits/dodo_routes.py.

None of these can charge anything. Charging happens exactly once, inside
the job-creation request, via paywall.guard() - see credits/paywall.py.
That separation is the point: an endpoint the browser can call freely
must never be able to move the ledger.

SYNC HANDLERS, DELIBERATELY (2026-09-12). Every one of these does
blocking SQLite work and awaits nothing. Declared as coroutines, that
work ran on the event loop, where a contended write under a 30-second
busy_timeout could stall every other request on the server. Declared as
plain functions, FastAPI runs them in its own threadpool and the loop
stays free. Nothing else changes: same signatures, same dependencies,
same responses.
"""

from __future__ import annotations

import logging

import asyncio

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field

from rate_limit import check_rate_limit

from . import ledger, paywall, turnstile
from .identity import client_ip
from .identity import Identity

log = logging.getLogger("credits.routes")
router = APIRouter(prefix="/credits", tags=["credits"])


class PreviewRequest(BaseModel):
    tool: str = Field(..., max_length=64)
    input_seconds: float | None = Field(default=None, ge=0, le=60 * 60 * 24)
    vocal_options: list[str] = Field(default_factory=list, max_length=4)


@router.get("/me")
def me(
    response: Response,
    identity: Identity = Depends(paywall.get_identity),
) -> dict:
    """Everything the UI needs in one call: balance, free ops remaining,
    which tools are metered, and the pack list.

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
    result = ledger.summary(identity)
    try:
        from . import subscriptions
        result["studio_pass"] = subscriptions.summary(identity)
    except Exception:  # noqa: BLE001
        log.warning("studio pass summary failed", exc_info=True)
    return result


@router.post("/preview")
def preview(
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
    options = [o for o in body.vocal_options if o in paywall.STUDIO_VOCAL_OPTIONS]
    return paywall.preview(identity, body.tool, body.input_seconds,
                           paywall.option_credits(options))


class TurnstileRequest(BaseModel):
    token: str = Field(..., min_length=10, max_length=4096)


def _turnstile_verify_limit(request: Request) -> None:
    # Closed over, not partial(): a partial would expose max_requests as
    # a query parameter.
    check_rate_limit(request, max_requests=20, window_seconds=3600)


@router.post("/turnstile/verify", dependencies=[Depends(_turnstile_verify_limit)])
async def turnstile_verify(
    body: TurnstileRequest,
    request: Request,
    identity: Identity = Depends(paywall.get_identity),
) -> dict:
    """Solve once, then free GPU runs continue for TURNSTILE_PASS_HOURS."""
    if not turnstile.enabled():
        return {"ok": True, "passed": False, "reason": "disabled"}
    ok = await turnstile.verify(body.token, client_ip(request) or None)
    if not ok:
        raise HTTPException(status_code=400, detail={"error": "turnstile_failed",
                                                     "message": "That check didn't pass. Try again."})
    await asyncio.to_thread(turnstile.mark_passed, identity.ip_hash)
    return {"ok": True, "passed": True}