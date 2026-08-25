"""Paywall decisions and the server-side enforcement guard.

This is the only place that answers "does this job cost a credit?" The
frontend asks the same question via a preview endpoint purely for UX; the
answer that counts is computed here, inside the actual job-creation request.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator

from fastapi import Depends, HTTPException, Request, Response

from . import ledger as ledger_mod
from .config import get_settings
from .identity import Identity, resolve_identity
from .ledger import Charge, InsufficientCredits


@dataclass(frozen=True)
class Decision:
    tool: str
    billable: bool
    credits: int
    reason: str  # paywall_disabled | tool_free | under_free_duration | billable


def decide(tool: str, input_seconds: float | None) -> Decision:
    s = get_settings()
    rule = s.rule_for(tool)

    if not s.paywall_enabled:
        return Decision(tool, False, 0, "paywall_disabled")
    if rule is None or not rule.enabled:
        return Decision(tool, False, 0, "tool_free")
    if rule.free_under_seconds and input_seconds is not None and input_seconds < rule.free_under_seconds:
        return Decision(tool, False, 0, "under_free_duration")
    # Unknown duration on a metered tool is billable — never fail open.
    return Decision(tool, True, rule.credits, "billable")


def preview(identity: Identity, tool: str, input_seconds: float | None) -> dict:
    decision = decide(tool, input_seconds)
    from .db import connect

    with connect() as conn:
        balance = ledger_mod.get_balance(conn, identity)
        remaining = ledger_mod.free_remaining(conn, identity)

    will_use = "none"
    if decision.billable:
        will_use = "free" if remaining >= decision.credits else ("credit" if balance >= decision.credits else "blocked")

    return {
        "tool": tool, "input_seconds": input_seconds, "billable": decision.billable,
        "reason": decision.reason, "credits_required": decision.credits, "will_use": will_use,
        "balance": balance, "free_remaining": remaining, "can_run": will_use != "blocked",
    }


def get_identity(request: Request, response: Response) -> Identity:
    return resolve_identity(request, response)


IdentityDep = Depends(get_identity)


def insufficient_credits_response(exc: InsufficientCredits) -> HTTPException:
    return HTTPException(status_code=402, detail=exc.to_payload())


@asynccontextmanager
async def guard(identity: Identity, *, job_id: str, tool: str,
                input_seconds: float | None) -> AsyncIterator[Charge]:
    """Charge, run the body, auto-refund if the body raises.

        async with paywall.guard(identity, job_id=jid, tool="stem-separation",
                                 input_seconds=dur) as charge:
            await enqueue_runpod_job(...)

    Raises 402 before the body runs if the caller can't pay. Any exception
    inside the block refunds the hold before propagating — a RunPod submit
    failure never costs a credit.
    """
    decision = decide(tool, input_seconds)
    try:
        charge = ledger_mod.charge_for_job(identity, job_id=job_id, tool=tool,
                                          credits_needed=max(decision.credits, 1),
                                          billable=decision.billable)
    except InsufficientCredits as exc:
        raise insufficient_credits_response(exc) from exc

    try:
        yield charge
    except Exception:
        ledger_mod.refund_job(job_id, reason="enqueue_failed")
        raise