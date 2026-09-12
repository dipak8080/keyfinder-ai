"""Paywall decisions and the server-side enforcement guard.

This is the only place that answers "does this job cost a credit?" The
frontend asks the same question via a preview endpoint purely for UX; the
answer that counts is computed here, inside the actual job-creation request.

It is also the only place that sees a refusal, so the gate_events writes
live here too - see migration 005.
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator

from fastapi import Depends, HTTPException, Request, Response

from . import ledger as ledger_mod
from .config import get_settings
from .identity import Identity, resolve_identity
from .ledger import Charge, InsufficientCredits

log = logging.getLogger("credits.paywall")

# Seconds within which a repeat of the same (subject, tool, event) is not
# written again. /credits/preview fires on every file drop and on every
# duration re-probe, so without this one person auditioning three files
# lands three rows that read as three separate refusals.
GATE_DEDUPE_SECONDS = 60


@dataclass(frozen=True)
class Decision:
    tool: str
    billable: bool
    credits: int       # credit cost charged to paying users
    free_ops: int      # monthly free allowance one job consumes (0 if not billable)
    reason: str  # paywall_disabled | tool_free | under_free_duration | billable


def decide(tool: str, input_seconds: float | None) -> Decision:
    s = get_settings()
    rule = s.rule_for(tool)

    if not s.paywall_enabled:
        return Decision(tool, False, 0, 0, "paywall_disabled")
    if rule is None or not rule.enabled:
        return Decision(tool, False, 0, 0, "tool_free")
    if rule.free_under_seconds and input_seconds is not None and input_seconds < rule.free_under_seconds:
        return Decision(tool, False, 0, 0, "under_free_duration")
    # Unknown duration on a metered tool is billable — never fail open.
    # free_ops is allowance, not credits: one job costs one free op
    # regardless of credit cost, so any credit price stays coverable.
    return Decision(tool, True, rule.credits, 1, "billable")


# ---------------------------------------------------------------------------
# Gate instrumentation - migration 005
# ---------------------------------------------------------------------------

def _recently_recorded(conn: sqlite3.Connection, *, event: str, tool: str,
                       subject_id: str) -> bool:
    row = conn.execute(
        """SELECT 1 FROM gate_events
           WHERE subject_id=? AND tool=? AND event=?
             AND created_at >= strftime('%Y-%m-%dT%H:%M:%S.000Z','now',?)
           LIMIT 1""",
        (subject_id, tool, event, f"-{GATE_DEDUPE_SECONDS} seconds"),
    ).fetchone()
    return row is not None


def record_gate_event(identity: Identity, *, event: str, tool: str,
                      credits_needed: int, balance: int, free_remaining: int,
                      input_seconds: float | None) -> None:
    """Best-effort funnel write. Never raises into the caller.

    A failure here must not turn a correct 402 into a 500, and must not
    stop a preview returning. That is the only reason the whole body sits
    inside try/except.
    """
    from .db import connect, now_iso, period_key, tx

    try:
        with connect() as conn:
            if _recently_recorded(conn, event=event, tool=tool,
                                  subject_id=identity.subject_id):
                return
            owner_type, owner_id = identity.owner
            with tx(conn):
                conn.execute(
                    """INSERT INTO gate_events
                       (event, tool, owner_type, owner_id, subject_id, account_id,
                        ip_hash, period, credits_needed, balance, free_remaining,
                        input_seconds, created_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (event, tool, owner_type, owner_id, identity.subject_id,
                     identity.account_id, identity.ip_hash, period_key(),
                     credits_needed, balance, free_remaining, input_seconds,
                     now_iso()),
                )
    except Exception:
        log.warning("gate event not recorded (%s/%s)", event, tool, exc_info=True)


def preview(identity: Identity, tool: str, input_seconds: float | None) -> dict:
    decision = decide(tool, input_seconds)
    from .db import connect

    with connect() as conn:
        balance = ledger_mod.get_balance(conn, identity)
        remaining = ledger_mod.free_remaining(conn, identity)

    will_use = "none"
    if decision.billable:
        will_use = "free" if remaining >= decision.free_ops else ("credit" if balance >= decision.credits else "blocked")

    if will_use == "blocked":
        record_gate_event(identity, event="preview_blocked", tool=tool,
                          credits_needed=decision.credits, balance=balance,
                          free_remaining=remaining, input_seconds=input_seconds)

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
                                          free_ops_needed=max(decision.free_ops, 1),
                                          billable=decision.billable)
    except InsufficientCredits as exc:
        record_gate_event(identity, event="submit_402", tool=tool,
                          credits_needed=exc.needed, balance=exc.balance,
                          free_remaining=exc.free_remaining,
                          input_seconds=input_seconds)
        raise insufficient_credits_response(exc) from exc

    try:
        yield charge
    except Exception:
        ledger_mod.refund_job(job_id, reason="enqueue_failed")
        raise