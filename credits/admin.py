"""
credits/admin.py - The operator's view. Six endpoints, no more.

WHAT THIS IS FOR, AND WHY KO-FI'S DASHBOARD ISN'T ENOUGH
--------------------------------------------------------
Ko-fi tells you a payment happened. It cannot tell you whether the
credits landed, whether they were spent, whether a job failed and
refunded, or what any of it cost you in GPU time. Those live here.

The question that actually arrives in your inbox is "I paid $8 and got
nothing" - and answering it needs three facts Ko-fi does not have: did
the webhook arrive, did the ledger move, and is the balance sitting on
an account whose email differs by a typo from the one they're writing
from. /users/lookup answers all three in one call.

SCOPE DISCIPLINE
----------------
Everything here is READ-ONLY except /adjust, which exists because the
alternative to a manual lever is editing SQLite by hand on a production
box at 2am. There is deliberately no "create user", no "delete order",
no "edit balance to N" - the ledger is append-only and stays that way,
so /adjust writes a NEW row with a reason attached rather than mutating
history. Every correction is auditable afterwards, including yours.

AUTH: X-Admin-Token, compared in constant time against
CREDITS_ADMIN_TOKEN. Unset means these routes 404 rather than 403 -
an unconfigured admin surface should be invisible, not merely locked,
because a 403 confirms the path exists and is worth attacking.

This deliberately does NOT reuse ADMIN_STATUS_KEY from the host
config.py. Same reason the credits DB is a separate file: money-touching
surfaces get their own credential, so rotating one doesn't force
rotating the other, and a leaked cookie-upload key can't move credits.
"""

from __future__ import annotations

import hmac
import logging

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field

from . import ledger, metering
from .config import get_settings, reload_settings
from .db import connect, healthcheck, now_iso, tx

log = logging.getLogger("credits.admin")
router = APIRouter(prefix="/admin/credits", tags=["admin"])


def require_admin(x_admin_token: str | None = Header(default=None, alias="X-Admin-Token")) -> None:
    settings = get_settings()
    expected = settings.admin_token
    if not expected or not x_admin_token:
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    if not hmac.compare_digest(expected.strip(), x_admin_token.strip()):
        # Same 404 as an unset token: never confirm the path exists.
        raise HTTPException(status_code=404, detail={"error": "not_found"})


ADMIN = [Depends(require_admin)]


# ---------------------------------------------------------------------------
# 1. Is the money working?
# ---------------------------------------------------------------------------

@router.get("/overview", dependencies=ADMIN)
async def overview(days: int = Query(default=30, ge=1, le=365)) -> dict:
    """The one screen. Paywall state, outstanding liability, unit economics.

    credits_outstanding is a LIABILITY, not a score: it's credits people
    have paid for and not yet spent, i.e. GPU work you already owe. It
    going up is revenue; it going up while est_cost_usd goes up faster
    is a pricing problem.
    """
    settings = get_settings()
    with connect() as conn:
        accounts = conn.execute("SELECT COUNT(*) AS n FROM accounts").fetchone()["n"]
        outstanding = conn.execute(
            "SELECT COALESCE(SUM(delta),0) AS n FROM credit_ledger"
        ).fetchone()["n"]
        holds = conn.execute(
            "SELECT COUNT(*) AS n FROM job_charges WHERE status='held'"
        ).fetchone()["n"]
        refunded = conn.execute(
            "SELECT COUNT(*) AS n FROM job_charges WHERE status='refunded'"
        ).fetchone()["n"]
        # A webhook that arrived and never finished processing is a paid
        # order with no credits. This is the number to check first when
        # someone says they paid and got nothing.
        stuck_webhooks = conn.execute(
            "SELECT COUNT(*) AS n FROM webhook_events WHERE processed_at IS NULL"
        ).fetchone()["n"]

    return {
        "paywall": {
            "enabled": settings.paywall_enabled,
            "provider": settings.payments_provider,
            "metered_routes": [r.tool for r in settings.tool_rules.values() if r.enabled],
            "free_monthly_ops": settings.free_monthly_ops,
            "free_monthly_ops_per_ip": settings.free_monthly_ops_per_ip,
        },
        "accounts": accounts,
        "credits_outstanding": outstanding,
        "holds_open": holds,
        "jobs_refunded": refunded,
        "webhooks_unprocessed": stuck_webhooks,
        "usage": metering.totals(days),
        "db": healthcheck(),
    }


# ---------------------------------------------------------------------------
# 2. What is it costing me?
# ---------------------------------------------------------------------------

@router.get("/costs", dependencies=ADMIN)
async def costs(
    days: int = Query(default=30, ge=1, le=365),
    tool: str | None = Query(default=None, max_length=64),
) -> dict:
    """Day by day, tool by tool. This is the data that decides the price.

    est_cost_usd is a FLOOR - it counts the worker's reported run time
    and not RunPod's cold start or transfers. See metering.py. Measured
    2026-08-29: metering reported ~$0.30 over a window in which the
    RunPod balance moved ~$2.90, and the entire gap is cold starts. Read
    these numbers as a lower bound, never as the invoice.

    `tool` filters the daily rows only; `totals` deliberately stays
    unfiltered, because the question it answers - "what am I spending
    across everything?" - is not the same question, and quietly
    narrowing it to one tool would make a partial figure look like a
    complete one.
    """
    daily = metering.daily_costs(days)
    if tool:
        daily = [row for row in daily if row.get("tool") == tool]
    return {"daily": daily, "totals": metering.totals(days), "tool": tool}


@router.get("/jobs", dependencies=ADMIN)
async def recent_jobs(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    tool: str | None = Query(default=None, max_length=64),
    status: str | None = Query(default=None, max_length=32),
    charge_type: str | None = Query(default=None, max_length=16),
    days: int | None = Query(default=None, ge=1, le=365),
    email: str | None = Query(default=None, max_length=254),
) -> dict:
    """Recent GPU jobs with cost and billing outcome joined together.

    The join is the useful part: a row where charge_type='credit' and
    charge_status='refunded' is a customer who paid, failed, and got
    their credit back - exactly the sequence worth eyeballing after any
    deploy.

    FILTERS ADDED 2026-08-29, for the Next.js admin UI. Every one of
    them answers a question that previously meant fetching 500 rows and
    filtering in the browser:

        tool         "what is audio-to-midi-hq actually costing?"
        status       failed | timeout - the ones you chase
        charge_type  credit isolates paying customers from free-tier noise
        days         same window semantics as /costs
        email        THIS customer's jobs, joined through accounts

    email is the one that matters most for support. /users/lookup
    already returns a customer's CHARGES, but not their GPU costs or
    failure reasons - so "they say it failed twice, what happened?" took
    two endpoints and a manual join. Now it takes one.

    PAGINATION, not just a bigger limit. An admin UI needs a stable page
    2, and `total` is what lets it render "showing 100 of 1,842" rather
    than leaving the operator to guess whether they are seeing
    everything. The count runs against the same WHERE clause, so the two
    numbers can never disagree.

    ORDER BY created_at DESC, job_id DESC - the tiebreak matters, and
    job_id is the right column for it. created_at has second resolution,
    so two jobs submitted in the same second have no defined order;
    without a tiebreak the same row can appear on page 1 and page 2, or
    vanish between them, as the offset moves.

    job_id rather than a rowid: gpu_job_metrics declares
    `job_id TEXT PRIMARY KEY` and has no `id` column at all. An earlier
    draft of this used m.id and would have raised "no such column" on
    the first request - worth stating, because the table's sibling
    credit_ledger DOES have an autoincrement id, and assuming they match
    is an easy mistake to repeat.

    FILTERS ARE PARAMETERISED, never interpolated. Values reach this
    function straight from a query string; the clause list below is
    built from fixed SQL fragments with ? placeholders, and the values
    ride in the params tuple. That is not caution for its own sake -
    this endpoint reads the database that holds the money.
    """
    clauses: list[str] = []
    params: list = []

    if tool:
        clauses.append("m.tool = ?")
        params.append(tool)
    if status:
        clauses.append("m.status = ?")
        params.append(status)
    if charge_type:
        clauses.append("m.charge_type = ?")
        params.append(charge_type)
    if days:
        # Same expression metering.totals() uses, so a count here and a
        # cost figure there always describe the same window.
        clauses.append("m.created_at >= strftime('%Y-%m-%dT%H:%M:%SZ','now',?)")
        params.append(f"-{days} days")
    if email:
        # Joined through accounts rather than matched on the metrics row:
        # gpu_job_metrics stores account_id, not the address, and the
        # address is what a support request arrives with.
        clauses.append(
            "m.account_id IN (SELECT id FROM accounts WHERE email = ?)"
        )
        params.append(email.strip().lower())

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    with connect() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) AS n FROM gpu_job_metrics m {where}", tuple(params)
        ).fetchone()["n"]

        rows = conn.execute(
            f"""SELECT m.job_id, m.tool, m.status, m.input_seconds, m.gpu_seconds,
                       m.est_cost_usd, m.charge_type, m.paywall_enabled, m.error,
                       m.created_at, m.ended_at,
                       c.status AS charge_status, c.refund_reason
                FROM gpu_job_metrics m
                LEFT JOIN job_charges c ON c.job_id = m.job_id
                {where}
                ORDER BY m.created_at DESC, m.job_id DESC
                LIMIT ? OFFSET ?""",
            tuple(params) + (limit, offset),
        ).fetchall()

    return {
        "jobs": [dict(r) for r in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
        "filters": {
            "tool": tool, "status": status, "charge_type": charge_type,
            "days": days, "email": email,
        },
    }


@router.get("/jobs/filters", dependencies=ADMIN)
async def job_filter_options() -> dict:
    """What values the filters above can actually take, read from the data.

    Exists so the admin UI's dropdowns are populated from what is really
    in the table rather than from a hardcoded list that goes stale the
    next time a tool is added. The same reasoning as the host app's
    /admin/endpoints serving its own tool list and noise patterns: the
    backend is the thing that knows, so the frontend should read rather
    than repeat.

    This codebase has now had four separate hand-maintained lists of
    "which tools exist" drift out of sync in one week. A fifth, living
    in a React component, is not worth adding.
    """
    with connect() as conn:
        tools = [r["tool"] for r in conn.execute(
            "SELECT DISTINCT tool FROM gpu_job_metrics WHERE tool IS NOT NULL ORDER BY tool"
        )]
        statuses = [r["status"] for r in conn.execute(
            "SELECT DISTINCT status FROM gpu_job_metrics WHERE status IS NOT NULL ORDER BY status"
        )]
        charge_types = [r["charge_type"] for r in conn.execute(
            "SELECT DISTINCT charge_type FROM gpu_job_metrics WHERE charge_type IS NOT NULL ORDER BY charge_type"
        )]
    return {"tools": tools, "statuses": statuses, "charge_types": charge_types}


# ---------------------------------------------------------------------------
# 3. "I paid and got nothing"
# ---------------------------------------------------------------------------

@router.get("/users/lookup", dependencies=ADMIN)
async def lookup_user(email: str = Query(..., max_length=254)) -> dict:
    """Everything about one customer, by the email they paid with.

    Built for one specific support message. Returns found=false rather
    than 404 when there's no account, because "no account for that
    email" IS the answer most of the time - they paid with a different
    address, and the fix is /adjust or telling them to check the other
    inbox.

    Ledger entries come back newest-first with their reasons, so a
    balance of 0 is immediately explainable: bought 30, spent 30, versus
    bought 30 and refunded 30 are very different conversations.
    """
    email = email.strip().lower()
    with connect() as conn:
        account = conn.execute(
            "SELECT id, email, status, created_at, last_login_at FROM accounts WHERE email=?",
            (email,),
        ).fetchone()

        # Orders are recorded even if the account link somehow failed,
        # so check them regardless - a paid order with no account is
        # precisely the broken state worth surfacing.
        orders = conn.execute(
            """SELECT provider, provider_order_id, pack, credits, amount_cents,
                      currency, status, created_at
               FROM orders WHERE email=? ORDER BY created_at DESC LIMIT 50""",
            (email,),
        ).fetchall()

        if account is None:
            return {
                "found": False,
                "email": email,
                "orders": [dict(o) for o in orders],
                "hint": (
                    "No account for that email. If orders is non-empty the webhook "
                    "ran but the account link failed. If both are empty, they paid "
                    "with a different address - search Ko-fi for the transaction."
                ),
            }

        account_id = account["id"]
        balance = conn.execute(
            """SELECT COALESCE(SUM(delta),0) AS b FROM credit_ledger
               WHERE (owner_type='account' AND owner_id=?)
                  OR (owner_type='subject' AND owner_id IN
                      (SELECT id FROM subjects WHERE account_id=?))""",
            (account_id, account_id),
        ).fetchone()["b"]

        entries = conn.execute(
            """SELECT delta, kind, job_id, order_id, note, created_at
               FROM credit_ledger
               WHERE (owner_type='account' AND owner_id=?)
                  OR (owner_type='subject' AND owner_id IN
                      (SELECT id FROM subjects WHERE account_id=?))
               ORDER BY id DESC LIMIT 50""",
            (account_id, account_id),
        ).fetchall()

        devices = conn.execute(
            "SELECT COUNT(*) AS n FROM subjects WHERE account_id=?", (account_id,)
        ).fetchone()["n"]

        jobs = conn.execute(
            """SELECT job_id, tool, charge_type, credits, status, created_at, refund_reason
               FROM job_charges WHERE owner_type='account' AND owner_id=?
               ORDER BY created_at DESC LIMIT 25""",
            (account_id,),
        ).fetchall()

    return {
        "found": True,
        "account": dict(account),
        "balance": balance,
        "linked_devices": devices,
        "orders": [dict(o) for o in orders],
        "ledger": [dict(e) for e in entries],
        "jobs": [dict(j) for j in jobs],
    }


@router.get("/webhooks", dependencies=ADMIN)
async def recent_webhooks(
    limit: int = Query(default=50, ge=1, le=200),
    unprocessed_only: bool = False,
) -> dict:
    """Delivery log. `unprocessed_only=true` is the triage view.

    A row with processed_at set is done. A row with an error and no
    processed_at is a payment that arrived and failed to apply - the
    provider will have retried, but if the error is permanent (an
    unmapped shop code, say) it never succeeded and those credits do not
    exist. Fix the config, then /adjust the customer manually; the
    webhook won't be redelivered days later.

    The raw payload is NOT returned here - it contains the buyer's email
    and the verification token echo. Use /users/lookup for the parts
    that matter.
    """
    sql = """SELECT event_id, provider, event_name, received_at, processed_at, error
             FROM webhook_events {where} ORDER BY received_at DESC LIMIT ?"""
    where = "WHERE processed_at IS NULL" if unprocessed_only else ""
    with connect() as conn:
        rows = conn.execute(sql.format(where=where), (limit,)).fetchall()
    return {"webhooks": [dict(r) for r in rows]}


# ---------------------------------------------------------------------------
# 4. The manual lever
# ---------------------------------------------------------------------------

class AdjustRequest(BaseModel):
    email: str = Field(..., max_length=254)
    delta: int = Field(..., ge=-1000, le=1000)
    note: str = Field(..., min_length=3, max_length=200)


@router.post("/adjust", dependencies=ADMIN)
async def adjust(body: AdjustRequest) -> dict:
    """Grant or remove credits by hand, with a mandatory reason.

    Creates the account if the email is unknown - that is the common
    case, not an edge one: someone paid with an address that never
    reached the webhook, and the fix is to credit them now and let the
    magic link find them later.

    `note` is required rather than optional on purpose. Six months from
    now, an unexplained +30 in the ledger is indistinguishable from a
    bug, and the only person who can tell the difference is you, today.
    """
    from .identity import get_or_create_account

    email = body.email.strip().lower()
    with connect() as conn, tx(conn):
        account_id = get_or_create_account(conn, email)
        applied = ledger.grant(
            conn,
            owner_type="account",
            owner_id=account_id,
            amount=body.delta,
            kind="admin_adjust",
            # Timestamped so repeated corrections to the same account are
            # each recorded rather than silently deduplicated.
            idempotency_key=f"admin:{account_id}:{now_iso()}",
            note=body.note,
        )
        balance = conn.execute(
            """SELECT COALESCE(SUM(delta),0) AS b FROM credit_ledger
               WHERE (owner_type='account' AND owner_id=?)
                  OR (owner_type='subject' AND owner_id IN
                      (SELECT id FROM subjects WHERE account_id=?))""",
            (account_id, account_id),
        ).fetchone()["b"]

    log.warning("[ADMIN] %+d credits to %s (%s) - balance now %d",
                body.delta, email, body.note, balance)
    return {"ok": True, "applied": applied, "email": email, "balance": balance}


@router.post("/sweep", dependencies=ADMIN)
async def sweep() -> dict:
    """Force the orphaned-hold sweep instead of waiting 15 minutes.

    Useful right after a deploy: a restart kills in-flight jobs, and this
    returns their credits immediately rather than on the next tick.
    """
    return {"refunded": ledger.sweep_stale_holds()}


@router.post("/reload-config", dependencies=ADMIN)
async def reload_config() -> dict:
    """Re-read env WITHOUT a restart.

    Honest caveat: this only helps if the environment of the RUNNING
    process changed, which for a Docker container it normally hasn't -
    editing .env and calling this does nothing, because the container
    still holds the values it booted with. Flipping PAYWALL_ENABLED for
    real means editing .env and restarting the container.

    What it IS for: dropping a cached settings object after a config
    change that was applied some other way, and confirming what the
    process currently believes.
    """
    settings = reload_settings()
    return {
        "ok": True,
        "paywall_enabled": settings.paywall_enabled,
        "provider": settings.payments_provider,
        "metered_routes": [r.tool for r in settings.tool_rules.values() if r.enabled],
    }