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
import os
from pathlib import Path

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field

from . import ledger, metering, settings_store
from .config import get_settings, reload_settings
from .db import connect, healthcheck, now_iso, tx

log = logging.getLogger("credits.admin")
router = APIRouter(prefix="/admin/credits", tags=["admin"])

# Written by deploy.yml the moment af-switch succeeds. Lives on the shared
# data volume so every container, live or draining, reads the same answer.
ACTIVE_SLOT_FILE = os.environ.get("ACTIVE_SLOT_FILE", "/app/data/.active_slot")
_KNOWN_SLOTS = frozenset({"a", "b"})


def require_admin(x_admin_token: str | None = Header(default=None, alias="X-Admin-Token")) -> None:
    settings = get_settings()
    expected = settings.admin_token
    if not expected or not x_admin_token:
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    if not hmac.compare_digest(expected.strip(), x_admin_token.strip()):
        # Same 404 as an unset token: never confirm the path exists.
        raise HTTPException(status_code=404, detail={"error": "not_found"})


ADMIN = [Depends(require_admin)]


def slot_info() -> dict:
    """Which container answered, and which one is live.

    Attached to every admin READ. Reads are deliberately not gated - see
    require_active_slot - but that leaves the operator able to read one
    container and write to another with nothing in either response saying
    so. A draining container reports source: "env" values from the
    PREVIOUS image's environment, which is confidently wrong rather than
    merely stale.
    """
    mine = os.environ.get("INSTANCE_SLOT", "").strip() or None
    try:
        active = Path(ACTIVE_SLOT_FILE).read_text(encoding="utf-8")[:16].strip() or None
    except Exception:  # noqa: BLE001
        active = None
    return {
        "answered_by": mine,
        "active": active,
        "is_active": None if not mine or active not in _KNOWN_SLOTS else mine == active,
    }


def _enforced_separation_limits():
    """What separation_limits is actually enforcing, after its clamp.

    The settings table reports the ROW; _limit() clamps at read time
    against KNOWN_KEYS, so a legacy row or an out-of-range env var is
    shown as one number and enforced as another. Same failure as the two
    /limits fallbacks disagreeing: a panel that states a value nothing
    enforces. The clamp knew the truth and only told the log.
    """
    try:
        from separation_limits import current_limits

        return current_limits()
    except Exception:  # noqa: BLE001
        return None


def require_active_slot() -> None:
    """Refuse writes on a container that is draining, not serving.

    WHY. deploy.yml is blue/green: the previous container keeps running
    after a switch so its in-flight jobs finish, and drain_old.sh stops
    it once idle. That is correct and users depend on it. But both
    containers mount the same data volume and therefore the same
    credits.db - so the draining one, running the PREVIOUS build, can
    still write settings rows its own validation approves and the new
    build would reject. That happened twice in one session, once writing
    an open-redirect FRONTEND_URL.

    Reads the active slot from the shared volume, written by deploy.yml
    when af-switch succeeds, and compares it to this container's own
    INSTANCE_SLOT. Reads are untouched - checking config on a draining
    container is useful, changing it is not.

    FAILS OPEN on purpose. No slot file, no INSTANCE_SLOT, or an
    unreadable file all mean "cannot tell", which is the state of every
    local dev run and of production before this ships. Only a confident
    mismatch refuses.
    """
    mine = os.environ.get("INSTANCE_SLOT", "").strip()
    if not mine:
        return
    try:
        active = Path(ACTIVE_SLOT_FILE).read_text(encoding="utf-8")[:16].strip()
    except Exception:  # noqa: BLE001
        return
    # Only a value naming a real slot is trusted enough to refuse on.
    # Anything else - a stray edit, a partial write, a letter from some
    # future layout - means "cannot tell", and a confused file must not
    # be able to 409 every container at once with no API path back: the
    # recovery would itself be a write.
    if active not in _KNOWN_SLOTS:
        log.warning("active slot file holds %r, ignoring", active)
        return
    if active == mine:
        return
    log.warning("refused admin write on draining slot %s (active is %s)", mine, active)
    raise HTTPException(
        status_code=409,
        detail={
            "error": "not_active_slot",
            "message": (
                f"This container is slot {mine}, which is draining. Slot {active} "
                f"is live. Send config changes there so they are validated by the "
                f"build that is actually serving traffic."
            ),
        },
    )


ADMIN_WRITE = [Depends(require_admin), Depends(require_active_slot)]


# ---------------------------------------------------------------------------
# 1. Is the money working?
# ---------------------------------------------------------------------------

@router.get("/overview", dependencies=ADMIN)
def overview(days: int = Query(default=30, ge=1, le=365)) -> dict:
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
        "gate": gate_funnel(days),
        "db": healthcheck(),
        "slot": slot_info(),
    }


# ---------------------------------------------------------------------------
# 2. What is it costing me?
# ---------------------------------------------------------------------------

@router.get("/costs", dependencies=ADMIN)
def costs(
    days: int = Query(default=30, ge=1, le=365),
    tool: str | None = Query(default=None, max_length=64),
    date_from: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    date_to: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
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
    if date_from and date_to:
        clauses = ["day >= ?", "day <= ?"]
        params: list = [date_from, date_to]
        if tool:
            clauses.append("tool = ?")
            params.append(tool)
        with connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM gpu_cost_daily WHERE {' AND '.join(clauses)}"
                " ORDER BY day DESC, tool",
                tuple(params),
            ).fetchall()
        return {
            "daily": [dict(r) for r in rows],
            "totals": metering.totals(days),
            "tool": tool,
            "date_from": date_from,
            "date_to": date_to,
        }

    daily = metering.daily_costs(days)
    if tool:
        daily = [row for row in daily if row.get("tool") == tool]
    return {"daily": daily, "totals": metering.totals(days), "tool": tool}


@router.get("/jobs", dependencies=ADMIN)
def recent_jobs(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    tool: str | None = Query(default=None, max_length=64),
    status: str | None = Query(default=None, max_length=32),
    charge_type: str | None = Query(default=None, max_length=16),
    days: int | None = Query(default=None, ge=1, le=365),
    email: str | None = Query(default=None, max_length=254),
    date_from: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    date_to: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    has_account: bool | None = Query(default=None),
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
        if status == "failed_all":
            clauses.append(
                "m.status IN ('failed','timeout','cancelled')"
                " AND COALESCE(m.failure_side,'server') = 'server'"
            )
        elif status == "rejected":
            clauses.append("m.failure_side = 'client'")
        else:
            clauses.append("m.status = ?")
            params.append(status)
    if charge_type:
        clauses.append("m.charge_type = ?")
        params.append(charge_type)
    if date_from:
        clauses.append("m.created_at >= ?")
        params.append(f"{date_from}T00:00:00Z")
    if date_to:
        clauses.append("m.created_at < strftime('%Y-%m-%dT%H:%M:%SZ', ?, '+1 day')")
        params.append(f"{date_to}T00:00:00Z")
    if days and not (date_from or date_to):
        # Same expression metering.totals() uses, so a count here and a
        # cost figure there always describe the same window.
        clauses.append("m.created_at >= strftime('%Y-%m-%dT%H:%M:%SZ','now',?)")
        params.append(f"-{days} days")
    if has_account is not None:
        clauses.append("m.account_id IS NOT NULL" if has_account else "m.account_id IS NULL")
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
                       m.failure_side,
                       m.created_at, m.ended_at,
                       c.status AS charge_status, c.refund_reason,
                       a.email AS email
                FROM gpu_job_metrics m
                LEFT JOIN job_charges c ON c.job_id = m.job_id
                LEFT JOIN accounts a ON a.id = m.account_id
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
            "date_from": date_from, "date_to": date_to, "has_account": has_account,
        },
    }


@router.get("/jobs/filters", dependencies=ADMIN)
def job_filter_options() -> dict:
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
# 2b. Who hit the wall? (migration 005)
# ---------------------------------------------------------------------------

def gate_funnel(days: int = 30) -> dict:
    """People stopped by the paywall, and what happened next.

    COUNTED BY SUBJECT, NOT BY EVENT. /credits/preview fires on every
    file drop, so raw rows overstate people by a wide margin. The
    headline numbers here are COUNT(DISTINCT subject_id); `events` is
    kept alongside only so the ratio between them is visible.

    conversion_pct is blocked-subjects to buyers, and it is a CEILING,
    not a measurement: `orders` has no subject_id, so a buyer cannot be
    matched back to the subject that was blocked. Someone who bought
    without ever being stopped still counts in the numerator. Read it as
    "no better than this", and read the trend rather than the absolute.
    """
    window = f"-{days} days"
    with connect() as conn:
        totals = conn.execute(
            """SELECT event,
                      COUNT(*)                   AS events,
                      COUNT(DISTINCT subject_id) AS subjects,
                      COUNT(DISTINCT ip_hash)    AS ips
               FROM gate_events
               WHERE created_at >= strftime('%Y-%m-%dT%H:%M:%SZ','now',?)
               GROUP BY event""",
            (window,),
        ).fetchall()

        by_tool = conn.execute(
            """SELECT tool, event,
                      COUNT(*)                   AS events,
                      COUNT(DISTINCT subject_id) AS subjects
               FROM gate_events
               WHERE created_at >= strftime('%Y-%m-%dT%H:%M:%SZ','now',?)
               GROUP BY tool, event
               ORDER BY subjects DESC""",
            (window,),
        ).fetchall()

        blocked_subjects = conn.execute(
            """SELECT COUNT(DISTINCT subject_id) AS n FROM gate_events
               WHERE created_at >= strftime('%Y-%m-%dT%H:%M:%SZ','now',?)""",
            (window,),
        ).fetchone()["n"]

        # Signed-in people who were blocked are the highest-intent group
        # in the table: they already have an account and still could not
        # run the job.
        blocked_with_account = conn.execute(
            """SELECT COUNT(DISTINCT account_id) AS n FROM gate_events
               WHERE account_id IS NOT NULL
                 AND created_at >= strftime('%Y-%m-%dT%H:%M:%SZ','now',?)""",
            (window,),
        ).fetchone()["n"]

        buyers = conn.execute(
            """SELECT COUNT(DISTINCT email) AS n FROM orders
               WHERE status='paid' AND test_mode=0
                 AND created_at >= strftime('%Y-%m-%dT%H:%M:%SZ','now',?)""",
            (window,),
        ).fetchone()["n"]

    out = {
        "days": days,
        "by_event": {r["event"]: dict(r) for r in totals},
        "by_tool": [dict(r) for r in by_tool],
        "blocked_subjects": blocked_subjects,
        "blocked_accounts": blocked_with_account,
        "buyers_in_window": buyers,
    }
    if blocked_subjects:
        out["conversion_pct_ceiling"] = round(100.0 * buyers / blocked_subjects, 2)
    return out


@router.get("/gate", dependencies=ADMIN)
def gate(days: int = Query(default=30, ge=1, le=365)) -> dict:
    """The funnel: gate seen -> gate hit on submit -> bought."""
    return gate_funnel(days)


@router.get("/gate/daily", dependencies=ADMIN)
def gate_daily(
    days: int = Query(default=30, ge=1, le=365),
    tool: str | None = Query(default=None, max_length=64),
) -> dict:
    """Day by day, off the gate_daily view."""
    clauses = ["day >= strftime('%Y-%m-%d','now',?)"]
    params: list = [f"-{days} days"]
    if tool:
        clauses.append("tool = ?")
        params.append(tool)
    with connect() as conn:
        rows = conn.execute(
            f"SELECT * FROM gate_daily WHERE {' AND '.join(clauses)}"
            " ORDER BY day DESC, tool, event",
            tuple(params),
        ).fetchall()
    return {"daily": [dict(r) for r in rows], "days": days, "tool": tool}


# ---------------------------------------------------------------------------
# 3. "I paid and got nothing"
# ---------------------------------------------------------------------------

@router.get("/users/lookup", dependencies=ADMIN)
def lookup_user(email: str = Query(..., max_length=254)) -> dict:
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
def recent_webhooks(
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


@router.post("/adjust", dependencies=ADMIN_WRITE)
def adjust(body: AdjustRequest) -> dict:
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


# GATED because sweep_stale_holds() writes to the shared credits.db and an
# older build could settle holds belonging to jobs still running in the
# live container. Worth being clear-eyed: main.py runs the same sweep on a
# timer inside EVERY container, so this is an operator-error guard, not a
# concurrency guard. The draining container is not read-only.
@router.post("/sweep", dependencies=ADMIN_WRITE)
def sweep() -> dict:
    """Force the orphaned-hold sweep instead of waiting 15 minutes.

    Useful right after a deploy: a restart kills in-flight jobs, and this
    returns their credits immediately rather than on the next tick.
    """
    return {"refunded": ledger.sweep_stale_holds()}


@router.post("/reload-config", dependencies=ADMIN)
def reload_config() -> dict:
    """Drop the cached Settings and report what the process now believes.

    The old caveat here is gone: settings now resolve from the settings
    table first, which a running container CAN see. Editing .env still
    needs a restart, but every knob in /settings applies immediately.
    """
    settings = reload_settings()
    return {
        "ok": True,
        "paywall_enabled": settings.paywall_enabled,
        "provider": settings.payments_provider,
        "metered_routes": [r.tool for r in settings.tool_rules.values() if r.enabled],
    }


# ---------------------------------------------------------------------------
# 7. Runtime configuration
# ---------------------------------------------------------------------------

class SettingsUpdate(BaseModel):
    values: dict[str, str | int | float | bool | None] = Field(default_factory=dict)
    note: str = Field(default="", max_length=500)


@router.get("/settings", dependencies=ADMIN)
def list_settings() -> dict:
    """Every tunable key with its effective value and where it came from.

    env_value is reported only for keys in settings_store.KNOWN_KEYS.
    That allowlist, not LOCKED_KEYS, is what keeps this endpoint from
    being a read primitive for the container's environment: writes accept
    any key, so a blocklist alone could be walked around by writing a junk
    override row for a credential and reading its env_value back.
    """
    return {
        "settings": settings_store.describe(),
        "locked": sorted(settings_store.LOCKED_KEYS),
        "slot": slot_info(),
        "enforced": {"separation": _enforced_separation_limits()},
    }


@router.put("/settings", dependencies=ADMIN_WRITE)
def update_settings(body: SettingsUpdate) -> dict:
    """Set or clear overrides. Null clears a key back to env or default.

    Validated by a trial build of the whole Settings object, so a value
    that would fail a boot invariant is rejected here rather than at the
    next restart. Nothing is written unless every key in the batch passes.
    """
    if not body.values:
        raise HTTPException(400, detail={"error": "no_values"})
    if len(body.values) > 50:
        raise HTTPException(400, detail={"error": "too_many_keys"})

    payload: dict[str, str | None] = {}
    for key, value in body.values.items():
        if value is None:
            payload[key] = None
        elif isinstance(value, bool):
            payload[key] = "true" if value else "false"
        else:
            payload[key] = str(value)

    try:
        settings_store.set_many(payload, actor="admin", note=body.note)
    except ValueError as exc:
        raise HTTPException(400, detail={"error": "invalid_settings", "message": str(exc)})

    settings = reload_settings()
    log.info("settings changed: %s", ", ".join(sorted(payload)))
    return {
        "ok": True,
        "changed": sorted(payload),
        "paywall_enabled": settings.paywall_enabled,
        "free_monthly_ops": settings.free_monthly_ops,
        "free_monthly_ops_per_ip": settings.free_monthly_ops_per_ip,
        "metered_routes": [r.tool for r in settings.tool_rules.values() if r.enabled],
        # What is now ENFORCED, not just what was stored. _limit() clamps at
        # read time, so a row and the value in force can differ - which is
        # the case the clamp exists to surface, and the worst moment to make
        # the caller issue a second request to find out.
        "enforced": {"separation": _enforced_separation_limits()},
    }


@router.delete("/settings/{key}", dependencies=ADMIN_WRITE)
def clear_setting(key: str) -> dict:
    """Revert one key to its env value or code default."""
    try:
        settings_store.clear(key, actor="admin")
    except ValueError as exc:
        raise HTTPException(400, detail={"error": "invalid_settings", "message": str(exc)})
    reload_settings()
    return {
        "ok": True,
        "cleared": key,
        "enforced": {"separation": _enforced_separation_limits()},
    }


@router.get("/settings/audit", dependencies=ADMIN)
def settings_audit(
    limit: int = Query(default=100, ge=1, le=1000),
    key: str | None = Query(default=None, max_length=128),
) -> dict:
    """Who changed what, when, and what it was before."""
    return {"entries": settings_store.audit(limit=limit, key=key)}