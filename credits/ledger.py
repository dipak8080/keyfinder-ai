"""Credit ledger.

credit_ledger is append-only — balance is SUM(delta), nothing expires.
The free tier is a separate monthly counter, tracked against both the owner
(account or anonymous subject) and the IP hash, so clearing cookies gets a
new subject but the same IP bucket.

job_charges is the idempotency anchor: exactly one row per job that ever
reached the paywall, so a retried request or a doubled webhook can never
double-charge or double-refund.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Literal

from .config import get_settings
from .db import connect, next_period_start_iso, now_iso, period_key, tx, utcnow
from .identity import Identity

log = logging.getLogger("credits.ledger")

ChargeType = Literal["free", "credit", "none"]


class InsufficientCredits(Exception):
    def __init__(self, *, balance: int, free_remaining: int, tool: str, needed: int):
        self.balance = balance
        self.free_remaining = free_remaining
        self.tool = tool
        self.needed = needed
        super().__init__("insufficient credits")

    def to_payload(self) -> dict:
        s = get_settings()
        return {
            "error": "insufficient_credits",
            "message": "You're out of credits for this tool.",
            "tool": self.tool,
            "credits_needed": self.needed,
            "balance": self.balance,
            "free_remaining": self.free_remaining,
            "free_resets_at": next_period_start_iso(),
            "packs": [
                {"key": p.key, "credits": p.credits, "price_usd": p.price_usd, "label": p.label,
                 "buy_url": p.resolved_buy_url(s.payments_provider, s.provider_store_slug)}
                for p in s.packs_sorted()
            ],
        }


@dataclass
class Charge:
    job_id: str
    tool: str
    charge_type: ChargeType
    credits: int
    balance_after: int
    free_remaining_after: int

    def as_dict(self) -> dict:
        return asdict(self)


# --- reads --------------------------------------------------------------

def _owner_clause(identity: Identity) -> tuple[str, tuple]:
    if identity.account_id:
        return (
            "((owner_type='account' AND owner_id=?)"
            " OR (owner_type='subject' AND owner_id IN (SELECT id FROM subjects WHERE account_id=?)))",
            (identity.account_id, identity.account_id),
        )
    return ("(owner_type='subject' AND owner_id=?)", (identity.subject_id,))


def get_balance(conn: sqlite3.Connection, identity: Identity) -> int:
    clause, params = _owner_clause(identity)
    row = conn.execute(f"SELECT COALESCE(SUM(delta),0) AS b FROM credit_ledger WHERE {clause}", params).fetchone()
    return int(row["b"] or 0)


def _free_used(conn: sqlite3.Connection, period: str, scope: str, key: str) -> int:
    row = conn.execute(
        "SELECT used FROM free_usage WHERE period=? AND scope=? AND scope_key=?", (period, scope, key)
    ).fetchone()
    return int(row["used"]) if row else 0


def free_remaining(conn: sqlite3.Connection, identity: Identity, period: str | None = None) -> int:
    s = get_settings()
    period = period or period_key()
    owner_used = _free_used(conn, period, "owner", identity.owner_key)
    ip_used = _free_used(conn, period, "ip", identity.ip_hash)
    return max(0, min(s.free_monthly_ops - owner_used, s.free_monthly_ops_per_ip - ip_used))


# Route key -> the FREE (anonymous) limit, read from the host config.py
# so the numbers still live where every other limit lives. Imported
# lazily inside summary() because credits/ must stay importable without
# the host app present (the test harness relies on that).
def _free_route_limits() -> dict:
    try:
        from config import (
            SEPARATION_HQ_RATE_LIMIT_MAX_REQUESTS,
            SEPARATION_HQ_RATE_LIMIT_WINDOW_SECONDS,
            STEMS_HQ_RATE_LIMIT_MAX_REQUESTS,
            STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS,
            YOUTUBE_SEPARATE_HQ_RATE_LIMIT_MAX_REQUESTS,
            YOUTUBE_SEPARATE_HQ_RATE_LIMIT_WINDOW_SECONDS,
            YOUTUBE_STEMS_HQ_RATE_LIMIT_MAX_REQUESTS,
            YOUTUBE_STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS,
        )
    except ImportError:
        return {}
    return {
        "separate-hq": (SEPARATION_HQ_RATE_LIMIT_MAX_REQUESTS, SEPARATION_HQ_RATE_LIMIT_WINDOW_SECONDS),
        "stems-hq": (STEMS_HQ_RATE_LIMIT_MAX_REQUESTS, STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS),
        "youtube/separate-hq": (YOUTUBE_SEPARATE_HQ_RATE_LIMIT_MAX_REQUESTS, YOUTUBE_SEPARATE_HQ_RATE_LIMIT_WINDOW_SECONDS),
        "youtube/stems-hq": (YOUTUBE_STEMS_HQ_RATE_LIMIT_MAX_REQUESTS, YOUTUBE_STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS),
    }


def summary(identity: Identity) -> dict:
    s = get_settings()
    period = period_key()
    with connect() as conn:
        balance = get_balance(conn, identity)
        remaining = free_remaining(conn, identity, period)
        clause, params = _owner_clause(identity)
        recent = conn.execute(
            f"SELECT delta, kind, created_at, note FROM credit_ledger WHERE {clause} ORDER BY id DESC LIMIT 10",
            params,
        ).fetchall()
        # Open holds: credits reserved for jobs that have not reached a
        # terminal state. The frontend gives up polling an HQ job at 32
        # minutes but CREDIT_HOLD_TIMEOUT_MINUTES is 90, so between those
        # two a user can be staring at a stuck job wondering if they were
        # charged for nothing. Exposing the count lets the UI say
        # something true at that moment - "still processing, and if it
        # fails your credit comes back automatically" - instead of going
        # quiet, which is the worst-feeling failure there is.
        held = conn.execute(
            "SELECT COUNT(*) AS n FROM job_charges"
            " WHERE status='held' AND charge_type='credit'"
            "   AND owner_type=? AND owner_id=?",
            identity.owner,
        ).fetchone()["n"]

    return {
        "authenticated": identity.is_authenticated,
        "email": identity.email,
        "balance": balance,
        "free_monthly_ops": s.free_monthly_ops,
        "free_remaining": remaining,
        "free_resets_at": next_period_start_iso(),
        "paywall": {
            "enabled": s.paywall_enabled,
            "tools": {
                r.tool: {"enabled": s.paywall_enabled and r.enabled,
                         "free_under_seconds": r.free_under_seconds, "credits": r.credits}
                for r in s.tool_rules.values()
            },
        },
        "packs": [
            {"key": p.key, "credits": p.credits, "price_usd": p.price_usd, "label": p.label,
             "buy_url": p.resolved_buy_url(s.payments_provider, s.provider_store_slug)}
            for p in s.packs_sorted()
        ],
        "held_credits": held,
        "rate_limit": _rate_limit_block(identity),
        "recent": [dict(r) for r in recent],
    }


def _rate_limit_block(identity: Identity) -> dict:
    """Which rate limits actually apply to THIS caller right now.

    Imported lazily: credits.limits imports the host app's rate_limit
    module, and this keeps ledger.py importable on its own.
    """
    free_limits = _free_route_limits()
    if not free_limits:
        return {"tier": "free", "tools": {}}
    try:
        from .limits import summary_for
        return summary_for(identity, free_limits)
    except Exception:  # noqa: BLE001
        log.exception("could not resolve rate-limit summary")
        return {"tier": "free", "tools": {}}


# --- writes ---------------------------------------------------------------

def grant(conn: sqlite3.Connection, *, owner_type: str, owner_id: str, amount: int, kind: str,
          idempotency_key: str, order_id: str | None = None, job_id: str | None = None,
          note: str | None = None) -> bool:
    """Insert a ledger row. False if idempotency_key already exists (safe replay)."""
    try:
        conn.execute(
            """INSERT INTO credit_ledger (owner_type, owner_id, delta, kind, job_id, order_id,
               idempotency_key, note, created_at) VALUES (?,?,?,?,?,?,?,?,?)""",
            (owner_type, owner_id, amount, kind, job_id, order_id, idempotency_key, note, now_iso()),
        )
        return True
    except sqlite3.IntegrityError:
        log.info("ledger entry %s already applied — skipping", idempotency_key)
        return False


def _bump_free(conn: sqlite3.Connection, period: str, scope: str, key: str, delta: int) -> None:
    conn.execute(
        """INSERT INTO free_usage (period, scope, scope_key, used, updated_at) VALUES (?,?,?,MAX(0,?),?)
           ON CONFLICT(period, scope, scope_key) DO UPDATE SET used=MAX(0, used+?), updated_at=?""",
        (period, scope, key, delta, now_iso(), delta, now_iso()),
    )


def charge_for_job(identity: Identity, *, job_id: str, tool: str, credits_needed: int = 1,
                   billable: bool = True) -> Charge:
    """Reserve payment BEFORE the GPU work is enqueued.

    billable=False records a 'none' charge (paywall off, or under the free
    duration threshold) so metering still has a row to join against.
    Raises InsufficientCredits if the caller has neither free ops nor credits.
    Idempotent: replaying the same job_id returns the original charge.
    """
    period = period_key()
    with connect() as conn, tx(conn):
        existing = conn.execute("SELECT * FROM job_charges WHERE job_id=?", (job_id,)).fetchone()
        if existing:
            return Charge(job_id=job_id, tool=existing["tool"], charge_type=existing["charge_type"],
                         credits=existing["credits"], balance_after=get_balance(conn, identity),
                         free_remaining_after=free_remaining(conn, identity, period))

        owner_type, owner_id = identity.owner

        if not billable:
            charge_type, credits = "none", 0
        else:
            remaining = free_remaining(conn, identity, period)
            if remaining >= credits_needed:
                charge_type, credits = "free", 0
                _bump_free(conn, period, "owner", identity.owner_key, credits_needed)
                _bump_free(conn, period, "ip", identity.ip_hash, credits_needed)
            else:
                balance = get_balance(conn, identity)
                if balance < credits_needed:
                    raise InsufficientCredits(balance=balance, free_remaining=remaining,
                                             tool=tool, needed=credits_needed)
                charge_type, credits = "credit", credits_needed
                grant(conn, owner_type=owner_type, owner_id=owner_id, amount=-credits_needed,
                     kind="job_hold", idempotency_key=f"job_hold:{job_id}", job_id=job_id, note=tool)

        conn.execute(
            """INSERT INTO job_charges (job_id, tool, charge_type, owner_type, owner_id, subject_id,
               ip_hash, period, credits, status, created_at) VALUES (?,?,?,?,?,?,?,?,?,'held',?)""",
            (job_id, tool, charge_type, owner_type, owner_id, identity.subject_id,
             identity.ip_hash, period, credits, now_iso()),
        )
        return Charge(job_id=job_id, tool=tool, charge_type=charge_type, credits=credits,
                     balance_after=get_balance(conn, identity),
                     free_remaining_after=free_remaining(conn, identity, period))


def settle_or_refund(job_id: str, succeeded: bool, reason: str = "job_failed") -> None:
    """Close out a job's charge based on its outcome. THE hook that makes
    "a failed job is refunded automatically" true without an asterisk.

    WHY THIS EXISTS
    ---------------
    Before this, refunds happened in exactly two places: paywall.guard()
    if the ENQUEUE raised (spawning an asyncio task is not a
    failure-prone operation, so essentially never), and
    sweep_stale_holds() 90 minutes later. Neither covers the case that
    actually occurs - the job is accepted, runs, and fails on the GPU
    worker.

    So a paying user watched their job fail with their credit held for
    an hour and a half. Technically recoverable, experientially
    indistinguishable from being robbed, and landing at the single worst
    moment in the product: right after someone paid and did not get the
    thing.

    Called from _run_tool_job's `finally` in routes/_shared.py - one
    call site, every job-based tool, present and future. The `finally`
    placement is deliberate: it runs on success, on every exception
    path, AND on the CancelledError a redeploy triggers, so there is no
    terminal state it can miss.

    NO-OP FOR UNMETERED TOOLS. Eighteen ffmpeg tools share that runner
    and none have a charge row. Returning quietly is what lets the call
    site stay unconditional rather than growing an "is this metered?"
    branch that would go stale the next time a tool is added.

    sweep_stale_holds() stays as the backstop for what no `finally` can
    cover: a task garbage-collected mid-run, or the container killed
    outright.

    NOTE: the two transcription routes with their own background runners
    (routes/video_transcribe.py, routes/youtube_transcribe.py) call this
    directly from their own `finally` blocks - they do not go through
    _run_tool_job, so they inherit nothing from it.
    """
    if succeeded:
        settle_job(job_id)
    else:
        refund_job(job_id, reason=reason)


def refund_job(job_id: str, reason: str = "job_failed") -> bool:
    """Return the credit or free op. Idempotent — safe from worker, poller and sweeper at once."""
    with connect() as conn, tx(conn):
        row = conn.execute("SELECT * FROM job_charges WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            # NOT a warning. Every unmetered tool reaches this on every
            # failure, because the call site in _run_tool_job is
            # deliberately unconditional. WARNING here would produce
            # noise from eighteen tools that have nothing to do with
            # credits, and noise is how a real warning stops being read.
            log.debug("no charge row for job %s - nothing to refund", job_id)
            return False
        if row["status"] == "refunded":
            return False

        if row["charge_type"] == "credit":
            grant(conn, owner_type=row["owner_type"], owner_id=row["owner_id"], amount=row["credits"],
                 kind="job_refund", idempotency_key=f"job_refund:{job_id}", job_id=job_id, note=reason)
        elif row["charge_type"] == "free":
            # KNOWN LIMITATION, safe only while every rule costs 1 credit.
            # charge_for_job() bumps the free counters by credits_needed,
            # this returns exactly 1. Add a tool with credits > 1 and a
            # failed free-tier job permanently eats an op the user never
            # spent. job_charges.credits is 0 for free charges so it
            # cannot be read back from there - the fix is a free_ops
            # column on job_charges, written at charge time. Do that
            # BEFORE raising any rule above 1.
            _bump_free(conn, row["period"], "owner", f"{row['owner_type']}:{row['owner_id']}", -1)
            _bump_free(conn, row["period"], "ip", row["ip_hash"], -1)

        conn.execute("UPDATE job_charges SET status='refunded', refunded_at=?, refund_reason=? WHERE job_id=?",
                    (now_iso(), reason, job_id))
    log.info("refunded job %s (%s)", job_id, reason)
    return True


def settle_job(job_id: str) -> None:
    """Mark a successful job's charge final so the sweeper leaves it alone."""
    with connect() as conn, tx(conn):
        conn.execute("UPDATE job_charges SET status='settled', settled_at=? WHERE job_id=? AND status='held'",
                    (now_iso(), job_id))


def sweep_stale_holds() -> int:
    """Refund holds for jobs that never reached a terminal state (worker crash, restart)."""
    s = get_settings()
    cutoff = (utcnow() - timedelta(minutes=s.hold_timeout_minutes)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    with connect() as conn:
        stale = conn.execute(
            "SELECT job_id FROM job_charges WHERE status='held' AND created_at<?", (cutoff,)
        ).fetchall()
    for row in stale:
        refund_job(row["job_id"], reason="timeout_no_result")
    if stale:
        log.warning("swept %d stale credit holds", len(stale))
    return len(stale)


# Merge bookkeeping lives in free_usage under its OWN scope rather than a
# separate table, so this needs no migration: the primary key is already
# (period, scope, scope_key), and nothing else reads a scope it does not
# name explicitly. free_remaining() reads only 'owner' and 'ip', so a
# 'merged' row is inert to every existing query.
_MERGE_SCOPE = "merged"


def merge_free_usage(conn: sqlite3.Connection, subject_id: str, account_id: str) -> None:
    """On sign-in, fold the browser's anonymous free usage into the account
    so logging in doesn't hand out a second free allowance.

    THE LOGOUT HOLE THIS FIXES (2026-08-27)
    ---------------------------------------
    The original implementation added the subject's usage to the account
    and then DELETED the subject's free_usage row. That looked like tidy
    bookkeeping and was a free-credit dispenser:

        spend both free ops anonymously   -> subject:X used=2
        sign in                           -> account:Y used=2, subject:X DELETED
        sign out                          -> af_sid is a 2-year cookie, so
                                             the same subject:X comes back
                                             with no row at all
        free_remaining()                  -> 2 again

    Repeatable on demand. The IP counter is never merged and never
    deleted, so free_monthly_ops_per_ip (4) was the only thing bounding
    it - which means the advertised "2 free per month" was really "4 per
    month per IP, for anyone who noticed". Not catastrophic, entirely
    trivial to discover.

    THE FIX: never delete. The subject row stays exactly as it was, so a
    signed-out browser is still counted against what it actually spent.
    Idempotency - the thing the DELETE was really providing - comes from
    a merge marker recording how much of this subject's usage has
    already been folded into this account.

    RECONCILES BY DELTA, not by a boolean. The interleaving that makes a
    simple "already merged?" flag wrong:

        subject:X used=1 -> sign in  -> account +1, marker=1
        sign out, spend 1 more       -> subject:X used=2
        sign in again                -> used(2) - marker(1) = 1 -> account +1

    A boolean would skip that second op entirely and hand back a free
    run. Storing the amount makes repeat sign-ins exact rather than
    merely safe.

    Called inside the caller's transaction (sign-in), so a failure here
    rolls back with the rest of it rather than leaving a half-merged
    counter.
    """
    period = period_key()
    subject_key = f"subject:{subject_id}"
    used = _free_used(conn, period, "owner", subject_key)
    if used <= 0:
        return

    merge_key = f"{subject_id}>{account_id}"
    already = _free_used(conn, period, _MERGE_SCOPE, merge_key)
    delta = used - already
    if delta <= 0:
        # Everything this subject spent is already reflected on the
        # account. Signing in and out repeatedly must not keep adding.
        return

    _bump_free(conn, period, "owner", f"account:{account_id}", delta)
    _bump_free(conn, period, _MERGE_SCOPE, merge_key, delta)
    log.info("merged %d free op(s) from %s into account %s", delta, subject_id, account_id)