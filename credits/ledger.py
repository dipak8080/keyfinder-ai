"""Credit ledger.

credit_ledger is append-only — balance is SUM(delta), nothing expires.
The free tier is a separate monthly counter, tracked against both the owner
(account or anonymous subject) and the IP hash, so clearing cookies gets a
new subject but the same IP bucket.

job_charges is the idempotency anchor: exactly one row per job that ever
reached the paywall, so a retried request or a doubled webhook can never
double-charge or double-refund. It also records free_ops - how much
ALLOWANCE a job consumed, as distinct from how many credits - because
those two are different numbers and only one of them was being stored.
See charge_for_job() and migration 003.

--------------------------------------------------------------------------
FREE-TIER ACCOUNTING (rewritten 2026-08-27)

Two holes, same shape, opposite directions. Both let one person take four
free ops a month instead of two, and both were reachable by clicking
"sign out".

The mechanism behind both: /auth/logout deletes the SESSION cookie and
sets subjects.account_id = NULL, but af_sid — the subject cookie — has a
two-year max age and survives untouched. So signing out swaps
identity.owner_key from "account:Y" back to "subject:X" while leaving
the same browser in place. Whether that swap gives free ops back depends
entirely on what each counter holds at that moment.

HOLE 1 (found in production). merge_free_usage() folded the anonymous
subject's usage into the account at sign-in and then DELETED the
subject's row:

    spend both free ops anonymously  -> subject:X used=2
    sign in                          -> account:Y used=2, subject:X DELETED
    sign out                         -> subject:X returns with no row
    free_remaining()                 -> 2 again

HOLE 2 (found while fixing hole 1, and NOT closed by fixing it). Usage
accrued while signed in only ever touched the account counter, so a
browser that signed in BEFORE spending anything kept a pristine subject
counter:

    sign in on a fresh browser       -> subject:X used=0, account:Y used=0
    spend both free ops              -> account:Y used=2, subject:X STILL 0
    sign out                         -> subject:X used=0
    free_remaining()                 -> 2 again

Fixing only hole 1 would have left hole 2 wide open, which is worth
stating plainly: the first fix was aimed at the symptom that got
reported, not at the thing causing it.

THE FIX. Stop treating "which counter" as a choice made per request.
Every free op is charged to BOTH keys — the owner key AND the subject
key — and free_remaining() reads the MAX of the two. Whichever identity
the browser is wearing when it next asks, the number it gets already
includes everything that browser and that account have spent.

merge_free_usage() then becomes a symmetric levelling: raise both keys
to max(subject_used, account_used). That is idempotent by construction —
raising to a maximum you already sit at changes nothing — which is what
removes the need for the merge-marker table an earlier attempt at this
introduced. It also needs no schema change at all, and that matters: the
first attempt tried to record merges as a third scope in free_usage,
which carries CHECK (scope IN ('owner','ip')), so every magic link
500'd until it was reverted.

WHAT THIS DELIBERATELY DOES NOT DO. It does not try to make the free
tier unbeatable. A genuinely new browser on a genuinely new IP is a new
person as far as this code can tell, and treating that as fraud would
mean punishing shared offices and mobile CGNAT. The IP counter
(free_monthly_ops_per_ip, 4) is the backstop, and the real ceiling on
abuse is that a free op costs about two cents.
--------------------------------------------------------------------------
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


def _free_owner_keys(owner_type: str, owner_id: str, subject_id: str | None) -> list[str]:
    """Every owner-scope key a single free op must be written to.

    For an ANONYMOUS caller that is one key: subject:X, and owner_key is
    already exactly that, so the list has one entry.

    For a SIGNED-IN caller it is two: account:Y and subject:X. Writing
    only the account key is what left hole 2 open - see the module
    docstring. The subject key is the one that survives sign-out, so it
    has to carry the same number.

    Deduplicated rather than assumed distinct: identity.owner returns
    ("subject", subject_id) for anonymous callers, so the two keys are
    the SAME string then, and bumping twice would silently double-charge
    every anonymous free op. That is a one-line mistake with a very
    confusing symptom - 2 free ops that behave like 1 - which is why the
    dedupe lives here rather than at the three call sites.

    subject_id is Optional because job_charges.subject_id is nullable in
    principle; a row without one still gets its owner key returned.
    """
    keys = [f"{owner_type}:{owner_id}"]
    if subject_id:
        subject_key = f"subject:{subject_id}"
        if subject_key not in keys:
            keys.append(subject_key)
    return keys


def free_remaining(conn: sqlite3.Connection, identity: Identity, period: str | None = None) -> int:
    """How many free ops this caller has left this month.

    Takes the MAX across the owner key and the subject key rather than
    reading whichever one identity happens to be wearing. Those two
    diverge precisely when someone signs in or out mid-month, and the
    honest answer is "everything this browser OR this account has
    spent", not "whichever number happens to be smaller right now".

    Then takes the MIN against the IP allowance, which is a separate
    guard with a separate purpose: the owner keys bound one identity,
    the IP key bounds one network. Both apply.
    """
    s = get_settings()
    period = period or period_key()
    owner_type, owner_id = identity.owner
    owner_used = max(
        _free_used(conn, period, "owner", key)
        for key in _free_owner_keys(owner_type, owner_id, identity.subject_id)
    )
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
            AUDIO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS,
            AUDIO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS,
            MIDI_HQ_RATE_LIMIT_MAX_REQUESTS,
            MIDI_HQ_RATE_LIMIT_WINDOW_SECONDS,
        )
    except ImportError:
        return {}
    return {
        "separate-hq": (SEPARATION_HQ_RATE_LIMIT_MAX_REQUESTS, SEPARATION_HQ_RATE_LIMIT_WINDOW_SECONDS),
        "stems-hq": (STEMS_HQ_RATE_LIMIT_MAX_REQUESTS, STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS),
        "youtube/separate-hq": (YOUTUBE_SEPARATE_HQ_RATE_LIMIT_MAX_REQUESTS, YOUTUBE_SEPARATE_HQ_RATE_LIMIT_WINDOW_SECONDS),
        "youtube/stems-hq": (YOUTUBE_STEMS_HQ_RATE_LIMIT_MAX_REQUESTS, YOUTUBE_STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS),
        # ADDED 2026-08-29. Both were metered without being added here,
        # so GET /credits/me reported a rate_limit block covering four of
        # the six metered tools and silently omitting two.
        #
        # WHY THAT MATTERED MORE THAN IT LOOKS. The frontend's fallback
        # for a missing key is /limits, which serves the FREE numbers and
        # nothing else - it is static and cacheable precisely because it
        # does not know who is asking. So a credit holder on
        # /audio-to-midi-hq was shown "2 per hour" while the limiter was
        # actually enforcing 30. A UI that lies pessimistically is still
        # a UI that lies, and this one lied specifically to the people
        # who had paid.
        #
        # "transcribe" is ONE key for three routes - /speech-to-text,
        # /youtube/transcribe and /video-to-text - because they share one
        # credits rule, one RunPod endpoint and one semaphore. But
        # rate_limit.py keys its window on (ip, path), so those three
        # have SEPARATE per-IP buckets at the same number. The value
        # below is /speech-to-text's; all three are 2/hour today, and if
        # they ever diverge this line reports whichever one it names
        # rather than being wrong about all of them. The per-route
        # numbers stay available in /limits.
        "transcribe": (AUDIO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS, AUDIO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS),
        "audio-to-midi-hq": (MIDI_HQ_RATE_LIMIT_MAX_REQUESTS, MIDI_HQ_RATE_LIMIT_WINDOW_SECONDS),
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

        # How many free monthly ops this job consumes. Zero for 'credit'
        # and 'none' charges, which spend no allowance at all - only the
        # 'free' branch below sets it. Stored on the charge row so
        # refund_job() can return exactly what was taken instead of
        # assuming 1, which is what it did until 2026-08-29 and which was
        # correct only by coincidence of every rule costing 1 credit.
        free_ops = 0

        if not billable:
            charge_type, credits = "none", 0
        else:
            remaining = free_remaining(conn, identity, period)
            if remaining >= credits_needed:
                charge_type, credits = "free", 0
                free_ops = credits_needed
                # BOTH owner keys, not just the active one. For a
                # signed-in caller that means account:Y AND subject:X -
                # the subject key is what survives sign-out, and leaving
                # it at zero is exactly what hole 2 exploited. For an
                # anonymous caller the helper returns a single key, so
                # this stays one write.
                for key in _free_owner_keys(owner_type, owner_id, identity.subject_id):
                    _bump_free(conn, period, "owner", key, credits_needed)
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
               ip_hash, period, credits, free_ops, status, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,'held',?)""",
            (job_id, tool, charge_type, owner_type, owner_id, identity.subject_id,
             identity.ip_hash, period, credits, free_ops, now_iso()),
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
            # Mirrors the charge exactly: every key that was bumped gets
            # decremented, read back FROM THE ROW rather than from the
            # current identity. That distinction is load-bearing - a job
            # can fail long after the person signed out, and refunding
            # against whoever holds the cookie now would credit the
            # wrong counter. job_charges stores owner_type, owner_id AND
            # subject_id at charge time precisely so this can be exact.
            #
            # free_ops is read the same way, and for the same reason
            # (fixed 2026-08-29). This used to be a hardcoded -1 while
            # charge_for_job() bumped by credits_needed - correct only
            # because every rule costs 1 credit, and silently wrong the
            # moment one did not. The amount could not be derived: for a
            # free charge job_charges.credits is 0 by design, because no
            # credits moved. So the allowance spent is now recorded
            # explicitly at charge time.
            #
            # The `or 1` fallback covers rows written before migration
            # 003. It matches that migration's DEFAULT 1, and 1 is the
            # correct historical value for all of them rather than a
            # placeholder - every existing charge was made under a
            # 1-credit rule.
            ops = row["free_ops"] if "free_ops" in row.keys() else None
            ops = int(ops) if ops is not None else 1
            for key in _free_owner_keys(row["owner_type"], row["owner_id"], row["subject_id"]):
                _bump_free(conn, row["period"], "owner", key, -ops)
            _bump_free(conn, row["period"], "ip", row["ip_hash"], -ops)

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


def merge_free_usage(conn: sqlite3.Connection, subject_id: str, account_id: str) -> None:
    """On sign-in, level this browser and this account to whichever has
    spent more free ops this month.

    Called from /auth/verify inside the sign-in transaction, so a failure
    here rolls back the whole sign-in rather than leaving a half-merged
    counter. That property is why this function must stay boring: it is
    on the critical path of every magic link on the site.

    WHY LEVELLING RATHER THAN ADDING
    --------------------------------
    The original implementation ADDED the subject's usage to the
    account's, then deleted the subject's row to stop a second sign-in
    adding it twice. That delete was hole 1 (see the module docstring),
    and removing it alone would have made the addition non-idempotent -
    sign in, sign out, sign in again, and the account's counter climbs
    with every round trip even though nothing was spent.

    Raising both keys to their maximum has no such problem: it is
    idempotent by construction, because raising to a maximum you already
    sit at is a no-op. Every sign-in can call it, in any order, any
    number of times.

    An earlier attempt solved the idempotency problem with a
    merge-marker row instead, written to free_usage under a third scope.
    That is what took sign-in down in production: free_usage carries
    CHECK (scope IN ('owner','ip')) and the insert raised inside this
    very transaction, so every magic link returned 500 until it was
    reverted. Levelling needs no marker, no new table and no schema
    change - which is the main reason to prefer it, on a database that
    holds the money.

    BOTH DIRECTIONS, deliberately. Raising the ACCOUNT to the subject's
    level is the obvious half: it stops someone spending their free ops
    anonymously and then signing in for two more. Raising the SUBJECT to
    the account's level is the half that is easy to leave out, and it
    closes the case where a second device signs into an already-exhausted
    account and then signs out - without it, that browser walks away with
    a full allowance it did nothing to earn.

    The cost of that second half is that a browser which signs into an
    exhausted account loses its own free allowance for the month. That
    is the right trade: it is the same person in every normal case, and
    the alternative is a two-click reset anyone can find.
    """
    period = period_key()
    subject_key = f"subject:{subject_id}"
    account_key = f"account:{account_id}"

    subject_used = _free_used(conn, period, "owner", subject_key)
    account_used = _free_used(conn, period, "owner", account_key)
    level = max(subject_used, account_used)
    if level <= 0:
        return

    if level > account_used:
        _bump_free(conn, period, "owner", account_key, level - account_used)
    if level > subject_used:
        _bump_free(conn, period, "owner", subject_key, level - subject_used)

    if level > min(subject_used, account_used):
        log.info(
            "levelled free usage to %d for subject %s / account %s (was %d / %d)",
            level, subject_id, account_id, subject_used, account_used,
        )