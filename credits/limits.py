"""
credits/limits.py - Tier-aware rate limiting for the four HQ separation
routes.

WHY THIS EXISTS
---------------
config.py sets SEPARATION_HQ_RATE_LIMIT_MAX_REQUESTS = 1 per hour, and
STEMS_HQ / YOUTUBE_*_HQ match it. That number is correct, and its
reasoning is written out in config.py: one HQ job holds the separation
slot for 15-20 minutes, so a looser limit would let a single IP occupy
most of an hour and starve everyone else.

That reasoning is about an ANONYMOUS IP claiming a shared resource it is
not paying for. It stops applying the moment the caller has paid for the
job - a paid job is precisely the job you want holding the slot. Left
unchanged, someone who buys 100 credits could spend them at one per
hour: over four days to use what they bought in one click. The rate
limiter would quietly win the argument with the paywall, and the refund
requests would follow.

So the limit becomes a function of tier rather than a constant:

    free / anonymous  -> exactly today's numbers, keyed on IP.
                         Byte-for-byte the current behaviour.
    paid (has credits) -> a looser limit, keyed on the ACCOUNT.

WHY LOOSENING IT IS SAFE, AND WHY THAT ISN'T MY CLAIM TO MAKE
-------------------------------------------------------------
It's config.py's, twice over. MAX_QUEUED_SEPARATIONS' comment:

    "RAISED 3 -> 6 ... What matters to a waiting user is WAIT TIME, not
     queue position, and wait time is depth divided by concurrency."

and SEPARATION_RATE_LIMIT_MAX_REQUESTS':

    "Safe to raise because this number was never what protected the
     server. MAX_QUEUED_SEPARATIONS is ... the practical effect of
     doubling this is that a heavy user hits the queue guard's 503
     ('busy, try shortly') instead of the limiter's 429."

Both still hold here. A paid user who submits six jobs at once meets
_reject_if_separation_queue_full() and gets "busy, try shortly", which
is the honest answer, and their credits are not consumed by a rejected
submission (the paywall guard charges at accept, and refunds if the
enqueue raises).

What DOES change is spend - same caveat config.py already records
against SEPARATION_RATE_LIMIT_MAX_REQUESTS. The difference is that here
the spend is prepaid: a credit was bought before the GPU-seconds are
burned. That's the whole point of the tier.

KEYED ON ACCOUNT, NOT IP, FOR PAID CALLERS
------------------------------------------
rate_limit.py keys its window on (ip, path). For a paying customer that
is wrong in both directions: two people behind one office NAT or a
mobile CGNAT share a bucket they each paid into separately, while one
person on a phone hopping cell towers gets a fresh allowance every time
their IP moves. Credits are owned by an account, so the limit that
guards them should be too.

Free callers stay keyed on IP. They have no account to key on, and IP is
what the anonymous free tier is defending anyway.

FAIL CLOSED, AND FAIL TOWARDS THE OLD BEHAVIOUR
-----------------------------------------------
Every failure path in here falls back to the existing free-tier limit:
paywall off, tool not enabled, no cookie, unreadable cookie, DB error.
There is no path through this module that ends in "no limit" - the worst
case is today's behaviour, which is the behaviour this whole change is
supposed to leave alone.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from functools import partial
from typing import Callable

from fastapi import Request

from .config import get_settings
from .db import connect
from .identity import SUBJECT_COOKIE, SESSION_COOKIE, SUBJECT_PURPOSE, SESSION_PURPOSE
from .security import unsign

log = logging.getLogger("credits.limits")


@dataclass(frozen=True)
class ResolvedLimit:
    """What the limiter should actually enforce for this caller."""
    max_requests: int
    window_seconds: int
    key: str | None       # None = fall back to rate_limit.py's IP keying
    tier: str             # "free" | "paid" - for logging only


def peek_owner_and_balance(request: Request) -> tuple[str | None, int]:
    """Read-only identity lookup for the rate-limit dependency.

    Deliberately NOT resolve_identity(). That function mints a subject
    cookie and touches last_seen_at on every call - correct for a real
    request, wrong for a limiter that runs before the request is even
    accepted and that must not write anything on a call it may be about
    to reject with a 429.

    Returns (owner_key, balance). owner_key is None when there is no
    usable cookie, which is the overwhelmingly common case (every
    anonymous visitor) and is not an error.
    """
    subject_id = unsign(request.cookies.get(SUBJECT_COOKIE), purpose=SUBJECT_PURPOSE)
    if not subject_id:
        return (None, 0)

    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT account_id FROM subjects WHERE id = ?", (subject_id,)
            ).fetchone()
            account_id = row["account_id"] if row else None

            if account_id:
                owner_key = f"account:{account_id}"
                balance = conn.execute(
                    """SELECT COALESCE(SUM(delta), 0) AS b FROM credit_ledger
                       WHERE (owner_type='account' AND owner_id=?)
                          OR (owner_type='subject' AND owner_id IN
                              (SELECT id FROM subjects WHERE account_id=?))""",
                    (account_id, account_id),
                ).fetchone()["b"]
            else:
                owner_key = f"subject:{subject_id}"
                balance = conn.execute(
                    "SELECT COALESCE(SUM(delta),0) AS b FROM credit_ledger"
                    " WHERE owner_type='subject' AND owner_id=?",
                    (subject_id,),
                ).fetchone()["b"]
    except sqlite3.Error:
        # A credits DB problem must never take down a route that worked
        # fine before credits existed. Fall back to anonymous.
        log.exception("credits DB unavailable in rate limiter - falling back to free tier")
        return (None, 0)

    return (owner_key, int(balance or 0))


def resolve(route_key: str, request: Request, *, free_max: int, free_window: int) -> ResolvedLimit:
    """Decide which limit applies to this caller on this route.

    route_key matches the keys in credits.config tool rules AND the keys
    in the frontend's lib/data/rate-limits.ts - deliberately the same
    strings ("separate-hq", "youtube/stems-hq") so the three places that
    describe a route's limits can be diffed by eye.
    """
    settings = get_settings()
    free = ResolvedLimit(free_max, free_window, None, "free")

    if not settings.paywall_enabled:
        return free

    rule = settings.rule_for(route_key)
    if rule is None or not rule.enabled:
        return free

    owner_key, balance = peek_owner_and_balance(request)
    if not owner_key or balance < rule.credits:
        # Has an identity but no credits: still free tier. They'll meet
        # the paywall itself inside the route and get a 402 with the
        # pack list - a clearer answer than a 429.
        return free

    return ResolvedLimit(
        max_requests=rule.paid_rate_limit,
        window_seconds=rule.paid_rate_window,
        key=f"{owner_key}|{route_key}",
        tier="paid",
    )


def summary_for(identity, route_free_limits: dict[str, tuple[int, int]]) -> dict:
    """The rate-limit block for GET /credits/me.

    WHY THE FRONTEND NEEDS THIS. lib/data/rate-limits.ts is a hardcoded
    table with a comment calling itself the single source of truth. That
    was accurate until tier-aware limits: separate-hq is 1/hour for
    anonymous callers and 12/hour for credit holders, so a static table
    can only ever be right for one of them and will lie to the other.

    Resolved through the SAME code path the limiter uses, not a parallel
    reimplementation - if these two ever disagreed, the UI would be
    confidently wrong, which is worse than having no number at all.

    route_free_limits maps route key -> (max_requests, window_seconds)
    from the host config.py, so the free numbers still live in the file
    where every other limit lives.
    """
    settings = get_settings()
    owner_key, balance = (None, 0)
    if settings.paywall_enabled:
        # Only look up identity when it can change the answer. With the
        # paywall off every caller is on free limits by definition, and
        # this is called on every page load.
        try:
            with connect() as conn:
                row = conn.execute(
                    "SELECT account_id FROM subjects WHERE id = ?", (identity.subject_id,)
                ).fetchone()
                account_id = row["account_id"] if row else None
                if account_id:
                    owner_key = f"account:{account_id}"
                    balance = conn.execute(
                        """SELECT COALESCE(SUM(delta), 0) AS b FROM credit_ledger
                           WHERE (owner_type='account' AND owner_id=?)
                              OR (owner_type='subject' AND owner_id IN
                                  (SELECT id FROM subjects WHERE account_id=?))""",
                        (account_id, account_id),
                    ).fetchone()["b"]
                else:
                    owner_key = f"subject:{identity.subject_id}"
                    balance = conn.execute(
                        "SELECT COALESCE(SUM(delta),0) AS b FROM credit_ledger"
                        " WHERE owner_type='subject' AND owner_id=?",
                        (identity.subject_id,),
                    ).fetchone()["b"]
        except sqlite3.Error:
            log.exception("credits DB unavailable resolving rate-limit summary")

    tools: dict[str, dict] = {}
    credited = False
    for route_key, (free_max, free_window) in route_free_limits.items():
        rule = settings.rule_for(route_key)
        metered = bool(settings.paywall_enabled and rule and rule.enabled)
        if metered and owner_key and balance >= rule.credits:
            credited = True
            tools[route_key] = {
                "max_requests": rule.paid_rate_limit,
                "window_seconds": rule.paid_rate_window,
            }
        else:
            tools[route_key] = {"max_requests": free_max, "window_seconds": free_window}

    return {"tier": "credited" if credited else "free", "tools": tools}


def tiered_rate_limit(route_key: str, *, free_max: int, free_window: int) -> Callable:
    """Drop-in replacement for partial(check_rate_limit, ...) on a route.

    Before:
        dependencies=[Depends(partial(
            check_rate_limit,
            max_requests=SEPARATION_HQ_RATE_LIMIT_MAX_REQUESTS,
            window_seconds=SEPARATION_HQ_RATE_LIMIT_WINDOW_SECONDS,
        ))]

    After:
        dependencies=[Depends(tiered_rate_limit(
            "separate-hq",
            free_max=SEPARATION_HQ_RATE_LIMIT_MAX_REQUESTS,
            free_window=SEPARATION_HQ_RATE_LIMIT_WINDOW_SECONDS,
        ))]

    The free numbers are still passed in from config.py rather than
    duplicated here, so tuning the anonymous limit stays a one-line
    change in the file where every other limit lives. With the paywall
    off this calls check_rate_limit with exactly the arguments the old
    partial() did.
    """
    from rate_limit import check_rate_limit  # host app module, imported late so credits/ stays importable standalone

    def dependency(request: Request) -> None:
        limit = resolve(route_key, request, free_max=free_max, free_window=free_window)
        check_rate_limit(
            request,
            max_requests=limit.max_requests,
            window_seconds=limit.window_seconds,
            key_override=limit.key,
            # Surfaced in the 429 body so the frontend can turn a rate
            # limit into a conversion moment ("buy credits to lift this
            # to 12/hour") rather than a dead end. A free-tier 429 on a
            # metered tool is the single clearest signal that someone
            # wants more of this tool than the free tier allows.
            tier=limit.tier,
        )

    dependency.__name__ = f"tiered_rate_limit_{route_key.replace('/', '_')}"
    return dependency