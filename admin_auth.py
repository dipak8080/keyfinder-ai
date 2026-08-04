"""
admin_auth.py - Rate limiting and brute-force lockout for every /admin/*
route in the app (routes.py, cookie_upload.py, log_stream.py).

WHY THIS EXISTS:

Every admin route validated ADMIN_STATUS_KEY with a bare equality check
and nothing else - no rate limit, no lockout, no cost to guessing wrong.
A Strix pentest run against a local dev copy demonstrated exactly why
that's not enough: an automated brute-force agent fired thousands of key
guesses against /admin/upload-cookies in quick succession with nothing
slowing it down. Against the dev server it caused the target to become
unresponsive; against production the same volume would either eventually
guess a weak key, or at minimum burn CPU/bandwidth for free.

Public tool routes (/convert, /separate, etc.) already have
check_rate_limit from rate_limit.py, tuned for LEGITIMATE USERS -
generous enough that a real person isn't throttled mid-workflow. Admin
routes need the opposite tuning: only the site owner should ever call
these, so genuine admin traffic is naturally low-frequency, and anything
resembling volume is almost certainly an attacker. Reusing the tool
rate limiter's generous defaults here would have done nothing to stop
the Strix bruteforce - hence a separate, much stricter guard.

TWO LAYERS, deliberately separate concerns:

1. RATE LIMIT (guard_admin_request) - caps total requests per IP to any
   admin route, regardless of whether the key is right or wrong. Stops
   raw request-volume abuse (the DoS-by-brute-force pattern Strix
   demonstrated) even before a single key is checked.

2. LOCKOUT (verify_admin_key) - caps WRONG-KEY attempts specifically,
   separate from the rate limit above. A legitimate admin who is rate
   limited can just wait out the window; an attacker whose real goal is
   guessing the key gets locked out much faster and for much longer,
   specifically triggered by wrong answers rather than raw traffic.
   This is what actually stops a brute-force from ever completing, not
   just slows it down.

Both are in-memory, per-process dicts guarded by a lock - the same
pattern as rate_limit.py's own limiter. Fine for a single-VPS
deployment; the whole point is stopping automated abuse within a single
process's lifetime, not distributed coordination.
"""
import time
import threading
from typing import Optional

from fastapi import HTTPException, Request

from config import (
    logger,
    ADMIN_STATUS_KEY,
    ADMIN_RATE_LIMIT_MAX_REQUESTS,
    ADMIN_RATE_LIMIT_WINDOW_SECONDS,
    ADMIN_LOCKOUT_THRESHOLD,
    ADMIN_LOCKOUT_WINDOW_SECONDS,
    ADMIN_LOCKOUT_DURATION_SECONDS,
)

_lock = threading.Lock()

# ip -> list of request timestamps (rolling window), for the raw
# request-volume cap. Separate dict from rate_limit.py's own store so
# admin traffic can never share a bucket with tool traffic by accident.
_admin_request_log: dict = {}

# ip -> list of WRONG-KEY timestamps (rolling window) - what actually
# drives lockout, independent of how many *correct* requests that IP made.
_admin_auth_failures: dict = {}

# ip -> timestamp until which this IP is locked out entirely, regardless
# of whether its next key attempt would have been correct.
_admin_locked_until: dict = {}


def _get_client_ip(request: Request) -> str:
    """Same X-Forwarded-For-first logic as log_stream.py's
    _get_real_client_ip() - nginx sits in front of this app, so
    request.client.host would just be nginx's own address, not the real
    caller's. Duplicated here rather than imported to keep this module
    free of a dependency on log_stream.py, which is one of the three
    files this module itself protects."""
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    return request.client.host if request.client else "-"


def _prune(events: list, window_seconds: int, now: float) -> list:
    cutoff = now - window_seconds
    return [t for t in events if t > cutoff]


def guard_admin_request(request: Request) -> str:
    """
    FastAPI dependency - add to every /admin/* route:

        @router.get("/admin/status")
        async def admin_status(request: Request, key: str = Query(...)):
            client_ip = guard_admin_request(request)
            verify_admin_key(key, client_ip)
            ...

    Raises 429 if this IP has made too many admin requests recently
    (ADMIN_RATE_LIMIT_MAX_REQUESTS per ADMIN_RATE_LIMIT_WINDOW_SECONDS),
    or 403 immediately if this IP is currently locked out from a prior
    run of wrong-key attempts (see verify_admin_key). Returns the
    resolved client IP so the caller doesn't need to re-derive it for
    the verify_admin_key() call that follows.

    Deliberately checked BEFORE the key is ever compared - an IP that's
    already over its request budget or already locked out gets rejected
    without spending any time on the key check at all.
    """
    client_ip = _get_client_ip(request)
    now = time.time()

    with _lock:
        locked_until = _admin_locked_until.get(client_ip, 0)
        if now < locked_until:
            remaining = int(locked_until - now)
            logger.warning(
                f"[ADMIN_AUTH] Rejected - '{client_ip}' is locked out for "
                f"{remaining}s more (too many wrong-key attempts)"
            )
            raise HTTPException(
                403,
                f"Too many failed admin authentication attempts. Try again in {remaining}s."
            )

        events = _prune(_admin_request_log.get(client_ip, []), ADMIN_RATE_LIMIT_WINDOW_SECONDS, now)
        if len(events) >= ADMIN_RATE_LIMIT_MAX_REQUESTS:
            logger.warning(
                f"[ADMIN_AUTH] Rejected - '{client_ip}' exceeded "
                f"{ADMIN_RATE_LIMIT_MAX_REQUESTS} admin requests/"
                f"{ADMIN_RATE_LIMIT_WINDOW_SECONDS}s"
            )
            raise HTTPException(429, "Too many admin requests. Please slow down.")

        events.append(now)
        _admin_request_log[client_ip] = events

    return client_ip


def verify_admin_key(key: str, client_ip: str):
    """
    Checks key against ADMIN_STATUS_KEY. On a WRONG key, records a
    failure for this IP and - once ADMIN_LOCKOUT_THRESHOLD wrong
    attempts land within ADMIN_LOCKOUT_WINDOW_SECONDS - locks the IP out
    entirely for ADMIN_LOCKOUT_DURATION_SECONDS, independent of the
    request-volume rate limit in guard_admin_request().

    This is the layer that actually stops a brute-force from completing:
    guard_admin_request() alone only slows raw request volume, but an
    attacker patient enough to stay under that cap could still
    eventually try every key in a wordlist. Locking out after a small
    number of WRONG answers specifically closes that gap - a legitimate
    admin who mistypes the key once or twice never notices this, an
    automated guesser hits the wall almost immediately.

    Call this AFTER guard_admin_request() in every admin route, passing
    the client_ip it returned - so lockout state and rate-limit state
    are always keyed by the same IP resolution.
    """
    if key == ADMIN_STATUS_KEY:
        return  # correct - nothing to record, request proceeds normally

    now = time.time()
    with _lock:
        failures = _prune(_admin_auth_failures.get(client_ip, []), ADMIN_LOCKOUT_WINDOW_SECONDS, now)
        failures.append(now)
        _admin_auth_failures[client_ip] = failures

        if len(failures) >= ADMIN_LOCKOUT_THRESHOLD:
            _admin_locked_until[client_ip] = now + ADMIN_LOCKOUT_DURATION_SECONDS
            lockout_min = ADMIN_LOCKOUT_DURATION_SECONDS // 60
            logger.critical(
                f"[ADMIN_AUTH] LOCKOUT TRIGGERED - '{client_ip}' made "
                f"{len(failures)} wrong admin-key attempts within "
                f"{ADMIN_LOCKOUT_WINDOW_SECONDS}s. Locked out for {lockout_min} min. "
                f"This is the signature of an automated brute-force attempt."
            )

    logger.warning(f"[ADMIN_AUTH] Wrong admin key from '{client_ip}' ({len(failures)}/{ADMIN_LOCKOUT_THRESHOLD} before lockout)")
    raise HTTPException(403, "Invalid admin key")