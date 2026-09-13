"""
rate_limit.py - Per-IP rate limiting for the heavy endpoints
(/download, /analyze, /separate).

Sliding window, no external dependency (Redis etc.) needed.

check_rate_limit() accepts OPTIONAL max_requests/window_seconds overrides
so different routes can have different limits (e.g. /separate's much
stricter 1-per-hour vs. /download and /analyze's shared default) -
existing usage via plain Depends(check_rate_limit) is unaffected, since
both params default to the original global config values.

key_override replaces the IP half of the window key, so paid callers are
counted against an account rather than whatever IP they are on. See
credits/limits.py for who passes it.

TWO BACKENDS, CHOSEN BY COST (2026-09-12)
-----------------------------------------
The window used to be in-memory only, and said so: "Resets on restart".
That is fine for /convert, where the limit exists to stop someone
hammering ffmpeg. It is not fine for the metered GPU tools, where the
free-tier window is the throttle standing between a stranger and paid
RunPod time - and where a deploy, of which there are several on a busy
day, handed every caller a fresh allowance.

So: any call that passes `tier` - which is only credits/limits.py, and
therefore exactly the set of tools that cost money to run - is counted
in SQLite and survives a restart. Everything else keeps the in-memory
dict and is byte-identical to before, including the ~35 call sites that
pass nothing.

The 429 shape, Retry-After, and _format_duration are shared by both
paths on purpose: a paid user hitting a limit should get exactly the
same well-formed error a free one does.

check_rate_limit is a SYNC FastAPI dependency, which FastAPI runs in its
own threadpool, so the SQLite work here never touches the event loop.
"""
import os
import sqlite3
import time
import threading
from pathlib import Path

from fastapi import Request, HTTPException

from client_ip import get_client_ip

from config import (
    logger,
    RATE_LIMIT_ENABLED,
    RATE_LIMIT_MAX_REQUESTS,
    RATE_LIMIT_WINDOW_SECONDS,
)

_lock = threading.Lock()
# (ip, path) -> list of request timestamps within the current window
#
# With key_override in play a key may instead be (override_string, path).
# The two shapes share this dict deliberately and cannot collide: an
# override is always "account:<uuid>|<route>" or "subject:<uuid>|<route>",
# which is not a value _get_client_ip() can ever return.
_requests = {}

_DB_PATH = os.environ.get("RATE_LIMIT_DB_PATH", "")
_db_lock = threading.Lock()
_db_ready = False


def _db_file() -> str:
    if _DB_PATH:
        return _DB_PATH
    try:
        from credits.config import get_settings
        base = Path(get_settings().db_path).parent
    except Exception:
        base = Path("data")
    return str(base / "rate_limits.db")


def _connect() -> sqlite3.Connection:
    """Caller MUST close. `with sqlite3.connect(...)` is a TRANSACTION
    context manager, not a closing one - it never released the handle."""
    global _db_ready
    path = _db_file()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    if not _db_ready:
        with _db_lock:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS rate_hits (
                       bucket TEXT NOT NULL,
                       ts     REAL NOT NULL
                   )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_rate_hits ON rate_hits (bucket, ts)"
            )
            _db_ready = True
    return conn


def _persistent_check_and_record(bucket: str, window: float, limit: int, now: float):
    """Prune, count, and (if under the limit) record - as ONE transaction.

    Read and write used to be two separate connections with nothing
    between them, so N concurrent requests on the same bucket all read
    the same pre-insert count and all passed a limit of 1. BEGIN
    IMMEDIATE takes SQLite's write lock up front, which serializes this
    across threads AND across uvicorn workers; busy_timeout (set in
    _connect) is what makes the losers wait rather than error.

    Returns (allowed, timestamps_before_this_request).
    """
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("DELETE FROM rate_hits WHERE bucket = ? AND ts < ?",
                         (bucket, now - window))
            rows = conn.execute(
                "SELECT ts FROM rate_hits WHERE bucket = ? ORDER BY ts", (bucket,)
            ).fetchall()
            timestamps = [float(r[0]) for r in rows]

            allowed = len(timestamps) < limit
            if allowed:
                conn.execute("INSERT INTO rate_hits (bucket, ts) VALUES (?, ?)",
                             (bucket, now))
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        return allowed, timestamps
    finally:
        conn.close()


def _sweep_persistent(now: float) -> None:
    """Drops rows older than any window we use. Cheap and rare."""
    conn = _connect()
    try:
        conn.execute("DELETE FROM rate_hits WHERE ts < ?", (now - 172800,))
    finally:
        conn.close()


def _sweep_memory(now: float) -> None:
    """Drops buckets whose newest hit is older than any window in use.

    _requests pruned timestamps INSIDE a bucket but never removed the
    bucket itself, so every unique (ip, path) ever seen stayed resident
    for the container's lifetime. Called under _lock.
    """
    dead = [
        k for k, ts in _requests.items()
        if not ts or ts[-1] < now - (_key_windows.get(k, 3600) + _MEM_KEY_SLACK_SECONDS)
    ]
    for k in dead:
        del _requests[k]
        _key_windows.pop(k, None)
    if dead:
        logger.info(f"[RATE LIMIT] Swept {len(dead)} idle in-memory buckets "
                    f"({len(_requests)} remain)")


_last_sweep = 0.0
_last_mem_sweep = 0.0

# Slack added on top of a bucket's OWN window before it may be swept.
#
# This used to be one flat number for every key, which forced a choice
# between two wrong answers once a daily window existed: 7200 dropped an
# idle daily bucket after two hours (any of the ~35 non-persistent call
# sites triggers a sweep, and the sweep scans everything), while a flat
# 172800 kept every cheap per-minute bucket resident for 48 hours to
# serve a fallback path only the separation buckets ever use.
#
# _key_windows records each bucket's window at insert, so the sweep can
# ask "how long does THIS key need" instead of applying the longest
# window in the app to all of them.
_MEM_KEY_SLACK_SECONDS = 3600
_MEM_SWEEP_INTERVAL_SECONDS = 600
_key_windows: dict = {}


def _get_client_ip(request: Request) -> str:
    # CF-Connecting-IP first: X-Forwarded-For's first entry is whatever
    # the caller put there, and this value IS the bucket key for every
    # free-tier limit. See client_ip.py.
    return get_client_ip(request, default="unknown")


def _format_duration(seconds: int) -> str:
    """
    Turns a raw seconds value into a human-readable string for
    user-facing rate limit messages, e.g. 3600 -> "1 hour",
    90 -> "1 min 30 sec", 45 -> "45 seconds".

    Since the 429 error message is built dynamically from
    effective_window at request time, this keeps the user-facing text
    automatically in sync with whatever RATE_LIMIT_WINDOW_SECONDS /
    SEPARATION_RATE_LIMIT_WINDOW_SECONDS (or any future per-route
    override) is set to - no separate frontend copy to maintain.
    """
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds} second{'s' if seconds != 1 else ''}"

    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)

    parts = []
    if hours:
        parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes:
        parts.append(f"{minutes} min")
    if secs and not hours:  # skip seconds once we're talking in hours
        parts.append(f"{secs} sec")

    return " ".join(parts)


def _reject(ip, path, key_override, timestamps, effective_max, effective_window,
            now, tier, route=None):
    # route is logged ALONGSIDE the bucket, never instead of it. With a
    # shared bucket the two differ, and the route is the half that says
    # which endpoint someone is actually hitting - the signal the
    # endpoint-hopping investigation ran on in the first place.
    where = path if not route or route == path else f"{path} (route {route})"
    logger.warning(
        f"[RATE LIMIT] Blocked {ip} on {where} - {len(timestamps)} requests in window"
        + (f" (keyed on {key_override})" if key_override else "")
    )
    retry_after = int(effective_window - (now - timestamps[0])) if timestamps else effective_window
    message = (
        f"Too many requests. Please wait a moment before trying again "
        f"(limit: {effective_max} request(s) per {_format_duration(effective_window)})."
    )
    # Detail stays a plain STRING when no tier is passed - that is
    # what all ~35 existing call sites produce today and what the
    # frontend's parseDetail() already handles. Only the credits
    # routes get the structured form, so nothing else changes shape.
    detail = message if tier is None else {
        "kind": "rate_limited",
        "message": message,
        "tier": tier,
        "max_requests": effective_max,
        "window_seconds": effective_window,
        "retry_after_seconds": max(retry_after, 1),
    }
    raise HTTPException(
        429, detail, headers={"Retry-After": str(max(retry_after, 1))},
    )


def check_rate_limits(
    request: Request,
    windows,
    key_override: str = None,
    tier: str = None,
):
    """Check SEVERAL windows atomically, then record in all of them.

    windows is a sequence of (bucket_key, max_requests, window_seconds).

    WHY THIS EXISTS. Calling check_rate_limit twice records a hit on the
    first window even when the second then rejects, so a caller stopped
    by a daily cap still burned an hourly slot, and vice versa. There is
    no ordering that avoids it - only checking everything before
    recording anything does.

    One connection, one BEGIN IMMEDIATE: prune and count every bucket,
    reject on the first window over its limit with nothing written, and
    insert for all of them only if all pass. Taking the write lock up
    front is also what serializes this across threads AND uvicorn
    workers, the same reason _persistent_check_and_record takes it.

    Returns a RECEIPT - the (bucket, ts) rows written - so a caller whose
    request later fails for an unrelated reason can hand back the slots
    it never used. See refund_rate_limit_hits.
    """
    global _last_sweep

    if not RATE_LIMIT_ENABLED:
        return []

    ip = _get_client_ip(request)
    subject = key_override if key_override is not None else ip
    now = time.time()
    route = request.url.path

    specs = [(f"{subject}|{bucket}", bucket, int(limit), float(window))
             for bucket, limit, window in windows]

    try:
        if now - _last_sweep > 3600:
            _last_sweep = now
            _sweep_persistent(now)

        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                failure = None
                for full_key, bucket, limit, window in specs:
                    conn.execute("DELETE FROM rate_hits WHERE bucket = ? AND ts < ?",
                                 (full_key, now - window))
                    rows = conn.execute(
                        "SELECT ts FROM rate_hits WHERE bucket = ? ORDER BY ts", (full_key,)
                    ).fetchall()
                    timestamps = [float(r[0]) for r in rows]
                    if len(timestamps) >= limit:
                        failure = (bucket, timestamps, limit, window)
                        break

                if failure is not None:
                    conn.execute("ROLLBACK")
                else:
                    for full_key, _bucket, _limit, _window in specs:
                        conn.execute("INSERT INTO rate_hits (bucket, ts) VALUES (?, ?)",
                                     (full_key, now))
                    conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()

        if failure is not None:
            bucket, timestamps, limit, window = failure
            _reject(ip, bucket, key_override, timestamps, limit, int(window),
                    now, tier, route=route)

        return [(full_key, now) for full_key, _b, _l, _w in specs]

    except HTTPException:
        raise
    except Exception:
        # Same posture as the single-window path: a DB problem must not
        # take the site down, and must not silently hand out unlimited
        # free GPU either. Fall back to the in-memory windows and say so.
        logger.error(
            f"[RATE LIMIT] persistent multi-window unavailable for {route}, "
            f"falling back to in-memory", exc_info=True,
        )
        with _lock:
            # Key shape matches the single-window path: (subject, bucket).
            # full_key already contains the subject, so using it here made
            # a second, incompatible shape in the same dict.
            for full_key, bucket, limit, window in specs:
                key = (subject, bucket)
                timestamps = [t for t in _requests.get(key, []) if t >= now - window]
                if len(timestamps) >= limit:
                    _reject(ip, bucket, key_override, timestamps, limit,
                            int(window), now, tier, route=route)
                _requests[key] = timestamps
            for _full_key, bucket, _limit, window in specs:
                _requests[(subject, bucket)].append(now)
                _key_windows[(subject, bucket)] = window
        # No receipt: these hits live in memory, and the refund path only
        # knows how to delete SQLite rows. So during a DB outage slots are
        # counted but never refunded - the safe direction, and worth an
        # alert once spend alerting exists.
        return []


def refund_rate_limit_hits(receipt) -> None:
    """Hand back slots recorded for a request that never used them.

    A rate limit protects a RESOURCE. A submission rejected afterwards
    for a full queue, a disabled tool or a bad file consumed no GPU, no
    queue slot and no worker time, so charging it against an allowance
    protects nothing - it just locks someone out. Under a daily cap that
    matters: a broken uploader could otherwise spend a whole day's
    allowance on requests that never ran.

    Deletes the exact rows by (bucket, ts), so a concurrent request that
    recorded its own hit at a different timestamp is untouched. Best
    effort by design: failing to refund must never turn into a 500 on a
    response that has already been decided.
    """
    if not receipt:
        return
    try:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for bucket, ts in receipt:
                    # By rowid, one row at a time. `now` is read before
                    # BEGIN IMMEDIATE, so the write lock serializes the
                    # inserts but not the clock read: two requests in the
                    # same bucket can land on an identical ts, and a bare
                    # DELETE on (bucket, ts) then hands back a slot
                    # belonging to a request still in flight.
                    conn.execute(
                        "DELETE FROM rate_hits WHERE rowid = ("
                        " SELECT rowid FROM rate_hits WHERE bucket = ? AND ts = ?"
                        " LIMIT 1)",
                        (bucket, ts),
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
    except Exception:
        logger.warning("[RATE LIMIT] could not refund %d hit(s)", len(receipt), exc_info=True)


def check_rate_limit(
    request: Request,
    max_requests: int = None,
    window_seconds: int = None,
    key_override: str = None,
    tier: str = None,
    bucket_key: str = None,
    persistent: bool = False,
):
    """
    Use as a FastAPI dependency on rate-limited routes:
        # Default (shared) limit:
        @router.post("/download", dependencies=[Depends(check_rate_limit)])

        # Custom per-route limit:
        from functools import partial
        @router.post("/separate", dependencies=[
            Depends(partial(check_rate_limit, max_requests=1, window_seconds=3600))
        ])

    Raises a clean 429 if the caller's IP has exceeded max_requests
    within window_seconds on this specific path.

    tier, when passed, does two things. It is echoed in the 429 detail as
    a machine-readable field, so the frontend knows a "free" tier limit
    on a metered tool can be lifted by buying credits. It ALSO selects
    the persistent backend, because tier is passed by exactly the callers
    whose tools cost real GPU money and whose throttle must therefore
    outlive a deploy.
    """
    global _last_sweep, _last_mem_sweep

    if not RATE_LIMIT_ENABLED:
        return

    effective_max = max_requests if max_requests is not None else RATE_LIMIT_MAX_REQUESTS
    effective_window = window_seconds if window_seconds is not None else RATE_LIMIT_WINDOW_SECONDS

    # bucket_key replaces the PATH half of the key, so several routes can
    # share one window. Without it the key is per-path, which is why the
    # four standard separation routes each had an independent allowance
    # and one IP could spend all four in the same hour.
    path = bucket_key if bucket_key is not None else request.url.path
    ip = _get_client_ip(request)
    subject = key_override if key_override is not None else ip

    now = time.time()

    # tier still implies persistence, but persistence no longer implies
    # tier: a route can have a restart-proof window while keeping the
    # plain-string 429 detail its frontend already parses.
    if tier is None and not persistent:
        key = (subject, path)
        with _lock:
            if now - _last_mem_sweep > _MEM_SWEEP_INTERVAL_SECONDS:
                _last_mem_sweep = now
                _sweep_memory(now)

            timestamps = _requests.get(key, [])
            cutoff = now - effective_window
            timestamps = [t for t in timestamps if t >= cutoff]

            if len(timestamps) >= effective_max:
                _requests[key] = timestamps
                _reject(ip, path, key_override, timestamps,
                        effective_max, effective_window, now, tier,
                        route=request.url.path)

            timestamps.append(now)
            _requests[key] = timestamps
            _key_windows[key] = effective_window
        return

    # Metered path: survives restarts. A DB failure must never hand out
    # free GPU time silently, but it must also never take the site down,
    # so it falls back to the in-memory window and says so.
    bucket = f"{subject}|{path}"
    try:
        if now - _last_sweep > 3600:
            _last_sweep = now
            _sweep_persistent(now)

        allowed, timestamps = _persistent_check_and_record(
            bucket, effective_window, effective_max, now
        )

        if not allowed:
            _reject(ip, path, key_override, timestamps,
                    effective_max, effective_window, now, tier,
                    route=request.url.path)
    except HTTPException:
        raise
    except Exception:
        logger.error(
            f"[RATE LIMIT] persistent window unavailable for {path}, "
            f"falling back to in-memory", exc_info=True,
        )
        key = (subject, path)
        with _lock:
            timestamps = [t for t in _requests.get(key, []) if t >= now - effective_window]
            if len(timestamps) >= effective_max:
                _reject(ip, path, key_override, timestamps,
                        effective_max, effective_window, now, tier,
                        route=request.url.path)
            timestamps.append(now)
            _requests[key] = timestamps