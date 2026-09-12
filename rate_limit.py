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


def _persistent_window(bucket: str, window: float, now: float) -> list:
    """Timestamps still inside the window, oldest first. Prunes as it goes."""
    with _connect() as conn:
        conn.execute("DELETE FROM rate_hits WHERE bucket = ? AND ts < ?",
                     (bucket, now - window))
        rows = conn.execute(
            "SELECT ts FROM rate_hits WHERE bucket = ? ORDER BY ts", (bucket,)
        ).fetchall()
    return [float(r[0]) for r in rows]


def _persistent_record(bucket: str, now: float) -> None:
    with _connect() as conn:
        conn.execute("INSERT INTO rate_hits (bucket, ts) VALUES (?, ?)", (bucket, now))


def _sweep_persistent(now: float) -> None:
    """Drops rows older than any window we use. Cheap and rare."""
    with _connect() as conn:
        conn.execute("DELETE FROM rate_hits WHERE ts < ?", (now - 86400,))


_last_sweep = 0.0


def _get_client_ip(request: Request) -> str:
    # Behind a reverse proxy (Nginx/Caddy on the VPS, same as it was
    # behind Railway's proxy before) - the real client IP is in
    # X-Forwarded-For, not request.client.host (which would just be the
    # proxy's internal address, identical for every request).
    forwarded = request.headers.get("x-forwarded-for")
    return forwarded.split(",")[0].strip() if forwarded else (request.client.host if request.client else "unknown")


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


def _reject(ip, path, key_override, timestamps, effective_max, effective_window, now, tier):
    logger.warning(
        f"[RATE LIMIT] Blocked {ip} on {path} - {len(timestamps)} requests in window"
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


def check_rate_limit(
    request: Request,
    max_requests: int = None,
    window_seconds: int = None,
    key_override: str = None,
    tier: str = None,
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
    global _last_sweep

    if not RATE_LIMIT_ENABLED:
        return

    effective_max = max_requests if max_requests is not None else RATE_LIMIT_MAX_REQUESTS
    effective_window = window_seconds if window_seconds is not None else RATE_LIMIT_WINDOW_SECONDS

    path = request.url.path
    ip = _get_client_ip(request)
    subject = key_override if key_override is not None else ip

    now = time.time()

    if tier is None:
        key = (subject, path)
        with _lock:
            timestamps = _requests.get(key, [])
            cutoff = now - effective_window
            timestamps = [t for t in timestamps if t >= cutoff]

            if len(timestamps) >= effective_max:
                _reject(ip, path, key_override, timestamps,
                        effective_max, effective_window, now, tier)

            timestamps.append(now)
            _requests[key] = timestamps
        return

    # Metered path: survives restarts. A DB failure must never hand out
    # free GPU time silently, but it must also never take the site down,
    # so it falls back to the in-memory window and says so.
    bucket = f"{subject}|{path}"
    try:
        if now - _last_sweep > 3600:
            _last_sweep = now
            _sweep_persistent(now)

        timestamps = _persistent_window(bucket, effective_window, now)

        if len(timestamps) >= effective_max:
            _reject(ip, path, key_override, timestamps,
                    effective_max, effective_window, now, tier)

        _persistent_record(bucket, now)
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
                        effective_max, effective_window, now, tier)
            timestamps.append(now)
            _requests[key] = timestamps