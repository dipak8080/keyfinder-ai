"""
rate_limit.py - Simple per-IP rate limiting for the heavy endpoints
(/download, /analyze, /separate).

In-memory sliding window, no external dependency (Redis etc.) needed.
Resets on restart and is PER INSTANCE - fine for a single VPS.

check_rate_limit() now accepts OPTIONAL max_requests/window_seconds
overrides so different routes can have different limits (e.g. /separate's
much stricter 1-per-hour vs. /download and /analyze's shared default) -
existing usage via plain Depends(check_rate_limit) is unaffected, since
both params default to the original global config values.
"""
import time
import threading

from fastapi import Request, HTTPException

from config import (
    logger,
    RATE_LIMIT_ENABLED,
    RATE_LIMIT_MAX_REQUESTS,
    RATE_LIMIT_WINDOW_SECONDS,
)

_lock = threading.Lock()
# (ip, path) -> list of request timestamps within the current window
_requests = {}


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


def check_rate_limit(
    request: Request,
    max_requests: int = None,
    window_seconds: int = None,
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
    within window_seconds on this specific path. max_requests/
    window_seconds default to the global RATE_LIMIT_* config values if
    not explicitly overridden, so existing call sites are unaffected.
    """
    if not RATE_LIMIT_ENABLED:
        return

    effective_max = max_requests if max_requests is not None else RATE_LIMIT_MAX_REQUESTS
    effective_window = window_seconds if window_seconds is not None else RATE_LIMIT_WINDOW_SECONDS

    path = request.url.path
    ip = _get_client_ip(request)

    now = time.time()
    key = (ip, path)

    with _lock:
        timestamps = _requests.get(key, [])
        cutoff = now - effective_window
        timestamps = [t for t in timestamps if t >= cutoff]

        if len(timestamps) >= effective_max:
            logger.warning(f"[RATE LIMIT] Blocked {ip} on {path} - {len(timestamps)} requests in window")
            retry_after = int(effective_window - (now - timestamps[0])) if timestamps else effective_window
            raise HTTPException(
                429,
                f"Too many requests. Please wait a moment before trying again "
                f"(limit: {effective_max} request(s) per {_format_duration(effective_window)}).",
                headers={"Retry-After": str(max(retry_after, 1))},
            )

        timestamps.append(now)
        _requests[key] = timestamps