"""
rate_limit.py - Simple per-IP rate limiting for the heavy endpoints
(/download, /analyze).

In-memory sliding window, no external dependency (Redis etc.) needed. Good
enough to stop a single IP from hammering the API and running up your
Railway bill; resets on restart and is PER INSTANCE (doesn't share state
across multiple Railway replicas if you ever scale horizontally) - a real
limitation, but a solid first line of defense for a single-instance
deployment.
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


def check_rate_limit(request: Request):
    """
    Use as a FastAPI dependency on rate-limited routes:
        @router.post("/download", dependencies=[Depends(check_rate_limit)])
    Raises a clean 429 if the caller's IP has exceeded
    RATE_LIMIT_MAX_REQUESTS within RATE_LIMIT_WINDOW_SECONDS on this path.
    """
    if not RATE_LIMIT_ENABLED:
        return

    path = request.url.path

    # Railway sits behind a proxy - the real client IP is in
    # X-Forwarded-For, not request.client.host (which would just be
    # Railway's internal proxy address, the same for every request).
    forwarded = request.headers.get("x-forwarded-for")
    ip = forwarded.split(",")[0].strip() if forwarded else (request.client.host if request.client else "unknown")

    now = time.time()
    key = (ip, path)

    with _lock:
        timestamps = _requests.get(key, [])
        cutoff = now - RATE_LIMIT_WINDOW_SECONDS
        timestamps = [t for t in timestamps if t >= cutoff]

        if len(timestamps) >= RATE_LIMIT_MAX_REQUESTS:
            logger.warning(f"[RATE LIMIT] Blocked {ip} on {path} - {len(timestamps)} requests in window")
            raise HTTPException(
                429,
                f"Too many requests. Please wait a moment before trying again "
                f"(limit: {RATE_LIMIT_MAX_REQUESTS} requests per {RATE_LIMIT_WINDOW_SECONDS} seconds)."
            )

        timestamps.append(now)
        _requests[key] = timestamps