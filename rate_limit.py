"""
rate_limit.py - Per-IP rate limiting for the heavy endpoints.

Sliding window over Redis sorted sets: one key per bucket, one member per
request, scored by timestamp. Pruning, counting and recording happen inside a
single Lua script, which Redis runs atomically - the same guarantee SQLite's
BEGIN IMMEDIATE used to provide, without the write-lock contention.

ONE BACKEND NOW. The window used to be split: in-memory for the ~35 cheap
call sites, SQLite for anything passing `tier`. The in-memory half reset on
every deploy and, worse, was per-container - two deploy slots meant two
independent allowances and double the real limit. Everything is in Redis now
and shared across slots.

The in-memory dict survives ONLY as the fallback for a Redis outage. A Redis
failure must never take the site down, and must never silently hand out
unlimited free GPU either, so it degrades to a per-container window and logs
loudly.

check_rate_limit() accepts optional max_requests/window_seconds overrides so
routes can have different limits. key_override replaces the IP half of the
key, so paid callers are counted against an account rather than an IP - see
credits/limits.py. bucket_key replaces the PATH half, so several routes can
share one window.

`tier`, when passed, is echoed in the 429 detail as a machine-readable field
so the frontend knows a free-tier limit can be lifted by buying credits. It no
longer selects a backend. `persistent` is accepted for call-site
compatibility and is now a no-op, since every window is persistent.

check_rate_limit is a SYNC FastAPI dependency, which FastAPI runs in its own
threadpool, so the Redis round trip never touches the event loop.
"""
import threading
import time
import uuid

from fastapi import Request, HTTPException

from client_ip import get_client_ip, normalise_for_bucketing
from redis_store import client as _r

from config import (
    logger,
    RATE_LIMIT_ENABLED,
    RATE_LIMIT_MAX_REQUESTS,
    RATE_LIMIT_WINDOW_SECONDS,
)

_BUCKET_PREFIX = "af:rl:"

# Slack on a bucket's own key expiry, so a key cannot vanish between the prune
# and the next request landing in the same window.
_EXPIRE_SLACK_SECONDS = 60

# Fallback only - written when Redis is unreachable. See module docstring.
_lock = threading.Lock()
_requests = {}
_key_windows = {}
_last_mem_sweep = 0.0
_MEM_KEY_SLACK_SECONDS = 3600
_MEM_SWEEP_INTERVAL_SECONDS = 600


# Prunes every window, rejects on the first one over its limit with nothing
# written, and records in all of them only if all pass. Atomic by virtue of
# being a Lua script: Redis runs it to completion with no interleaving, which
# is what stops N concurrent requests on one bucket all reading the same
# pre-insert count and all passing a limit of 1.
#
# ARGV: now, member, then (limit, window) per key, in KEYS order.
# Returns {0, {}} on success, or {failed_index, {scores...}} on rejection.
_CHECK_AND_RECORD = _r.register_script("""
local now = tonumber(ARGV[1])
local member = ARGV[2]

for i = 1, #KEYS do
  local limit  = tonumber(ARGV[2 + (i - 1) * 2 + 1])
  local window = tonumber(ARGV[2 + (i - 1) * 2 + 2])
  redis.call('ZREMRANGEBYSCORE', KEYS[i], '-inf', now - window)
  if redis.call('ZCARD', KEYS[i]) >= limit then
    local raw = redis.call('ZRANGE', KEYS[i], 0, -1, 'WITHSCORES')
    local scores = {}
    for j = 2, #raw, 2 do
      scores[#scores + 1] = raw[j]
    end
    return {i, scores}
  end
end

for i = 1, #KEYS do
  local window = tonumber(ARGV[2 + (i - 1) * 2 + 2])
  redis.call('ZADD', KEYS[i], now, member)
  redis.call('EXPIRE', KEYS[i], math.ceil(window) + """ + str(_EXPIRE_SLACK_SECONDS) + """)
end

return {0, {}}
""")


def _bucket_key(subject: str, bucket: str) -> str:
    return f"{_BUCKET_PREFIX}{subject}|{bucket}"


def _sweep_memory(now: float) -> None:
    """Drops fallback buckets whose newest hit is older than their own window.

    Pruning timestamps inside a bucket never removed the bucket itself, so
    every unique key ever seen stayed resident for the container's lifetime.
    Called under _lock.
    """
    dead = [
        k for k, ts in _requests.items()
        if not ts or ts[-1] < now - (_key_windows.get(k, 3600) + _MEM_KEY_SLACK_SECONDS)
    ]
    for k in dead:
        del _requests[k]
        _key_windows.pop(k, None)
    if dead:
        logger.info(f"[RATE LIMIT] Swept {len(dead)} idle in-memory fallback buckets "
                    f"({len(_requests)} remain)")


def _maybe_sweep_memory(now: float) -> None:
    global _last_mem_sweep
    with _lock:
        if now - _last_mem_sweep > _MEM_SWEEP_INTERVAL_SECONDS:
            _last_mem_sweep = now
            _sweep_memory(now)


def _get_client_ip(request: Request) -> str:
    # CF-Connecting-IP first: X-Forwarded-For's first entry is whatever the
    # caller put there, and this value IS the bucket key for every free-tier
    # limit. Collapsed to a /64 on IPv6, or a caller gets a fresh allowance
    # per address in a prefix they already hold. See client_ip.py.
    return normalise_for_bucketing(get_client_ip(request, default="unknown"))


def _format_duration(seconds: int) -> str:
    """
    Turns a raw seconds value into human-readable text for the 429 message,
    e.g. 3600 -> "1 hour", 90 -> "1 min 30 sec", 45 -> "45 seconds". Built at
    request time from effective_window, so the user-facing text stays in sync
    with config with no separate frontend copy to maintain.
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
    if secs and not hours:
        parts.append(f"{secs} sec")

    return " ".join(parts)


def _reject(ip, path, key_override, timestamps, effective_max, effective_window,
            now, tier, route=None):
    # route is logged ALONGSIDE the bucket, never instead of it. With a shared
    # bucket the two differ, and the route is the half that says which
    # endpoint someone is actually hitting.
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
    # Detail stays a plain STRING when no tier is passed - that is what the
    # ~35 existing call sites produce and what the frontend's parseDetail()
    # already handles. Only the credits routes get the structured form.
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


def _fallback_check(subject, specs, ip, key_override, tier, route, now):
    """Per-container window used only while Redis is unreachable.

    Checks every window before recording in any, matching the Redis path: a
    caller stopped by a daily cap must not have burned an hourly slot.
    """
    with _lock:
        for _key, bucket, limit, window in specs:
            mem_key = (subject, bucket)
            timestamps = [t for t in _requests.get(mem_key, []) if t >= now - window]
            _requests[mem_key] = timestamps
            if len(timestamps) >= limit:
                _reject(ip, bucket, key_override, timestamps, limit,
                        int(window), now, tier, route=route)
        for _key, bucket, _limit, window in specs:
            mem_key = (subject, bucket)
            _requests[mem_key].append(now)
            _key_windows[mem_key] = window


def _check_and_record(subject, specs, ip, key_override, tier, route, now):
    """Runs the Lua script, rejects on failure, returns a refund receipt.

    Falls back to the in-memory window on any Redis error. No receipt is
    returned in that case - those hits live in a single container's memory and
    the refund path only knows how to remove Redis members, so during an
    outage slots are counted but never refunded. That is the safe direction.
    """
    member = uuid.uuid4().hex
    keys = [key for key, _bucket, _limit, _window in specs]
    args = [now, member]
    for _key, _bucket, limit, window in specs:
        args.append(limit)
        args.append(window)

    try:
        failed_index, scores = _CHECK_AND_RECORD(keys=keys, args=args)
    except HTTPException:
        raise
    except Exception:
        logger.error(
            f"[RATE LIMIT] Redis unavailable for {route}, falling back to "
            f"in-memory (per-container) windows", exc_info=True,
        )
        _fallback_check(subject, specs, ip, key_override, tier, route, now)
        return []

    if failed_index:
        _key, bucket, limit, window = specs[int(failed_index) - 1]
        timestamps = sorted(float(s) for s in scores)
        _reject(ip, bucket, key_override, timestamps, limit, int(window),
                now, tier, route=route)

    return [(key, member) for key, _bucket, _limit, _window in specs]


def check_rate_limits(
    request: Request,
    windows,
    key_override: str = None,
    tier: str = None,
):
    """Check SEVERAL windows atomically, then record in all of them.

    windows is a sequence of (bucket_key, max_requests, window_seconds).

    WHY THIS EXISTS. Calling check_rate_limit twice records a hit on the first
    window even when the second then rejects, so a caller stopped by a daily
    cap still burned an hourly slot. There is no ordering that avoids it -
    only checking everything before recording anything does.

    Returns a RECEIPT of what was written, so a caller whose request later
    fails for an unrelated reason can hand back the slots it never used. See
    refund_rate_limit_hits.
    """
    if not RATE_LIMIT_ENABLED:
        return []

    ip = _get_client_ip(request)
    subject = key_override if key_override is not None else ip
    now = time.time()
    route = request.url.path

    _maybe_sweep_memory(now)

    specs = [
        (_bucket_key(subject, bucket), bucket, int(limit), float(window))
        for bucket, limit, window in windows
    ]

    return _check_and_record(subject, specs, ip, key_override, tier, route, now)


def refund_rate_limit_hits(receipt) -> None:
    """Hand back slots recorded for a request that never used them.

    A rate limit protects a RESOURCE. A submission rejected afterwards for a
    full queue, a disabled tool or a bad file consumed no GPU and no queue
    slot, so charging it against an allowance protects nothing - it just locks
    someone out. Under a daily cap a broken uploader could otherwise spend a
    whole day's allowance on requests that never ran.

    Removes this request's own member from each bucket, so a concurrent
    request is untouched even when both landed on an identical timestamp.
    Best effort: failing to refund must never turn into a 500 on a response
    that has already been decided.
    """
    if not receipt:
        return
    try:
        pipe = _r.pipeline()
        for key, member in receipt:
            pipe.zrem(key, member)
        pipe.execute()
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
        @router.post("/download", dependencies=[Depends(check_rate_limit)])

        @router.post("/separate", dependencies=[
            Depends(rate_limited(max_requests=1, window_seconds=3600))
        ])
    Never partial(check_rate_limit, ...) - see rate_limited() below for why.

    Raises a clean 429 if the caller has exceeded max_requests within
    window_seconds on this path.

    `persistent` is accepted and ignored: every window is persistent now.
    """
    if not RATE_LIMIT_ENABLED:
        return

    effective_max = max_requests if max_requests is not None else RATE_LIMIT_MAX_REQUESTS
    effective_window = window_seconds if window_seconds is not None else RATE_LIMIT_WINDOW_SECONDS

    path = bucket_key if bucket_key is not None else request.url.path
    ip = _get_client_ip(request)
    subject = key_override if key_override is not None else ip
    now = time.time()

    _maybe_sweep_memory(now)

    specs = [(
        _bucket_key(subject, path),
        path,
        int(effective_max),
        float(effective_window),
    )]

    _check_and_record(subject, specs, ip, key_override, tier,
                      request.url.path, now)

def rate_limited(max_requests: int, window_seconds: int):
    """Route dependency with the limit closed over, NOT partial(check_rate_limit, ...).

    A partial keeps every unbound parameter of check_rate_limit in the
    signature FastAPI inspects, and FastAPI exposes each one as an optional
    query parameter. That made ?max_requests=999999 or ?bucket_key=anything
    a working bypass on every route that used the partial form. Only
    `request` is visible here.

        @router.post("/download", dependencies=[Depends(rate_limited(30, 3600))])
    """
    def dependency(request: Request) -> None:
        check_rate_limit(request, max_requests=max_requests, window_seconds=window_seconds)
    dependency.__name__ = f"rate_limited_{max_requests}_per_{window_seconds}s"
    return dependency