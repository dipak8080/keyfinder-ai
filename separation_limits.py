"""
separation_limits.py - ONE allowance across the four standard separation
routes, enforced on two windows.

THE PROBLEM THIS CLOSES
-----------------------
rate_limit.py keys its window on (ip, path). /separate, /stems,
/youtube/separate and /youtube/stems are four paths onto ONE Demucs run,
so each held an independent 6/hour bucket and a single IP could spend all
four in the same hour: 24 standard separations, none of them metered,
none of them consuming a credit. config.py's comment beside
STEMS_RATE_LIMIT_MAX_REQUESTS has described this since the limits were
split; bucket_key is what finally lets four routes share a number instead
of merely being assigned the same one.

WHY TWO WINDOWS AND NOT ONE
---------------------------
They bound different things and either alone leaves a hole.

  HOURLY (10) bounds the QUEUE. One person splitting an album does 8-14
  tracks in a sitting; past that they are holding MAX_CONCURRENT_SEPARATIONS
  against everyone else. But 10/hour permits 240/day, which at ~$0.002 a
  standard job is ~$0.50/day from one IP - more than the whole site
  currently spends.

  DAILY (30) bounds the BILL. Above any plausible human day, including a
  double album, while capping one IP near $2/month. For a single IP to
  matter financially it now has to run flat out for weeks.

PERSISTENT, DELIBERATELY
------------------------
Both windows pass persistent=True. A daily counter that resets on every
deploy is not a daily counter, and there are several deploys on a busy
day. They pass tier=None so the 429 body stays a plain string, which is
what the frontend's parseDetail() already handles on these routes - only
the metered HQ routes get the structured form.

NUMBERS ARE RUNTIME-EDITABLE
----------------------------
Resolved settings table -> env -> config.py default on every request, so
tuning these is a PUT to /admin/credits/settings and not a redeploy. The
read is a single indexed SQLite lookup on a WAL database and falls back
to the config constant on any failure.
"""

from __future__ import annotations

import os
import threading

from fastapi import Request

from config import logger
from rate_limit import check_rate_limits, refund_rate_limit_hits

SHARED_BUCKET = "separation-standard"

# The fallback must NOT come from config.py. Those constants read the
# same env vars _limit() already reads below, so an out-of-range env var
# poisoned the value and the fallback together - which is how 30000 got
# enforced as 30000, and then, once clamped, as 500 from env but 30 from
# a settings row. One bad value, two answers, depending only on where it
# was written. Held here as literals so the fallback is always a number
# someone chose and both paths agree.
#
# config.py keeps its constants: routes/admin.py falls back to them when
# this module fails to import, and that path needs them. Drift between
# the two copies surfaces as /limits reporting a stale number on a path
# that already exists to be approximate, which is why this is a comment
# rather than a mechanism.
_DEFAULTS = {
    "SEPARATION_SHARED_RATE_LIMIT_MAX_REQUESTS": 10,
    "SEPARATION_SHARED_RATE_LIMIT_WINDOW_SECONDS": 3600,
    "SEPARATION_SHARED_DAILY_MAX_REQUESTS": 30,
    "SEPARATION_SHARED_DAILY_WINDOW_SECONDS": 86400,
}


# Warn once per distinct (key, value). _limit() runs inside
# current_limits(), which serves shared_separation_limit AND /limits AND
# /credits/me - the last two on nearly every page load. Warning per call
# turned one bad value into a WARNING row per request in system_logs,
# which shares a volume with credits.db and rate_limits.db. Disk pressure
# on that volume is the DB-error condition that makes the persistent
# limiter fall back to memory, silently resetting the 30/day counter - so
# a single mistyped limit could walk into the one silent cost-control
# failure left.
_warned: set = set()
# Locked for the same reason rate_limit._requests and settings_store._cache
# are. _limit() runs on the threadpool from four routes plus /limits and
# /credits/me, and _forget_warnings ITERATES this set while _warn_once adds
# to it. In practice the set holds at most four entries, so the
# comprehension completes in one bytecode burst and the race has not been
# reproducible - but the clear() at 64 is where it would bite, and this was
# the one shared mutable in the codebase without a lock.
_warned_lock = threading.Lock()


def _warn_once(token, message: str, *args) -> None:
    with _warned_lock:
        if token in _warned:
            return
        if len(_warned) > 64:
            _warned.clear()
        _warned.add(token)
    # Logged OUTSIDE the lock: BufferLogHandler writes this to SQLite.
    logger.warning(message, *args)


def _forget_warnings(name: str) -> None:
    """Drop every remembered complaint about this key once it resolves
    cleanly, so reintroducing the same bad string later warns again."""
    with _warned_lock:
        for token in [t for t in _warned if t[0] == name]:
            _warned.discard(token)


def _safe_default(default: int, low: int, high) -> int:
    """The fallback to use when the resolved value is out of range.

    `default` is the config.py constant, and config.py reads THE SAME ENV
    VAR - so SEPARATION_SHARED_DAILY_MAX_REQUESTS=30000 poisons the value
    and the fallback together, and the clamp warned then enforced 30000
    anyway. Falling back to the code default is right whenever the code
    default is itself sane; when it is not, the bound is the only number
    left that was actually chosen by someone.

    NO LONGER REACHABLE FROM AN ENV VAR. _DEFAULTS holds literals now, so
    the fallback cannot be poisoned by the same variable that poisoned the
    value. Kept as a guard against someone later editing _DEFAULTS out of
    range, which is the only way in left.

    The earlier version clamped to the BOUND, which meant an env var of
    30000 enforced 500/day - about $1/day from a single IP, against a
    whole-site spend of roughly $0.55/day. "Someone asked for a big
    number" and "someone typed an extra zero" are indistinguishable here,
    and the branch should not gamble on the generous reading.
    """
    if default < low:
        return low
    if high is not None and default > high:
        return high
    return default


def _bounds(name: str):
    """The min/max KNOWN_KEYS declares for this key, if any."""
    try:
        from credits.settings_store import KNOWN_KEYS

        meta = KNOWN_KEYS.get(name) or {}
        return meta.get("min"), meta.get("max")
    except Exception:  # noqa: BLE001
        return None, None


def _limit(name: str) -> int:
    """Resolve settings table -> env -> _DEFAULTS, CLAMPED to KNOWN_KEYS.

    Bounded at READ time, not only at write time. _check_type guards
    set_many and nothing else, so a row written before max shipped, a row
    written by a draining container running an older build, or a plain env
    var all reach here unchecked. That is the same asymmetry the free-tier
    keys got an absolute boot invariant for; these are the keys that
    motivated the mechanism in the first place.

    Out-of-range falls back to the code default and says so, rather than
    silently enforcing a number nobody chose.
    """
    default = _DEFAULTS.get(name)
    if default is None:
        # Unreachable: _validate_defaults() below refuses to import
        # without every key current_limits() asks for, so a missing entry
        # fails the deploy health check and rolls back rather than
        # surfacing here. Kept anyway because _limit() used to take its
        # default as an argument and now depends on a lookup table, and a
        # bare KeyError would be a 500 on all four separation routes.
        #
        # Falls back to the KNOWN_KEYS MAXIMUM, not zero and not the
        # minimum. Zero blocks every separation request, and the minimum
        # (1/hour) is an outage wearing a limit's clothes; this module's
        # posture everywhere else is fail-open, and the maximum is at
        # least a number someone chose.
        _, high = _bounds(name)
        fallback = high if high is not None else 0
        _warn_once((name, "__missing__"),
                   "[SEPARATION LIMIT] %s has no entry in _DEFAULTS, using %s", name, fallback)
        return fallback
    try:
        from credits.settings_store import resolve

        raw = resolve(name)
    except Exception:  # noqa: BLE001
        raw = None
    if raw in (None, ""):
        raw = os.environ.get(name)
    low, high = _bounds(name)
    low = 1 if low is None else low
    fallback = _safe_default(default, low, high)

    try:
        value = int(raw) if raw not in (None, "") else default
    except (TypeError, ValueError):
        _warn_once((name, raw), "[SEPARATION LIMIT] %s=%r is not an integer, using %s",
                   name, raw, fallback)
        return fallback

    if value < low:
        _warn_once((name, raw), "[SEPARATION LIMIT] %s=%s is below the %s minimum, using %s",
                   name, value, low, fallback)
        return fallback
    if high is not None and value > high:
        _warn_once((name, raw), "[SEPARATION LIMIT] %s=%s exceeds the %s maximum, using %s",
                   name, value, high, fallback)
        return fallback
    _forget_warnings(name)
    return value


def _validate_defaults() -> None:
    """Every key current_limits() resolves must have a literal default.

    At IMPORT, so a missing entry fails the deploy health check and rolls
    back - the same fail-at-boot treatment credits/config.py gives its
    own contradictions. The alternative is discovering it as a 500 on
    /separate in production.
    """
    required = {
        "SEPARATION_SHARED_RATE_LIMIT_MAX_REQUESTS",
        "SEPARATION_SHARED_RATE_LIMIT_WINDOW_SECONDS",
        "SEPARATION_SHARED_DAILY_MAX_REQUESTS",
        "SEPARATION_SHARED_DAILY_WINDOW_SECONDS",
    }
    missing = sorted(required - set(_DEFAULTS))
    if missing:
        raise RuntimeError(
            f"separation_limits._DEFAULTS is missing {missing}. Every key "
            f"current_limits() resolves needs a literal default here - it "
            f"cannot come from config.py, which reads the same env vars."
        )


_validate_defaults()


def current_limits() -> dict:
    """What the four standard routes are enforcing right now."""
    return {
        "bucket": SHARED_BUCKET,
        "hourly_max": _limit("SEPARATION_SHARED_RATE_LIMIT_MAX_REQUESTS"),
        "hourly_window": _limit("SEPARATION_SHARED_RATE_LIMIT_WINDOW_SECONDS"),
        "daily_max": _limit("SEPARATION_SHARED_DAILY_MAX_REQUESTS"),
        "daily_window": _limit("SEPARATION_SHARED_DAILY_WINDOW_SECONDS"),
    }


def shared_separation_limit(request: Request) -> None:
    """FastAPI dependency for /separate, /stems, /youtube/separate, /youtube/stems.

    Both windows are checked before either records, in one transaction,
    so a caller stopped by one does not spend a slot in the other.

    The receipt is stashed on request.state for refund_unused_slots(),
    which the middleware calls when the route answers with an error.
    """
    limits = current_limits()
    receipt = check_rate_limits(
        request,
        windows=[
            (f"{SHARED_BUCKET}:hour", limits["hourly_max"], limits["hourly_window"]),
            (f"{SHARED_BUCKET}:day", limits["daily_max"], limits["daily_window"]),
        ],
    )
    request.state.separation_rate_receipt = receipt


def refund_unused_slots(request: Request) -> None:
    """Give back this request's slots. Idempotent - clears the receipt."""
    receipt = getattr(request.state, "separation_rate_receipt", None)
    if not receipt:
        return
    request.state.separation_rate_receipt = None
    refund_rate_limit_hits(receipt)
    logger.info("[SEPARATION LIMIT] refunded %d slot(s) for %s",
                len(receipt), request.url.path)


shared_separation_limit.__name__ = "shared_separation_limit"