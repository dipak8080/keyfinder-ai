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

from fastapi import Request

from config import (
    logger,
    SEPARATION_SHARED_RATE_LIMIT_MAX_REQUESTS,
    SEPARATION_SHARED_RATE_LIMIT_WINDOW_SECONDS,
    SEPARATION_SHARED_DAILY_MAX_REQUESTS,
    SEPARATION_SHARED_DAILY_WINDOW_SECONDS,
)
from rate_limit import check_rate_limits, refund_rate_limit_hits

SHARED_BUCKET = "separation-standard"


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


def _warn_once(token, message: str, *args) -> None:
    if token in _warned:
        return
    if len(_warned) > 64:
        _warned.clear()
    _warned.add(token)
    logger.warning(message, *args)


def _safe_default(default: int, low: int, high) -> int:
    """The fallback to use when the resolved value is out of range.

    `default` is the config.py constant, and config.py reads THE SAME ENV
    VAR - so SEPARATION_SHARED_DAILY_MAX_REQUESTS=30000 poisons the value
    and the fallback together, and the clamp warned then enforced 30000
    anyway. Falling back to the code default is right whenever the code
    default is itself sane; when it is not, the bound is the only number
    left that was actually chosen by someone.

    WORTH BEING EXPLICIT ABOUT THE TRADE. An env var of 30000 lands on
    max (500/day), not on the intended 30. That is 16x looser than the
    default, and it is deliberate: reaching this branch means someone
    edited .env and redeployed to ask for a big number, so honouring the
    largest value we consider sane respects the intent while capping the
    damage - and it warns once. Reverting silently to 30 would be the
    safer number and the more surprising behaviour.

    Note config.py itself will not survive a NON-NUMERIC env var here -
    int(os.environ.get(...)) raises at import, as it does for all ~50 of
    its constants. That is pre-existing and defensible (a config typo
    fails the health check and rolls the deploy back), so it is left
    alone rather than made inconsistent for one key.
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


def _limit(name: str, default: int) -> int:
    """Resolve settings table -> env -> default, CLAMPED to KNOWN_KEYS.

    Bounded at READ time, not only at write time. _check_type guards
    set_many and nothing else, so a row written before max shipped, a row
    written by a draining container running an older build, or a plain env
    var all reach here unchecked. That is the same asymmetry the free-tier
    keys got an absolute boot invariant for; these are the keys that
    motivated the mechanism in the first place.

    Out-of-range falls back to the code default and says so, rather than
    silently enforcing a number nobody chose.
    """
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
    return value


def current_limits() -> dict:
    """What the four standard routes are enforcing right now."""
    return {
        "bucket": SHARED_BUCKET,
        "hourly_max": _limit("SEPARATION_SHARED_RATE_LIMIT_MAX_REQUESTS",
                             SEPARATION_SHARED_RATE_LIMIT_MAX_REQUESTS),
        "hourly_window": _limit("SEPARATION_SHARED_RATE_LIMIT_WINDOW_SECONDS",
                                SEPARATION_SHARED_RATE_LIMIT_WINDOW_SECONDS),
        "daily_max": _limit("SEPARATION_SHARED_DAILY_MAX_REQUESTS",
                            SEPARATION_SHARED_DAILY_MAX_REQUESTS),
        "daily_window": _limit("SEPARATION_SHARED_DAILY_WINDOW_SECONDS",
                               SEPARATION_SHARED_DAILY_WINDOW_SECONDS),
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