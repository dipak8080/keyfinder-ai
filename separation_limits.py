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
from rate_limit import check_rate_limit

SHARED_BUCKET = "separation-standard"


def _limit(name: str, default: int) -> int:
    try:
        from credits.settings_store import resolve

        raw = resolve(name)
    except Exception:  # noqa: BLE001
        raw = None
    if raw in (None, ""):
        raw = os.environ.get(name)
    try:
        value = int(raw) if raw not in (None, "") else default
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


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

    HOURLY IS CHECKED FIRST, and the order is load-bearing. Each window
    records a hit when it passes, so whichever runs first spends a slot
    even if the second then rejects. Checking the tighter window first
    means the only leak is an hourly slot burned by someone the daily
    limit has already stopped for the rest of the day - invisible, since
    it expires in an hour and they are blocked regardless. The reverse
    order leaks a daily slot to someone who was merely going too fast for
    a minute, which they would feel tomorrow.
    """
    limits = current_limits()

    check_rate_limit(
        request,
        max_requests=limits["hourly_max"],
        window_seconds=limits["hourly_window"],
        bucket_key=f"{SHARED_BUCKET}:hour",
        persistent=True,
    )
    check_rate_limit(
        request,
        max_requests=limits["daily_max"],
        window_seconds=limits["daily_window"],
        bucket_key=f"{SHARED_BUCKET}:day",
        persistent=True,
    )


shared_separation_limit.__name__ = "shared_separation_limit"