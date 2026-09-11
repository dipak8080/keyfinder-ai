"""
monitoring.py - Lightweight in-process failure tracking + alerting.

No external monitoring service required (though you CAN add a free Discord
webhook for instant phone/desktop pings - see ALERT_WEBHOOK_URL in config.py).

Tracks recent successes/failures per endpoint in memory, and posts to a
webhook when failures spike past a threshold within a time window - with a
cooldown so an ongoing outage doesn't spam you every few seconds.

Intentionally simple: in-memory, per-instance, no database. If you ever run
multiple replicas, each instance tracks its own failures
independently (not a combined view) - fine for now, worth revisiting if you
scale horizontally later.
"""
import sys
import time
import threading
from typing import Optional
import requests

from config import (
    logger,
    ALERT_WEBHOOK_URL,
    FAILURE_ALERT_THRESHOLD,
    FAILURE_ALERT_WINDOW_SECONDS,
    ALERT_COOLDOWN_SECONDS,
)

_lock = threading.Lock()

# endpoint -> list of (timestamp, success: bool)
_events = {}

# endpoint -> last alert timestamp, so we don't spam the same alert
_last_alert_at = {}

# For /admin/status uptime reporting.
_started_at = time.time()


def _prune_old(endpoint: str, now: float):
    cutoff = now - FAILURE_ALERT_WINDOW_SECONDS
    _events[endpoint] = [(t, ok) for (t, ok) in _events.get(endpoint, []) if t >= cutoff]


def _send_alert(message: str):
    if not ALERT_WEBHOOK_URL:
        # No webhook configured - still log CRITICAL so it's visible in
        # `docker logs` even without external alerting set up.
        logger.critical(f"[ALERT] {message} (no ALERT_WEBHOOK_URL set in .env - add one to get pinged externally)")
        return
    try:
        # Discord webhooks read "content"; Slack webhooks read "text".
        # Sending both keys means the same code works for either service.
        requests.post(ALERT_WEBHOOK_URL, json={"content": message, "text": message}, timeout=5)
        logger.info(f"[ALERT] Sent webhook alert: {message}")
    except Exception as e:
        logger.error(f"[ALERT] Failed to send webhook alert: {e}")


def alert_now(message: str):
    """
    Public entry point for one-off, immediate alerts that shouldn't wait on
    the failure-threshold/cooldown logic in record_result() - e.g. the
    proxy circuit breaker tripping in youtube.py. That's a single, distinct
    event worth knowing about right away (it usually means "proxy is out of
    credit"), not something that should require FAILURE_ALERT_THRESHOLD
    failures to first pile up before you hear about it.

    Wrapped so monitoring can never raise into the caller's request path.
    """
    try:
        _send_alert(message)
    except Exception as e:
        logger.warning(f"[monitoring] alert_now failed (non-fatal): {e}")


def is_client_side(exc) -> bool:
    """One definition of "the user's or the video's outcome, not ours",
    shared by alerts and job metering. True for exceptions that say so
    (client_side = True: InputRejected, SeparationRejected, rejected
    downloads) and for any 4xx HTTPException."""
    if exc is None:
        return False
    if getattr(exc, "client_side", False) is True:
        return True
    code = getattr(exc, "status_code", None)
    return isinstance(code, int) and 400 <= code < 500


def record_result(endpoint: str, success: bool, client_side: bool = False):
    """
    Call this once per request, right where you already know whether it
    succeeded or failed. Cheap, non-blocking, and wrapped so it can NEVER
    raise or break the real request it's being called from.

    A 4xx on its way out (removed video, too long, age-gated, bad input)
    is the user's or the video's outcome, not a server failure, so it never
    counts toward the alert. Read from the in-flight exception, which every
    caller's `finally` already has, so no route needs changing. 2026-09-11:
    two removed videos and five over-length requests paged Discord.
    Job runners catch their exceptions before this runs, so they pass
    client_side explicitly.
    """
    try:
        if not success and (client_side or is_client_side(sys.exc_info()[1])):
            logger.debug(f"[monitoring] {endpoint} ended client side, not counted")
            success = True
        now = time.time()
        with _lock:
            _events.setdefault(endpoint, []).append((now, success))
            _prune_old(endpoint, now)

            recent = _events[endpoint]
            failures = sum(1 for (_, ok) in recent if not ok)

            if failures >= FAILURE_ALERT_THRESHOLD:
                last_alert = _last_alert_at.get(endpoint, 0)
                if now - last_alert >= ALERT_COOLDOWN_SECONDS:
                    _last_alert_at[endpoint] = now
                    message = (
                        f"AudioForges backend: {failures} failures on {endpoint} "
                        f"in the last {FAILURE_ALERT_WINDOW_SECONDS // 60} min. "
                        f"Check the admin log dashboard, or on the VPS: "
                        f"docker logs audioforges-api 2>&1 | grep -E "
                        f"'\\[COOKIES\\]|\\[PROXY\\]|\\[CLIENT\\]|Failed after all'"
                    )
                    _send_alert(message)
    except Exception as e:
        # Monitoring itself must never break a real request.
        logger.warning(f"[monitoring] record_result failed (non-fatal): {e}")


def get_status_snapshot() -> dict:
    """Returns recent stats per endpoint, for the /admin/status endpoint."""
    now = time.time()
    with _lock:
        snapshot = {}
        for endpoint in list(_events.keys()):
            _prune_old(endpoint, now)
            recent = _events[endpoint]
            total = len(recent)
            failures = sum(1 for (_, ok) in recent if not ok)
            snapshot[endpoint] = {
                "total_recent": total,
                "failures_recent": failures,
                "window_seconds": FAILURE_ALERT_WINDOW_SECONDS,
            }
    return {
        "uptime_seconds": int(now - _started_at),
        "endpoints": snapshot,
    }