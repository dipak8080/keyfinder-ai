"""
gpu_budget.py - Global GPU-minute spend breaker for Demucs separation.

WHY THIS EXISTS (2026-08-09): per-IP rate limits (SEPARATION_RATE_LIMIT_*
etc. in config.py) only slow down ONE IP. They do nothing against a
patient single user staying just under the limit for a whole month, or
against many rotating IPs each staying under their own limit. On a
per-second-billed GPU (RunPod, $0.24/hr at time of writing), that gap is
a real financial exposure - one IP alone, sustained at 1 request/hour for
30 days, is ~$172 in the worst case, far past any budget a solo dev
signs up for.

Per-IP limits and this breaker solve DIFFERENT problems and both are
needed:
  - Per-IP limits: fairness, stops one user from starving everyone else
  - This breaker: a hard ceiling on TOTAL spend, regardless of how many
    IPs or how patiently the budget is drawn down

Two-stage, not a single cliff:
  - SOFT threshold: HQ (the 5x-cost tier) is disabled. Standard keeps
    running - most legitimate usage is unaffected, only the expensive
    tier gets cut first.
  - HARD threshold: everything stops, both tiers, until the monthly
    reset or a manual reset.

Tracks GPU-SECONDS actually spent, not request counts - counted whether
the job succeeded or failed, because RunPod bills for the compute either
way. Fed from _run_tool_job's own elapsed-time measurement in routes.py,
so there's no separate timing mechanism to keep in sync with reality.

Auto-resets on a new calendar month. Manually resettable via
POST /admin/reset-gpu-budget for when you correct the threshold after
seeing real RunPod numbers, or top up mid-month.
"""
import threading
import time
from datetime import datetime, timezone

from config import (
    logger,
    GPU_BUDGET_SOFT_THRESHOLD_MINUTES,
    GPU_BUDGET_HARD_THRESHOLD_MINUTES,
    GPU_HOURLY_COST_USD,
)
from monitoring import alert_now

_lock = threading.Lock()
_state = {
    "month_key": None,       # "2026-08" - which calendar month _seconds_used covers
    "seconds_used": 0.0,
    "soft_tripped": False,   # HQ disabled
    "hard_tripped": False,   # everything disabled
}


def _current_month_key() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.year:04d}-{now.month:02d}"


def _roll_over_if_new_month_locked() -> None:
    """Caller must hold _lock. Resets the counter the first time this is
    touched in a new calendar month - no cron job needed, the rollover
    just happens lazily on next use."""
    key = _current_month_key()
    if _state["month_key"] != key:
        if _state["month_key"] is not None:
            logger.info(
                f"[GPU_BUDGET] New month ({key}) - resetting spend counter. "
                f"Previous month used {_state['seconds_used'] / 60:.1f} min "
                f"(~${_state['seconds_used'] / 3600 * GPU_HOURLY_COST_USD:.2f})."
            )
        _state["month_key"] = key
        _state["seconds_used"] = 0.0
        _state["soft_tripped"] = False
        _state["hard_tripped"] = False


def record_gpu_seconds(seconds: float) -> None:
    """
    Called once per separation job (upload or YouTube-chained, standard
    or HQ) from routes.py's _run_tool_job, in a `finally` so it's counted
    whether the job succeeded, failed, or was cancelled - RunPod bills
    for the GPU-seconds either way.
    """
    if seconds <= 0:
        return

    newly_soft = False
    newly_hard = False

    with _lock:
        _roll_over_if_new_month_locked()
        _state["seconds_used"] += seconds
        minutes_used = _state["seconds_used"] / 60

        if minutes_used >= GPU_BUDGET_HARD_THRESHOLD_MINUTES and not _state["hard_tripped"]:
            _state["hard_tripped"] = True
            _state["soft_tripped"] = True  # hard implies soft
            newly_hard = True
        elif minutes_used >= GPU_BUDGET_SOFT_THRESHOLD_MINUTES and not _state["soft_tripped"]:
            _state["soft_tripped"] = True
            newly_soft = True

    if newly_hard:
        msg = (
            f"[GPU_BUDGET] HARD LIMIT HIT - {minutes_used:.1f} GPU-min used this "
            f"month (~${minutes_used / 60 * GPU_HOURLY_COST_USD:.2f}), threshold is "
            f"{GPU_BUDGET_HARD_THRESHOLD_MINUTES} min. ALL separation (standard and "
            f"HQ, upload and YouTube) is now disabled until next month or a manual "
            f"reset via POST /admin/reset-gpu-budget."
        )
        logger.critical(msg)
        alert_now(msg)
    elif newly_soft:
        msg = (
            f"[GPU_BUDGET] Soft limit hit - {minutes_used:.1f} GPU-min used this "
            f"month (~${minutes_used / 60 * GPU_HOURLY_COST_USD:.2f}), threshold is "
            f"{GPU_BUDGET_SOFT_THRESHOLD_MINUTES} min. Studio Quality (HQ) is now "
            f"disabled for the rest of the month; standard separation keeps running. "
            f"Hard limit is {GPU_BUDGET_HARD_THRESHOLD_MINUTES} min."
        )
        logger.warning(msg)
        alert_now(msg)


def hq_blocked() -> bool:
    """True once either threshold has tripped - HQ is the first thing
    cut, so it's blocked at BOTH the soft and hard stage."""
    with _lock:
        _roll_over_if_new_month_locked()
        return _state["soft_tripped"]


def all_separation_blocked() -> bool:
    """True only once the hard threshold trips - standard separation
    keeps running through the soft stage."""
    with _lock:
        _roll_over_if_new_month_locked()
        return _state["hard_tripped"]


def budget_status() -> dict:
    """Snapshot for /admin/status."""
    with _lock:
        _roll_over_if_new_month_locked()
        minutes_used = _state["seconds_used"] / 60
        return {
            "month": _state["month_key"],
            "minutes_used": round(minutes_used, 1),
            "estimated_cost_usd": round(minutes_used / 60 * GPU_HOURLY_COST_USD, 2),
            "soft_threshold_minutes": GPU_BUDGET_SOFT_THRESHOLD_MINUTES,
            "hard_threshold_minutes": GPU_BUDGET_HARD_THRESHOLD_MINUTES,
            "hq_blocked": _state["soft_tripped"],
            "all_separation_blocked": _state["hard_tripped"],
            "percent_of_hard_limit": round(
                minutes_used / GPU_BUDGET_HARD_THRESHOLD_MINUTES * 100, 1
            ) if GPU_BUDGET_HARD_THRESHOLD_MINUTES else 0,
        }


def reset_budget() -> None:
    """Manual override - e.g. after correcting the threshold with real
    RunPod numbers, or topping up mid-month."""
    with _lock:
        _state["month_key"] = _current_month_key()
        _state["seconds_used"] = 0.0
        _state["soft_tripped"] = False
        _state["hard_tripped"] = False
    logger.info("[GPU_BUDGET] Manually reset.")