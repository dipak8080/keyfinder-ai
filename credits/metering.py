"""
credits/metering.py - What every GPU job actually cost.

WHY THIS EXISTS SEPARATELY FROM THE LEDGER
------------------------------------------
The ledger answers "what did the user pay?". This answers "what did it
cost me?". Those are different questions and only one of them was ever
being recorded.

Every separation job gets a row here REGARDLESS of paywall state, tier,
or whether a credit changed hands. That is the entire point of shipping
with PAYWALL_ENABLED=false first: run for a few weeks, collect real
numbers, and decide whether $0.20 a credit clears the cost before asking
anyone for money. Guessing the price first and measuring later is how
you find out you were underwater after selling a hundred packs.

WHAT IT DELIBERATELY DOES NOT DO
--------------------------------
It does not enforce a budget. separation.py's docstring already explains
at length why the old self-tracked spend breaker was removed - it
undercounted (RunPod bills for cold start, model load and both
transfers, not just the Demucs run), it reset on every deploy, and it
tracked spending rather than balance. All three problems apply equally
to anything built here, so this records and reports and never blocks.

The real ceiling stays RunPod's own account balance, and the manual
lever stays SEPARATION_HQ_ENABLED. This file is a meter, not a valve.

WHICH NUMBER IS THE COST
------------------------
gpu_seconds comes from what the WORKER reports for its own run, the same
value separation.py already logs. That is the honest number for
comparing tools against each other, but it is NOT the whole bill -
RunPod also charges for the cold start and transfers that sit outside
it. So est_cost_usd is a FLOOR, not an invoice. Treat a tool whose
estimate is close to its revenue as already losing money.
"""

from __future__ import annotations

import logging
from typing import Any

from .config import get_settings
from .db import connect, now_iso, tx

log = logging.getLogger("credits.metering")

TERMINAL = ("completed", "failed", "cancelled", "timeout")


def record_job_created(
    *,
    job_id: str,
    tool: str,
    subject_id: str | None = None,
    account_id: str | None = None,
    ip_hash: str | None = None,
    input_seconds: float | None = None,
    input_bytes: int | None = None,
    charge_type: str | None = None,
) -> None:
    """Open a metrics row at submit time.

    Called for EVERY separation job, free or paid, metered or not.
    charge_type is 'none' when the paywall is off - which is what lets a
    later query separate "jobs that would have been billable" from jobs
    that never could be.

    Swallows its own errors on purpose: a metering failure must never
    turn a working separation into a failed one. A missing row costs you
    one data point; a raised exception here would cost the user their
    job.
    """
    settings = get_settings()
    try:
        with connect() as conn, tx(conn):
            conn.execute(
                """INSERT OR REPLACE INTO gpu_job_metrics
                   (job_id, tool, subject_id, account_id, ip_hash, charge_type,
                    paywall_enabled, input_seconds, input_bytes, status, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,'created',?)""",
                (job_id, tool, subject_id, account_id, ip_hash, charge_type,
                 1 if settings.paywall_enabled else 0,
                 input_seconds, input_bytes, now_iso()),
            )
    except Exception:  # noqa: BLE001
        log.exception("failed to open metrics row for %s", job_id)


def record_input_duration(job_id: str, input_seconds: float | None) -> None:
    """Fill in the input duration once it's actually known.

    WHY THIS IS SEPARATE from record_job_created(). The submit path only
    ffprobes on the BILLABLE routes - the standard path deliberately
    doesn't pay for a probe it has no decision to make with. But
    _run_demucs_on_gpu() probes every job regardless, because it has to
    enforce max_duration_seconds. So the number exists for every job; it
    just arrives a moment later than the row does.

    Calling this from there is what makes input_minutes complete across
    ALL four routes instead of HQ-only. Without it the cost-per-minute
    figures would silently describe paid jobs only, which is exactly the
    subset most likely to mislead when setting a price.

    Uses COALESCE-free assignment on purpose: this value is more
    authoritative than whatever the submit path guessed, since it comes
    from the same probe the duration limit is enforced against.
    """
    if input_seconds is None:
        return
    try:
        with connect() as conn, tx(conn):
            conn.execute(
                "UPDATE gpu_job_metrics SET input_seconds=? WHERE job_id=?",
                (float(input_seconds), job_id),
            )
    except Exception:  # noqa: BLE001
        log.exception("failed to record input duration for %s", job_id)


def record_input_duration_safe(job_id: str, file_path: str) -> None:
    """ffprobe a file and record its duration, swallowing everything.

    For the chained YouTube routes, where the file only exists inside the
    background task - the submit path had nothing to probe. Without this
    the two /youtube/*-hq rows would carry a null input_seconds, and the
    cost report's input_minutes would silently describe uploads only.

    Deliberately best-effort and silent: this runs on a path where the
    job is already accepted and paid for. A probe failure must not turn
    a working separation into a failed one over a metrics column.
    """
    try:
        import subprocess

        from config import FFMPEG_PATH

        out = subprocess.run(
            [FFMPEG_PATH.replace("ffmpeg", "ffprobe"), "-v", "error",
             "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", file_path],
            capture_output=True, text=True, timeout=30, check=True,
        )
        record_input_duration(job_id, float(out.stdout.strip()))
    except Exception:  # noqa: BLE001
        log.debug("could not probe duration for %s", job_id)


def record_job_finished(
    job_id: str,
    *,
    status: str,
    gpu_seconds: float | None = None,
    queue_seconds: float | None = None,
    wall_seconds: float | None = None,
    runpod_job_id: str | None = None,
    gpu_type: str | None = None,
    error: str | None = None,
) -> None:
    """Close the row with the outcome and the worker's reported time.

    Idempotent by way of being a plain UPDATE - calling it twice writes
    the same values twice, which is harmless. COALESCE on every optional
    column means a second call with less information can never blank out
    what a first call already recorded.
    """
    settings = get_settings()
    normalised = (status or "").strip().lower()
    if normalised not in TERMINAL:
        normalised = "completed" if normalised == "success" else "failed"

    est_cost = (
        round(gpu_seconds * settings.runpod_usd_per_gpu_second, 6)
        if gpu_seconds else None
    )
    try:
        with connect() as conn, tx(conn):
            conn.execute(
                """UPDATE gpu_job_metrics
                   SET status=?, ended_at=?, error=?,
                       gpu_seconds=COALESCE(?, gpu_seconds),
                       queue_seconds=COALESCE(?, queue_seconds),
                       wall_seconds=COALESCE(?, wall_seconds),
                       runpod_job_id=COALESCE(?, runpod_job_id),
                       gpu_type=COALESCE(?, gpu_type),
                       est_cost_usd=COALESCE(?, est_cost_usd)
                   WHERE job_id=?""",
                (normalised, now_iso(), error, gpu_seconds, queue_seconds, wall_seconds,
                 runpod_job_id, gpu_type, est_cost, job_id),
            )
    except Exception:  # noqa: BLE001
        log.exception("failed to close metrics row for %s", job_id)


def gpu_seconds_from_runpod(payload: dict[str, Any] | None) -> tuple[float | None, float | None]:
    """RunPod's /status reports executionTime and delayTime in MILLISECONDS.

    Kept here rather than at the call site because the unit is the easy
    thing to get wrong, and getting it wrong by 1000x would make every
    cost estimate meaningless in a direction that looks plausible.
    """
    if not isinstance(payload, dict):
        return (None, None)
    exec_ms = payload.get("executionTime")
    delay_ms = payload.get("delayTime")
    return (
        float(exec_ms) / 1000.0 if isinstance(exec_ms, (int, float)) else None,
        float(delay_ms) / 1000.0 if isinstance(delay_ms, (int, float)) else None,
    )


def daily_costs(days: int = 30) -> list[dict]:
    """Per day, per tool: jobs, minutes in, GPU seconds out, dollars."""
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM gpu_cost_daily ORDER BY day DESC, tool LIMIT ?",
            (days * 8,),
        ).fetchall()
    return [dict(r) for r in rows]


def totals(days: int = 30) -> dict:
    """The unit economics summary - the numbers that decide the price."""
    with connect() as conn:
        row = conn.execute(
            """SELECT COUNT(*) AS jobs,
                      ROUND(SUM(COALESCE(gpu_seconds,0)), 1)        AS gpu_seconds,
                      ROUND(SUM(COALESCE(est_cost_usd,0)), 4)       AS est_cost_usd,
                      ROUND(SUM(COALESCE(input_seconds,0))/60.0, 1) AS input_minutes,
                      ROUND(AVG(COALESCE(gpu_seconds,0)), 2)        AS avg_gpu_seconds,
                      SUM(CASE WHEN charge_type='credit' THEN 1 ELSE 0 END) AS paid_jobs,
                      SUM(CASE WHEN charge_type='free'   THEN 1 ELSE 0 END) AS free_tier_jobs,
                      SUM(CASE WHEN charge_type='none'   THEN 1 ELSE 0 END) AS unmetered_jobs,
                      -- Every terminal state that isn't success. Counting
                      -- only status='failed' missed timeouts entirely -
                      -- and a timeout is the MOST expensive way to fail,
                      -- because it runs to the wall before being killed.
                      -- Reporting those as zero failures would hide the
                      -- single worst line item in the cost report.
                      SUM(CASE WHEN status IN ('failed','timeout','cancelled')
                               THEN 1 ELSE 0 END)                   AS failed_jobs,
                      ROUND(SUM(CASE WHEN status IN ('failed','timeout','cancelled')
                               THEN COALESCE(est_cost_usd,0) ELSE 0 END), 4) AS wasted_usd
               FROM gpu_job_metrics
               WHERE created_at >= strftime('%Y-%m-%dT%H:%M:%SZ','now',?)""",
            (f"-{days} days",),
        ).fetchone()
        revenue = conn.execute(
            "SELECT COALESCE(SUM(amount_cents),0) AS cents FROM orders"
            " WHERE status='paid' AND test_mode=0"
        ).fetchone()

    out = dict(row)
    out["lifetime_revenue_usd"] = round((revenue["cents"] or 0) / 100.0, 2)

    jobs = out.get("jobs") or 0
    cost = out.get("est_cost_usd") or 0
    if jobs:
        out["cost_per_job_usd"] = round(cost / jobs, 4)

    # The number that actually answers "is the price right?". Compared
    # against a credit's price, not against revenue: revenue is lifetime
    # and this window is `days`, so dividing them would mix timeframes.
    paid = out.get("paid_jobs") or 0
    if paid and cost:
        out["est_cost_per_paid_job_usd"] = round(cost / paid, 4)
    return out