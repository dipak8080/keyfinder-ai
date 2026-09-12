"""
routes/cancel.py - Stop a job that is still running.

    POST /jobs/{job_id}/cancel

ONE ROUTE FOR EVERY TOOL. Job ids are unique across tools and the
registry is keyed on them alone, so this does not need a per-tool
variant the way status and download do.

WHAT IT ACTUALLY DOES. It cancels the asyncio task. Everything that
matters follows from that on the existing paths:

  - run_worker_job cancels the RunPod job, so the GPU stops billing
  - _run_tool_job marks the job failed and refunds the credit

Before this existed, the frontend's Cancel button only stopped the
client polling. The GPU kept running, the credit stayed spent, and the
user had paid for a result nobody would ever fetch.

AUTHORISATION is the job id itself, matching /status, /preview and
/download on every tool in this codebase. The ids are random and
unguessable, and the worst a guess achieves is stopping a job and
refunding its owner.
"""

from __future__ import annotations

import logging

from functools import partial

from fastapi import APIRouter, Depends, HTTPException, Path

import job_tasks
from config import (
    CANCEL_RATE_LIMIT_MAX_REQUESTS,
    CANCEL_RATE_LIMIT_WINDOW_SECONDS,
)
from jobs import get_job
from rate_limit import check_rate_limit

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post(
    "/jobs/{job_id}/cancel",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=CANCEL_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=CANCEL_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def cancel_job_route(job_id: str = Path(..., max_length=64)) -> dict:
    """Stop an in-flight job and return its credit.

    Responses:
        {"cancelled": true}                  the task was asked to stop
        {"cancelled": false, "status": ...}  already terminal, nothing to do

    404 if the job id is unknown or has expired.

    IDEMPOTENT AND SAFE TO SPAM. Cancelling an already-finished job is
    not an error - the client pressed a button on a screen that was one
    poll out of date, which is the normal case, not a failure.
    """
    job = get_job(job_id)
    if job is None:
        raise HTTPException(404, "Job not found (it may have expired).")

    status = job.get("status")
    if status != "processing":
        return {"cancelled": False, "job_id": job_id, "status": status}

    if not job_tasks.cancel(job_id):
        # Registered as processing but no live task. Either it finished
        # microseconds ago, or the server restarted and this row is a
        # leftover the sweeper has not reached yet. Neither is worth an
        # error response.
        return {"cancelled": False, "job_id": job_id, "status": status}

    logger.info(f"[CANCEL] job={job_id} cancelled by user")
    return {"cancelled": True, "job_id": job_id, "status": "cancelling"}