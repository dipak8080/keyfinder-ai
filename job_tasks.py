"""
job_tasks.py - Maps a job id to the asyncio task running it, so a job
can be cancelled while it is in flight.

WHY THIS IS ALL IT TAKES. Everything downstream of the cancel already
exists and is already correct:

  runpod_client.run_worker_job catches asyncio.CancelledError and calls
  RunPod's own cancel endpoint, so the GPU stops billing.

  routes/_shared._run_tool_job catches CancelledError, calls
  mark_failed, and re-raises into a `finally` that runs
  settle_or_refund(job_id, succeeded=False) - so the credit comes back
  in the same instant rather than via the 90-minute sweeper.

The only missing piece was a way to FIND the task for a given job.
spawn_background_task drops every task into one anonymous set, which
keeps them from being garbage collected but makes them unaddressable.

USER CANCEL VS SHUTDOWN. Both arrive as CancelledError, and they need
different copy: "you stopped this" is not "the server restarted while
this was running". mark_user_cancelled() records the difference so
_run_tool_job can say the right thing.
"""

from __future__ import annotations

import asyncio
import logging
import threading

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_tasks: dict[str, asyncio.Task] = {}
_user_cancelled: set[str] = set()


def register(job_id: str, task: asyncio.Task | None) -> None:
    """Record the task, and drop any finished ones while we are here.

    The sweep matters because unregister() lives in _run_tool_job's
    finally, INSIDE the semaphore block - so a task cancelled while still
    queued for a slot never reaches it and would otherwise leave its
    entry behind forever. Job ids are unique, so without this the dict
    only grows.
    """
    if task is None:
        return
    with _lock:
        if len(_tasks) > 64:
            for done in [k for k, t in _tasks.items() if t.done()]:
                _tasks.pop(done, None)
                _user_cancelled.discard(done)
        _tasks[job_id] = task


def unregister(job_id: str) -> None:
    with _lock:
        _tasks.pop(job_id, None)
        _user_cancelled.discard(job_id)


def is_running(job_id: str) -> bool:
    with _lock:
        task = _tasks.get(job_id)
    return task is not None and not task.done()


def was_user_cancelled(job_id: str) -> bool:
    with _lock:
        return job_id in _user_cancelled


def mark_user_cancelled(job_id: str) -> None:
    with _lock:
        _user_cancelled.add(job_id)


def cancel(job_id: str) -> bool:
    """Ask the task to stop. True if a live task was found and asked.

    Cancellation is cooperative: this returns as soon as the request is
    delivered, not when the job has finished unwinding. The caller
    should report "stopping", and the job's own status goes terminal a
    moment later through the normal _run_tool_job path.
    """
    with _lock:
        task = _tasks.get(job_id)
        if task is None or task.done():
            return False
        _user_cancelled.add(job_id)
    task.cancel()
    logger.info(f"[JOB_TASKS] cancel requested for job={job_id}")
    return True


def running_count() -> int:
    with _lock:
        return sum(1 for t in _tasks.values() if not t.done())