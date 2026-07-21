"""
jobs.py - In-memory job tracking for long-running background work
(currently just vocal/instrumental separation).

Same pattern as rate_limit.py / monitoring.py: in-memory, per-instance,
thread-safe via a single lock. Fine for a single-VPS deployment; if this
ever scales to multiple instances behind a load balancer, job state would
need to move to something shared (Redis, etc.) since a status poll could
otherwise land on an instance that never actually ran the job.

Why a job table at all, instead of just returning the result directly:
Demucs separation takes 1-5+ minutes on CPU - far too long for a normal
synchronous HTTP request (browsers and most HTTP clients time out well
before that, and it would block a worker thread the whole time). Instead,
POST /separate returns a job_id almost immediately, the actual separation
runs in the background, and the frontend polls GET /separate/status/{id}
every few seconds until it flips to "complete" or "failed".
"""
import time
import threading
import uuid
from typing import Optional

from config import logger, SEPARATION_JOB_TTL_SECONDS

_lock = threading.Lock()

# job_id -> {
#   status: "processing" | "complete" | "failed",
#   created_at: float,
#   title: str,
#   error: Optional[str],
#   vocals_path: Optional[str],
#   instrumental_path: Optional[str],
# }
_jobs = {}


def create_job() -> str:
    job_id = uuid.uuid4().hex
    with _lock:
        _jobs[job_id] = {
            "status": "processing",
            "created_at": time.time(),
            "title": None,
            "error": None,
            "vocals_path": None,
            "instrumental_path": None,
        }
    return job_id


def mark_complete(job_id: str, title: str, vocals_path: str, instrumental_path: str):
    with _lock:
        if job_id in _jobs:
            _jobs[job_id].update({
                "status": "complete",
                "title": title,
                "vocals_path": vocals_path,
                "instrumental_path": instrumental_path,
            })


def mark_failed(job_id: str, error: str):
    with _lock:
        if job_id in _jobs:
            _jobs[job_id].update({
                "status": "failed",
                "error": error,
            })


def get_job(job_id: str) -> Optional[dict]:
    with _lock:
        job = _jobs.get(job_id)
        return dict(job) if job else None


def cleanup_expired_jobs():
    """
    Deletes job entries (and their on-disk stem files) older than
    SEPARATION_JOB_TTL_SECONDS. Call this periodically (e.g. once per
    request, or on a background timer) rather than relying on job entries
    to be cleaned up individually - a user who never comes back to
    download their result would otherwise leave both the job dict entry
    and its stem files on disk forever.
    """
    import os

    now = time.time()
    to_delete = []

    with _lock:
        for job_id, job in _jobs.items():
            if now - job["created_at"] > SEPARATION_JOB_TTL_SECONDS:
                to_delete.append(job_id)

        for job_id in to_delete:
            job = _jobs.pop(job_id, None)
            if not job:
                continue
            for path_key in ("vocals_path", "instrumental_path"):
                path = job.get(path_key)
                if path and os.path.exists(path):
                    try:
                        os.remove(path)
                    except Exception as e:
                        logger.warning(f"[JOBS] Failed to clean up expired stem file {path}: {e}")

    if to_delete:
        logger.info(f"[JOBS] Cleaned up {len(to_delete)} expired separation job(s)")