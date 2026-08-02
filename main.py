"""
main.py - App entrypoint. This file should stay tiny: create the app,
wire middleware/lifespan, mount routes. All logic lives elsewhere.
"""
import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import (
    logger,
    ALLOWED_ORIGINS,
)
from utils import ensure_cookies_file
from routes import router
from jobs import cleanup_expired_jobs, get_job_stats
from log_stream import RequestLoggerMiddleware, router as logs_router, attach_system_log_capture
from cookie_upload import router as cookie_upload_router


# How often the background sweep runs. Jobs are removed based on their
# own ttl_seconds, so this only controls how promptly an already-expired
# job's files are reclaimed - not how long they live. A minute is
# frequent enough that disk never drifts far, and cheap enough to be
# invisible: the sweep is a dict scan plus however many os.remove() calls
# the expired jobs earned.
JOB_CLEANUP_INTERVAL_SECONDS = int(os.environ.get("JOB_CLEANUP_INTERVAL_SECONDS", "60"))


async def _job_cleanup_loop():
    """
    Periodic TTL sweep for the job table.

    WHY THIS EXISTS AS A BACKGROUND TASK: cleanup_expired_jobs() used to
    be called at the top of ~20 route handlers ("opportunistic sweep"),
    which had three problems on a single-worker deployment:

      1. It ran blocking os.remove() calls directly on the event loop.
         Expiring one stems job means deleting four full-length WAVs, so
         a request that happened to trigger the sweep stalled every other
         connection - including the cheap status polls the frontend needs
         to stay responsive - for the duration.
      2. The cost was paid by whoever happened to submit next, making
         one unlucky user's request arbitrarily slower than the rest for
         reasons entirely unrelated to their own file.
      3. It only ran when traffic arrived. An idle server never swept, so
         files sat on a 30GB disk until the next visitor showed up.

    Running it here fixes all three: off the request path, on a fixed
    schedule, and dispatched to a worker thread so even the deletion
    itself never blocks the loop.
    """
    while True:
        try:
            await asyncio.sleep(JOB_CLEANUP_INTERVAL_SECONDS)
            # run_in_executor, not a direct call: cleanup does real
            # blocking disk I/O, and the whole point of moving it here
            # was to keep that off the event loop.
            await asyncio.get_running_loop().run_in_executor(None, cleanup_expired_jobs)
        except asyncio.CancelledError:
            # Normal shutdown - re-raise so the task actually stops
            # rather than looping forever through a swallowed cancel.
            raise
        except Exception as e:
            # A failed sweep must never kill the loop; the next tick
            # retries and the log line makes a persistent failure
            # (e.g. a permissions problem on the output dir) visible
            # instead of silently letting disk fill up.
            logger.error(f"[JOBS] Cleanup sweep failed: {e}", exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    attach_system_log_capture()  # starts capturing all logger.info()/error() calls app-wide
    ensure_cookies_file()
    logger.info(f"[CORS] Allowed origins: {ALLOWED_ORIGINS}")

    # One sweep immediately at boot: a redeploy recreates the container
    # while output files persist on the mounted volume, so anything
    # already past its TTL should go now rather than after the first
    # interval.
    try:
        cleanup_expired_jobs()
    except Exception as e:
        logger.error(f"[JOBS] Startup cleanup failed: {e}", exc_info=True)

    cleanup_task = asyncio.create_task(_job_cleanup_loop())
    logger.info(
        f"[JOBS] Background cleanup running every {JOB_CLEANUP_INTERVAL_SECONDS}s"
    )

    yield

    # Shutdown - stop the sweep and wait for it to actually unwind, so a
    # deletion in progress isn't torn down mid-write.
    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass

    stats = get_job_stats()
    logger.info(f"[JOBS] Shutting down with jobs in table: {stats}")


app = FastAPI(title="Audio Analysis API - ESSENTIA FIXED", version="12.6.0", lifespan=lifespan)

# Logs every HTTP request (timestamp, method, path, status, duration, IP) to SQLite
app.add_middleware(RequestLoggerMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
app.include_router(logs_router)  # /admin/logs live dashboard (HTTP + system logs)
app.include_router(cookie_upload_router)  # /admin/upload-cookies - upload cookies.txt directly, no base64