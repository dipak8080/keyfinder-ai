"""
gpu_internal_routes.py - The receiving end of the GPU worker's direct
HTTP file transfer (see gpu-worker/handler.py's own module docstring for
the full "why" - RunPod's job payload has a 10MB limit, so audio bytes
never travel through RunPod at all; they move directly between this VPS
and the worker over plain HTTP instead).

TWO ROUTES, BOTH MACHINE-TO-MACHINE ONLY - never called by a browser,
never listed in the tool picker:

  GET  /internal/gpu/input/{job_id}
       The worker fetches the source audio to separate from here.

  POST /internal/gpu/upload/{job_id}/{name}
       The worker pushes one finished stem's raw bytes here as soon as
       it's produced.

AUTH IS DELIBERATELY SEPARATE FROM admin_auth.py's system - that system
is built for a HUMAN typing a key into a browser (rate limiting tuned
for occasional manual use, a lockout that assumes a person is guessing
passwords). This is a fixed, known caller (the GPU worker) making a
predictable number of calls per job, so a single constant-time shared-
secret comparison is the right amount of complexity - not reusing
admin_auth's machinery, and not skipping auth either.

registers the input path in-process (see _input_registry below) rather
than trying to reconstruct a file path from job_id alone - that keeps
this file completely decoupled from whatever internal naming convention
_accept_upload()/build_temp_input_path() happen to use today, so a future
change to how uploads are named on disk can't silently break this.
"""
import hmac
import os

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse

from config import logger, SEPARATION_DIR, GPU_WORKER_SHARED_SECRET

router = APIRouter()

# job_id -> input file path, registered by separation.py right before it
# submits a job to the GPU worker, and read here when the worker comes
# to fetch it. No TTL/cleanup sweep needed: separation.py registers it
# immediately before submission and unregisters it in a `finally` once
# the job resolves (success or failure) - the entry's lifetime is tied
# directly to one request, not left to age out on its own.
_input_registry: dict = {}


def register_gpu_input(job_id: str, input_path: str) -> None:
    _input_registry[job_id] = input_path


def unregister_gpu_input(job_id: str) -> None:
    _input_registry.pop(job_id, None)


def _check_secret(request: Request) -> None:
    """
    Constant-time comparison via hmac.compare_digest - a plain `==` on
    secrets is vulnerable to a timing attack in principle (early-exit on
    the first mismatched byte leaks how many leading characters were
    correct); costs nothing extra to do this correctly.
    """
    if not GPU_WORKER_SHARED_SECRET:
        # Misconfigured deployment, not a real auth attempt - fail
        # closed rather than accepting every request because the secret
        # was never set.
        raise HTTPException(503, "GPU worker integration is not configured.")

    auth_header = request.headers.get("authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(401, "Missing or malformed Authorization header.")

    provided = auth_header[len("Bearer "):]
    if not hmac.compare_digest(provided, GPU_WORKER_SHARED_SECRET):
        raise HTTPException(401, "Invalid shared secret.")


@router.get("/internal/gpu/input/{job_id}")
async def get_gpu_input(job_id: str, request: Request):
    _check_secret(request)

    input_path = _input_registry.get(job_id)
    if not input_path or not os.path.exists(input_path):
        # Either this job_id was never registered (bad/forged request),
        # or the job already resolved and unregistered it (a worker
        # retrying a stale request) - either way, nothing to serve.
        logger.warning(f"[GPU_INTERNAL] Input requested for unknown/expired job_id={job_id}")
        raise HTTPException(404, "No input registered for this job id.")

    # FileResponse streams from disk rather than loading the whole file
    # into memory - the entire point of moving off base64-in-JSON was to
    # stop paying for a file's full size in memory/payload at once.
    return FileResponse(input_path, media_type="application/octet-stream")


@router.post("/internal/gpu/upload/{job_id}/{name}")
async def upload_gpu_result(job_id: str, name: str, request: Request):
    _check_secret(request)

    # `name` becomes part of a filesystem path below - the same
    # ASCII-alphanumeric-only discipline every other user/caller-
    # controlled path segment in this codebase already follows (see
    # utils.py's safe_extension), even though this caller is trusted:
    # defense in depth costs nothing here and this is exactly the kind
    # of narrow, easy-to-verify check worth keeping even on an internal
    # route.
    safe_name = "".join(c for c in name if c.isascii() and c.isalnum() or c == "_")
    if not safe_name or safe_name != name:
        raise HTTPException(400, "Invalid stem name.")

    dest_path = os.path.join(SEPARATION_DIR, f"{job_id}_{safe_name}.wav")

    try:
        # Streamed to disk in chunks, not buffered whole via
        # `await request.body()` - these are real audio files (tens of
        # MB), and reading one fully into memory before writing it is
        # exactly the kind of avoidable memory spike this VPS (no swap)
        # can't absorb for free.
        with open(dest_path, "wb") as f:
            async for chunk in request.stream():
                f.write(chunk)
    except Exception as e:
        logger.error(f"[GPU_INTERNAL] Failed writing upload for job={job_id} name={name}: {e}")
        raise HTTPException(500, "Failed to save uploaded file.")

    logger.info(f"[GPU_INTERNAL] job={job_id}: received '{safe_name}' -> {dest_path}")
    return {"status": "ok", "path": dest_path}