"""
gpu_internal_routes.py - The receiving end of the GPU worker's direct
HTTP file transfer. Audio bytes never travel through RunPod's job
payload (10MB limit); they move directly between this VPS and the worker
instead. See gpu-worker/handler.py for the other side.

TWO ROUTES, BOTH MACHINE-TO-MACHINE ONLY - never called by a browser,
never listed in the tool picker:

  GET  /internal/gpu/input/{job_id}
  POST /internal/gpu/upload/{job_id}/{name}

AUTH IS DELIBERATELY SEPARATE from admin_auth.py's system: that one is
built for a HUMAN typing a key into a browser (rate limits and lockouts
tuned for occasional manual use). This is a fixed machine caller making
a predictable number of calls per job, so a constant-time shared-secret
comparison is the right amount of machinery.

--------------------------------------------------------------------------
SECURITY HARDENING (2026-08-11) - three fixes, one of them a real bug

1. PATH TRAVERSAL VIA job_id (a genuine bug in the first version).
   `name` was sanitized before being used to build a filesystem path,
   but `job_id` - which lands in the SAME path - was not. A job_id of
   "../../etc/something" would have escaped SEPARATION_DIR entirely.
   Both segments are now validated against a strict hex-only pattern
   (job ids are uuid4().hex, so anything else is illegitimate by
   construction), and the resolved path is additionally checked to be
   inside SEPARATION_DIR - belt and braces, since a validation regex is
   only as good as its author's imagination and a resolved-path check
   is not.

2. UPLOADS ARE ONLY ACCEPTED FOR JOBS ACTUALLY IN FLIGHT.
   Previously any caller holding the secret could write a file for ANY
   job id, whether or not such a job existed. Uploads are now rejected
   unless that job_id is currently registered as in-flight (see
   register_gpu_input) - which is exactly the window during which a
   legitimate worker upload can occur. Narrows the blast radius of a
   leaked secret from "write files forever" to "write files only during
   a real job you also have to have triggered".

3. UPLOAD SIZE CAP.
   An unbounded streaming write is a disk-exhaustion vector - this VPS
   has 30GB and no swap. Uploads are cut off past MAX_GPU_UPLOAD_BYTES
   and the partial file removed. Sized generously (a 4-stem HQ split of
   a long track is legitimately large) but finite, which is the point.
--------------------------------------------------------------------------
"""
import hmac
import os
import re

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse

from config import logger, SEPARATION_DIR, GPU_WORKER_SHARED_SECRET

router = APIRouter()

# Job ids are uuid4().hex (see jobs.py's create_job) - 32 lowercase hex
# characters, nothing else, ever. Validating against what the system
# ACTUALLY produces (rather than a permissive "no slashes" check) means
# anything unexpected is rejected by default instead of being reasoned
# about case by case.
_JOB_ID_RE = re.compile(r"^[0-9a-f]{8,64}$")

# Stem names come from MODEL_STEM_NAMES / the separate task's fixed pair -
# all plain lowercase words, optionally underscored.
_STEM_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")

# Hard ceiling per uploaded file. A 4-stem HQ split of a 6-minute track
# is a few hundred MB across all stems, but any SINGLE stem well past
# this is not something this pipeline legitimately produces.
MAX_GPU_UPLOAD_BYTES = int(os.environ.get("MAX_GPU_UPLOAD_BYTES", str(500 * 1024 * 1024)))  # 500 MB

# job_id -> input file path, registered by separation.py immediately
# before it submits a job and unregistered in a `finally` once the job
# resolves. The entry's lifetime is tied to one request, so no TTL sweep
# is needed - and its presence is ALSO what authorises an upload for
# that job id (see hardening note 2).
_input_registry: dict = {}


def register_gpu_input(job_id: str, input_path: str) -> None:
    _input_registry[job_id] = input_path


def unregister_gpu_input(job_id: str) -> None:
    _input_registry.pop(job_id, None)


def is_job_in_flight(job_id: str) -> bool:
    return job_id in _input_registry


def _check_secret(request: Request) -> None:
    """
    Constant-time comparison via hmac.compare_digest - a plain `==` on a
    secret leaks, in principle, how many leading characters matched via
    timing. Costs nothing to do correctly.
    """
    if not GPU_WORKER_SHARED_SECRET:
        # Misconfigured deployment, not a real auth attempt - fail closed
        # rather than accepting everything because the secret is unset.
        raise HTTPException(503, "GPU worker integration is not configured.")

    auth_header = request.headers.get("authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(401, "Missing or malformed Authorization header.")

    provided = auth_header[len("Bearer "):]
    if not hmac.compare_digest(provided, GPU_WORKER_SHARED_SECRET):
        raise HTTPException(401, "Invalid shared secret.")


def _validated_job_id(job_id: str) -> str:
    if not _JOB_ID_RE.match(job_id or ""):
        logger.warning(f"[GPU_INTERNAL] Rejected malformed job_id: {job_id!r}")
        raise HTTPException(400, "Invalid job id.")
    return job_id


def _validated_stem_name(name: str) -> str:
    if not _STEM_NAME_RE.match(name or ""):
        logger.warning(f"[GPU_INTERNAL] Rejected malformed stem name: {name!r}")
        raise HTTPException(400, "Invalid stem name.")
    return name


def _safe_dest_path(job_id: str, name: str) -> str:
    """
    Builds the destination path and PROVES it stays inside
    SEPARATION_DIR.

    The regex checks above should already make traversal impossible -
    this is the independent second check, because "the regex is
    airtight" is exactly the kind of assumption that turns into a CVE.
    A resolved-path comparison doesn't care how clever the input was.
    """
    base = os.path.realpath(SEPARATION_DIR)
    dest = os.path.realpath(os.path.join(base, f"{job_id}_{name}.wav"))
    if not dest.startswith(base + os.sep):
        logger.error(
            f"[GPU_INTERNAL] Path escape attempt blocked: job_id={job_id!r} "
            f"name={name!r} resolved to {dest!r}"
        )
        raise HTTPException(400, "Invalid destination path.")
    return dest


@router.get("/internal/gpu/input/{job_id}")
async def get_gpu_input(job_id: str, request: Request):
    _check_secret(request)
    job_id = _validated_job_id(job_id)

    input_path = _input_registry.get(job_id)
    if not input_path or not os.path.exists(input_path):
        # Either never registered (forged/stale request) or the job
        # already resolved and unregistered it (a worker retrying after
        # the fact) - nothing legitimate to serve either way.
        logger.warning(f"[GPU_INTERNAL] Input requested for unknown/expired job_id={job_id}")
        raise HTTPException(404, "No input registered for this job id.")

    # FileResponse streams from disk - the whole point of abandoning
    # base64-in-JSON was to stop holding a file's full size in memory.
    return FileResponse(input_path, media_type="application/octet-stream")


@router.post("/internal/gpu/upload/{job_id}/{name}")
async def upload_gpu_result(job_id: str, name: str, request: Request):
    _check_secret(request)
    job_id = _validated_job_id(job_id)
    name = _validated_stem_name(name)

    # Only accept results for a job that is genuinely in flight right
    # now - see hardening note 2. A legitimate worker upload can only
    # ever happen inside this window.
    if not is_job_in_flight(job_id):
        logger.warning(
            f"[GPU_INTERNAL] Upload rejected for job_id={job_id} - not currently in flight."
        )
        raise HTTPException(409, "No in-flight job with this id.")

    dest_path = _safe_dest_path(job_id, name)

    written = 0
    try:
        # Streamed in chunks, never buffered whole via request.body() -
        # these are real audio files and this box has no swap.
        with open(dest_path, "wb") as f:
            async for chunk in request.stream():
                written += len(chunk)
                if written > MAX_GPU_UPLOAD_BYTES:
                    # Abort mid-stream rather than after the fact: the
                    # point is to never let the bytes land at all.
                    f.close()
                    os.remove(dest_path)
                    logger.error(
                        f"[GPU_INTERNAL] Upload for job={job_id} name={name} exceeded "
                        f"{MAX_GPU_UPLOAD_BYTES} bytes - aborted and partial file removed."
                    )
                    raise HTTPException(413, "Uploaded file too large.")
                f.write(chunk)
    except HTTPException:
        raise
    except Exception as e:
        # Never leave a half-written stem on disk to be mistaken for a
        # real result by _verify_output_files().
        try:
            if os.path.exists(dest_path):
                os.remove(dest_path)
        except Exception:
            pass
        logger.error(f"[GPU_INTERNAL] Failed writing upload for job={job_id} name={name}: {e}")
        raise HTTPException(500, "Failed to save uploaded file.")

    logger.info(
        f"[GPU_INTERNAL] job={job_id}: received '{name}' "
        f"({written / (1024 * 1024):.1f}MB) -> {dest_path}"
    )
    return {"status": "ok", "bytes": written}