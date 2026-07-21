"""
ADDITIVE snippet - paste into your existing routes.py.
Does NOT replace anything already there. Two parts:
  1. New imports to add near the top, alongside your existing imports.
  2. Four new endpoint functions to add anywhere in the file (after the
     existing /download and /analyze routes is the natural spot).
"""

# ============================================================
# PART 1 - add these imports near your existing imports at the top
# ============================================================

import shutil
from functools import partial
from fastapi.responses import FileResponse

from jobs import create_job, mark_complete, mark_failed, get_job, cleanup_expired_jobs
from separation import run_separation, SeparationError
from config import (
    SEPARATION_RATE_LIMIT_MAX_REQUESTS,
    SEPARATION_RATE_LIMIT_WINDOW_SECONDS,
    MAX_CONCURRENT_SEPARATIONS,
)
import asyncio

# One dedicated semaphore for separation, same pattern as
# _analysis_semaphore / _download_semaphore in utils.py - caps how many
# Demucs subprocesses can run at once (default 1, since it's the most
# RAM-hungry endpoint in this app). If you'd rather keep this alongside
# the other semaphores in utils.py instead of defining it here, move this
# one line there and import it the same way as the other two.
_separation_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SEPARATIONS)


# ============================================================
# PART 2 - add these four endpoints anywhere in the file
# ============================================================

async def _run_separation_background(job_id: str, file_path: str, original_filename: str):
    """
    Runs after POST /separate has already returned a response to the
    caller - this is what makes the endpoint non-blocking. Acquires the
    separation semaphore itself (rather than the route holding it before
    returning), since the whole point is the HTTP response doesn't wait
    for this to finish.
    """
    async with _separation_semaphore:
        try:
            vocals_path, instrumental_path = await run_blocking(run_separation, file_path, job_id)
            mark_complete(job_id, original_filename, vocals_path, instrumental_path)
            logger.info(f"[SEPARATION] Job {job_id} finished successfully")
        except SeparationError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[SEPARATION] Job {job_id} failed: {e}")
        except Exception as e:
            mark_failed(job_id, "Separation failed unexpectedly.")
            logger.error(f"[SEPARATION] Job {job_id} failed unexpectedly: {e}", exc_info=True)
        finally:
            cleanup_file(file_path)
            release_memory_to_os()


@router.post(
    "/separate",
    dependencies=[Depends(partial(check_rate_limit, max_requests=SEPARATION_RATE_LIMIT_MAX_REQUESTS, window_seconds=SEPARATION_RATE_LIMIT_WINDOW_SECONDS))],
)
async def separate_audio(file: UploadFile = File(...)):
    """
    Accepts an audio file, immediately returns a job_id, and runs the
    actual Demucs separation in the background - separation takes
    1-5+ minutes on CPU, far too long for a normal synchronous request.
    Poll GET /separate/status/{job_id} to track progress.
    """
    cleanup_expired_jobs()  # opportunistic sweep of old jobs/files

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(400, "Empty file")
    if len(content) > MAX_UPLOAD_BYTES:
        size_mb = len(content) / (1024 * 1024)
        raise HTTPException(400, f"File too large ({size_mb:.1f} MB). Maximum allowed size is {MAX_UPLOAD_BYTES // (1024*1024)} MB.")

    job_id = create_job()
    file_path = os.path.join(UPLOAD_DIR, f"{job_id}_{file.filename}")
    with open(file_path, "wb") as f:
        f.write(content)
    del content

    # Fire-and-forget: NOT awaited, so the response below returns right
    # away while this keeps running. FastAPI/asyncio keeps the task alive
    # on the event loop even after the response is sent.
    asyncio.create_task(_run_separation_background(job_id, file_path, file.filename))

    logger.info(f"[SEPARATION] Job {job_id} queued for '{file.filename}'")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/separate/status/{job_id}")
async def separation_status(job_id: str):
    job = get_job(job_id)
    if job is None:
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
    }


def _resolve_stem_path(job_id: str, stem: str) -> str:
    if stem not in ("vocals", "instrumental"):
        raise HTTPException(400, "stem must be 'vocals' or 'instrumental'")
    job = get_job(job_id)
    if job is None:
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["vocals_path"] if stem == "vocals" else job["instrumental_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Stem file not found (it may have expired).")
    return path


@router.get("/separate/preview/{job_id}")
async def separation_preview(job_id: str, stem: str = Query(...)):
    """Streams the audio inline for in-browser <audio> playback (no
    Content-Disposition: attachment header, unlike /download below)."""
    path = _resolve_stem_path(job_id, stem)
    return FileResponse(path, media_type="audio/wav")


@router.get("/separate/download/{job_id}")
async def separation_download(job_id: str, stem: str = Query(...)):
    """Same file as /preview, served as a downloadable attachment
    instead of inline playback."""
    path = _resolve_stem_path(job_id, stem)
    filename = f"{stem}.wav"
    return FileResponse(path, media_type="audio/wav", filename=filename)