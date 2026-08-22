"""
routes/separation.py - Demucs separation on a directly uploaded file:
/separate, /separate-hq (vocal/instrumental) and /stems, /stems-hq (full
4-stem). The /youtube/separate* and /youtube/stems* equivalents (which
chain a download in front of the same Demucs work) live in
routes/youtube.py instead - see that file's module docstring for why
they're grouped with the other YouTube tools rather than here.

Split out of the old monolithic routes.py (2026-08-14 restructure). Pure
move: every docstring, comment, and line of logic here is unchanged from
its original location.

Four routes sharing one model, one semaphore and one queue. The vocal
remover is NOT cheaper than the stem splitter: Demucs separates all four
sources internally either way, and --two-stems just sums three of them
for us.

--------------------------------------------------------------------------
WHAT CHANGED (2026-08-22): DOCSTRINGS, NOT BEHAVIOUR

Three route docstrings below described separation as CPU work on this
VPS - "1-5+ minutes on CPU", "roughly 5x the CPU time", "same CPU cost".
None of that has been true since the GPU migration: separation.py
submits to a RunPod Serverless worker and awaits an HTTP call, and the
only local work per job is a single ffprobe duration check. See
separation.py's own module docstring for the full architecture.

The relative claims were still right - HQ really does cost several times
what standard costs, and /stems really does cost the same as /separate -
so only the noun changed. But a docstring naming the wrong machine is
how a stale assumption survives a migration, which is exactly what
happened to MAX_CONCURRENT_SEPARATIONS: its comment argued from "4
cores" long after the cores stopped being involved, and that argument
kept a second paid RunPod worker idle. Corrected here in the same pass
that fixed the constant.

No code, status codes, or response shapes changed.
--------------------------------------------------------------------------
"""
import os
import time
import asyncio
from functools import partial

from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Query
from fastapi.responses import JSONResponse, FileResponse

from config import (
    SEPARATION_RATE_LIMIT_MAX_REQUESTS,
    SEPARATION_RATE_LIMIT_WINDOW_SECONDS,
    SEPARATION_MODEL,
    SEPARATION_OVERLAP,
    DEMUCS_TIMEOUT_SECONDS,
    MAX_SEPARATION_DURATION_SECONDS,
    SEPARATION_MODEL_HQ,
    SEPARATION_OVERLAP_HQ,
    DEMUCS_TIMEOUT_SECONDS_HQ,
    MAX_SEPARATION_DURATION_SECONDS_HQ,
    SEPARATION_HQ_ENABLED,
    SEPARATION_HQ_RATE_LIMIT_MAX_REQUESTS,
    SEPARATION_HQ_RATE_LIMIT_WINDOW_SECONDS,
    STEMS_RATE_LIMIT_MAX_REQUESTS,
    STEMS_RATE_LIMIT_WINDOW_SECONDS,
    STEMS_HQ_RATE_LIMIT_MAX_REQUESTS,
    STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS,
    MAX_QUEUED_SEPARATIONS,
)
from rate_limit import check_rate_limit
from jobs import (
    create_job,
    mark_complete,
    mark_stems_complete,
    get_job,
    count_processing,
    SEPARATION_JOB_TYPES,
)
from separation import run_separation, run_stem_separation
from utils import _separation_semaphore
from log_stream import remember_job_tags, set_job_context, tag_from_job

from ._shared import spawn_background_task, _accept_upload, _log_queued, _reject_if_separation_queue_full, _run_tool_job

router = APIRouter()


async def _queue_separation(
    file: UploadFile,
    *,
    job_type: str,
    tool: str,
    model: str,
    overlap: float,
    timeout_seconds: int,
    max_duration_seconds: int,
    metric_label: str,
    hq: bool = False,
) -> JSONResponse:
    """
    Shared submit path for all four separation routes. They differ only
    in run knobs, rate limit and output shape, so the accept-and-queue
    sequence lives here once.

    Knobs are resolved by the CALLER at submission time and passed in, so
    a config change (or the HQ kill switch flipping) can never alter a
    job that is already queued - it runs with the settings it was
    accepted under.

    `hq` is explicit rather than inferred from the model name because the
    GPU budget gate and billing need a reliable tier signal that survives
    someone adding a differently-named model later.
    """
    # `tool` here is the log-prefix string ("STEMS", "STEMS_HQ", ...),
    # which already encodes tier for historical reasons - but tool/tier
    # in the DATABASE are kept as two SEPARATE columns, deliberately, so
    # a filter for tool=STEMS matches BOTH tiers and tier=hq narrows
    # further. Stripping the suffix here is what keeps those two axes
    # from collapsing back into the single log-prefix string.
    base_tool = tool[:-3] if tool.endswith("_HQ") else tool
    set_job_context(tool=base_tool, tier="hq" if hq else "standard")

    _reject_if_separation_queue_full()

    original_filename = file.filename

    job_id = create_job(job_type=job_type)

    remember_job_tags(job_id)
    file_path, size = await _accept_upload(file, job_id, label=tool.lower())

    is_stems = job_type in ("stems",)

    if is_stems:
        # No run_blocking() here - run_stem_separation() is now `async
        # def` (it awaits an HTTP call to the RunPod GPU worker, not a
        # blocking local subprocess). run_blocking() exists specifically
        # to offload BLOCKING calls off the event loop; wrapping an
        # already-async function in it would be a real bug, not a style
        # choice - see separation.py's own module docstring for the full
        # "why this changed" reasoning.
        work = lambda: run_stem_separation(
            file_path, job_id, model, overlap, timeout_seconds, max_duration_seconds,
        )
        on_success = lambda stems: mark_stems_complete(job_id, original_filename, stems)
        success_detail = lambda stems: f"{len(stems)} stems"
        generic_error = "Stem separation failed unexpectedly."
    else:
        work = lambda: run_separation(
            file_path, job_id, model, overlap, timeout_seconds, max_duration_seconds,
        )
        on_success = lambda paths: mark_complete(job_id, original_filename, paths[0], paths[1])
        success_detail = None
        generic_error = "Separation failed unexpectedly."

    spawn_background_task(_run_tool_job(
        tool=tool,
        metric=metric_label,
        job_id=job_id,
        semaphore=_separation_semaphore,
        work=work,
        on_success=on_success,
        generic_error=generic_error,
        cleanup_paths=[file_path],
        success_detail=success_detail,
        # False: separation.py records the worker's own reported
        # gpu_seconds instead - see this function's gpu_billed docstring
        # for why counting both would double-bill the budget.
        gpu_billed=False,
    ))

    depth = count_processing(SEPARATION_JOB_TYPES)
    _log_queued(tool, job_id, original_filename, size, f"model={model} queue={depth}/{MAX_QUEUED_SEPARATIONS}")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.post(
    "/separate",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=SEPARATION_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=SEPARATION_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def separate_audio(file: UploadFile = File(...)):
    """
    Accepts an audio file, returns a job_id immediately, and runs Demucs
    vocal/instrumental separation in the background on the RunPod GPU
    worker. Poll GET /separate/status/{job_id}.

    Backgrounded because it still takes longer than a comfortable request
    window - roughly 20-60 seconds on GPU, plus a cold start when no
    worker is warm. (This docstring used to say "1-5+ minutes on CPU",
    which was true before the GPU migration and is the figure a lot of
    the surrounding copy was originally sized against.)
    """
    return await _queue_separation(
        file,
        job_type="separation",
        tool="SEPARATION",
        model=SEPARATION_MODEL,
        overlap=SEPARATION_OVERLAP,
        timeout_seconds=DEMUCS_TIMEOUT_SECONDS,
        max_duration_seconds=MAX_SEPARATION_DURATION_SECONDS,
        metric_label="/separate",
        hq=False,
    )


@router.post(
    "/separate-hq",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=SEPARATION_HQ_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=SEPARATION_HQ_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def separate_audio_hq(file: UploadFile = File(...)):
    """
    High-quality separation: htdemucs_ft (a 4-model ensemble) at raised
    overlap. Roughly 5x the compute of /separate - four forward passes
    instead of one, plus the overlap increase - so it gets a longer
    timeout, a TIGHTER input duration cap, and a stricter rate limit.

    That 5x is a ratio, not a wall-clock figure: it held when this ran on
    the VPS CPU and it still holds on the GPU worker, where it works out
    at roughly 1-2 minutes rather than the 15-20 the CPU path took.

    A separate route rather than a `quality` form field because rate-limit
    dependencies are evaluated before the request body is read - a
    Depends() cannot see a Form value, so per-tier limits need per-tier
    routes.
    """
    if not SEPARATION_HQ_ENABLED:
        raise HTTPException(
            503,
            "High quality separation is temporarily unavailable due to server load. "
            "Please use standard separation."
        )

    return await _queue_separation(
        file,
        job_type="separation",
        tool="SEPARATION_HQ",
        model=SEPARATION_MODEL_HQ,
        overlap=SEPARATION_OVERLAP_HQ,
        timeout_seconds=DEMUCS_TIMEOUT_SECONDS_HQ,
        max_duration_seconds=MAX_SEPARATION_DURATION_SECONDS_HQ,
        metric_label="/separate-hq",
        hq=True,
    )


@router.get("/separate/status/{job_id}")
async def separation_status(job_id: str):
    tag_from_job(job_id)
    job = get_job(job_id)
    if job is None:
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
        "elapsed_seconds": round(time.time() - job["created_at"], 1),
    }


def _resolve_stem_path(job_id: str, stem: str) -> str:
    tag_from_job(job_id)
    if stem not in ("vocals", "instrumental"):
        raise HTTPException(400, "stem must be 'vocals' or 'instrumental'")
    job = get_job(job_id)
    if job is None:
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] == "failed":
        raise HTTPException(409, job.get("error") or "This job failed.")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["vocals_path"] if stem == "vocals" else job["instrumental_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Stem file not found (it may have expired).")
    return path


@router.get("/separate/preview/{job_id}")
async def separation_preview(job_id: str, stem: str = Query(...)):
    """Streams the audio inline for in-browser <audio> playback - no
    Content-Disposition: attachment, unlike /download below."""
    path = _resolve_stem_path(job_id, stem)
    return FileResponse(path, media_type="audio/wav")


@router.get("/separate/download/{job_id}")
async def separation_download(job_id: str, stem: str = Query(...)):
    """Same file as /preview, served as a downloadable attachment."""
    path = _resolve_stem_path(job_id, stem)
    return FileResponse(path, media_type="audio/wav", filename=f"{stem}.wav")


@router.post(
    "/stems",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=STEMS_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=STEMS_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def stems_route(file: UploadFile = File(...)):
    """
    Full 4-stem separation (vocals/drums/bass/other). Same model, same
    semaphore and the same compute cost as /separate - the only
    difference is that the four internally-separated sources are kept as
    individual files instead of three being summed into one
    instrumental.

    Worth restating because it is genuinely counterintuitive and drives
    the rate limits: --two-stems does NOT make the vocal remover cheaper.
    Demucs separates all four sources either way.
    """
    return await _queue_separation(
        file,
        job_type="stems",
        tool="STEMS",
        model=SEPARATION_MODEL,
        overlap=SEPARATION_OVERLAP,
        timeout_seconds=DEMUCS_TIMEOUT_SECONDS,
        max_duration_seconds=MAX_SEPARATION_DURATION_SECONDS,
        metric_label="/stems",
        hq=False,
    )


@router.post(
    "/stems-hq",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=STEMS_HQ_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=STEMS_HQ_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def stems_route_hq(file: UploadFile = File(...)):
    """High-quality full stem separation - same knobs and kill switch as
    /separate-hq."""
    if not SEPARATION_HQ_ENABLED:
        raise HTTPException(
            503,
            "High quality separation is temporarily unavailable due to server load. "
            "Please use standard stem separation."
        )

    return await _queue_separation(
        file,
        job_type="stems",
        tool="STEMS_HQ",
        model=SEPARATION_MODEL_HQ,
        overlap=SEPARATION_OVERLAP_HQ,
        timeout_seconds=DEMUCS_TIMEOUT_SECONDS_HQ,
        max_duration_seconds=MAX_SEPARATION_DURATION_SECONDS_HQ,
        metric_label="/stems-hq",
        hq=True,
    )


@router.get("/stems/status/{job_id}")
async def stems_status(job_id: str):
    """Returns the usual status fields plus the stem names actually
    available, so the frontend renders download buttons from the response
    instead of hardcoding names that would break if a different model
    were ever configured."""
    tag_from_job(job_id)
    job = get_job(job_id)
    if job is None or job["job_type"] != "stems":
        raise HTTPException(404, "Job not found (it may have expired).")
    stems = job.get("stems") or {}
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
        "stems": sorted(stems.keys()),
        "elapsed_seconds": round(time.time() - job["created_at"], 1),
    }


def _resolve_stems_file(job_id: str, stem: str) -> str:
    """Validates the requested stem against the job's OWN stem dict rather
    than a hardcoded tuple, so the valid set always follows whatever model
    produced the job."""
    tag_from_job(job_id)
    job = get_job(job_id)
    if job is None or job["job_type"] != "stems":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] == "failed":
        raise HTTPException(409, job.get("error") or "This job failed.")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    stems = job.get("stems") or {}
    if stem not in stems:
        raise HTTPException(400, f"stem must be one of: {', '.join(sorted(stems.keys()))}")
    path = stems[stem]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Stem file not found (it may have expired).")
    return path


@router.get("/stems/preview/{job_id}")
async def stems_preview(job_id: str, stem: str = Query(...)):
    path = _resolve_stems_file(job_id, stem)
    return FileResponse(path, media_type="audio/wav")


@router.get("/stems/download/{job_id}")
async def stems_download(job_id: str, stem: str = Query(...)):
    path = _resolve_stems_file(job_id, stem)
    return FileResponse(path, media_type="audio/wav", filename=f"{stem}.wav")