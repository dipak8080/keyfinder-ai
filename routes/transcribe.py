"""
routes/transcribe.py - /speech-to-text: Whisper transcription.

Split out of the old monolithic routes.py (2026-08-14 restructure). Pure
move: every docstring, comment, and line of logic here is unchanged from
its original location. Nothing in this file changes behaviour.

On its OWN semaphore, not the ffmpeg pool. Whisper inference is a
heavy, sustained CPU+RAM operation unlike a stateless ffmpeg
subprocess; sharing the pool would let one transcription starve fast,
cheap operations like /volume or /trim of their slots.

Structurally different from every other tool too: no /preview (there
is no audio output) and its result route is /result, returning JSON.
"""
import asyncio
from functools import partial

from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from fastapi.responses import JSONResponse

from config import (
    AUDIO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS,
    TRANSCRIPTION_JOB_TTL_SECONDS,
    MAX_TRANSCRIPTION_DURATION_SECONDS,
)
from utils import run_blocking, _transcription_semaphore
from rate_limit import check_rate_limit
from jobs import create_job, mark_transcription_complete, get_job
from speech_to_text import transcribe
from log_stream import set_job_context, remember_job_tags, tag_from_job

from ._shared import (
    _validated_input_format,
    _accept_upload,
    _validate_duration_or_reject,
    _log_queued,
    _run_tool_job,
    _tool_status,
)

router = APIRouter()


@router.post(
    "/speech-to-text",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def speech_to_text_route(file: UploadFile = File(...)):
    """Poll GET /speech-to-text/status/{job_id}, then
    GET /speech-to-text/result/{job_id} once complete."""
    set_job_context(tool="SPEECH_TO_TEXT", tier="standard")

    _validated_input_format(file.filename)
    original_filename = file.filename

    job_id = create_job(job_type="transcribe", ttl_seconds=TRANSCRIPTION_JOB_TTL_SECONDS)

    remember_job_tags(job_id)
    input_path, size = await _accept_upload(file, job_id, label="transcribe")

    # Its own, tighter duration cap - transcription time scales with
    # length and even int8 CPU inference is slow, so this is capped well
    # below the other tools' 20 minutes.
    await _validate_duration_or_reject(job_id, input_path, MAX_TRANSCRIPTION_DURATION_SECONDS)

    asyncio.create_task(_run_tool_job(
        tool="SPEECH_TO_TEXT",
        metric="/speech-to-text",
        job_id=job_id,
        semaphore=_transcription_semaphore,
        work=lambda: run_blocking(transcribe, input_path),
        on_success=lambda result: mark_transcription_complete(job_id, original_filename, result),
        generic_error="Transcription failed unexpectedly.",
        cleanup_paths=[input_path],
        success_detail=lambda r: f"{len(r.get('segments') or [])} segments, lang={r.get('language')}",
    ))

    _log_queued("SPEECH_TO_TEXT", job_id, original_filename, size)
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/speech-to-text/status/{job_id}")
async def speech_to_text_status(job_id: str):
    return _tool_status(job_id, "transcribe")


@router.get("/speech-to-text/result/{job_id}")
async def speech_to_text_result(job_id: str):
    """Returns transcript JSON directly - no file involved, unlike every
    other tool's /download route."""
    tag_from_job(job_id)
    job = get_job(job_id)
    if job is None or job["job_type"] != "transcribe":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] == "failed":
        raise HTTPException(409, job.get("error") or "This job failed.")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    result = job.get("result_data")
    if not result:
        raise HTTPException(404, "Transcript not found (it may have expired).")
    return JSONResponse(result)