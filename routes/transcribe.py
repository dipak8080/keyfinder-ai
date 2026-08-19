"""
routes/transcribe.py - /speech-to-text: Whisper transcription.

Split out of the old monolithic routes.py (2026-08-14 restructure).

On its OWN semaphore, not the ffmpeg pool. Whisper inference is a
heavy, sustained CPU+RAM operation unlike a stateless ffmpeg
subprocess; sharing the pool would let one transcription starve fast,
cheap operations like /volume or /trim of their slots.

Structurally different from every other tool too: no /preview (there
is no audio output) and its result route is /result, returning JSON.

OPTIONS: accepts language / task / mode as optional multipart form
fields. All three default to the endpoint's original behaviour
(auto-detect, verbatim transcription, balanced beam size), so a client
that posts only `file` gets byte-identical behaviour to before these
existed - the pre-existing Next.js form keeps working untouched.

VALIDATION ORDER matters here and is deliberate:

    1. model availability   (503 - no point accepting anything)
    2. option validation    (400 - free, no I/O)
    3. filename/format      (400 - free, no I/O)
    4. create job + accept upload   (writes to disk)
    5. duration check       (spawns ffprobe)

Steps 1-3 are pure CPU on already-parsed values, so rejecting there
costs nothing. Doing them after step 4 would burn a job ID, a disk
write, and a cleanup cycle to tell someone they typo'd a language code.
"""
from functools import partial

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends
from fastapi.responses import JSONResponse

from config import (
    AUDIO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS,
    TRANSCRIPTION_JOB_TTL_SECONDS,
    MAX_TRANSCRIPTION_DURATION_SECONDS,
    logger,
)
from utils import run_blocking, _transcription_semaphore
from rate_limit import check_rate_limit
from jobs import create_job, mark_transcription_complete, get_job
from audio_common import AudioToolError
from log_stream import set_job_context, remember_job_tags, tag_from_job

from speech_to_text import (
    transcribe,
    get_language_options,
    is_available as transcription_available,
    # Private by name, imported deliberately: these are the SAME
    # normalizers transcribe() runs internally. Reusing them here rather
    # than re-implementing the checks means the route and the worker can
    # never disagree about what counts as a valid language code - a
    # duplicate implementation would drift the first time either side
    # changed (locale handling, a new mode, a renamed task).
    _normalize_language,
    _normalize_task,
    _normalize_mode,
)

from ._shared import (
    spawn_background_task,
    _validated_input_format,
    _accept_upload,
    _validate_duration_or_reject,
    _log_queued,
    _run_tool_job,
    _tool_status,
)

router = APIRouter()


def _require_transcription_available():
    """503 if the Whisper model never loaded at startup.

    speech_to_text.py deliberately survives a failed model load so that
    one broken download doesn't take down /convert, /trim and every
    other tool with it. The cost of that choice is that this endpoint
    has to check - otherwise the failure surfaces only after the user
    has uploaded a file and waited for a job that was never going to
    run.
    """
    if not transcription_available():
        logger.error("[SPEECH_TO_TEXT] Request rejected - model unavailable (see startup logs).")
        raise HTTPException(
            503,
            "Transcription is temporarily unavailable. Please try again later.",
        )


def _validated_options(language, task, mode):
    """Normalize and validate the three option fields.

    Returns (language, task, mode) ready to hand to transcribe().
    Raises HTTPException(400) with the user-facing message.

    Empty-string handling is the subtle part: an HTML form that renders
    an unselected <select> posts `language=""`, which is NOT the same as
    omitting the field. Both must mean auto-detect, and neither may
    reach transcribe() as a literal language code.
    """
    try:
        return (
            _normalize_language(language),
            _normalize_task(task),
            _normalize_mode(mode)[0],  # (name, beam_size) - route only needs the name
        )
    except AudioToolError as e:
        # AudioToolError messages from these normalizers are already
        # written for end users, so they pass straight through.
        raise HTTPException(400, str(e))


@router.post(
    "/speech-to-text",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def speech_to_text_route(
    file: UploadFile = File(...),
    language: str = Form(None),
    task: str = Form("transcribe"),
    mode: str = Form(None),
):
    """Poll GET /speech-to-text/status/{job_id}, then
    GET /speech-to-text/result/{job_id} once complete.

    Form fields (all optional):
        language - ISO-639-1 code ("ne", "hi", ...) to force a language.
                   Omit, or send "" / "auto", to auto-detect.
        task     - "transcribe" (keep source language) or "translate"
                   (emit English regardless of what was spoken).
        mode     - speed tier; see GET /speech-to-text/languages for the
                   current list.
    """
    set_job_context(tool="SPEECH_TO_TEXT", tier="standard")

    # --- 1-3: everything free, before any disk or job is touched ---
    _require_transcription_available()
    language, task, mode = _validated_options(language, task, mode)

    # Guard before _validated_input_format: a multipart part with no
    # filename arrives as None (or the literal "" from some clients),
    # and "no extension" is a confusing way to report "no file".
    if not file.filename:
        raise HTTPException(400, "No file was uploaded. Please choose a file and try again.")

    _validated_input_format(file.filename)
    original_filename = file.filename

    # --- 4: from here on we own resources that need cleaning up ---
    job_id = create_job(job_type="transcribe", ttl_seconds=TRANSCRIPTION_JOB_TTL_SECONDS)

    remember_job_tags(job_id)
    input_path, size = await _accept_upload(file, job_id, label="transcribe")

    # --- 5: its own duration cap - transcription time scales with
    # length and even int8 CPU inference is slow, so this is capped
    # independently of the other tools rather than sharing their limit.
    await _validate_duration_or_reject(job_id, input_path, MAX_TRANSCRIPTION_DURATION_SECONDS)

    spawn_background_task(_run_tool_job(
        tool="SPEECH_TO_TEXT",
        metric="/speech-to-text",
        job_id=job_id,
        semaphore=_transcription_semaphore,
        # Positional, not keyword: run_blocking forwards *args to the
        # executor and is not guaranteed to pass keywords through.
        # Order must stay (path, language, task, mode) to match
        # transcribe()'s signature.
        work=lambda: run_blocking(transcribe, input_path, language, task, mode),
        on_success=lambda result: mark_transcription_complete(job_id, original_filename, result),
        generic_error="Transcription failed unexpectedly.",
        cleanup_paths=[input_path],
        success_detail=lambda r: (
            f"{len(r.get('segments') or [])} segments, "
            f"lang={r.get('language')}, task={r.get('task')}, mode={r.get('mode')}"
        ),
    ))

    _log_queued("SPEECH_TO_TEXT", job_id, original_filename, size)
    logger.info(
        f"[SPEECH_TO_TEXT] Job {job_id} queued with "
        f"language={language or 'auto'}, task={task}, mode={mode}"
    )

    # Echo the resolved options back. The frontend can then show
    # "Translating to English..." rather than guessing, and a caller who
    # sent something the server normalized (e.g. "en-US" -> "en") can
    # see what actually took effect.
    return JSONResponse({
        "job_id": job_id,
        "status": "processing",
        "options": {
            "language": language,          # null when auto-detecting
            "task": task,
            "mode": mode,
        },
    })


@router.get("/speech-to-text/languages")
async def speech_to_text_languages():
    """Options payload for the frontend selector.

    Single source of truth: the Next.js app fetches this instead of
    hardcoding a language list that would silently drift the moment the
    installed faster-whisper version changes.

    Static for the lifetime of the deployment, so it carries a long
    cache header - this lets Cloudflare absorb it entirely rather than
    hitting origin on every page load of /speech-to-text.
    """
    return JSONResponse(
        get_language_options(),
        headers={"Cache-Control": "public, max-age=3600"},
    )


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