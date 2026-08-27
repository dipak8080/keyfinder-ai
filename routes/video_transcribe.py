"""
routes/video_transcribe.py - /video-to-text: upload a video, get a
transcript.

ITS OWN MODULE, not a widened /speech-to-text. Same reasoning as
routes/youtube_transcribe.py: the failure profile is different (missing
audio track, unreadable container, ffmpeg extraction) and mixing it into
the audio route would mean a "transcription is broken" report could be a
Whisper problem or an ffmpeg problem, in the same file, under the same
log prefix. Separate module, separate job type, separate log prefix, so
the two can never be confused.

It also keeps ALLOWED_VIDEO_INPUT_FORMATS honest. config.py states
plainly that video is an input to /video-to-audio "and nowhere else";
rather than quietly breaking that rule by widening the audio route's
accepted set, this adds a second endpoint that owns video explicitly.

WHAT IS SHARED: extraction reuses video_to_audio.py wholesale -
probe_audio_stream() and extract_audio(), including its stream-copy fast
path. Nothing about ffmpeg is reimplemented here.

TWO PHASES, TWO SEMAPHORES, HELD ONE AT A TIME:

    upload -> probe (submit time) -> extract (ffmpeg pool)
                                  -> transcribe (transcription pool)

The extract phase must NOT hold the transcription slot: ffmpeg
extraction is cheap and parallel-friendly, Whisper inference is neither,
and letting a 200MB re-encode occupy the single transcription slot would
block every audio upload on the site for no reason.

DURATION IS CHECKED BEFORE EXTRACTION, and this is the main design win
over doing it the obvious way. extract_audio() enforces
VIDEO_EXTRACT_MAX_DURATION_SECONDS (60 min), which is the right cap for
/video-to-audio and far too generous here - transcription caps at 20.
Calling extract_audio() first would mean a 45-minute video gets fully
extracted, possibly re-encoded, and only then rejected. probe_audio_stream()
is a single ffprobe call, so checking there costs about a second and saves
up to five minutes of pointless work.

probe_audio_stream() also raises on a video with no audio track at all,
which is the single most common bad upload for this endpoint (screen
recordings, silent clips, GIF-derived MP4s) - caught at submit time with
an immediate 400 rather than a job that fails a minute later.

--------------------------------------------------------------------------
METERED (2026-08-27), 1 credit under the shared "transcribe" rule - the
SAME rule key as /speech-to-text and /youtube/transcribe, deliberately.
All three hit one RunPod endpoint and one MAX_CONCURRENT_TRANSCRIPTIONS
pool, so they should draw on one bucket rather than handing a single
caller three independent budgets for one resource. (config.py already
records that exact complaint about the per-path separation limits.)

REFUNDS ARE **NOT** INHERITED HERE. This module has its own background
runner - it does not go through _run_tool_job - so the settle_or_refund()
that lives in that runner's `finally` never fires for these jobs. The
call is therefore made explicitly in _run_video_transcribe's own
`finally` below. Miss it and the 90-minute sweeper still returns the
credit, so nobody is robbed - they just wait an hour and a half staring
at a failed job, which is experientially indistinguishable from theft.
--------------------------------------------------------------------------
"""
import asyncio
import os
import time

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends
from fastapi.responses import JSONResponse

from config import (
    logger,
    UPLOAD_DIR,
    MAX_TRANSCRIPTION_DURATION_SECONDS,
    TRANSCRIPTION_JOB_TTL_SECONDS,
    MAX_VIDEO_TRANSCRIBE_BYTES,
    VIDEO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS,
    VIDEO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS,
)
from utils import (
    run_blocking,
    acquire_slot_or_503,
    cleanup_file,
    release_memory_to_os,
    _transcription_semaphore,
    _audio_tools_semaphore,
)
from jobs import (
    create_job,
    mark_transcription_complete,
    mark_failed,
    fail_if_unfinished,
    get_job,
)
from monitoring import record_result
from audio_common import AudioToolError
from log_stream import set_job_context, remember_job_tags, tag_from_job

from video_to_audio import (
    validate_video_input_format,
    probe_audio_stream,
    extract_audio,
    # Private by name, imported deliberately: this is the SAME table
    # extract_audio() consults to decide whether it can stream-copy. This
    # module reads it to pick a target format that lands on the copy path,
    # and reusing the one table is what guarantees the choice stays
    # correct if a codec is ever added or removed there.
    _COPY_COMPATIBLE,
)

from transcription import transcribe_job, is_available as transcription_available
from speech_to_text import (
    # Same normalizers transcribe() runs internally - see the note in
    # routes/transcribe.py. Reusing them is what stops the three
    # transcription endpoints drifting on what counts as a valid
    # language code.
    _normalize_language,
    _normalize_task,
    _normalize_mode,
)

# Credits. settle_or_refund is imported DIRECTLY here, unlike in
# routes/transcribe.py, because this module runs its own background task
# - see the module docstring.
from credits import paywall, metering
from credits.identity import Identity
from credits.limits import tiered_rate_limit
from credits.ledger import settle_or_refund

from ._shared import (
    spawn_background_task,
    _accept_upload,
    _tool_status,
    _reject_if_transcription_queue_full,
)

router = APIRouter()

TOOL = "VIDEO_TRANSCRIBE"
METRIC = "/video-to-text"
JOB_TYPE = "video_transcribe"
TOOL_KEY = "transcribe"   # shared credits rule key - see module docstring

# Fallback when the source codec has no stream-copy target. WAV is
# chosen over a compressed format on purpose: this file is a throwaway
# intermediate that Whisper reads once and is then deleted, so encode
# SPEED matters and encode SIZE does not. pcm_s16le is the cheapest
# thing ffmpeg can produce, and re-encoding to mp3/aac here would only
# add lossy generation loss on top of already-lossy source audio for no
# benefit whatsoever.
_FALLBACK_FORMAT = "wav"


def _cheapest_target_format(codec: str) -> str:
    """
    Picks an extraction target that lets extract_audio() stream-copy,
    falling back to WAV when the codec has no copy-compatible container.

    This is what makes the common case nearly free. Most uploads are MP4
    with AAC audio; targeting m4a means ffmpeg moves bytes between
    containers with no decode at all, finishing in about a second whether
    the input is 5MB or 100MB. Targeting WAV unconditionally - the
    obvious implementation - would force a full decode-and-re-encode of
    every single video for no gain, since Whisper reads m4a perfectly
    well via PyAV.

    Deterministic ordering (sorted) rather than set iteration order, so
    the same codec always produces the same target and a log line from
    two different days is comparable.
    """
    targets = _COPY_COMPATIBLE.get(codec)
    if targets:
        return sorted(targets)[0]
    return _FALLBACK_FORMAT


def _validated_options(language, task, mode):
    """Normalize and validate the three option fields, converting the
    worker's AudioToolError into a 400. Identical contract to the helper
    of the same name in routes/transcribe.py and
    routes/youtube_transcribe.py - all three call the same underlying
    normalizers, so a language code accepted by one endpoint is always
    accepted by the others."""
    try:
        return (
            _normalize_language(language),
            _normalize_task(task),
            _normalize_mode(mode)[0],
        )
    except AudioToolError as e:
        raise HTTPException(400, str(e))


def _validated_video_format(filename):
    """
    validate_video_input_format() raises AudioToolError, which is not an
    HTTPException - calling it bare would turn "you uploaded a .txt" into
    a 500. Same wrapper reasoning as _validated_input_format() in
    _shared.py, against the video format set instead of the audio one.
    """
    try:
        return validate_video_input_format(filename)
    except AudioToolError as e:
        raise HTTPException(400, str(e))


async def _run_video_transcribe(job_id, video_path, original_filename,
                                target_format, language, task, mode):
    """
    Extract, then transcribe. Two different semaphores, acquired and
    released one at a time so the ffmpeg phase never occupies the single
    transcription slot.

    tool/tier are NOT set here: the calling route set them before
    spawn_background_task() copied the context, same as every other
    runner in this package.
    """
    audio_path = os.path.join(UPLOAD_DIR, f"{job_id}_extracted.{target_format}")
    succeeded = False
    started = time.monotonic()
    holding = None   # which semaphore, if any, is currently held

    try:
        # ---------- PHASE 1: EXTRACTION (ffmpeg pool) ----------
        await acquire_slot_or_503(_audio_tools_semaphore, "video-transcribe-extract")
        holding = _audio_tools_semaphore
        try:
            copied = await run_blocking(extract_audio, video_path, audio_path, target_format)
            logger.info(
                f"[{TOOL}] job={job_id} extracted audio "
                f"({'stream copy' if copied else 're-encoded'} -> {target_format}) "
                f"in {time.monotonic() - started:.1f}s"
            )
        finally:
            # Released BEFORE the transcription slot is requested. Holding
            # both would let one video job occupy an ffmpeg slot for the
            # entire multi-minute Whisper run.
            holding.release()
            holding = None

        # The source video is dead weight from here on - only the
        # extracted audio is still needed, and a 100MB file sitting on
        # disk for the length of a transcription is worth avoiding on a
        # 30GB volume.
        cleanup_file(video_path)
        video_path = None

        # ---------- PHASE 2: TRANSCRIPTION (transcription pool) ----------
        wait_started = time.monotonic()
        await acquire_slot_or_503(_transcription_semaphore, "video-transcribe")
        holding = _transcription_semaphore

        waited = time.monotonic() - wait_started
        if waited > 1.0:
            logger.info(f"[{TOOL}] job={job_id} waited {waited:.1f}s for a transcription slot")

        logger.info(
            f"[{TOOL}] job={job_id} transcribing '{original_filename}' "
            f"language={language or 'auto'}, task={task}, mode={mode}"
        )

        # Backend dispatcher - see transcription.py. job_id is passed for
        # METERING only: the dispatcher records the worker's reported GPU
        # seconds against this job's gpu_job_metrics row, which is the
        # only place that number is visible before it is stripped from
        # the transcript.
        result = await transcribe_job(audio_path, language, task, mode, job_id=job_id)

        mark_transcription_complete(job_id, original_filename, result)
        succeeded = True
        logger.info(
            f"[{TOOL}] job={job_id} COMPLETE in {time.monotonic() - started:.1f}s "
            f"({len(result.get('segments') or [])} segments, lang={result.get('language')}, "
            f"task={result.get('task')}, mode={result.get('mode')})"
        )

    except AudioToolError as e:
        # Expected and user-actionable: no speech detected, extraction
        # failed, unreadable container. Message is already written for
        # the end user.
        mark_failed(job_id, str(e))
        logger.warning(f"[{TOOL}] job={job_id} FAILED in {time.monotonic() - started:.1f}s: {e}")

    except HTTPException as e:
        # Almost always a queue-wait 503 from acquire_slot_or_503. In a
        # background task there is no HTTP layer to catch this, so it
        # must be handled here or it escapes and strands the job at
        # "processing" forever.
        detail = e.detail if isinstance(e.detail, str) else "The server was too busy."
        mark_failed(job_id, detail)
        logger.warning(f"[{TOOL}] job={job_id} rejected: {detail}")

    except asyncio.CancelledError:
        mark_failed(job_id, "The server restarted while this job was running.")
        logger.warning(f"[{TOOL}] job={job_id} CANCELLED (shutdown)")
        raise

    except Exception as e:
        mark_failed(job_id, "Transcription failed unexpectedly.")
        logger.error(f"[{TOOL}] job={job_id} FAILED (unexpected): {e}", exc_info=True)

    finally:
        # MONEY FIRST, before anything that could itself raise - the same
        # ordering rule _run_tool_job's finally follows, for the same
        # reason. A failed paid job must return its credit even if
        # cleanup or the metrics call then goes wrong.
        #
        # This module needs the call EXPLICITLY because it does not use
        # _run_tool_job; there is no inherited `finally` to ride on.
        #
        # It sits in the finally rather than the except-chain so it also
        # covers the CancelledError path above, which re-raises INTO this
        # block - a redeploy killing an in-flight paid job returns the
        # credit in that instant rather than 90 minutes later.
        settle_or_refund(job_id, succeeded, reason="video_transcribe_failed")

        # Closes the gpu_job_metrics row the route opened. gpu_seconds is
        # left to transcription.py's GPU path, which is the only place
        # that sees what RunPod actually reported - a wall-clock number
        # taken here would also span queue wait and cold start, which are
        # real latency but not the GPU-seconds figure that belongs in a
        # cost comparison.
        metering.record_job_finished(
            job_id, status="completed" if succeeded else "failed"
        )

        # Only ever one semaphore is held at a time, and `holding` tracks
        # which - releasing blind would over-release whichever phase had
        # already returned its slot, permanently inflating that pool.
        if holding is not None:
            holding.release()
        fail_if_unfinished(job_id, "Transcription failed unexpectedly.")
        # video_path is None once phase 1 cleaned it up; cleanup_file
        # tolerates a missing file either way, so a failure before that
        # point still removes the upload.
        if video_path:
            cleanup_file(video_path)
        cleanup_file(audio_path)
        release_memory_to_os()
        record_result(METRIC, succeeded)


@router.post(
    "/video-to-text",
    dependencies=[Depends(tiered_rate_limit(
        TOOL_KEY,
        free_max=VIDEO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS,
        free_window=VIDEO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def video_to_text_route(
    file: UploadFile = File(...),
    language: str = Form(None),
    task: str = Form("transcribe"),
    mode: str = Form(None),
    identity: Identity = paywall.IdentityDep,
):
    """Poll GET /video-to-text/status/{job_id}, then
    GET /video-to-text/result/{job_id} once complete.

    Form fields (all optional except the file):
        file     - video file: MP4, MOV, MKV, AVI, WEBM, FLV, WMV, M4V,
                   3GP, MPEG, MPG.
        language - ISO-639-1 code to force a language; omit or send
                   "" / "auto" to detect it automatically.
        task     - "transcribe" (source language) or "translate" (English).
        mode     - speed tier; see GET /speech-to-text/languages.
    """
    set_job_context(tool=TOOL, tier="standard")

    # --- free checks, before anything is written to disk ---
    if not transcription_available():
        logger.error(f"[{TOOL}] Request rejected - model unavailable (see startup logs).")
        raise HTTPException(
            503, "Transcription is temporarily unavailable. Please try again later."
        )

    language, task, mode = _validated_options(language, task, mode)

    if not file.filename:
        raise HTTPException(400, "No file was uploaded. Please choose a video and try again.")

    _validated_video_format(file.filename)
    original_filename = file.filename

    # Capacity gate last among the free checks - see the equivalent note
    # in routes/transcribe.py for why input errors are reported before
    # server-capacity ones.
    _reject_if_transcription_queue_full()

    # --- from here on we own resources that need cleaning up ---
    job_id = create_job(job_type=JOB_TYPE, ttl_seconds=TRANSCRIPTION_JOB_TTL_SECONDS)
    remember_job_tags(job_id)

    # Its own, much larger byte cap than the audio route's: video is
    # roughly an order of magnitude bigger for the same running time.
    # Safe only because uploads stream to disk in chunks rather than
    # being read whole into memory - see upload.py.
    video_path, size = await _accept_upload(
        file, job_id, label="video_transcribe", max_bytes=MAX_VIDEO_TRANSCRIBE_BYTES
    )

    # Probe BEFORE extraction, and reject synchronously. One ffprobe call
    # (~1s) rules out the two most common bad uploads - no audio track at
    # all, and a video far longer than transcription can accept - instead
    # of discovering either after a multi-minute extraction. See the
    # module docstring for why extract_audio()'s own duration cap is not
    # sufficient here.
    #
    # It is also what gives the paywall a duration to decide on, which is
    # why the charge sits below this rather than above it.
    try:
        codec, duration = await run_blocking(probe_audio_stream, video_path)
    except AudioToolError as e:
        cleanup_file(video_path)
        mark_failed(job_id, str(e))
        raise HTTPException(400, str(e))

    if duration > MAX_TRANSCRIPTION_DURATION_SECONDS:
        message = (
            f"Video audio is {duration / 60:.1f} min long, which exceeds the "
            f"{MAX_TRANSCRIPTION_DURATION_SECONDS // 60} min transcription limit."
        )
        cleanup_file(video_path)
        mark_failed(job_id, message)
        raise HTTPException(400, message)

    target_format = _cheapest_target_format(codec)

    # CHARGE, then enqueue. Same race-handling as routes/transcribe.py -
    # the affordability pre-check in the dependency has already turned
    # away anyone who plainly cannot pay, so a 402 here is two tabs
    # spending the last credit at once.
    try:
        async with paywall.guard(
            identity, job_id=job_id, tool=TOOL_KEY, input_seconds=duration
        ) as charge:
            metering.record_job_created(
                job_id=job_id,
                tool=TOOL_KEY,
                subject_id=identity.subject_id,
                account_id=identity.account_id,
                ip_hash=identity.ip_hash,
                input_seconds=duration,
                input_bytes=size,
                charge_type=charge.charge_type,
            )

            spawn_background_task(_run_video_transcribe(
                job_id, video_path, original_filename, target_format, language, task, mode
            ))
    except HTTPException:
        cleanup_file(video_path)
        mark_failed(job_id, "Out of credits.")
        raise

    logger.info(
        f"[{TOOL}] job={job_id} queued '{original_filename}' "
        f"{size / (1024 * 1024):.1f}MB ({codec}, {duration:.1f}s -> {target_format}) "
        f"language={language or 'auto'}, task={task}, mode={mode} "
        f"charge={charge.charge_type}"
    )

    return JSONResponse({
        "job_id": job_id,
        "status": "processing",
        "options": {"language": language, "task": task, "mode": mode},
        "billing": {
            "charged": charge.charge_type,
            "balance": charge.balance_after,
            "free_remaining": charge.free_remaining_after,
        },
    })


@router.get("/video-to-text/status/{job_id}")
async def video_to_text_status(job_id: str):
    return _tool_status(job_id, JOB_TYPE)


@router.get("/video-to-text/result/{job_id}")
async def video_to_text_result(job_id: str):
    """Returns transcript JSON directly - identical shape and contract to
    /speech-to-text/result and /youtube/transcribe/result."""
    tag_from_job(job_id)
    job = get_job(job_id)
    if job is None or job["job_type"] != JOB_TYPE:
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] == "failed":
        raise HTTPException(409, job.get("error") or "This job failed.")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    result = job.get("result_data")
    if not result:
        raise HTTPException(404, "Transcript not found (it may have expired).")
    return JSONResponse(result)