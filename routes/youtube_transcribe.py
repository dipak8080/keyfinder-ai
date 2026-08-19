"""
routes/youtube_transcribe.py - /youtube/transcribe: paste a YouTube URL,
get a transcript.

DELIBERATELY ITS OWN MODULE, not another route inside routes/youtube.py.
That file is already 1000+ lines covering download, analyze, separate,
separate-hq, stems and stems-hq, and it is the module that changes most
often - every yt-dlp breakage, client-ladder tweak and SABR workaround
lands there. Transcription has an entirely different failure profile
(model, semaphore, duration cap) and mixing the two would mean a
"YouTube is broken" report could equally be a download problem or a
Whisper problem, in the same file, with the same log prefix.

WHAT IS SHARED, AND WHY: the DOWNLOAD half reuses _chain_download() from
routes/youtube.py rather than reimplementing it. That function encodes
hard-won behaviour - acquiring the download slot INSIDE the try so a
queue-wait 503 can't escape a background task and strand the job on
"processing" forever, releasing the slot exactly once in `finally`, and
classifying every yt-dlp failure mode into a user-facing message.
Copying it here would mean the next fix to any of that reaches only one
of the two copies. So: download logic shared and battle-tested,
transcription logic entirely local to this file. Isolation where the
bugs will actually be, not isolation for its own sake.

TWO SEMAPHORES, HELD ONE AT A TIME: same rule as every other /youtube/*
chained tool. _chain_download releases the download slot before this
module acquires the transcription slot, so a 15-minute Whisper run never
also ties up a download slot for its whole duration.

DURATION IS CHECKED AFTER DOWNLOAD, not before, and that is a known
tradeoff. youtube.py's own MAX_VIDEO_DURATION_SECONDS caps what will
download at all, but transcription's cap is stricter and independent
(CPU inference runs near realtime, so a 60-minute video would occupy the
single transcription slot for the better part of an hour). Rejecting
after the download wastes the fetch; rejecting before would need a
separate yt-dlp metadata round trip. The download is cached by video_id,
so the wasted work is paid at most once per video - which is why the
cheap-and-simple order was chosen here.
"""
import asyncio
import time
from functools import partial

from fastapi import APIRouter, Form, HTTPException, Depends
from fastapi.responses import JSONResponse

from config import (
    logger,
    MAX_TRANSCRIPTION_DURATION_SECONDS,
    TRANSCRIPTION_JOB_TTL_SECONDS,
    YOUTUBE_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS,
    YOUTUBE_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS,
)
from utils import run_blocking, acquire_slot_or_503, _transcription_semaphore
from rate_limit import check_rate_limit
from jobs import create_job, mark_transcription_complete, mark_failed, fail_if_unfinished, get_job
from monitoring import record_result
from audio_common import AudioToolError, validate_duration
from log_stream import set_job_context, remember_job_tags, tag_from_job
from youtube import is_valid_youtube_url

from transcription import transcribe_job, is_available as transcription_available
from speech_to_text import (
    # Same normalizers transcribe() runs internally - see the note in
    # routes/transcribe.py. Reusing them is what stops this route and the
    # upload route drifting on what counts as a valid language code.
    _normalize_language,
    _normalize_task,
    _normalize_mode,
)

from ._shared import (
    spawn_background_task,
    _tool_status,
    _reject_if_transcription_queue_full,
)
from .youtube import _chain_download

router = APIRouter()

TOOL = "YOUTUBE_TRANSCRIBE"
METRIC = "/youtube/transcribe"
JOB_TYPE = "youtube_transcribe"


def _validated_options(language, task, mode):
    """Normalize and validate the three option fields, converting the
    worker's AudioToolError into a 400. Identical contract to
    routes/transcribe.py's helper of the same name - both call the same
    underlying normalizers, so a language code accepted by one endpoint
    is always accepted by the other."""
    try:
        return (
            _normalize_language(language),
            _normalize_task(task),
            _normalize_mode(mode)[0],
        )
    except AudioToolError as e:
        raise HTTPException(400, str(e))


async def _run_youtube_transcribe(job_id: str, url: str, language, task, mode):
    """Download, then transcribe. Two different semaphores, held one at a
    time - _chain_download has already released the download slot by the
    time this acquires the transcription one.

    tool/tier are NOT set here: the calling route set them before
    spawn_background_task() copied the context, same as every other
    /youtube/* runner.
    """
    downloaded = await _chain_download(job_id, url, TOOL, METRIC)
    if downloaded is None:
        # _chain_download already marked the job and recorded the metric.
        # This guard exists only so a future change there that forgets to
        # mark can't leave the job stranded on "processing".
        fail_if_unfinished(job_id, "Download failed.")
        return

    file_path, title = downloaded
    succeeded = False
    acquired = False
    started = time.monotonic()

    try:
        # Duration gate BEFORE taking the transcription slot. A file too
        # long to transcribe must not first occupy the one slot the whole
        # site shares, and validate_duration spawns ffprobe so it goes
        # through run_blocking rather than stalling the event loop.
        try:
            duration = await run_blocking(
                validate_duration, file_path, MAX_TRANSCRIPTION_DURATION_SECONDS
            )
        except AudioToolError as e:
            mark_failed(job_id, str(e))
            logger.warning(f"[{TOOL}] job={job_id} rejected on duration: {e}")
            record_result(METRIC, False)
            return

        await acquire_slot_or_503(_transcription_semaphore, "youtube-transcribe")
        acquired = True

        waited = time.monotonic() - started
        if waited > 1.0:
            logger.info(f"[{TOOL}] job={job_id} waited {waited:.1f}s for a transcription slot")

        logger.info(
            f"[{TOOL}] job={job_id} transcribing '{title}' "
            f"({duration:.1f}s audio) language={language or 'auto'}, task={task}, mode={mode}"
        )

        # Backend dispatcher - see transcription.py. Awaited directly
        # because the GPU path is network-bound and the local path
        # already wraps itself in run_blocking internally.
        result = await transcribe_job(file_path, language, task, mode)

        # title, not a filename: for a chained job the video title IS the
        # user-facing name, and it is what mark_transcription_complete
        # stores for the result payload.
        mark_transcription_complete(job_id, title, result)
        succeeded = True
        logger.info(
            f"[{TOOL}] job={job_id} COMPLETE in {time.monotonic() - started:.1f}s "
            f"({len(result.get('segments') or [])} segments, lang={result.get('language')}, "
            f"task={result.get('task')}, mode={result.get('mode')})"
        )

    except AudioToolError as e:
        # Expected, user-actionable: no speech detected, unreadable file,
        # model unavailable. Message is already written for the end user.
        mark_failed(job_id, str(e))
        logger.warning(f"[{TOOL}] job={job_id} FAILED in {time.monotonic() - started:.1f}s: {e}")

    except HTTPException as e:
        # Almost always the queue-wait 503 from acquire_slot_or_503. In a
        # background task there is no HTTP layer to catch this, so it must
        # be handled here or it escapes the task and strands the job.
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
        fail_if_unfinished(job_id, "Transcription failed unexpectedly.")
        # The downloaded WAV is this job's own temp copy - the shared
        # cache entry in cache.py is separate and untouched by this
        # cleanup, so deleting here does not cost a future job its cache
        # hit.
        from utils import cleanup_file, release_memory_to_os
        cleanup_file(file_path)
        release_memory_to_os()
        if acquired:
            _transcription_semaphore.release()
        record_result(METRIC, succeeded)


@router.post(
    "/youtube/transcribe",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=YOUTUBE_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=YOUTUBE_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def youtube_transcribe_route(
    url: str = Form(...),
    language: str = Form(None),
    task: str = Form("transcribe"),
    mode: str = Form(None),
):
    """Poll GET /youtube/transcribe/status/{job_id}, then
    GET /youtube/transcribe/result/{job_id} once complete.

    Form fields:
        url      - YouTube video URL (required).
        language - ISO-639-1 code to force a language; omit or send
                   "" / "auto" to detect it automatically.
        task     - "transcribe" (source language) or "translate" (English).
        mode     - speed tier; see GET /speech-to-text/languages.
    """
    # Availability FIRST, before anything else is spent.
    #
    # This endpoint is the one where skipping the check is most expensive.
    # /speech-to-text and /video-to-text fail on an upload the user has
    # already sent; this one would accept the job and then spend a
    # download slot, up to DOWNLOAD_WALL_CLOCK_TIMEOUT_SECONDS of wall
    # clock, and paid residential proxy bandwidth fetching a video that
    # was never going to be transcribed - failing only at the handoff.
    #
    # Matters more on the GPU backend than it ever did on CPU: "available"
    # there means a set of env vars is present, so a typo'd endpoint id
    # makes EVERY request take this path.
    if not transcription_available():
        logger.error("[%s] Request rejected - transcription unavailable "
                     "(see startup logs)." % TOOL)
        raise HTTPException(
            503, "Transcription is temporarily unavailable. Please try again later."
        )

    if not is_valid_youtube_url(url):
        raise HTTPException(400, "Please provide a valid YouTube video URL.")

    # Validated before the job exists: a bad language code should cost a
    # 400, not a job row and a full YouTube download.
    language, task, mode = _validated_options(language, task, mode)

    # Same whole-server capacity gate as /speech-to-text. Both endpoints
    # share ONE transcription semaphore, so both must respect one queue
    # bound - guarding only the upload route would let YouTube jobs fill
    # the queue unnoticed, and vice versa.
    #
    # Checked here, before create_job, which also means before any
    # YouTube download starts: a refused submission costs no proxy
    # bandwidth, no yt-dlp subprocess, and no disk.
    _reject_if_transcription_queue_full()

    # Set BEFORE spawn_background_task(): create_task() copies the context
    # at the moment it is called, and this is also what tags the POST's
    # own row in request_logs.
    set_job_context(tool=TOOL, tier="standard")

    job_id = create_job(job_type=JOB_TYPE, ttl_seconds=TRANSCRIPTION_JOB_TTL_SECONDS)

    remember_job_tags(job_id)
    spawn_background_task(_run_youtube_transcribe(job_id, url, language, task, mode))

    logger.info(
        f"[{TOOL}] job={job_id} queued for {url} "
        f"(language={language or 'auto'}, task={task}, mode={mode})"
    )
    return JSONResponse({
        "job_id": job_id,
        "status": "processing",
        "options": {"language": language, "task": task, "mode": mode},
    })


@router.get("/youtube/transcribe/status/{job_id}")
async def youtube_transcribe_status(job_id: str):
    return _tool_status(job_id, JOB_TYPE)


@router.get("/youtube/transcribe/result/{job_id}")
async def youtube_transcribe_result(job_id: str):
    """Returns transcript JSON directly - no file involved, same contract
    as /speech-to-text/result."""
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