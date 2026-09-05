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

--------------------------------------------------------------------------
METERED (2026-08-27), 1 credit under the shared "transcribe" rule - the
same key as /speech-to-text and /video-to-text, because all three draw on
one RunPod endpoint and one MAX_CONCURRENT_TRANSCRIPTIONS pool.

THIS ROUTE CHARGES WITH THE DURATION UNKNOWN, and that is the one real
asymmetry against the other two. They ffprobe at submit time; here the
file does not exist until the download finishes inside the background
task. paywall.decide() handles it correctly by design - "Unknown duration
on a metered tool is billable - never fail open" - so the charge lands at
submit with input_seconds=None.

The consequence is that a job charged at submit can still fail its
duration check, or fail to download at all, AFTER the credit is taken.
Both paths therefore refund explicitly below. record_input_duration()
fills the real number in once validate_duration has it, so the cost
report is not left describing uploads only.
--------------------------------------------------------------------------
"""
import asyncio
import time

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

# Credits. settle_or_refund imported directly: this module runs its own
# background task and does not inherit _run_tool_job's `finally`.
from credits import paywall, metering
from credits.identity import Identity
from credits.limits import tiered_rate_limit
from credits.ledger import settle_or_refund

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
TOOL_KEY = "transcribe"   # shared credits rule key - see module docstring


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
        #
        # THE REFUND IS THE PART THAT MATTERS NOW. This route charges at
        # submit, before the download is attempted, so a YouTube failure
        # - a blocked video, an expired cookie, a dead CDN edge - lands
        # AFTER the credit is taken. Returning early without this line
        # would leave the hold sitting until the 90-minute sweeper found
        # it, on the single most common failure path this endpoint has.
        settle_or_refund(job_id, False, reason="youtube_download_failed")
        metering.record_job_finished(job_id, status="failed", error="download_failed")
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
            # Charged at submit with the duration unknown, rejected here
            # once it was known - the credit goes straight back. Handled
            # inside this except rather than left to the `finally` below
            # because this path returns early.
            settle_or_refund(job_id, False, reason="too_long_for_transcription")
            metering.record_job_finished(job_id, status="failed", error="duration_exceeded")
            return

        # The real input duration, finally available. Without this the
        # /youtube/transcribe rows would carry a null input_seconds and
        # the cost report's input_minutes would silently describe direct
        # uploads only - see credits/metering.py.
        metering.record_input_duration(job_id, duration)

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
        #
        # job_id is passed for METERING only: the dispatcher records the
        # worker's reported GPU seconds against this job's
        # gpu_job_metrics row. It matters most on THIS route, where the
        # row is opened at submit with a null input_seconds - without the
        # cost figure too, a youtube/transcribe row would carry almost no
        # usable numbers at all.
        result = await transcribe_job(file_path, language, task, mode, job_id=job_id)

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
        # MONEY FIRST, same ordering rule as _run_tool_job's finally.
        # Covers the success path, every exception above, and the
        # CancelledError a redeploy fires - which re-raises INTO this
        # block, so an in-flight paid job killed by a deploy gets its
        # credit back in that instant rather than 90 minutes later.
        settle_or_refund(job_id, succeeded, reason="youtube_transcribe_failed")
        metering.record_job_finished(
            job_id, status="completed" if succeeded else "failed"
        )

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
    dependencies=[Depends(tiered_rate_limit(
        TOOL_KEY,
        free_max=YOUTUBE_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS,
        free_window=YOUTUBE_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def youtube_transcribe_route(
    url: str = Form(...),
    language: str = Form(None),
    task: str = Form("transcribe"),
    mode: str = Form(None),
    identity: Identity = paywall.IdentityDep,
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

    # CHARGE, then enqueue. input_seconds=None on purpose - the file does
    # not exist yet, and paywall.decide() treats unknown duration on a
    # metered tool as billable rather than failing open. The runner
    # refunds if the download or the duration check then fails.
    try:
        async with paywall.guard(
            identity, job_id=job_id, tool=TOOL_KEY, input_seconds=None
        ) as charge:
            metering.record_job_created(
                job_id=job_id,
                tool=TOOL_KEY,
                subject_id=identity.subject_id,
                account_id=identity.account_id,
                ip_hash=identity.ip_hash,
                input_seconds=None,     # filled in by record_input_duration later
                charge_type=charge.charge_type,
            )

            spawn_background_task(
                _run_youtube_transcribe(job_id, url, language, task, mode)
            )
    except HTTPException:
        # 402. Nothing was downloaded and no file exists yet, so there is
        # nothing to clean up beyond marking the job.
        mark_failed(job_id, "Out of credits.")
        raise

    logger.info(
        f"[{TOOL}] job={job_id} queued for {url} "
        f"(language={language or 'auto'}, task={task}, mode={mode}, "
        f"charge={charge.charge_type})"
    )
    return JSONResponse({
        "job_id": job_id,
        "status": "processing",
        "options": {"language": language, "task": task, "mode": mode},
        "billing": {
            "charged": charge.charge_type,
            "credits": charge.credits,
            "balance": charge.balance_after,
            "free_remaining": charge.free_remaining_after,
        },
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