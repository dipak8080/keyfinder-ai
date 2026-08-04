"""
routes.py - HTTP wiring only. Every endpoint here does the same four
things and nothing else: validate the request, receive the upload, hand
the real work to a module that knows how to do it, and report status.
All business logic lives in youtube.py / audio_analysis.py / utils.py /
separation.py / audio_converter.py / audio_cutter.py / volume_booster.py /
pitch_changer.py / tempo_changer.py / reverse_audio.py / noise_remover.py /
voice_cleaner.py / echo_remover.py / silence_remover.py / speech_to_text.py
/ video_to_audio.py / audio_joiner.py / audio_loudnorm.py /
silence_splitter.py / youtube_chain.py / audio_effects.py.

Each async tool exposes POST (submit), GET .../status, GET .../preview
(inline playback) and GET .../download; /speech-to-text and
/youtube/analyze return inline JSON from .../result instead of a file.

--------------------------------------------------------------------------
WHAT CHANGED (2026-08-02) AND WHY

Five problems, each of which had produced real user-visible failures that
left no useful trace in the logs:

1. UPLOADS WERE BUFFERED WHOLE IN MEMORY, THEN SIZE-CHECKED.
   Twenty routes did `content = await file.read()`, checked len(), then
   wrote the buffer to disk - all three steps synchronous, on the event
   loop, on a box with NO SWAP (Incus container VPS; swapon is not
   permitted). The size check ran AFTER the whole body was resident, so
   the limit bounded nothing: an oversized upload was fully buffered
   before being rejected. All twenty now call save_upload() from
   upload.py, which streams in 1MB chunks, enforces the cap mid-stream,
   deletes the partial file on rejection, and returns 413 (not a generic
   400) so the frontend can tell "too big" from "wrong format".

2. TTL CLEANUP RAN ON THE REQUEST PATH.
   cleanup_expired_jobs() was called at the top of ~20 handlers. Expiring
   one stems job means deleting four full-length WAVs, so whoever
   happened to submit next paid for it with a stalled event loop - and an
   idle server never swept at all. It now runs on a 60s background timer
   in main.py; every call here is gone.

3. BACKGROUND JOBS COULD STICK ON "processing" FOREVER.
   Each _run_*_background() marked its job failed inside `except`, but an
   exception raised outside those handlers skipped all of them - most
   realistically acquire_slot_or_503() raising HTTPException inside a
   background task, where no HTTP layer exists to catch it. The job then
   never reached a terminal state: the frontend polled until its own
   client-side timeout and reported something vague, the input file
   waited for TTL, and NOTHING was logged as a failure. Every background
   task now calls jobs.fail_if_unfinished() from a `finally`.

4. THE SEPARATION QUEUE WAS UNBOUNDED.
   MAX_CONCURRENT_SEPARATIONS caps how many Demucs runs happen at once,
   but the semaphore is acquired inside the background task - so extra
   submissions were accepted and queued in memory without limit, each
   holding its upload on disk. Ten queued jobs on a one-slot machine is
   ~50 minutes of invisible waiting. Submissions now check
   jobs.count_processing() against MAX_QUEUED_SEPARATIONS and return a
   clean 503 that says so.

5. LOGS COULDN'T ANSWER "WHAT HAPPENED TO THIS REQUEST?"
   Job lines recorded queue and completion but not size, not duration,
   not the failure's shape. Every job now logs a start line (file, size)
   and an end line (COMPLETE/FAILED plus elapsed seconds), both carrying
   job=<id>. log_stream.py's middleware already tags every line emitted
   during a request - including from tasks it spawned - with a request
   id, so one failure can be traced from the HTTP row straight through to
   the ffmpeg error.

STRUCTURAL NOTE: fifteen near-identical _run_*_background() functions
collapsed into one _run_tool_job(). They differed only in which worker
function to call and which mark_*_complete() to use, and keeping fifteen
copies meant every fix to the error handling had to be applied fifteen
times - which is exactly how items 3 and 5 above came to be missing in
the first place.
--------------------------------------------------------------------------
"""
import os
import time
import uuid
import base64
import asyncio
from typing import Callable, List, Optional, Sequence
from functools import partial

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends, Query, Request
from fastapi.responses import JSONResponse, FileResponse

from config import (
    logger,
    UPLOAD_DIR,
    MAX_UPLOAD_BYTES,
    ANALYSIS_MAX_SECONDS,
    ADMIN_STATUS_KEY,
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
    MAX_CONCURRENT_SEPARATIONS,
    MAX_QUEUED_SEPARATIONS,
    MAX_CONCURRENT_AUDIO_TOOLS,
    AUDIO_CONVERT_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_CONVERT_RATE_LIMIT_WINDOW_SECONDS,
    AUDIO_CONVERSION_MATRIX,
    AUDIO_TRIM_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_TRIM_RATE_LIMIT_WINDOW_SECONDS,
    AUDIO_VOLUME_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_VOLUME_RATE_LIMIT_WINDOW_SECONDS,
    VOLUME_GAIN_MIN_DB,
    VOLUME_GAIN_MAX_DB,
    AUDIO_PITCH_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_PITCH_RATE_LIMIT_WINDOW_SECONDS,
    PITCH_SHIFT_MIN_SEMITONES,
    PITCH_SHIFT_MAX_SEMITONES,
    AUDIO_TEMPO_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_TEMPO_RATE_LIMIT_WINDOW_SECONDS,
    TEMPO_MIN_FACTOR,
    TEMPO_MAX_FACTOR,
    AUDIO_REVERSE_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_REVERSE_RATE_LIMIT_WINDOW_SECONDS,
    AUDIO_NOISE_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_NOISE_RATE_LIMIT_WINDOW_SECONDS,
    NOISE_REDUCTION_MIN_STRENGTH,
    NOISE_REDUCTION_MAX_STRENGTH,
    AUDIO_VOICE_CLEAN_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_VOICE_CLEAN_RATE_LIMIT_WINDOW_SECONDS,
    AUDIO_ECHO_REMOVE_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_ECHO_REMOVE_RATE_LIMIT_WINDOW_SECONDS,
    AUDIO_SILENCE_REMOVE_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_SILENCE_REMOVE_RATE_LIMIT_WINDOW_SECONDS,
    SILENCE_THRESHOLD_MIN_DB,
    SILENCE_THRESHOLD_MAX_DB,
    SILENCE_MIN_DURATION_SECONDS,
    SILENCE_MAX_DURATION_SECONDS,
    MAX_CONCURRENT_TRANSCRIPTIONS,
    TRANSCRIPTION_JOB_TTL_SECONDS,
    MAX_TRANSCRIPTION_DURATION_SECONDS,
    AUDIO_TRANSCRIBE_RATE_LIMIT_MAX_REQUESTS,
    AUDIO_TRANSCRIBE_RATE_LIMIT_WINDOW_SECONDS,
    ALLOWED_AUDIO_INPUT_FORMATS,
    MAX_VIDEO_UPLOAD_BYTES,
    VIDEO_TO_AUDIO_RATE_LIMIT_MAX_REQUESTS,
    VIDEO_TO_AUDIO_RATE_LIMIT_WINDOW_SECONDS,
    JOIN_MAX_FILES,
    JOIN_MAX_TOTAL_BYTES,
    JOIN_RATE_LIMIT_MAX_REQUESTS,
    JOIN_RATE_LIMIT_WINDOW_SECONDS,
    LOUDNORM_RATE_LIMIT_MAX_REQUESTS,
    LOUDNORM_RATE_LIMIT_WINDOW_SECONDS,
    SILENCE_SPLIT_RATE_LIMIT_MAX_REQUESTS,
    SILENCE_SPLIT_RATE_LIMIT_WINDOW_SECONDS,
    YOUTUBE_CHAIN_RATE_LIMIT_MAX_REQUESTS,
    YOUTUBE_CHAIN_RATE_LIMIT_WINDOW_SECONDS,
    YOUTUBE_ANALYZE_JOB_TTL_SECONDS,
    FADE_MAX_SECONDS,
    FADE_RATE_LIMIT_MAX_REQUESTS,
    FADE_RATE_LIMIT_WINDOW_SECONDS,
    CHANNELS_RATE_LIMIT_MAX_REQUESTS,
    CHANNELS_RATE_LIMIT_WINDOW_SECONDS,
    RESAMPLE_ALLOWED_RATES,
    RESAMPLE_ALLOWED_BIT_DEPTHS,
    RESAMPLE_RATE_LIMIT_MAX_REQUESTS,
    RESAMPLE_RATE_LIMIT_WINDOW_SECONDS,
    RINGTONE_MAX_DURATION_SECONDS,
    RINGTONE_RATE_LIMIT_MAX_REQUESTS,
    RINGTONE_RATE_LIMIT_WINDOW_SECONDS,
)
from upload import save_upload, save_uploads
from utils import (
    cleanup_file,
    release_memory_to_os,
    run_blocking,
    acquire_slot_or_503,
    get_camelot,
    _analysis_semaphore,
    _download_semaphore,
)
from youtube import (
    download_with_fallback,
    is_bot_check_error,
    is_geo_restricted_error,
    is_age_restricted_error,
    is_members_only_error,
    is_not_yet_live_error,
    is_permanent_error,
    is_valid_youtube_url,
    extract_video_id,
    VideoTooLongError,
    proxy_available,
    reset_proxy_circuit_breaker,
    get_cookie_accounts,
    ytdlp_alert_logger,
)
from audio_analysis import detect_key_bpm_essentia, cross_check_with_librosa, trim_audio_for_analysis
from rate_limit import check_rate_limit
from cache import get_cached_audio, put_cached_audio, get_cache_stats, clear_cache, set_cache_max_gb
from monitoring import record_result, get_status_snapshot
from download_progress import make_progress_hook
from jobs import (
    create_job,
    mark_complete,
    mark_stems_complete,
    mark_tool_complete,
    mark_transcription_complete,
    mark_data_complete,
    mark_failed,
    fail_if_unfinished,
    get_job,
    get_job_stats,
    count_processing,
    SEPARATION_JOB_TYPES,
)
from separation import run_separation, run_stem_separation, SeparationError
from audio_common import (
    validate_input_format,
    validate_conversion_pair,
    build_temp_input_path,
    build_output_path,
    AudioToolError,
    validate_duration,
    get_audio_mime_type,
)
from audio_converter import convert_audio
from audio_cutter import trim_audio
from volume_booster import apply_volume_gain
from pitch_changer import shift_pitch
from tempo_changer import change_tempo
from reverse_audio import reverse_audio
from noise_remover import remove_noise
from voice_cleaner import clean_voice
from echo_remover import remove_echo
from silence_remover import remove_silence
from speech_to_text import transcribe
from video_to_audio import extract_audio, validate_video_input_format
from audio_joiner import join_audio
from audio_loudnorm import normalize_loudness, resolve_target_lufs
from silence_splitter import split_on_silence
from youtube_chain import download_audio_to_file, ChainDownloadError
from audio_effects import apply_fade, convert_channels, resample_audio, make_ringtone
from admin_auth import guard_admin_request, verify_admin_key

router = APIRouter()

# One dedicated semaphore for separation, same pattern as
# _analysis_semaphore / _download_semaphore in utils.py - caps how many
# Demucs subprocesses run at once (default 1, the most RAM-hungry work
# this app does). Note this is acquired INSIDE the background task, not
# by the route: that is what makes the endpoint non-blocking, and also
# why a separate queue-depth check is needed at submit time - see
# _reject_if_separation_queue_full() below.
_separation_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SEPARATIONS)

# Shared by every ffmpeg/rubberband audio tool. Much lighter per job than
# Demucs, so a higher cap is fine.
_audio_tools_semaphore = asyncio.Semaphore(MAX_CONCURRENT_AUDIO_TOOLS)

# Whisper gets its own: inference is a sustained CPU+RAM operation that
# would otherwise starve fast, cheap tools like /volume of their slots
# while it runs.
_transcription_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TRANSCRIPTIONS)


# ============================================================
# SHARED HELPERS
#
# Everything below exists to be used by many routes. When a behaviour
# needs changing - a limit, an error shape, a log field - it should be
# changeable HERE, once, rather than in twenty near-identical copies.
# ============================================================

def _mb(num_bytes: int) -> str:
    """Consistent size rendering for logs. One place so a grep for 'MB'
    across the log stream always matches the same format."""
    return f"{num_bytes / (1024 * 1024):.1f}MB"


def _log_queued(tool: str, job_id: str, filename: str, size_bytes: int, detail: str = ""):
    """
    The START line for a job. Deliberately carries the input size: a
    failure minutes later is much easier to reason about when the log
    already says whether the input was 2MB or 79MB, and there is no other
    record of it once the temp file is cleaned up.
    """
    suffix = f" {detail}" if detail else ""
    logger.info(
        f"[{tool}] job={job_id} queued '{filename}' {_mb(size_bytes)}{suffix}"
    )


async def _run_tool_job(
    *,
    tool: str,
    metric: str,
    job_id: str,
    semaphore: asyncio.Semaphore,
    work: Callable,
    on_success: Callable,
    generic_error: str,
    cleanup_paths: Sequence[str] = (),
    success_detail: Optional[Callable] = None,
):
    """
    The single background runner shared by every job-based audio tool.

    This replaces fifteen hand-written _run_*_background() functions that
    were identical apart from which worker to call and which
    mark_*_complete() to use. The duplication was not harmless: error
    handling, cleanup, metrics and logging all had to be repeated
    verbatim in each copy, and any improvement to one of them silently
    failed to reach the other fourteen.

    Arguments:
      tool           - log prefix, e.g. "CONVERT"
      metric         - record_result() label, e.g. "/convert"
      job_id         - the job to update
      semaphore      - which concurrency pool this work belongs to
      work           - zero-arg callable returning an awaitable (normally
                       a run_blocking(...) call)
      on_success     - callable(result) that marks the job complete
      generic_error  - user-facing message for an unexpected failure;
                       deliberately vague, since the detail belongs in
                       the logs, not in a response to an anonymous caller
      cleanup_paths  - input files to delete once the work is done, win
                       or lose
      success_detail - optional callable(result) -> str, appended to the
                       COMPLETE log line (e.g. "4 stems", "182.3s total")

    The `finally` block runs in a fixed order that matters:
      fail_if_unfinished() FIRST, so a job is guaranteed terminal even if
      an exception escaped every except clause above (the acquire_slot_
      or_503-inside-a-background-task case, which no `except AudioTool
      Error` or `except Exception` here would catch if it were raised
      before the try). Then file cleanup, then memory release, then the
      metric - each independent of the others.
    """
    started = time.monotonic()
    succeeded = False

    async with semaphore:
        waited = time.monotonic() - started
        if waited > 1.0:
            # Only logged when it actually happened. A long wait here is
            # the difference between "the tool is slow" and "the tool was
            # queued behind someone else's job", which is otherwise
            # invisible and looks identical to the user.
            logger.info(f"[{tool}] job={job_id} waited {waited:.1f}s for a free slot")

        run_started = time.monotonic()
        try:
            result = await work()
            on_success(result)
            succeeded = True
            detail = ""
            if success_detail is not None:
                try:
                    detail = f" ({success_detail(result)})"
                except Exception:
                    # A broken log-detail callable must never turn a
                    # successful job into a failed one.
                    detail = ""
            logger.info(
                f"[{tool}] job={job_id} COMPLETE in {time.monotonic() - run_started:.1f}s{detail}"
            )

        except AudioToolError as e:
            # Expected, user-actionable failure - the message is written
            # for the person who uploaded the file, so it passes through
            # to them unchanged.
            mark_failed(job_id, str(e))
            logger.warning(
                f"[{tool}] job={job_id} FAILED in {time.monotonic() - run_started:.1f}s: {e}"
            )

        except SeparationError as e:
            mark_failed(job_id, str(e))
            logger.warning(
                f"[{tool}] job={job_id} FAILED in {time.monotonic() - run_started:.1f}s: {e}"
            )

        except asyncio.CancelledError:
            # Shutdown. Mark it so a client polling across a redeploy
            # gets a real answer instead of an eternal "processing", then
            # re-raise so the task actually stops.
            mark_failed(job_id, "The server restarted while this job was running.")
            logger.warning(f"[{tool}] job={job_id} CANCELLED (shutdown)")
            raise

        except Exception as e:
            mark_failed(job_id, generic_error)
            logger.error(
                f"[{tool}] job={job_id} FAILED in {time.monotonic() - run_started:.1f}s "
                f"(unexpected): {e}",
                exc_info=True,
            )

        finally:
            fail_if_unfinished(job_id, generic_error)
            for path in cleanup_paths:
                cleanup_file(path)
            release_memory_to_os()
            record_result(metric, succeeded)


async def _accept_upload(
    file: UploadFile,
    job_id: str,
    label: str,
    max_bytes: int = MAX_UPLOAD_BYTES,
) -> tuple:
    """
    Streams one upload to disk for a job that already exists, returning
    (input_path, size_bytes).

    The job is created BEFORE the upload rather than after, so that a
    rejected or failed transfer can be recorded against a real job id
    instead of vanishing. That is why the HTTPException is caught and
    re-raised here: without mark_failed(), a client that had already been
    handed a job id (or that retries) would find nothing explaining what
    happened.
    """
    input_path = build_temp_input_path(job_id, file.filename)
    try:
        size = await save_upload(file, input_path, max_bytes, label=label)
    except HTTPException as e:
        mark_failed(job_id, e.detail if isinstance(e.detail, str) else "Upload rejected.")
        raise
    return input_path, size


async def _validate_duration_or_reject(
    job_id: str,
    input_path: str,
    max_seconds: Optional[int] = None,
) -> float:
    """
    Runs the ffprobe duration check and turns a failure into a synchronous
    400, cleaning up as it goes.

    Two things worth noting:

    - It is dispatched through run_blocking(). validate_duration() spawns
      ffprobe, and calling it directly from an async handler (as this
      file previously did in eleven places) blocks the event loop for the
      whole probe. On a single worker that stalls every other connection,
      including the status polls the frontend depends on.

    - It runs at SUBMIT time, not inside the background task, so an
      out-of-range file gets an immediate 400 the frontend can show
      against the upload form - rather than a job that is accepted, then
      fails a second later for a reason the user could have been told
      instantly.
    """
    try:
        if max_seconds is None:
            return await run_blocking(validate_duration, input_path)
        return await run_blocking(validate_duration, input_path, max_seconds)
    except AudioToolError as e:
        cleanup_file(input_path)
        mark_failed(job_id, str(e))
        raise HTTPException(400, str(e))


def _reject_if_separation_queue_full():
    """
    The bounded queue for every Demucs-backed route.

    MAX_CONCURRENT_SEPARATIONS bounds how many separations RUN at once,
    but the semaphore enforcing it is acquired inside the background
    task - so before this check existed, submissions were never refused,
    they simply queued in memory with no ceiling. Each waiting job held
    an uploaded file on disk and a job-table entry, and the person
    watching the spinner had no way to know they were twelfth in line
    behind ~50 minutes of work.

    Rejecting at submit time is strictly kinder: the file is never
    uploaded, the disk is never touched, and the caller gets a specific
    reason with a suggestion instead of an open-ended wait that looks
    exactly like the site being broken.

    503 rather than 429 is deliberate - this is not the caller's rate
    being too high, it is the server being at capacity, and the two mean
    different things to a client deciding whether to retry.
    """
    depth = count_processing(SEPARATION_JOB_TYPES)
    if depth >= MAX_QUEUED_SEPARATIONS:
        logger.warning(
            f"[SEPARATION] Rejected submission - queue full "
            f"({depth}/{MAX_QUEUED_SEPARATIONS} jobs in flight)"
        )
        raise HTTPException(
            503,
            "The separation queue is full right now - each job takes several "
            "minutes and only one runs at a time. Please try again in a few minutes.",
        )


def _resolve_tool_output_path(job_id: str, expected_type: str) -> tuple:
    """
    Shared lookup behind every audio tool's preview and download route.
    Returns (path, output_format).

    Checking job_type is what stops a job id from one tool being used to
    read another tool's output - the id alone is not a capability, the
    pairing of id and tool is.
    """
    job = get_job(job_id)
    if job is None or job["job_type"] != expected_type:
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] == "failed":
        raise HTTPException(409, job.get("error") or "This job failed.")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["output_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Output file not found (it may have expired).")
    return path, (job.get("output_format") or "bin")


def _tool_status(job_id: str, expected_type: str) -> dict:
    """
    Shared status response for every single-output tool.

    job_type is validated here too, so polling with an id that belongs to
    a different tool returns 404 rather than a confusing "complete" for
    something the caller never submitted.
    """
    job = get_job(job_id)
    if job is None or job["job_type"] != expected_type:
        raise HTTPException(404, "Job not found (it may have expired).")
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
    }


# ============================================================
# /download - YouTube URL to MP3/WAV (synchronous, cached)
#
# The only tool that takes no upload, which is why it was the ONLY tool
# still working during the incident that prompted this rewrite: nothing
# to buffer, nothing to stall the loop with, and its result is cached.
# ============================================================

@router.post("/download", dependencies=[Depends(check_rate_limit)])
async def download_audio(url: str = Form(...), format: str = Form("mp3")):
    if format not in ["mp3", "wav"]:
        raise HTTPException(400, "Format must be 'mp3' or 'wav'")

    if not is_valid_youtube_url(url):
        logger.warning(f"[DOWNLOAD] Rejected - not a recognizable YouTube URL: {url}")
        raise HTTPException(400, "Please provide a valid YouTube video URL.")

    started = time.monotonic()
    video_id = extract_video_id(url)

    if video_id:
        try:
            cached_audio, cached_title = await run_blocking(get_cached_audio, video_id, format)
        except Exception as cache_err:
            logger.warning(f"[CACHE] Lookup failed (non-fatal, downloading fresh): {cache_err}")
            cached_audio, cached_title = None, None

        if cached_audio:
            cached_b64 = base64.b64encode(cached_audio).decode('utf-8')
            logger.info(
                f"[CACHE] HIT '{cached_title}' ({format}) {_mb(len(cached_audio))} "
                f"in {time.monotonic() - started:.2f}s"
            )
            record_result("/download", True)
            return JSONResponse({"title": cached_title or "Unknown", "audio": cached_b64, "format": format})

    temp_id = str(uuid.uuid4())
    temp_path = os.path.join(UPLOAD_DIR, f"{temp_id}.%(ext)s")
    output_file = os.path.join(UPLOAD_DIR, f"{temp_id}.{format}")

    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': temp_path,
        'quiet': False,
        'verbose': True,
        'noplaylist': True,
        'ffmpeg_location': '/usr/bin/ffmpeg',
        # Equivalent of yt-dlp's --force-ipv4 CLI flag. VPSDime assigned
        # this VPS a real, working global IPv6 address on 2026-08-03 (see
        # ticket - `curl -6` on the HOST now succeeds), but that
        # connectivity does NOT reach this Docker container: Docker's
        # default bridge networking does not forward IPv6 into containers
        # unless explicitly configured (fixed-cidr-v6 in
        # /etc/docker/daemon.json). Confirmed via
        # `docker exec audioforges-api curl -6 https://ipv6.google.com`
        # failing with "Network is unreachable" on every resolved address,
        # the same failure the HOST used to show before VPSDime's fix.
        # Until Docker's IPv6 networking is separately configured (a
        # bigger infra change, tracked separately - not done here),
        # yt-dlp inside this container still has no usable IPv6 path, so
        # googlevideo.com edges that are IPv6-only remain unreachable.
        # Pinning source_address to 0.0.0.0 keeps every connection this
        # YoutubeDL instance opens on IPv4, avoiding that dead path.
        'source_address': '0.0.0.0',
        'extractor_args': {
            'youtubepot-bgutilscript': {
                'script_path': ['/root/bgutil-ytdlp-pot-provider/server/build/generate_once.js']
            }
        },
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': format,
            'preferredquality': '192',
        }],
        'remote_components': {'ejs:github'},
        'logger': ytdlp_alert_logger,
        'progress_hooks': [make_progress_hook(video_id or url)],
    }

    proxy_url = os.environ.get('YT_PROXY_URL')
    available_accounts = get_cookie_accounts()
    logger.info(
        f"[COOKIES] accounts_available={len(available_accounts)} "
        f"[PROXY] configured={bool(proxy_url)} circuit_breaker={'OPEN' if not proxy_available() else 'CLOSED'} "
        f"url={url}"
    )

    await acquire_slot_or_503(_download_semaphore, "download")

    audio_data = None
    succeeded = False
    try:
        try:
            info = await run_blocking(download_with_fallback, ydl_opts, url, proxy_url)
            title = info.get('title', 'Unknown')
        except VideoTooLongError as e:
            logger.warning(f"[DOWNLOAD] Rejected - video too long: {e}")
            raise HTTPException(400, str(e))
        except Exception as e:
            error_text = str(e)

            # Each branch below maps a yt-dlp failure onto the status code
            # that actually describes it. This matters more than it looks:
            # a 404 tells the frontend "this video is gone, don't retry",
            # while a 503 means "try again shortly" - collapsing them all
            # into 500 (as the generic fallback does) is what turns a
            # clear problem into an unexplained one.
            if is_permanent_error(error_text):
                logger.warning(f"[DOWNLOAD] Permanent error for {url}: {error_text}")
                raise HTTPException(
                    404,
                    "This video is unavailable - it may have been deleted, made private, "
                    "or removed for copyright reasons. Please try a different video."
                )

            if is_geo_restricted_error(error_text):
                logger.warning(f"[DOWNLOAD] Geo-restricted: {url}")
                raise HTTPException(
                    451,
                    "This video is restricted by the uploader to specific countries and "
                    "isn't available from our server's location. This isn't something we "
                    "can fix on our end for this particular video - try a different one."
                )

            if is_age_restricted_error(error_text):
                logger.warning(f"[DOWNLOAD] Age-restricted: {url}")
                raise HTTPException(
                    403,
                    "This video is age-restricted by YouTube and requires a verified "
                    "account to view. We're not able to download age-restricted content "
                    "at this time - try a different video."
                )

            if is_members_only_error(error_text):
                logger.warning(f"[DOWNLOAD] Members-only: {url}")
                raise HTTPException(
                    403,
                    "This video is exclusive to that channel's paid members and isn't "
                    "publicly downloadable - try a different video."
                )

            if is_not_yet_live_error(error_text):
                logger.warning(f"[DOWNLOAD] Not yet live: {url}")
                raise HTTPException(
                    409,
                    "This video is a scheduled premiere or live stream that hasn't "
                    "started yet - try again once it's live, or try a different video."
                )

            if is_bot_check_error(error_text):
                logger.error(f"[DOWNLOAD] Bot verification / format restriction: {url}")
                raise HTTPException(
                    503,
                    "This video is temporarily unavailable for download because YouTube is "
                    "requiring bot verification or is restricting available formats for this client. "
                    "Please try again in a few minutes."
                )

            logger.error(f"[DOWNLOAD] Failed after all attempts: {error_text}")
            raise HTTPException(500, f"Failed: {error_text}")

        if not os.path.exists(output_file):
            logger.error(f"[DOWNLOAD] Expected output missing after download: {output_file}")
            raise HTTPException(500, "Failed: audio file was not produced by the downloader")

        audio_bytes = await run_blocking(_read_file_bytes, output_file)
        audio_data = base64.b64encode(audio_bytes).decode('utf-8')

        if video_id:
            try:
                await run_blocking(put_cached_audio, video_id, format, audio_bytes, title)
            except Exception as cache_err:
                logger.warning(f"[CACHE] Save failed (non-fatal): {cache_err}")

        raw_size = len(audio_bytes)
        del audio_bytes

        logger.info(
            f"[DOWNLOAD] COMPLETE '{title}' ({format}) {_mb(raw_size)} "
            f"in {time.monotonic() - started:.1f}s"
        )

        succeeded = True
        return JSONResponse({"title": title, "audio": audio_data, "format": format})

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[DOWNLOAD] Unexpected error: {e}", exc_info=True)
        raise HTTPException(500, f"Failed: {str(e)}")
    finally:
        cleanup_file(output_file)
        if audio_data is not None:
            del audio_data
        release_memory_to_os()
        _download_semaphore.release()
        record_result("/download", succeeded)


def _read_file_bytes(path: str) -> bytes:
    """Blocking read, dispatched via run_blocking from /download.

    Reading a finished download can mean pulling tens of megabytes off
    disk; doing that inline in the async handler blocks every other
    connection for its duration, which on a single worker is the whole
    server."""
    with open(path, "rb") as f:
        return f.read()


# ============================================================
# /analyze - Key + BPM detection (synchronous)
#
# Synchronous rather than job-based because analysis only ever looks at
# the first ANALYSIS_MAX_SECONDS of audio, so it finishes inside a normal
# request window even for a long track.
# ============================================================

@router.post("/analyze", dependencies=[Depends(check_rate_limit)])
async def analyze_audio(file: UploadFile = File(...)):
    started = time.monotonic()

    file_id = str(uuid.uuid4())
    file_path = os.path.join(UPLOAD_DIR, f"{file_id}_{file.filename}")
    analysis_path = file_path

    # The upload is streamed to disk BEFORE the semaphore is taken. The
    # transfer is I/O-bound and holds no CPU, so making it wait for an
    # analysis slot would occupy a slot doing nothing while the bytes
    # arrive - and on a slow connection that is most of the request.
    size = await save_upload(file, file_path, MAX_UPLOAD_BYTES, label="analyze")

    await acquire_slot_or_503(_analysis_semaphore, "analysis")

    succeeded = False
    try:
        logger.info(f"[ANALYZE] Started '{file.filename}' {_mb(size)}")

        if ANALYSIS_MAX_SECONDS is not None:
            analysis_path = await run_blocking(trim_audio_for_analysis, file_path, ANALYSIS_MAX_SECONDS)

        audio_array = None
        try:
            key, scale, key_conf, bpm, bpm_conf, audio_array, essentia_sr = await run_blocking(
                detect_key_bpm_essentia, analysis_path
            )

            key, scale, key_conf, bpm, bpm_conf, agreement = await run_blocking(
                cross_check_with_librosa, audio_array, essentia_sr, key, scale, key_conf, bpm, bpm_conf
            )
        finally:
            # Freed here rather than at the end of the request: the array
            # is the largest thing in memory during analysis, and holding
            # it while the response is serialized doubles peak usage for
            # no reason on a box with no swap.
            if audio_array is not None:
                del audio_array
            release_memory_to_os()

        camelot = get_camelot(key, scale)

        result = {
            "key": f"{key} {scale}",
            "camelot": camelot,
            "bpm": bpm,
            "confidence": int(min(0.99, key_conf) * 100),
            "bpm_confidence": min(99, bpm_conf),
            "cross_check": agreement,
        }

        logger.info(
            f"[ANALYZE] COMPLETE '{file.filename}' in {time.monotonic() - started:.1f}s: "
            f"{result['key']} / {result['camelot']} / {result['bpm']} BPM"
        )

        succeeded = True
        return JSONResponse(result)

    except HTTPException:
        raise
    except AudioToolError as e:
        logger.warning(f"[ANALYZE] FAILED '{file.filename}': {e}")
        raise HTTPException(400, str(e))
    except Exception as e:
        logger.error(f"[ANALYZE] FAILED '{file.filename}' (unexpected): {e}", exc_info=True)
        raise HTTPException(500, "Could not analyze this file. It may be corrupt or in an unsupported format.")
    finally:
        cleanup_file(file_path)
        if analysis_path != file_path:
            cleanup_file(analysis_path)
        release_memory_to_os()
        _analysis_semaphore.release()
        record_result("/analyze", succeeded)


# ============================================================
# /separate and /stems - Demucs (async job flow)
#
# Four routes (/separate, /separate-hq, /stems, /stems-hq) sharing one
# model, one semaphore and one queue. The vocal remover is NOT cheaper
# than the stem splitter: Demucs separates all four sources internally
# either way, and --two-stems just sums three of them for us.
# ============================================================

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
) -> JSONResponse:
    """
    Shared submit path for all four separation routes. They differ only
    in run knobs, rate limit and output shape, so the accept-and-queue
    sequence lives here once.

    Knobs are resolved by the CALLER at submission time and passed in, so
    a config change (or the HQ kill switch flipping) can never alter a
    job that is already queued - it runs with the settings it was
    accepted under.
    """
    _reject_if_separation_queue_full()

    original_filename = file.filename

    job_id = create_job(job_type=job_type)
    file_path, size = await _accept_upload(file, job_id, label=tool.lower())

    is_stems = job_type in ("stems",)

    if is_stems:
        work = lambda: run_blocking(
            run_stem_separation, file_path, job_id,
            model, overlap, timeout_seconds, max_duration_seconds,
        )
        on_success = lambda stems: mark_stems_complete(job_id, original_filename, stems)
        success_detail = lambda stems: f"{len(stems)} stems"
        generic_error = "Stem separation failed unexpectedly."
    else:
        work = lambda: run_blocking(
            run_separation, file_path, job_id,
            model, overlap, timeout_seconds, max_duration_seconds,
        )
        on_success = lambda paths: mark_complete(job_id, original_filename, paths[0], paths[1])
        success_detail = None
        generic_error = "Separation failed unexpectedly."

    asyncio.create_task(_run_tool_job(
        tool=tool,
        metric=metric_label,
        job_id=job_id,
        semaphore=_separation_semaphore,
        work=work,
        on_success=on_success,
        generic_error=generic_error,
        cleanup_paths=[file_path],
        success_detail=success_detail,
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
    vocal/instrumental separation in the background - it takes 1-5+
    minutes on CPU, far beyond a normal request window. Poll
    GET /separate/status/{job_id}.
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
    overlap. Roughly 5x the CPU time of /separate, so it gets a longer
    timeout, a TIGHTER input duration cap, and a stricter rate limit.

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
    )


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
    semaphore and same CPU cost as /separate - the only difference is
    that the four internally-separated sources are kept as individual
    files instead of three being summed into one instrumental.
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
    )


@router.get("/stems/status/{job_id}")
async def stems_status(job_id: str):
    """Returns the usual status fields plus the stem names actually
    available, so the frontend renders download buttons from the response
    instead of hardcoding names that would break if a different model
    were ever configured."""
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
    }


def _resolve_stems_file(job_id: str, stem: str) -> str:
    """Validates the requested stem against the job's OWN stem dict rather
    than a hardcoded tuple, so the valid set always follows whatever model
    produced the job."""
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


# ============================================================
# AUDIO TOOLS - the ffmpeg/rubberband family
#
# Ten routes (convert, trim, volume, pitch, tempo, reverse, noise-remove,
# voice-clean, echo-remove, silence-remove) plus fade, channels, resample
# and ringtone further down, all sharing one submit path, one background
# runner and one semaphore. Each POST differs only in what it validates
# and which worker it calls.
# ============================================================

def _validated_input_format(filename: str) -> str:
    """
    validate_input_format() raises AudioToolError, which is not an
    HTTPException - so calling it bare (as every route here previously
    did) turned "you uploaded a .txt" into a 500 Internal Server Error.
    A wrong file extension is the caller's mistake, not the server's, and
    400 is what lets the frontend say something useful about it.
    """
    try:
        return validate_input_format(filename)
    except AudioToolError as e:
        raise HTTPException(400, str(e))


async def _submit_audio_tool(
    file: UploadFile,
    *,
    job_type: str,
    tool: str,
    metric: str,
    build_work: Callable,
    output_format: Optional[str] = None,
    check_duration: bool = True,
    max_duration_seconds: Optional[int] = None,
    log_detail: str = "",
    generic_error: str = "Processing failed unexpectedly.",
    semaphore: Optional[asyncio.Semaphore] = None,
) -> JSONResponse:
    """
    Shared submit path for every single-input, single-output audio tool.

    Order of operations is deliberate:
      1. Validate the FILENAME's format first - free, and rejects an
         obviously wrong file before a byte is transferred.
      2. Create the job, so an upload that fails partway has somewhere to
         record why.
      3. Stream the upload to disk with the size cap enforced per chunk.
      4. Probe duration (off the event loop) and reject synchronously if
         it's too long - the caller learns immediately rather than being
         handed a job id that fails a second later.
      5. Only then queue the background work.

    build_work(input_path, output_path) returns a zero-arg callable that
    the runner awaits. Passing a builder rather than the paths themselves
    keeps every tool's actual worker call visible at its own route, which
    is the part worth reading.
    """
    source_format = _validated_input_format(file.filename)
    out_fmt = output_format or source_format

    # Captured NOW, not read inside the background lambda below. The
    # UploadFile is closed once the response is sent, and while .filename
    # happens to be a plain str that survives that, depending on it would
    # be relying on an implementation detail of Starlette.
    original_filename = file.filename

    job_id = create_job(job_type=job_type)
    input_path, size = await _accept_upload(file, job_id, label=job_type)
    output_path = build_output_path(job_id, out_fmt)

    if check_duration:
        await _validate_duration_or_reject(job_id, input_path, max_duration_seconds)

    asyncio.create_task(_run_tool_job(
        tool=tool,
        metric=metric,
        job_id=job_id,
        semaphore=semaphore or _audio_tools_semaphore,
        work=build_work(input_path, output_path),
        on_success=lambda _: mark_tool_complete(job_id, original_filename, output_path, out_fmt),
        generic_error=generic_error,
        cleanup_paths=[input_path],
    ))

    _log_queued(tool, job_id, original_filename, size, log_detail)
    return JSONResponse({"job_id": job_id, "status": "processing"})


# ---------- /convert ----------

@router.post(
    "/convert",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_CONVERT_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_CONVERT_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def convert_audio_route(file: UploadFile = File(...), target_format: str = Form(...)):
    """Format conversion. Poll GET /convert/status/{job_id}."""
    target_format = target_format.strip().lower()
    source_format = _validated_input_format(file.filename)
    try:
        validate_conversion_pair(source_format, target_format, AUDIO_CONVERSION_MATRIX)
    except AudioToolError as e:
        raise HTTPException(400, str(e))

    return await _submit_audio_tool(
        file,
        job_type="convert",
        tool="CONVERT",
        metric="/convert",
        output_format=target_format,
        # Conversion cost barely scales with length (it's a re-encode, not
        # an analysis), so it's the one tool exempt from the duration cap
        # - the size cap alone is enough.
        check_duration=False,
        build_work=lambda inp, out: (
            lambda: run_blocking(convert_audio, inp, out, source_format, target_format)
        ),
        log_detail=f"{source_format} -> {target_format}",
        generic_error="Conversion failed unexpectedly.",
    )


@router.get("/convert/status/{job_id}")
async def convert_status(job_id: str):
    return _tool_status(job_id, "convert")


@router.get("/convert/preview/{job_id}")
async def convert_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "convert")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/convert/download/{job_id}")
async def convert_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "convert")
    return FileResponse(path, media_type="application/octet-stream", filename=f"converted.{fmt}")


# ---------- /trim ----------

@router.post(
    "/trim",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_TRIM_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_TRIM_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def trim_audio_route(
    file: UploadFile = File(...),
    start_seconds: float = Form(...),
    end_seconds: float = Form(...),
):
    """Cut to a start-end range. Poll GET /trim/status/{job_id}."""
    if start_seconds < 0 or end_seconds <= start_seconds:
        raise HTTPException(
            400,
            "Invalid range: end_seconds must be greater than start_seconds, "
            "and both must be non-negative."
        )

    source_format = _validated_input_format(file.filename)
    original_filename = file.filename

    job_id = create_job(job_type="trim")
    input_path, size = await _accept_upload(file, job_id, label="trim")
    output_path = build_output_path(job_id, source_format)

    # Trim needs the real duration for two separate reasons, so it can't
    # use the shared helper's fire-and-forget check: the cap has to be
    # enforced AND the value is passed to trim_audio() itself, and
    # end_seconds has to be range-checked against it.
    duration = await _validate_duration_or_reject(job_id, input_path)

    if end_seconds > duration:
        cleanup_file(input_path)
        mark_failed(job_id, "Requested range is past the end of the audio.")
        raise HTTPException(
            400,
            f"end_seconds ({end_seconds}s) exceeds the audio's actual duration ({duration:.1f}s)."
        )

    asyncio.create_task(_run_tool_job(
        tool="TRIM",
        metric="/trim",
        job_id=job_id,
        semaphore=_audio_tools_semaphore,
        work=lambda: run_blocking(trim_audio, input_path, output_path, start_seconds, end_seconds, duration),
        on_success=lambda _: mark_tool_complete(job_id, original_filename, output_path, source_format),
        generic_error="Trim failed unexpectedly.",
        cleanup_paths=[input_path],
    ))

    _log_queued("TRIM", job_id, original_filename, size, f"[{start_seconds}s -> {end_seconds}s of {duration:.1f}s]")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/trim/status/{job_id}")
async def trim_status(job_id: str):
    return _tool_status(job_id, "trim")


@router.get("/trim/preview/{job_id}")
async def trim_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "trim")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/trim/download/{job_id}")
async def trim_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "trim")
    return FileResponse(path, media_type="application/octet-stream", filename=f"trimmed.{fmt}")


# ---------- /volume ----------

@router.post(
    "/volume",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_VOLUME_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_VOLUME_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def volume_route(file: UploadFile = File(...), gain_db: float = Form(...)):
    """Gain boost or reduction. Poll GET /volume/status/{job_id}."""
    if gain_db < VOLUME_GAIN_MIN_DB or gain_db > VOLUME_GAIN_MAX_DB:
        raise HTTPException(400, f"gain_db must be between {VOLUME_GAIN_MIN_DB} and {VOLUME_GAIN_MAX_DB}.")

    return await _submit_audio_tool(
        file,
        job_type="volume",
        tool="VOLUME",
        metric="/volume",
        build_work=lambda inp, out: (lambda: run_blocking(apply_volume_gain, inp, out, gain_db)),
        log_detail=f"{gain_db:+.1f}dB",
        generic_error="Volume adjustment failed unexpectedly.",
    )


@router.get("/volume/status/{job_id}")
async def volume_status(job_id: str):
    return _tool_status(job_id, "volume")


@router.get("/volume/preview/{job_id}")
async def volume_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "volume")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/volume/download/{job_id}")
async def volume_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "volume")
    return FileResponse(path, media_type="application/octet-stream", filename=f"volume_adjusted.{fmt}")


# ---------- /pitch ----------

@router.post(
    "/pitch",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_PITCH_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_PITCH_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def pitch_route(file: UploadFile = File(...), semitones: float = Form(...)):
    """Pitch shift, independent of tempo (rubberband)."""
    if semitones < PITCH_SHIFT_MIN_SEMITONES or semitones > PITCH_SHIFT_MAX_SEMITONES:
        raise HTTPException(
            400,
            f"semitones must be between {PITCH_SHIFT_MIN_SEMITONES} and {PITCH_SHIFT_MAX_SEMITONES}."
        )

    return await _submit_audio_tool(
        file,
        job_type="pitch",
        tool="PITCH",
        metric="/pitch",
        build_work=lambda inp, out: (lambda: run_blocking(shift_pitch, inp, out, semitones)),
        log_detail=f"{semitones:+.1f} semitones",
        generic_error="Pitch shift failed unexpectedly.",
    )


@router.get("/pitch/status/{job_id}")
async def pitch_status(job_id: str):
    return _tool_status(job_id, "pitch")


@router.get("/pitch/preview/{job_id}")
async def pitch_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "pitch")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/pitch/download/{job_id}")
async def pitch_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "pitch")
    return FileResponse(path, media_type="application/octet-stream", filename=f"pitch_shifted.{fmt}")


# ---------- /tempo ----------

@router.post(
    "/tempo",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_TEMPO_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_TEMPO_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def tempo_route(file: UploadFile = File(...), tempo_factor: float = Form(...)):
    """Tempo/speed change, independent of pitch (rubberband)."""
    if tempo_factor < TEMPO_MIN_FACTOR or tempo_factor > TEMPO_MAX_FACTOR:
        raise HTTPException(400, f"tempo_factor must be between {TEMPO_MIN_FACTOR} and {TEMPO_MAX_FACTOR}.")

    return await _submit_audio_tool(
        file,
        job_type="tempo",
        tool="TEMPO",
        metric="/tempo",
        build_work=lambda inp, out: (lambda: run_blocking(change_tempo, inp, out, tempo_factor)),
        log_detail=f"x{tempo_factor:.2f}",
        generic_error="Tempo change failed unexpectedly.",
    )


@router.get("/tempo/status/{job_id}")
async def tempo_status(job_id: str):
    return _tool_status(job_id, "tempo")


@router.get("/tempo/preview/{job_id}")
async def tempo_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "tempo")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/tempo/download/{job_id}")
async def tempo_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "tempo")
    return FileResponse(path, media_type="application/octet-stream", filename=f"tempo_changed.{fmt}")


# ---------- /reverse ----------

@router.post(
    "/reverse",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_REVERSE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_REVERSE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def reverse_route(file: UploadFile = File(...)):
    """Reverse playback."""
    return await _submit_audio_tool(
        file,
        job_type="reverse",
        tool="REVERSE",
        metric="/reverse",
        build_work=lambda inp, out: (lambda: run_blocking(reverse_audio, inp, out)),
        generic_error="Reverse failed unexpectedly.",
    )


@router.get("/reverse/status/{job_id}")
async def reverse_status(job_id: str):
    return _tool_status(job_id, "reverse")


@router.get("/reverse/preview/{job_id}")
async def reverse_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "reverse")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/reverse/download/{job_id}")
async def reverse_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "reverse")
    return FileResponse(path, media_type="application/octet-stream", filename=f"reversed.{fmt}")


# ---------- /noise-remove ----------

@router.post(
    "/noise-remove",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_NOISE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_NOISE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def noise_remove_route(file: UploadFile = File(...), strength: float = Form(12.0)):
    """Background noise reduction (ffmpeg afftdn)."""
    if strength < NOISE_REDUCTION_MIN_STRENGTH or strength > NOISE_REDUCTION_MAX_STRENGTH:
        raise HTTPException(
            400,
            f"strength must be between {NOISE_REDUCTION_MIN_STRENGTH} and {NOISE_REDUCTION_MAX_STRENGTH}."
        )

    return await _submit_audio_tool(
        file,
        job_type="noise_remove",
        tool="NOISE",
        metric="/noise-remove",
        build_work=lambda inp, out: (lambda: run_blocking(remove_noise, inp, out, strength)),
        log_detail=f"strength={strength}",
        generic_error="Noise removal failed unexpectedly.",
    )


@router.get("/noise-remove/status/{job_id}")
async def noise_remove_status(job_id: str):
    return _tool_status(job_id, "noise_remove")


@router.get("/noise-remove/preview/{job_id}")
async def noise_remove_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "noise_remove")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/noise-remove/download/{job_id}")
async def noise_remove_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "noise_remove")
    return FileResponse(path, media_type="application/octet-stream", filename=f"denoised.{fmt}")


# ---------- /voice-clean ----------

@router.post(
    "/voice-clean",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_VOICE_CLEAN_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_VOICE_CLEAN_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def voice_clean_route(file: UploadFile = File(...)):
    """Speech-optimized cleanup preset."""
    return await _submit_audio_tool(
        file,
        job_type="voice_clean",
        tool="VOICE_CLEAN",
        metric="/voice-clean",
        build_work=lambda inp, out: (lambda: run_blocking(clean_voice, inp, out)),
        generic_error="Voice cleanup failed unexpectedly.",
    )


@router.get("/voice-clean/status/{job_id}")
async def voice_clean_status(job_id: str):
    return _tool_status(job_id, "voice_clean")


@router.get("/voice-clean/preview/{job_id}")
async def voice_clean_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "voice_clean")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/voice-clean/download/{job_id}")
async def voice_clean_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "voice_clean")
    return FileResponse(path, media_type="application/octet-stream", filename=f"voice_cleaned.{fmt}")


# ---------- /echo-remove ----------

@router.post(
    "/echo-remove",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_ECHO_REMOVE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_ECHO_REMOVE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def echo_remove_route(file: UploadFile = File(...)):
    """Echo / reverb tail suppression."""
    return await _submit_audio_tool(
        file,
        job_type="echo_remove",
        tool="ECHO_REMOVE",
        metric="/echo-remove",
        build_work=lambda inp, out: (lambda: run_blocking(remove_echo, inp, out)),
        generic_error="Echo removal failed unexpectedly.",
    )


@router.get("/echo-remove/status/{job_id}")
async def echo_remove_status(job_id: str):
    return _tool_status(job_id, "echo_remove")


@router.get("/echo-remove/preview/{job_id}")
async def echo_remove_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "echo_remove")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/echo-remove/download/{job_id}")
async def echo_remove_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "echo_remove")
    return FileResponse(path, media_type="application/octet-stream", filename=f"echo_removed.{fmt}")


# ---------- /silence-remove ----------

@router.post(
    "/silence-remove",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=AUDIO_SILENCE_REMOVE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=AUDIO_SILENCE_REMOVE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def silence_remove_route(
    file: UploadFile = File(...),
    threshold_db: float = Form(-30.0),
    min_duration_seconds: float = Form(0.5),
):
    """Strips silent gaps throughout the recording."""
    if threshold_db < SILENCE_THRESHOLD_MIN_DB or threshold_db > SILENCE_THRESHOLD_MAX_DB:
        raise HTTPException(
            400,
            f"threshold_db must be between {SILENCE_THRESHOLD_MIN_DB} and {SILENCE_THRESHOLD_MAX_DB}."
        )
    if min_duration_seconds < SILENCE_MIN_DURATION_SECONDS or min_duration_seconds > SILENCE_MAX_DURATION_SECONDS:
        raise HTTPException(
            400,
            f"min_duration_seconds must be between {SILENCE_MIN_DURATION_SECONDS} "
            f"and {SILENCE_MAX_DURATION_SECONDS}."
        )

    return await _submit_audio_tool(
        file,
        job_type="silence_remove",
        tool="SILENCE_REMOVE",
        metric="/silence-remove",
        build_work=lambda inp, out: (
            lambda: run_blocking(remove_silence, inp, out, threshold_db, min_duration_seconds)
        ),
        log_detail=f"threshold={threshold_db}dB min_dur={min_duration_seconds}s",
        generic_error="Silence removal failed unexpectedly.",
    )


@router.get("/silence-remove/status/{job_id}")
async def silence_remove_status(job_id: str):
    return _tool_status(job_id, "silence_remove")


@router.get("/silence-remove/preview/{job_id}")
async def silence_remove_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "silence_remove")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/silence-remove/download/{job_id}")
async def silence_remove_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "silence_remove")
    return FileResponse(path, media_type="application/octet-stream", filename=f"silence_removed.{fmt}")


# ---------- /loudnorm ----------

@router.post(
    "/loudnorm",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=LOUDNORM_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=LOUDNORM_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def loudnorm_route(
    file: UploadFile = File(...),
    preset: str = Form("streaming"),
    custom_lufs: float = Form(None),
):
    """Two-pass LUFS loudness normalization."""
    try:
        target_lufs = resolve_target_lufs(preset, custom_lufs)
    except AudioToolError as e:
        raise HTTPException(400, str(e))

    return await _submit_audio_tool(
        file,
        job_type="loudnorm",
        tool="LOUDNORM",
        metric="/loudnorm",
        build_work=lambda inp, out: (lambda: run_blocking(normalize_loudness, inp, out, target_lufs)),
        log_detail=f"-> {target_lufs} LUFS",
        generic_error="Loudness normalization failed unexpectedly.",
    )


@router.get("/loudnorm/status/{job_id}")
async def loudnorm_status(job_id: str):
    return _tool_status(job_id, "loudnorm")


@router.get("/loudnorm/preview/{job_id}")
async def loudnorm_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "loudnorm")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/loudnorm/download/{job_id}")
async def loudnorm_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "loudnorm")
    return FileResponse(path, media_type="application/octet-stream", filename=f"normalized.{fmt}")


# ---------- /fade ----------

@router.post(
    "/fade",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=FADE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=FADE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def fade_route(
    file: UploadFile = File(...),
    fade_in_seconds: float = Form(0.0),
    fade_out_seconds: float = Form(0.0),
):
    """Fade in and/or out."""
    if fade_in_seconds <= 0 and fade_out_seconds <= 0:
        raise HTTPException(400, "At least one of fade_in_seconds or fade_out_seconds must be greater than 0.")
    if fade_in_seconds < 0 or fade_in_seconds > FADE_MAX_SECONDS:
        raise HTTPException(400, f"fade_in_seconds must be between 0 and {FADE_MAX_SECONDS}.")
    if fade_out_seconds < 0 or fade_out_seconds > FADE_MAX_SECONDS:
        raise HTTPException(400, f"fade_out_seconds must be between 0 and {FADE_MAX_SECONDS}.")

    source_format = _validated_input_format(file.filename)

    return await _submit_audio_tool(
        file,
        job_type="fade",
        tool="FADE",
        metric="/fade",
        build_work=lambda inp, out: (
            lambda: run_blocking(apply_fade, inp, out, source_format, fade_in_seconds, fade_out_seconds)
        ),
        log_detail=f"in={fade_in_seconds}s out={fade_out_seconds}s",
        generic_error="Fade failed unexpectedly.",
    )


@router.get("/fade/status/{job_id}")
async def fade_status(job_id: str):
    return _tool_status(job_id, "fade")


@router.get("/fade/preview/{job_id}")
async def fade_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "fade")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/fade/download/{job_id}")
async def fade_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "fade")
    return FileResponse(path, media_type="application/octet-stream", filename=f"faded.{fmt}")


# ---------- /channels ----------

@router.post(
    "/channels",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=CHANNELS_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=CHANNELS_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def channels_route(file: UploadFile = File(...), target: str = Form(...)):
    """Mono <-> stereo conversion."""
    target = target.strip().lower()
    if target not in ("mono", "stereo"):
        raise HTTPException(400, "target must be 'mono' or 'stereo'.")

    source_format = _validated_input_format(file.filename)

    return await _submit_audio_tool(
        file,
        job_type="channels",
        tool="CHANNELS",
        metric="/channels",
        build_work=lambda inp, out: (
            lambda: run_blocking(convert_channels, inp, out, source_format, target)
        ),
        log_detail=f"-> {target}",
        generic_error="Channel conversion failed unexpectedly.",
    )


@router.get("/channels/status/{job_id}")
async def channels_status(job_id: str):
    return _tool_status(job_id, "channels")


@router.get("/channels/preview/{job_id}")
async def channels_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "channels")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/channels/download/{job_id}")
async def channels_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "channels")
    return FileResponse(path, media_type="application/octet-stream", filename=f"converted.{fmt}")


# ---------- /resample ----------

@router.post(
    "/resample",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=RESAMPLE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=RESAMPLE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def resample_route(
    file: UploadFile = File(...),
    sample_rate: int = Form(...),
    bit_depth: int = Form(None),
):
    """Sample rate / bit depth conversion."""
    if sample_rate not in RESAMPLE_ALLOWED_RATES:
        raise HTTPException(
            400, f"sample_rate must be one of: {', '.join(str(r) for r in RESAMPLE_ALLOWED_RATES)}"
        )
    if bit_depth is not None and bit_depth not in RESAMPLE_ALLOWED_BIT_DEPTHS:
        raise HTTPException(
            400, f"bit_depth must be one of: {', '.join(str(b) for b in RESAMPLE_ALLOWED_BIT_DEPTHS)}"
        )

    source_format = _validated_input_format(file.filename)

    return await _submit_audio_tool(
        file,
        job_type="resample",
        tool="RESAMPLE",
        metric="/resample",
        build_work=lambda inp, out: (
            lambda: run_blocking(resample_audio, inp, out, source_format, sample_rate, bit_depth)
        ),
        log_detail=f"-> {sample_rate}Hz" + (f"/{bit_depth}bit" if bit_depth else ""),
        generic_error="Resampling failed unexpectedly.",
    )


@router.get("/resample/status/{job_id}")
async def resample_status(job_id: str):
    return _tool_status(job_id, "resample")


@router.get("/resample/preview/{job_id}")
async def resample_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "resample")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/resample/download/{job_id}")
async def resample_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "resample")
    return FileResponse(path, media_type="application/octet-stream", filename=f"resampled.{fmt}")


# ---------- /ringtone ----------
#
# .m4r is not a distinct codec - it's an M4A (AAC) file that iOS
# recognizes by extension. make_ringtone() writes standard .m4a bytes;
# only the download route's filename carries .m4r.

@router.post(
    "/ringtone",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=RINGTONE_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=RINGTONE_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def ringtone_route(
    file: UploadFile = File(...),
    start_seconds: float = Form(0.0),
    duration_seconds: float = Form(30.0),
):
    """Trim + M4A-as-M4R ringtone maker."""
    if duration_seconds <= 0 or duration_seconds > RINGTONE_MAX_DURATION_SECONDS:
        raise HTTPException(400, f"duration_seconds must be between 0 and {RINGTONE_MAX_DURATION_SECONDS}.")
    if start_seconds < 0:
        raise HTTPException(400, "start_seconds must be non-negative.")

    return await _submit_audio_tool(
        file,
        job_type="ringtone",
        tool="RINGTONE",
        metric="/ringtone",
        output_format="m4a",
        build_work=lambda inp, out: (
            lambda: run_blocking(make_ringtone, inp, out, start_seconds, duration_seconds)
        ),
        log_detail=f"[{start_seconds}s +{duration_seconds}s]",
        generic_error="Ringtone creation failed unexpectedly.",
    )


@router.get("/ringtone/status/{job_id}")
async def ringtone_status(job_id: str):
    return _tool_status(job_id, "ringtone")


@router.get("/ringtone/preview/{job_id}")
async def ringtone_preview(job_id: str):
    path, _ = _resolve_tool_output_path(job_id, "ringtone")
    return FileResponse(path, media_type="audio/mp4")


@router.get("/ringtone/download/{job_id}")
async def ringtone_download(job_id: str):
    path, _ = _resolve_tool_output_path(job_id, "ringtone")
    return FileResponse(path, media_type="audio/mp4", filename="ringtone.m4r")


# ============================================================
# /speech-to-text - Whisper transcription (async job flow)
#
# On its OWN semaphore, not the ffmpeg pool. Whisper inference is a
# heavy, sustained CPU+RAM operation unlike a stateless ffmpeg
# subprocess; sharing the pool would let one transcription starve fast,
# cheap operations like /volume or /trim of their slots.
#
# Structurally different from every other tool too: no /preview (there
# is no audio output) and its result route is /result, returning JSON.
# ============================================================

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
    _validated_input_format(file.filename)
    original_filename = file.filename

    job_id = create_job(job_type="transcribe", ttl_seconds=TRANSCRIPTION_JOB_TTL_SECONDS)
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


# ============================================================
# /video-to-audio - Extract the audio track from a video file
#
# Its own, much higher size cap: a few minutes of phone video routinely
# exceeds what an audio upload ever would.
# ============================================================

@router.post(
    "/video-to-audio",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=VIDEO_TO_AUDIO_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=VIDEO_TO_AUDIO_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def video_to_audio_route(file: UploadFile = File(...), target_format: str = Form("mp3")):
    """Poll GET /video-to-audio/status/{job_id}."""
    target_format = target_format.strip().lower()
    if target_format not in ALLOWED_AUDIO_INPUT_FORMATS:
        raise HTTPException(
            400, f"target_format must be one of: {', '.join(sorted(ALLOWED_AUDIO_INPUT_FORMATS))}"
        )

    try:
        source_format = validate_video_input_format(file.filename)
    except AudioToolError as e:
        raise HTTPException(400, str(e))

    original_filename = file.filename
    job_id = create_job(job_type="video_to_audio")

    # Note the path is built directly rather than via
    # build_temp_input_path(): that helper is for audio extensions, and
    # ffmpeg needs the real container extension (.mp4/.mov/...) to
    # demux a video correctly.
    input_path = os.path.join(UPLOAD_DIR, f"{job_id}_{file.filename}")
    output_path = build_output_path(job_id, target_format)

    try:
        size = await save_upload(file, input_path, MAX_VIDEO_UPLOAD_BYTES, label="video_to_audio")
    except HTTPException as e:
        mark_failed(job_id, e.detail if isinstance(e.detail, str) else "Upload rejected.")
        raise

    asyncio.create_task(_run_tool_job(
        tool="VIDEO_TO_AUDIO",
        metric="/video-to-audio",
        job_id=job_id,
        semaphore=_audio_tools_semaphore,
        work=lambda: run_blocking(extract_audio, input_path, output_path, target_format),
        on_success=lambda _: mark_tool_complete(job_id, original_filename, output_path, target_format),
        generic_error="Audio extraction failed unexpectedly.",
        cleanup_paths=[input_path],
        success_detail=lambda copied: "stream copy" if copied else "re-encoded",
    ))

    _log_queued("VIDEO_TO_AUDIO", job_id, original_filename, size, f"{source_format} -> {target_format}")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/video-to-audio/status/{job_id}")
async def video_to_audio_status(job_id: str):
    return _tool_status(job_id, "video_to_audio")


@router.get("/video-to-audio/preview/{job_id}")
async def video_to_audio_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "video_to_audio")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/video-to-audio/download/{job_id}")
async def video_to_audio_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "video_to_audio")
    return FileResponse(path, media_type="application/octet-stream", filename=f"audio.{fmt}")


# ============================================================
# /join - Concatenate several audio files into one
#
# The only endpoint taking MULTIPLE uploads. Two consequences: the size
# cap is enforced across the whole batch as well as per file, and the
# ORDER of the uploaded files is the order of the output (FastAPI
# preserves List[UploadFile] ordering, so the frontend controls
# sequencing purely by the order it appends to the form).
# ============================================================

@router.post(
    "/join",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=JOIN_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=JOIN_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def join_route(files: List[UploadFile] = File(...), target_format: str = Form("mp3")):
    """Poll GET /join/status/{job_id}. Output order matches upload order."""
    target_format = target_format.strip().lower()
    if target_format not in ALLOWED_AUDIO_INPUT_FORMATS:
        raise HTTPException(
            400, f"target_format must be one of: {', '.join(sorted(ALLOWED_AUDIO_INPUT_FORMATS))}"
        )

    if len(files) < 2:
        raise HTTPException(400, "Joining needs at least two files.")
    if len(files) > JOIN_MAX_FILES:
        raise HTTPException(400, f"You can join up to {JOIN_MAX_FILES} files at a time.")

    # Every filename is checked before ANY byte is transferred - one bad
    # extension in a ten-file batch should not cost the user a 150MB
    # upload first.
    for f in files:
        _validated_input_format(f.filename)

    first_filename = files[0].filename

    job_id = create_job(job_type="join")

    dest_paths = [
        os.path.join(UPLOAD_DIR, f"{job_id}_{index}_{f.filename}")
        for index, f in enumerate(files)
    ]

    try:
        input_paths, total = await save_uploads(
            files, dest_paths, JOIN_MAX_TOTAL_BYTES, label="join"
        )
    except HTTPException as e:
        mark_failed(job_id, e.detail if isinstance(e.detail, str) else "Upload rejected.")
        raise

    output_path = build_output_path(job_id, target_format)

    asyncio.create_task(_run_tool_job(
        tool="JOIN",
        metric="/join",
        job_id=job_id,
        semaphore=_audio_tools_semaphore,
        work=lambda: run_blocking(join_audio, input_paths, output_path, target_format),
        on_success=lambda _: mark_tool_complete(job_id, first_filename, output_path, target_format),
        generic_error="Joining failed unexpectedly.",
        cleanup_paths=input_paths,
        success_detail=lambda duration: f"{duration:.1f}s total",
    ))

    logger.info(
        f"[JOIN] job={job_id} queued {len(files)} files -> {target_format} "
        f"({_mb(total)} combined)"
    )
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/join/status/{job_id}")
async def join_status(job_id: str):
    return _tool_status(job_id, "join")


@router.get("/join/preview/{job_id}")
async def join_preview(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "join")
    return FileResponse(path, media_type=get_audio_mime_type(fmt))


@router.get("/join/download/{job_id}")
async def join_download(job_id: str):
    path, fmt = _resolve_tool_output_path(job_id, "join")
    return FileResponse(path, media_type="application/octet-stream", filename=f"joined.{fmt}")


# ============================================================
# /silence-split - Cut a file into segments at silent gaps
#
# Reuses the "stems" storage shape: a {name: path} dict, the same
# mark_stems_complete(), and the same status-lists-available-names
# pattern. The only real difference from /stems is what produced the
# dict and how many entries it has.
# ============================================================

@router.post(
    "/silence-split",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=SILENCE_SPLIT_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=SILENCE_SPLIT_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def silence_split_route(
    file: UploadFile = File(...),
    target_format: str = Form("mp3"),
    threshold_db: float = Form(-30.0),
    min_duration_seconds: float = Form(0.5),
):
    """Poll GET /silence-split/status/{job_id} - the response lists the
    available segment names once complete."""
    _validated_input_format(file.filename)

    target_format = target_format.strip().lower()
    if target_format not in ALLOWED_AUDIO_INPUT_FORMATS:
        raise HTTPException(
            400, f"target_format must be one of: {', '.join(sorted(ALLOWED_AUDIO_INPUT_FORMATS))}"
        )
    if threshold_db < SILENCE_THRESHOLD_MIN_DB or threshold_db > SILENCE_THRESHOLD_MAX_DB:
        raise HTTPException(
            400,
            f"threshold_db must be between {SILENCE_THRESHOLD_MIN_DB} and {SILENCE_THRESHOLD_MAX_DB}."
        )
    if min_duration_seconds < SILENCE_MIN_DURATION_SECONDS or min_duration_seconds > SILENCE_MAX_DURATION_SECONDS:
        raise HTTPException(
            400,
            f"min_duration_seconds must be between {SILENCE_MIN_DURATION_SECONDS} "
            f"and {SILENCE_MAX_DURATION_SECONDS}."
        )

    original_filename = file.filename

    job_id = create_job(job_type="silence_split")
    input_path, size = await _accept_upload(file, job_id, label="silence_split")
    await _validate_duration_or_reject(job_id, input_path)

    asyncio.create_task(_run_tool_job(
        tool="SILENCE_SPLIT",
        metric="/silence-split",
        job_id=job_id,
        semaphore=_audio_tools_semaphore,
        work=lambda: run_blocking(
            split_on_silence, input_path, job_id, target_format, threshold_db, min_duration_seconds
        ),
        on_success=lambda segments: mark_stems_complete(job_id, original_filename, segments),
        generic_error="Splitting failed unexpectedly.",
        cleanup_paths=[input_path],
        success_detail=lambda segments: f"{len(segments)} segments",
    ))

    _log_queued("SILENCE_SPLIT", job_id, original_filename, size, f"threshold={threshold_db}dB")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/silence-split/status/{job_id}")
async def silence_split_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "silence_split":
        raise HTTPException(404, "Job not found (it may have expired).")
    segments = job.get("stems") or {}
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
        "segments": sorted(segments.keys()),
    }


def _resolve_silence_split_file(job_id: str, segment: str) -> str:
    job = get_job(job_id)
    if job is None or job["job_type"] != "silence_split":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] == "failed":
        raise HTTPException(409, job.get("error") or "This job failed.")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    segments = job.get("stems") or {}
    if segment not in segments:
        raise HTTPException(400, f"segment must be one of: {', '.join(sorted(segments.keys()))}")
    path = segments[segment]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Segment file not found (it may have expired).")
    return path


@router.get("/silence-split/preview/{job_id}")
async def silence_split_preview(job_id: str, segment: str = Query(...)):
    path = _resolve_silence_split_file(job_id, segment)
    return FileResponse(path, media_type=get_audio_mime_type(path.rsplit(".", 1)[-1]))


@router.get("/silence-split/download/{job_id}")
async def silence_split_download(job_id: str, segment: str = Query(...)):
    path = _resolve_silence_split_file(job_id, segment)
    ext = path.rsplit(".", 1)[-1]
    return FileResponse(path, media_type="application/octet-stream", filename=f"{segment}.{ext}")


# ============================================================
# /youtube/* - Paste a URL, get the processed result, skipping the
# manual download-then-reupload step.
#
# Each of these chains TWO of the app's heaviest operations in one
# background job: a YouTube download, then either analysis or Demucs
# separation. The download slot is RELEASED before the processing slot
# is acquired - the two are never held at once, so a slow separation
# doesn't also tie up a download slot for its whole duration.
# ============================================================

async def _chain_download(job_id: str, url: str, tool: str, metric: str) -> Optional[tuple]:
    """
    Shared first half of every /youtube/* job: acquire a download slot,
    fetch the audio, release the slot. Returns (file_path, title), or
    None if it failed (in which case the job is already marked and the
    metric already recorded).

    The acquire is INSIDE the try. acquire_slot_or_503() raises
    HTTPException when the queue wait times out - and in a background
    task there is no HTTP layer to catch that, so before this change it
    propagated straight out of the task, skipping every mark_failed()
    below it and leaving the job stuck on "processing" forever with no
    log line explaining why. That single detail accounted for a whole
    class of "it just spun forever" reports.

    The release lives in `finally` guarded by a flag, so the slot is
    returned exactly once whether the download succeeded, failed, or the
    acquire itself blew up.
    """
    acquired = False
    try:
        await acquire_slot_or_503(_download_semaphore, f"{tool.lower()}-download")
        acquired = True
        started = time.monotonic()
        file_path, title = await run_blocking(download_audio_to_file, url, job_id)
        logger.info(
            f"[{tool}] job={job_id} downloaded '{title}' in {time.monotonic() - started:.1f}s"
        )
        return file_path, title

    except ChainDownloadError as e:
        mark_failed(job_id, str(e))
        logger.warning(f"[{tool}] job={job_id} download FAILED: {e}")
        record_result(metric, False)
        return None

    except HTTPException as e:
        # Almost always the queue-wait 503 from acquire_slot_or_503.
        detail = e.detail if isinstance(e.detail, str) else "The server was too busy."
        mark_failed(job_id, detail)
        logger.warning(f"[{tool}] job={job_id} download rejected: {detail}")
        record_result(metric, False)
        return None

    except Exception as e:
        mark_failed(job_id, "Download failed unexpectedly.")
        logger.error(f"[{tool}] job={job_id} download FAILED (unexpected): {e}", exc_info=True)
        record_result(metric, False)
        return None

    finally:
        if acquired:
            _download_semaphore.release()


async def _run_youtube_analyze(job_id: str, url: str):
    """Download, then key/BPM analysis. Two different semaphores, held
    one at a time."""
    downloaded = await _chain_download(job_id, url, "YOUTUBE_ANALYZE", "/youtube/analyze")
    if downloaded is None:
        fail_if_unfinished(job_id, "Download failed.")
        return

    file_path, title = downloaded
    analysis_path = file_path
    succeeded = False
    acquired = False
    started = time.monotonic()

    try:
        await acquire_slot_or_503(_analysis_semaphore, "youtube-analyze")
        acquired = True

        if ANALYSIS_MAX_SECONDS is not None:
            analysis_path = await run_blocking(trim_audio_for_analysis, file_path, ANALYSIS_MAX_SECONDS)

        key, scale, key_conf, bpm, bpm_conf, audio_array, essentia_sr = await run_blocking(
            detect_key_bpm_essentia, analysis_path
        )
        key, scale, key_conf, bpm, bpm_conf, agreement = await run_blocking(
            cross_check_with_librosa, audio_array, essentia_sr, key, scale, key_conf, bpm, bpm_conf
        )
        del audio_array

        result = {
            "key": f"{key} {scale}",
            "camelot": get_camelot(key, scale),
            "bpm": bpm,
            "confidence": int(min(0.99, key_conf) * 100),
            "bpm_confidence": min(99, bpm_conf),
            "cross_check": agreement,
        }
        mark_data_complete(job_id, title, result)
        succeeded = True
        logger.info(
            f"[YOUTUBE_ANALYZE] job={job_id} COMPLETE in {time.monotonic() - started:.1f}s: "
            f"{result['key']} / {result['camelot']} / {result['bpm']} BPM"
        )

    except HTTPException as e:
        detail = e.detail if isinstance(e.detail, str) else "The server was too busy."
        mark_failed(job_id, detail)
        logger.warning(f"[YOUTUBE_ANALYZE] job={job_id} rejected: {detail}")

    except asyncio.CancelledError:
        mark_failed(job_id, "The server restarted while this job was running.")
        logger.warning(f"[YOUTUBE_ANALYZE] job={job_id} CANCELLED (shutdown)")
        raise

    except Exception as e:
        mark_failed(job_id, "Analysis failed unexpectedly.")
        logger.error(f"[YOUTUBE_ANALYZE] job={job_id} FAILED (unexpected): {e}", exc_info=True)

    finally:
        fail_if_unfinished(job_id, "Analysis failed unexpectedly.")
        cleanup_file(file_path)
        if analysis_path != file_path:
            cleanup_file(analysis_path)
        if acquired:
            _analysis_semaphore.release()
        release_memory_to_os()
        record_result("/youtube/analyze", succeeded)


async def _run_youtube_separation(job_id: str, url: str, *, stems: bool):
    """Download, then Demucs. One function for both /youtube/separate and
    /youtube/stems - they differ only in which worker runs and how the
    result is stored."""
    tool = "YOUTUBE_STEMS" if stems else "YOUTUBE_SEPARATE"
    metric = "/youtube/stems" if stems else "/youtube/separate"

    downloaded = await _chain_download(job_id, url, tool, metric)
    if downloaded is None:
        fail_if_unfinished(job_id, "Download failed.")
        return

    file_path, title = downloaded

    if stems:
        work = lambda: run_blocking(
            run_stem_separation, file_path, job_id,
            SEPARATION_MODEL, SEPARATION_OVERLAP,
            DEMUCS_TIMEOUT_SECONDS, MAX_SEPARATION_DURATION_SECONDS,
        )
        on_success = lambda result: mark_stems_complete(job_id, title, result)
        success_detail = lambda result: f"{len(result)} stems"
        generic_error = "Stem separation failed unexpectedly."
    else:
        work = lambda: run_blocking(
            run_separation, file_path, job_id,
            SEPARATION_MODEL, SEPARATION_OVERLAP,
            DEMUCS_TIMEOUT_SECONDS, MAX_SEPARATION_DURATION_SECONDS,
        )
        on_success = lambda paths: mark_complete(job_id, title, paths[0], paths[1])
        success_detail = None
        generic_error = "Separation failed unexpectedly."

    await _run_tool_job(
        tool=tool,
        metric=metric,
        job_id=job_id,
        semaphore=_separation_semaphore,
        work=work,
        on_success=on_success,
        generic_error=generic_error,
        cleanup_paths=[file_path],
        success_detail=success_detail,
    )


@router.post(
    "/youtube/analyze",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=YOUTUBE_CHAIN_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=YOUTUBE_CHAIN_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def youtube_analyze_route(url: str = Form(...)):
    """Poll GET /youtube/analyze/status/{job_id}, then .../result."""
    if not is_valid_youtube_url(url):
        raise HTTPException(400, "Please provide a valid YouTube video URL.")

    job_id = create_job(job_type="youtube_analyze", ttl_seconds=YOUTUBE_ANALYZE_JOB_TTL_SECONDS)
    asyncio.create_task(_run_youtube_analyze(job_id, url))

    logger.info(f"[YOUTUBE_ANALYZE] job={job_id} queued for {url}")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/youtube/analyze/status/{job_id}")
async def youtube_analyze_status(job_id: str):
    return _tool_status(job_id, "youtube_analyze")


@router.get("/youtube/analyze/result/{job_id}")
async def youtube_analyze_result(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "youtube_analyze":
        raise HTTPException(404, "Job not found (it may have expired).")
    if job["status"] == "failed":
        raise HTTPException(409, job.get("error") or "This job failed.")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    result = job.get("result_data")
    if not result:
        raise HTTPException(404, "Result not found (it may have expired).")
    return JSONResponse(result)


@router.post(
    "/youtube/separate",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=YOUTUBE_CHAIN_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=YOUTUBE_CHAIN_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def youtube_separate_route(url: str = Form(...)):
    """Downloads then runs standard-tier vocal/instrumental separation.
    Stem paths are stored the same way /separate stores them."""
    if not is_valid_youtube_url(url):
        raise HTTPException(400, "Please provide a valid YouTube video URL.")

    _reject_if_separation_queue_full()

    job_id = create_job(job_type="youtube_separate")
    asyncio.create_task(_run_youtube_separation(job_id, url, stems=False))

    logger.info(f"[YOUTUBE_SEPARATE] job={job_id} queued for {url}")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/youtube/separate/status/{job_id}")
async def youtube_separate_status(job_id: str):
    return _tool_status(job_id, "youtube_separate")


def _resolve_youtube_separate_path(job_id: str, stem: str) -> str:
    job = get_job(job_id)
    if job is None or job["job_type"] != "youtube_separate":
        raise HTTPException(404, "Job not found (it may have expired).")
    if stem not in ("vocals", "instrumental"):
        raise HTTPException(400, "stem must be 'vocals' or 'instrumental'")
    if job["status"] == "failed":
        raise HTTPException(409, job.get("error") or "This job failed.")
    if job["status"] != "complete":
        raise HTTPException(409, f"Job is not complete yet (status: {job['status']}).")
    path = job["vocals_path"] if stem == "vocals" else job["instrumental_path"]
    if not path or not os.path.exists(path):
        raise HTTPException(404, "Stem file not found (it may have expired).")
    return path


@router.get("/youtube/separate/preview/{job_id}")
async def youtube_separate_preview(job_id: str, stem: str = Query(...)):
    path = _resolve_youtube_separate_path(job_id, stem)
    return FileResponse(path, media_type="audio/wav")


@router.get("/youtube/separate/download/{job_id}")
async def youtube_separate_download(job_id: str, stem: str = Query(...)):
    path = _resolve_youtube_separate_path(job_id, stem)
    return FileResponse(path, media_type="audio/wav", filename=f"{stem}.wav")


@router.post(
    "/youtube/stems",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=YOUTUBE_CHAIN_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=YOUTUBE_CHAIN_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def youtube_stems_route(url: str = Form(...)):
    """Downloads then runs standard-tier full 4-stem separation."""
    if not is_valid_youtube_url(url):
        raise HTTPException(400, "Please provide a valid YouTube video URL.")

    _reject_if_separation_queue_full()

    job_id = create_job(job_type="youtube_stems")
    asyncio.create_task(_run_youtube_separation(job_id, url, stems=True))

    logger.info(f"[YOUTUBE_STEMS] job={job_id} queued for {url}")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/youtube/stems/status/{job_id}")
async def youtube_stems_status(job_id: str):
    job = get_job(job_id)
    if job is None or job["job_type"] != "youtube_stems":
        raise HTTPException(404, "Job not found (it may have expired).")
    stems = job.get("stems") or {}
    return {
        "job_id": job_id,
        "status": job["status"],
        "title": job.get("title"),
        "error": job.get("error"),
        "stems": sorted(stems.keys()),
    }


def _resolve_youtube_stems_file(job_id: str, stem: str) -> str:
    job = get_job(job_id)
    if job is None or job["job_type"] != "youtube_stems":
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


@router.get("/youtube/stems/preview/{job_id}")
async def youtube_stems_preview(job_id: str, stem: str = Query(...)):
    path = _resolve_youtube_stems_file(job_id, stem)
    return FileResponse(path, media_type="audio/wav")


@router.get("/youtube/stems/download/{job_id}")
async def youtube_stems_download(job_id: str, stem: str = Query(...)):
    path = _resolve_youtube_stems_file(job_id, stem)
    return FileResponse(path, media_type="audio/wav", filename=f"{stem}.wav")


# ============================================================
# ADMIN / META
# ============================================================

@router.post("/admin/clear-cache")
async def admin_clear_cache(request: Request, key: str = Query(...)):
    client_ip = guard_admin_request(request)
    verify_admin_key(key, client_ip)
    result = clear_cache()
    logger.info(f"[CACHE] Admin manually cleared cache: {result}")
    return {"status": "cache cleared", **result}


@router.post("/admin/cache/limit")
async def admin_set_cache_limit(request: Request, key: str = Query(...), gb: float = Query(..., gt=0, le=1000)):
    client_ip = guard_admin_request(request)
    verify_admin_key(key, client_ip)
    try:
        stats = set_cache_max_gb(gb)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"status": "updated", **stats}


@router.post("/admin/reset-proxy")
async def admin_reset_proxy(request: Request, key: str = Query(...)):
    client_ip = guard_admin_request(request)
    verify_admin_key(key, client_ip)
    reset_proxy_circuit_breaker()
    return {"status": "proxy circuit breaker reset"}


@router.get("/admin/status")
async def admin_status(request: Request, key: str = Query(...)):
    client_ip = guard_admin_request(request)
    verify_admin_key(key, client_ip)
    snapshot = get_status_snapshot()
    snapshot["proxy"] = {
        "circuit_breaker": "OPEN (proxy disabled)" if not proxy_available() else "CLOSED (proxy available)",
    }
    snapshot["cookies"] = {
        "accounts_available": len(get_cookie_accounts()),
    }
    snapshot["cache"] = {
        "enabled": True,
        "backend": "local-disk",
        **get_cache_stats(),
    }
    # Job-table state, including the separation queue depth the bounded
    # queue keys on. Worth having here rather than only in the logs: when
    # someone reports "it's stuck", this answers whether anything is
    # actually running, and how long the oldest in-flight job has been
    # going.
    snapshot["jobs"] = {
        **get_job_stats(),
        "separation_queue_limit": MAX_QUEUED_SEPARATIONS,
        "separation_concurrency": MAX_CONCURRENT_SEPARATIONS,
    }
    return snapshot


@router.get("/limits")
async def limits():
    """
    The single source of truth for every limit the frontend needs to
    enforce or display.

    Before this existed, the same numbers were hardcoded in ~20 page
    files, in the client-side validator, AND in config.py - and they
    drifted, which is how a 50MB per-file check ended up silently
    blocking uploads on a tool whose UI advertised a 150MB total. The
    frontend should read these at build time and render from them
    instead of repeating them.
    """
    return {
        "max_upload_bytes": MAX_UPLOAD_BYTES,
        "max_upload_mb": MAX_UPLOAD_BYTES // (1024 * 1024),
        "max_video_upload_bytes": MAX_VIDEO_UPLOAD_BYTES,
        "max_video_upload_mb": MAX_VIDEO_UPLOAD_BYTES // (1024 * 1024),
        "join": {
            "max_files": JOIN_MAX_FILES,
            "max_total_bytes": JOIN_MAX_TOTAL_BYTES,
            "max_total_mb": JOIN_MAX_TOTAL_BYTES // (1024 * 1024),
            # Stated explicitly because it is NOT implied by the total,
            # and the frontend enforces it separately.
            "max_per_file_mb": MAX_UPLOAD_BYTES // (1024 * 1024),
        },
        "allowed_audio_formats": sorted(ALLOWED_AUDIO_INPUT_FORMATS),
        "rate_limits": {
            "separate": SEPARATION_RATE_LIMIT_MAX_REQUESTS,
            "separate_hq": SEPARATION_HQ_RATE_LIMIT_MAX_REQUESTS,
            "stems": STEMS_RATE_LIMIT_MAX_REQUESTS,
            "stems_hq": STEMS_HQ_RATE_LIMIT_MAX_REQUESTS,
            "window_seconds": SEPARATION_RATE_LIMIT_WINDOW_SECONDS,
        },
        "features": {
            "separation_hq_enabled": SEPARATION_HQ_ENABLED,
        },
    }


@router.get("/health")
async def health():
    return {"status": "healthy"}


@router.get("/")
async def root():
    """
    Public service description. Kept deliberately thin - the exhaustive
    changelog that used to live here has moved to each module's own
    docstring, where it stays accurate because it sits next to the code
    it describes.

    `features` is read by the frontend's server-side getFeatureFlags() to
    decide whether to render the Studio Quality toggle at all. Only a
    boolean is exposed - not the model name, timeout, or any other
    internal detail - so there is nothing here for a client to learn
    about the feature beyond "on or off".
    """
    return {
        "status": "AudioForges API",
        "engine": (
            "Essentia (key/BPM) + Demucs (separation) + ffmpeg (conversion, trim, "
            "volume, reverse, fade, channels, resample, denoise, echo, silence) + "
            "rubberband (pitch, tempo) + faster-whisper (transcription)"
        ),
        "features": {
            "separation_hq_enabled": SEPARATION_HQ_ENABLED,
        },
        "limits": "/limits",
    }