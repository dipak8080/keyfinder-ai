"""
routes/youtube.py - /download (the busiest endpoint on this API) plus
every /youtube/* chained route: paste a URL, get the processed result,
skipping the manual download-then-reupload step.

Split out of the old monolithic routes.py (2026-08-14 restructure). Pure
move: every docstring, comment, and line of logic here is unchanged from
its original location. Nothing in this file changes behaviour.

/download lives here rather than in its own file: it's a YouTube tool
structurally, and grouping it with the /youtube/* chained tools means
every route that touches yt-dlp, cookie accounts, the proxy circuit
breaker, or the CDN degradation breaker is in ONE file - which matters
most on exactly the days this file gets touched under pressure, since
none of that shared context (breaker state, cookie health, proxy
escalation) has to be chased across two files.

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
   400) so the frontend can tell "too big" from "wrong format". (Not
   directly relevant to this file - /download and /youtube/* take no
   uploads - but kept here since the surrounding history explains why
   the rest of the app looks the way it does.)

2. TTL CLEANUP RAN ON THE REQUEST PATH.
   cleanup_expired_jobs() was called at the top of ~20 handlers. It now
   runs on a 60s background timer in main.py; every call here is gone.

3. BACKGROUND JOBS COULD STICK ON "processing" FOREVER.
   Each _run_*_background() marked its job failed inside `except`, but an
   exception raised outside those handlers skipped all of them - most
   realistically acquire_slot_or_503() raising HTTPException inside a
   background task, where no HTTP layer exists to catch it. Every
   background task now calls jobs.fail_if_unfinished() from a `finally`.
   See _chain_download() below for the /youtube/* specific version of
   this fix.

4. THE SEPARATION QUEUE WAS UNBOUNDED.
   See _shared.py's _reject_if_separation_queue_full() docstring for the
   full story - every /youtube/separate*, /youtube/stems* route below
   calls it before creating a job.

5. LOGS COULDN'T ANSWER "WHAT HAPPENED TO THIS REQUEST?"
   Every job now logs a start line (file, size) and an end line
   (COMPLETE/FAILED plus elapsed seconds), both carrying job=<id>.
--------------------------------------------------------------------------

WHAT CHANGED (2026-08-05):

/download's error-classification chain now has a dedicated branch for
CDN connect-timeouts (is_cdn_connect_timeout_error, from youtube.py) -
production logs showed repeated ~20s connect timeouts to the SAME
googlevideo media edge across otherwise-unrelated requests, taking a
full 73s (3 attempts x ~23s) to fail and then returning a generic 500.
Two things changed:
  - ydl_opts now sets socket_timeout=10, so a doomed connect-timeout
    fails faster per attempt.
  - The error chain now recognizes this failure shape explicitly and
    returns 503 ("try again shortly") instead of falling through to the
    generic 500 - this is transient infra flakiness on YouTube's/this
    server's networking, not a bug in this app, and the two deserve
    different status codes for the same reason every other branch in
    this chain already does.
should_use_proxy() in youtube.py was also updated to escalate this
failure shape to the proxy tier, since a different exit IP frequently
resolves to a different, reachable CDN edge - that change lives entirely
in youtube.py (the top-level module, not this routes file) and needs no
further changes here.
--------------------------------------------------------------------------

WHAT CHANGED (2026-08-07):

/download had no branch for a YouTube Music Premium restriction ("This
video is only available to Music Premium members"). This is the same
shape as the existing age-restricted / members-only handling: an
account-privilege problem, not an IP-reputation problem. A different
cookie account that happens to have Music Premium active could succeed
where the current one can't. is_music_premium_error() (added in
youtube.py) is now wired into the same three places its age-restricted /
members-only siblings already were, and gets its own 403 branch here.

should_use_proxy() in youtube.py briefly stopped escalating CDN
connect-timeouts to the proxy tier the same day, then was reverted a few
hours later once the proxy provider's own usage log showed 190/190
googlevideo fetches succeeding through it. CDN connect-timeouts DO
escalate to proxy again, same as geo-restriction and bot-check (see
should_use_proxy()'s docstring in youtube.py for the full history).

On top of that, a direct-path degradation breaker was added
(CDN_DEGRADED_THRESHOLD/_WINDOW_SECONDS/_COOLDOWN_SECONDS in config.py,
record_cdn_timeout()/direct_path_degraded() in youtube.py): once enough
direct-path CDN timeouts cluster together, further downloads skip the
doomed ~10s direct attempt entirely and go straight to proxy for a
cooldown window, surfaced via GET /admin/status's "cdn" block (see
admin.py) and resettable via POST /admin/reset-cdn-breaker (also
admin.py).
--------------------------------------------------------------------------

WHAT CHANGED (2026-08-10): TOOL / TIER TAGGING

Every route that creates a job now calls log_stream.set_job_context(tool,
tier) before kicking off any background task - see log_stream.py's own
"SCHEMA CHANGE" note for the write-side reasoning.

Call sites in THIS file, and why each is placed where it is:

  - youtube_analyze_route, youtube_separate_route,
    youtube_separate_hq_route, youtube_stems_route, youtube_stems_hq_route
    - set BEFORE asyncio.create_task(), not inside the spawned
    _run_youtube_*() function. asyncio.create_task() copies whatever
    context exists AT THE MOMENT it's called into the new task - the same
    mechanism request_id already relies on - so calling it earlier here
    is what makes the tag visible on the initial POST's own HTTP log row,
    not just on the background job's later lines.
  - download_audio() - the one synchronous tool with no job/background
    task at all. Tagged for consistency, so "which tool" is answerable
    the same way for every row in request_logs, not just the
    tiered/backgrounded ones.

Nothing here changes behaviour, status codes, or response shapes - every
added line is a single set_job_context(...) call with no side effects
beyond what gets written to the log tables.
--------------------------------------------------------------------------

NOTE: two names the old routes.py imported from youtube.py -
`download_with_fallback` and `VideoTooLongError` - are not referenced
anywhere in the route handlers below (or anywhere else in the old
routes.py). They are not imported here. If something outside routes.py
actually depended on routes.py re-exporting them, that would need a
separate import added back in - flagging this rather than silently
carrying two unused imports forward.
"""
import os
import time
import uuid
import base64
import asyncio
from typing import Optional
from functools import partial

from fastapi import APIRouter, HTTPException, Depends, Query, Form
from fastapi.responses import JSONResponse, FileResponse

from config import (
    logger,
    UPLOAD_DIR,
    ANALYSIS_MAX_SECONDS,
    DOWNLOAD_WALL_CLOCK_TIMEOUT_SECONDS,
    DOWNLOAD_RATE_LIMIT_MAX_REQUESTS,
    DOWNLOAD_RATE_LIMIT_WINDOW_SECONDS,
    SEPARATION_MODEL,
    SEPARATION_OVERLAP,
    DEMUCS_TIMEOUT_SECONDS,
    MAX_SEPARATION_DURATION_SECONDS,
    SEPARATION_MODEL_HQ,
    SEPARATION_OVERLAP_HQ,
    DEMUCS_TIMEOUT_SECONDS_HQ,
    MAX_SEPARATION_DURATION_SECONDS_HQ,
    SEPARATION_HQ_ENABLED,
    YOUTUBE_CHAIN_RATE_LIMIT_MAX_REQUESTS,
    YOUTUBE_CHAIN_RATE_LIMIT_WINDOW_SECONDS,
    YOUTUBE_CHAIN_HQ_RATE_LIMIT_MAX_REQUESTS,
    YOUTUBE_CHAIN_HQ_RATE_LIMIT_WINDOW_SECONDS,
    YOUTUBE_ANALYZE_JOB_TTL_SECONDS,
)
from utils import (
    cleanup_file,
    release_memory_to_os,
    run_blocking,
    run_in_killable_subprocess,
    acquire_slot_or_503,
    get_camelot,
    _analysis_semaphore,
    _download_semaphore,
    _separation_semaphore,
)
from youtube import (
    is_bot_check_error,
    is_geo_restricted_error,
    is_age_restricted_error,
    is_members_only_error,
    is_music_premium_error,
    is_not_yet_live_error,
    is_permanent_error,
    is_cdn_connect_timeout_error,
    is_valid_youtube_url,
    extract_video_id,
    proxy_available,
    get_cookie_accounts,
    ytdlp_alert_logger,
)
from audio_analysis import detect_key_bpm_essentia, cross_check_with_librosa, trim_audio_for_analysis
from rate_limit import check_rate_limit
from cache import get_cached_audio, put_cached_audio
from monitoring import record_result
from download_progress import make_progress_hook
from jobs import (
    create_job,
    mark_complete,
    mark_stems_complete,
    mark_data_complete,
    mark_failed,
    fail_if_unfinished,
    get_job,
)
from separation import run_separation, run_stem_separation
from youtube_chain import download_audio_to_file, ChainDownloadError
from log_stream import (
    get_current_request_id,
    set_job_context,
    remember_job_tags,
    tag_from_job,
)

from ._shared import _mb, _reject_if_separation_queue_full, _tool_status, _run_tool_job

router = APIRouter()


# ============================================================
# /download - YouTube URL to MP3/WAV (synchronous, cached)
#
# The only tool that takes no upload, which is why it was the ONLY tool
# still working during the incident that prompted this rewrite: nothing
# to buffer, nothing to stall the loop with, and its result is cached.
# ============================================================

@router.post(
    "/download",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=DOWNLOAD_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=DOWNLOAD_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def download_audio(url: str = Form(...), format: str = Form("mp3")):
    # Synchronous tool, no job - tagged anyway so its row in request_logs
    # reports "DOWNLOAD" the same consistent way every other tool's rows
    # do, instead of being the one row type where "which tool" has to be
    # inferred from the path.
    set_job_context(tool="DOWNLOAD", tier="standard")

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
        # Default connect timeout is 20s (yt-dlp's own default). A
        # connect-timeout to a dead/unreachable googlevideo edge is
        # guaranteed to fail identically on every retry against the SAME
        # IP (see is_cdn_connect_timeout_error in youtube.py) - lowering
        # this means a doomed attempt fails in ~10s instead of ~20s,
        # cutting the worst-case all-attempts-failed wall time roughly in
        # half before the proxy tier (or the 503 branch below) takes
        # over. 10s is still generous for a genuinely slow-but-working
        # connection; it is not so low that it risks false-failing normal
        # requests under typical latency.
        'socket_timeout': 10,
        'extractor_args': {
            'youtubepot-bgutilscript': {
                'script_path': ['/root/bgutil-ytdlp-pot-provider/server/build/generate_once.js']
            },
            'youtube': {
                'player_client': ['android_vr', 'android', 'web'],
            },
        },
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': format,
            'preferredquality': '192',
        }],
        'remote_components': ['ejs:github'],
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

    # download_worker.py runs in a separate process and reconstructs its
    # own logger/hooks internally - neither survives a JSON boundary, so
    # strip them here rather than pass them across.
    serializable_ydl_opts = {
        k: v for k, v in ydl_opts.items()
        if k not in ("logger", "progress_hooks")
    }

    await acquire_slot_or_503(_download_semaphore, "download")

    audio_data = None
    succeeded = False
    try:
        result = await run_in_killable_subprocess(
            serializable_ydl_opts, url, proxy_url,
            DOWNLOAD_WALL_CLOCK_TIMEOUT_SECONDS, temp_id,
            progress_label=video_id or url,
            request_id=get_current_request_id(),
        )

        if result["ok"]:
            title = result["title"]
        elif result["kind"] == "too_long":
            logger.warning(f"[DOWNLOAD] Rejected - video too long: {result['error']}")
            raise HTTPException(400, result["error"])
        elif result["kind"] == "timeout":
            logger.warning(
                f"[DOWNLOAD] Wall-clock timeout ({DOWNLOAD_WALL_CLOCK_TIMEOUT_SECONDS}s) - "
                f"process group killed, slot freed: {url}"
            )
            raise HTTPException(503, "This download is taking too long. Please try again.")
        elif result["kind"] == "crashed":
            logger.error(f"[DOWNLOAD] Worker process crashed: {result['error']}")
            raise HTTPException(500, f"Failed: {result['error']}")
        else:
            error_text = result["error"]

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

            if is_music_premium_error(error_text):
                logger.warning(f"[DOWNLOAD] Music Premium required: {url}")
                raise HTTPException(
                    403,
                    "This track is exclusive to YouTube Music Premium subscribers and "
                    "isn't publicly downloadable - try a different video."
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

            if is_cdn_connect_timeout_error(error_text):
                # A connect-timeout to a specific googlevideo media edge.
                # should_use_proxy() DOES escalate this to the proxy tier
                # (see its docstring in youtube.py for the back-and-forth
                # on that decision and the evidence that settled it) - so
                # reaching this branch means direct failed AND the proxy
                # attempt either wasn't available or also failed. The
                # direct-path degradation breaker (cdn_breaker_status(),
                # in admin.py's /admin/status; config.py's CDN_DEGRADED_*
                # knobs) separately tracks repeated direct-path timeouts
                # and, once enough cluster together, skips the doomed
                # ~10s direct attempt entirely for a cooldown window
                # rather than paying for it on every request. Either way
                # this is transient network flakiness, not a bug in this
                # app, so it gets a 503 ("try again") rather than a
                # generic 500.
                logger.warning(f"[DOWNLOAD] CDN edge timeout: {url}: {error_text}")
                raise HTTPException(
                    503,
                    "Couldn't reach YouTube's servers for this video. Please try again in a moment."
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

    tool/tier are NOT set here - by the time this runs, the calling route
    (youtube_separate_route, youtube_analyze_route, etc.) has already
    called set_job_context() before spawning this task, so the tag is
    already inherited. Setting it again here would just be a second call
    site for the same information.
    """
    acquired = False
    try:
        await acquire_slot_or_503(_download_semaphore, f"{tool.lower()}-download")
        acquired = True
        started = time.monotonic()
        # download_audio_to_file is now `async def` and handles its own
        # wall-clock timeout internally via run_in_killable_subprocess -
        # the outer asyncio.wait_for/run_blocking wrapping is gone since
        # a killed process group needs no further guarding here.
        file_path, title = await download_audio_to_file(url, job_id)
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


async def _run_youtube_separation(
    job_id: str,
    url: str,
    *,
    stems: bool,
    model: str,
    overlap: float,
    timeout_seconds: int,
    max_duration_seconds: int,
    hq: bool = False,
):
    """Download, then Demucs. One function for all four YouTube
    separation routes (/youtube/separate, /youtube/separate-hq,
    /youtube/stems, /youtube/stems-hq) - they differ only in which worker
    runs, how the result is stored, and which quality knobs are used.

    The knobs are passed in rather than read from config here, matching
    _queue_separation() in separation.py: they're resolved by the caller
    at SUBMISSION time, so a config change (or the HQ kill switch being
    flipped off) can never retroactively alter a job that's already
    queued. It runs with the settings it was accepted under.

    tool/tier: not set here either, same reasoning as _chain_download's
    docstring above - the calling route already set it before
    asyncio.create_task() spawned this function, so it's already
    inherited by the time this runs.
    """
    suffix = "_HQ" if hq else ""
    tool = ("YOUTUBE_STEMS" if stems else "YOUTUBE_SEPARATE") + suffix
    metric = ("/youtube/stems" if stems else "/youtube/separate") + ("-hq" if hq else "")

    downloaded = await _chain_download(job_id, url, tool, metric)
    if downloaded is None:
        fail_if_unfinished(job_id, "Download failed.")
        return

    file_path, title = downloaded

    if stems:
        # No run_blocking() - run_stem_separation()/run_separation() are
        # async (they await an HTTP call to the RunPod GPU worker), not
        # blocking local subprocess calls. Same reasoning as
        # separation.py's own _queue_separation().
        work = lambda: run_stem_separation(
            file_path, job_id, model, overlap, timeout_seconds, max_duration_seconds,
        )
        on_success = lambda result: mark_stems_complete(job_id, title, result)
        success_detail = lambda result: f"{len(result)} stems"
        generic_error = "Stem separation failed unexpectedly."
    else:
        work = lambda: run_separation(
            file_path, job_id, model, overlap, timeout_seconds, max_duration_seconds,
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
        # False: see separation.py's _queue_separation equivalent comment
        # - the real billed figure is recorded inside separation.py.
        gpu_billed=False,
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

    # Set BEFORE create_task(), not inside _run_youtube_analyze - see the
    # WHAT CHANGED note at the top of this file for why the timing here
    # matters: create_task() copies the context at the moment it's
    # called, and this is also what tags the POST's own HTTP log row.
    set_job_context(tool="YOUTUBE_ANALYZE", tier="standard")

    job_id = create_job(job_type="youtube_analyze", ttl_seconds=YOUTUBE_ANALYZE_JOB_TTL_SECONDS)

    remember_job_tags(job_id)
    asyncio.create_task(_run_youtube_analyze(job_id, url))

    logger.info(f"[YOUTUBE_ANALYZE] job={job_id} queued for {url}")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/youtube/analyze/status/{job_id}")
async def youtube_analyze_status(job_id: str):
    return _tool_status(job_id, "youtube_analyze")


@router.get("/youtube/analyze/result/{job_id}")
async def youtube_analyze_result(job_id: str):
    tag_from_job(job_id)
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

    set_job_context(tool="YOUTUBE_SEPARATE", tier="standard")

    _reject_if_separation_queue_full()

    job_id = create_job(job_type="youtube_separate")

    remember_job_tags(job_id)
    asyncio.create_task(_run_youtube_separation(
        job_id, url,
        stems=False,
        model=SEPARATION_MODEL,
        overlap=SEPARATION_OVERLAP,
        timeout_seconds=DEMUCS_TIMEOUT_SECONDS,
        max_duration_seconds=MAX_SEPARATION_DURATION_SECONDS,
    ))

    logger.info(f"[YOUTUBE_SEPARATE] job={job_id} queued for {url}")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.post(
    "/youtube/separate-hq",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=YOUTUBE_CHAIN_HQ_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=YOUTUBE_CHAIN_HQ_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def youtube_separate_hq_route(url: str = Form(...)):
    """
    High-quality YouTube vocal/instrumental separation - htdemucs_ft at
    raised overlap, same knobs as /separate-hq, with a download bolted
    on the front.

    Deliberately uses job_type="youtube_separate" (not a separate type):
    /separate and /separate-hq already share job_type="separation" for
    the same reason, so every existing status/preview/download route
    works for HQ jobs without a single change. The tier affects HOW the
    job runs, not what shape the result is - and now that the DB has a
    real `tier` column, that same distinction is queryable directly
    instead of needing to be reconstructed from job_type.

    A separate route rather than a `quality` form field because
    rate-limit dependencies are evaluated before the request body is
    read - a Depends() cannot see a Form value, so per-tier limits need
    per-tier routes.
    """
    if not SEPARATION_HQ_ENABLED:
        raise HTTPException(
            503,
            "High quality separation is temporarily unavailable due to server load. "
            "Please use standard separation."
        )

    if not is_valid_youtube_url(url):
        raise HTTPException(400, "Please provide a valid YouTube video URL.")

    set_job_context(tool="YOUTUBE_SEPARATE", tier="hq")

    _reject_if_separation_queue_full()

    job_id = create_job(job_type="youtube_separate")

    remember_job_tags(job_id)
    asyncio.create_task(_run_youtube_separation(
        job_id, url,
        stems=False,
        model=SEPARATION_MODEL_HQ,
        overlap=SEPARATION_OVERLAP_HQ,
        timeout_seconds=DEMUCS_TIMEOUT_SECONDS_HQ,
        max_duration_seconds=MAX_SEPARATION_DURATION_SECONDS_HQ,
        hq=True,
    ))

    logger.info(f"[YOUTUBE_SEPARATE_HQ] job={job_id} queued for {url}")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/youtube/separate/status/{job_id}")
async def youtube_separate_status(job_id: str):
    return _tool_status(job_id, "youtube_separate")


def _resolve_youtube_separate_path(job_id: str, stem: str) -> str:
    tag_from_job(job_id)
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

    set_job_context(tool="YOUTUBE_STEMS", tier="standard")

    _reject_if_separation_queue_full()

    job_id = create_job(job_type="youtube_stems")

    remember_job_tags(job_id)
    asyncio.create_task(_run_youtube_separation(
        job_id, url,
        stems=True,
        model=SEPARATION_MODEL,
        overlap=SEPARATION_OVERLAP,
        timeout_seconds=DEMUCS_TIMEOUT_SECONDS,
        max_duration_seconds=MAX_SEPARATION_DURATION_SECONDS,
    ))

    logger.info(f"[YOUTUBE_STEMS] job={job_id} queued for {url}")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.post(
    "/youtube/stems-hq",
    dependencies=[Depends(partial(
        check_rate_limit,
        max_requests=YOUTUBE_CHAIN_HQ_RATE_LIMIT_MAX_REQUESTS,
        window_seconds=YOUTUBE_CHAIN_HQ_RATE_LIMIT_WINDOW_SECONDS,
    ))],
)
async def youtube_stems_hq_route(url: str = Form(...)):
    """
    High-quality YouTube 4-stem separation - same knobs and kill switch
    as /stems-hq. Shares job_type="youtube_stems" with the standard
    tier so the existing status/preview/download routes need no changes;
    see youtube_separate_hq_route() for the full reasoning.
    """
    if not SEPARATION_HQ_ENABLED:
        raise HTTPException(
            503,
            "High quality separation is temporarily unavailable due to server load. "
            "Please use standard stem separation."
        )

    if not is_valid_youtube_url(url):
        raise HTTPException(400, "Please provide a valid YouTube video URL.")

    set_job_context(tool="YOUTUBE_STEMS", tier="hq")

    _reject_if_separation_queue_full()

    job_id = create_job(job_type="youtube_stems")

    remember_job_tags(job_id)
    asyncio.create_task(_run_youtube_separation(
        job_id, url,
        stems=True,
        model=SEPARATION_MODEL_HQ,
        overlap=SEPARATION_OVERLAP_HQ,
        timeout_seconds=DEMUCS_TIMEOUT_SECONDS_HQ,
        max_duration_seconds=MAX_SEPARATION_DURATION_SECONDS_HQ,
        hq=True,
    ))

    logger.info(f"[YOUTUBE_STEMS_HQ] job={job_id} queued for {url}")
    return JSONResponse({"job_id": job_id, "status": "processing"})


@router.get("/youtube/stems/status/{job_id}")
async def youtube_stems_status(job_id: str):
    tag_from_job(job_id)
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
        "elapsed_seconds": round(time.time() - job["created_at"], 1),
    }


def _resolve_youtube_stems_file(job_id: str, stem: str) -> str:
    tag_from_job(job_id)
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