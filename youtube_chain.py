"""
youtube_chain.py - Shared "download a YouTube URL to a local audio file"
step used by every /youtube/* chained tool (/youtube/analyze,
/youtube/separate, /youtube/stems).

WHY THIS EXISTS SEPARATELY FROM /download: routes.py's existing
/download route returns audio as a base64 JSON payload to the browser -
the user gets bytes, not a server-side file. The chained tools need the
opposite: audio that stays on the VPS's disk so it can be fed straight
into analysis or Demucs, with no round trip through the user's browser
at all. That's a different enough contract to warrant its own function
rather than bolting a "return a path instead" mode onto the existing
route.

WHY WAV, ALWAYS: separation and analysis both want the cleanest signal
they can get. Downloading as WAV avoids adding a second lossy encode on
top of whatever YouTube's own source compression already did (which an
mp3-then-reprocess chain would do) - this module always requests WAV
from yt-dlp regardless of what the final chained tool's output format
will be.

ERROR CLASSIFICATION IS DUPLICATED FROM routes.py's /download ROUTE ON
PURPOSE, WITH A DIFFERENT SHAPE: /download runs synchronously and raises
HTTPException directly back to the waiting request. Every /youtube/*
chained tool instead runs as a background job (same async job-flow
pattern as /separate, /convert, etc.) - by the time the download step
fails, the HTTP response with the job_id has already been sent, so there
is no request left to raise an HTTPException into. classify_download_error()
below returns a plain message STRING instead, which the caller passes to
jobs.mark_failed(). The underlying classifier functions
(is_permanent_error, is_geo_restricted_error, etc.) are still imported
from youtube.py - only the "what do we DO with the classification" logic
differs.
"""
import os
import uuid
from typing import Tuple

from config import DOWNLOAD_WALL_CLOCK_TIMEOUT_SECONDS, logger, UPLOAD_DIR
from utils import run_in_killable_subprocess
from youtube import (
    download_with_fallback,
    is_permanent_error,
    is_geo_restricted_error,
    is_age_restricted_error,
    is_members_only_error,
    is_not_yet_live_error,
    is_bot_check_error,
    VideoTooLongError,
    proxy_available,
    get_cookie_accounts,
    ytdlp_alert_logger,
    extract_video_id,
)
from cache import get_cached_audio, put_cached_audio


class ChainDownloadError(Exception):
    """Raised when the download half of a chained /youtube/* tool fails,
    with an already-user-facing message. Caller passes str(e) straight
    to jobs.mark_failed() - no further translation needed."""
    pass


def classify_download_error(error_text: str) -> str:
    """
    Maps a raw yt-dlp error string to the same user-facing messages
    /download already uses for each failure category, just returned as
    text instead of raised as an HTTPException (see module docstring for
    why). Falls through to a generic message for anything unrecognized,
    same as /download's final `except Exception` branch.
    """
    if is_permanent_error(error_text):
        return ("This video is unavailable - it may have been deleted, made private, "
                "or removed for copyright reasons. Please try a different video.")
    if is_geo_restricted_error(error_text):
        return ("This video is restricted by the uploader to specific countries and "
                "isn't available from our server's location.")
    if is_age_restricted_error(error_text):
        return ("This video is age-restricted by YouTube and requires a verified "
                "account to view. We're not able to download age-restricted content.")
    if is_members_only_error(error_text):
        return "This video is exclusive to that channel's paid members and isn't publicly downloadable."
    if is_not_yet_live_error(error_text):
        return ("This video is a scheduled premiere or live stream that hasn't started yet - "
                "try again once it's live.")
    if is_bot_check_error(error_text):
        return ("YouTube is currently requiring bot verification or restricting available "
                "formats for this video. Please try again in a few minutes.")
    return f"Could not download this video: {error_text}"


async def download_audio_to_file(url: str, job_id: str) -> Tuple[str, str]:
    """
    Downloads url as WAV to a local file and returns (file_path, title).

    Fully synchronous/blocking (same as youtube.py's download_with_fallback
    itself) - MUST be dispatched via utils.run_blocking() from the async
    route, same threading rule as every other blocking call in this
    codebase.

    Raises ChainDownloadError with an already-classified, user-facing
    message on any failure - video too long, blocked, geo-restricted,
    or any other yt-dlp failure.

    Uses job_id (not a fresh uuid) for the temp filename so a failed
    chained job's leftover download, if cleanup somehow doesn't run,
    is still identifiable by the same id shown in job status - same
    naming convention every other tool in this codebase already uses.

    CACHE CHECK (added 2026-08-13): every /youtube/* chained tool
    (analyze, separate, separate-hq, stems, stems-hq) used to call this
    with no cache check at all - only routes.py's plain /download route
    ever consulted get_cached_audio(). That meant re-running any chained
    tool on a video someone had already downloaded (via /download OR any
    other chained tool) paid for a full duplicate proxy/direct download
    of the same googlevideo.com bytes - confirmed via proxy usage logs
    showing googlevideo.com media transfer as ~76% of total proxy
    bandwidth, the single largest and most avoidable-when-duplicated
    cost. video_id is cached under format="wav" specifically, since this
    function always requests WAV regardless of what the calling tool's
    final output format will be (see module docstring above) - so a hit
    here is valid for every chained tool, not just one.
    """
    temp_path = os.path.join(UPLOAD_DIR, f"{job_id}_ytchain.%(ext)s")
    output_file = os.path.join(UPLOAD_DIR, f"{job_id}_ytchain.wav")

    video_id = extract_video_id(url)
    if video_id:
        try:
            cached_audio, cached_title = get_cached_audio(video_id, "wav")
        except Exception as cache_err:
            logger.warning(f"[YOUTUBE_CHAIN] Job {job_id}: cache lookup failed (non-fatal): {cache_err}")
            cached_audio, cached_title = None, None

        if cached_audio:
            try:
                with open(output_file, "wb") as f:
                    f.write(cached_audio)
                logger.info(
                    f"[YOUTUBE_CHAIN] Job {job_id}: cache HIT for {video_id} "
                    f"({len(cached_audio)} bytes) - skipped download entirely"
                )
                return output_file, cached_title or "Unknown"
            except OSError as e:
                # Fall through to a real download rather than failing the
                # job over a disk write error on what was meant to be a
                # fast path - same "cache failures never break the
                # primary flow" guarantee cache.py itself follows.
                logger.warning(
                    f"[YOUTUBE_CHAIN] Job {job_id}: failed to write cached audio to disk, "
                    f"falling back to a real download: {e}"
                )

    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': temp_path,
        'quiet': False,
        'verbose': True,
        'noplaylist': True,
        'ffmpeg_location': '/usr/bin/ffmpeg',
        # Equivalent of yt-dlp's --force-ipv4 CLI flag. Same reasoning as
        # routes.py's /download route: the VPS HOST has real IPv6 now
        # (VPSDime assigned it 2026-08-03), but Docker does not forward
        # IPv6 into this container by default, confirmed via
        # `docker exec audioforges-api curl -6 ...` still failing with
        # "Network is unreachable" on every address. Must stay in sync
        # with routes.py's identical option until Docker's IPv6
        # networking is separately configured (tracked as a future task,
        # not done here - editing /etc/docker/daemon.json is a bigger
        # infra change with its own risk, out of scope for this fix).
        'source_address': '0.0.0.0',
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
            'preferredcodec': 'wav',
            'preferredquality': '192',
        }],
        'remote_components': ['ejs:github'],
        'logger': ytdlp_alert_logger,
        # No progress_hooks here deliberately - those exist in /download
        # to drive a live download-progress UI on that specific route.
        # The chained tools poll job STATUS instead, same as every other
        # background job in this app, so a separate progress channel
        # would be unused plumbing.
    }

    proxy_url = os.environ.get('YT_PROXY_URL')
    logger.info(
        f"[YOUTUBE_CHAIN] Job {job_id}: accounts_available={len(get_cookie_accounts())} "
        f"proxy_configured={bool(proxy_url)} "
        f"circuit_breaker={'OPEN' if not proxy_available() else 'CLOSED'} url={url}"
    )

    # 'logger' is a live object and won't survive a JSON boundary -
    # download_worker.py reconstructs its own ytdlp_alert_logger
    # internally. No progress_hooks here (chained tools poll job status
    # instead), so nothing else needs stripping.
    serializable_ydl_opts = {k: v for k, v in ydl_opts.items() if k != "logger"}

    result = await run_in_killable_subprocess(
        serializable_ydl_opts, url, proxy_url,
        DOWNLOAD_WALL_CLOCK_TIMEOUT_SECONDS, job_id,
    )

    if result["ok"]:
        title = result["title"]
    elif result["kind"] == "too_long":
        raise ChainDownloadError(result["error"])
    elif result["kind"] == "timeout":
        raise ChainDownloadError("This download is taking too long. Please try again.")
    elif result["kind"] == "crashed":
        raise ChainDownloadError(f"Could not download this video: {result['error']}")
    else:
        raise ChainDownloadError(classify_download_error(result["error"]))

    if not os.path.exists(output_file):
        logger.error(f"[YOUTUBE_CHAIN] Job {job_id}: expected output file not found after download: {output_file}")
        raise ChainDownloadError("The audio file was not produced by the downloader.")

    if video_id:
        try:
            with open(output_file, "rb") as f:
                audio_bytes = f.read()
            put_cached_audio(video_id, "wav", audio_bytes, title)
        except Exception as cache_err:
            # Never let a caching failure break a download that already
            # succeeded - identical guarantee to routes.py's /download.
            logger.warning(f"[YOUTUBE_CHAIN] Job {job_id}: cache save failed (non-fatal): {cache_err}")

    return output_file, title