"""
youtube.py - Everything the /download route needs for talking to yt_dlp:
retry-with-backoff wrapper and YouTube bot-check detection.
"""
import time
import yt_dlp

from config import (
    logger,
    YT_DLP_MAX_ATTEMPTS,
    YT_DLP_BASE_BACKOFF_SECONDS,
    YT_BOT_CHECK_MARKERS,
    MAX_VIDEO_DURATION_SECONDS,
)


class VideoTooLongError(Exception):
    """Raised when a video's duration exceeds MAX_VIDEO_DURATION_SECONDS.
    Caught separately in routes.py to return a clean 400 instead of a
    generic 500, since this is a normal, expected rejection - not a bug."""
    def __init__(self, duration_seconds: int, limit_seconds: int):
        self.duration_seconds = duration_seconds
        self.limit_seconds = limit_seconds
        super().__init__(
            f"Video is {duration_seconds // 60} min long, which exceeds the "
            f"{limit_seconds // 60} min limit."
        )

# Errors where retrying can NEVER help - the video itself is the blocker,
# not anything transient about the network/bot-check. Retrying these just
# burns proxy bandwidth (each attempt still fires 6-7 requests: webpage +
# 4 player-client API calls) and makes the user wait through backoff delays
# for a result that was never going to change. Fail fast on these instead.
PERMANENT_ERROR_MARKERS = (
    "video unavailable",
    "this video is not available",
    "private video",
    "video has been removed",
    "account associated with this video has been terminated",
    "this video is no longer available",
    "video is no longer available",
    "copyright",
    "this video does not exist",
    "unable to extract video data",
)


def is_bot_check_error(error_text: str) -> bool:
    lowered = error_text.lower()
    return any(marker in lowered for marker in YT_BOT_CHECK_MARKERS)


def is_permanent_error(error_text: str) -> bool:
    """True if the error means the video itself can never be downloaded -
    no amount of retrying, cookie refreshing, or proxy switching helps."""
    lowered = error_text.lower()
    return any(marker in lowered for marker in PERMANENT_ERROR_MARKERS)


def check_video_duration(ydl_opts: dict, url: str):
    """
    Fetches ONLY metadata (download=False) - no video/audio data is
    transferred, so this is cheap on proxy bandwidth compared to a full
    download - and raises VideoTooLongError before the real download starts
    if the video exceeds MAX_VIDEO_DURATION_SECONDS. Called once, no retry
    loop: if this metadata fetch itself fails (bot-check, unavailable,
    etc.), we let extract_info_with_retry's normal retry/error handling
    deal with it moments later on the real attempt instead of duplicating
    that logic here.

    NOTE: same threading rule as extract_info_with_retry - must be called
    via utils.run_blocking(), never directly from `async def` code.
    """
    if MAX_VIDEO_DURATION_SECONDS is None:
        return

    try:
        with yt_dlp.YoutubeDL({**ydl_opts, 'quiet': True, 'verbose': False}) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as e:
        # Don't fail the request here on a metadata-check error - just skip
        # the duration check and let the real download attempt (with its
        # proper retry/error handling) be the source of truth.
        logger.warning(f"Duration pre-check failed (non-fatal, proceeding to real download): {e}")
        return

    duration = info.get('duration') if info else None
    if duration and duration > MAX_VIDEO_DURATION_SECONDS:
        logger.warning(
            f"Rejecting download: video duration {duration}s exceeds "
            f"MAX_VIDEO_DURATION_SECONDS={MAX_VIDEO_DURATION_SECONDS}s for URL: {url}"
        )
        raise VideoTooLongError(duration, MAX_VIDEO_DURATION_SECONDS)


def extract_info_with_retry(ydl_opts: dict, url: str):
    """
    NOTE: this function is fully synchronous/blocking (yt_dlp + time.sleep
    backoff). It must always be called via utils.run_blocking() from an
    async endpoint - never awaited or called directly from `async def` code
    - or it will freeze the event loop for the whole server during the
    retry backoff sleeps and the download itself.
    """
    last_exception = None

    for attempt in range(1, YT_DLP_MAX_ATTEMPTS + 1):
        try:
            logger.info(f"yt_dlp extract_info attempt {attempt}/{YT_DLP_MAX_ATTEMPTS} for URL: {url}")
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
            logger.info(f"yt_dlp extract_info succeeded on attempt {attempt}")
            return info
        except Exception as e:
            last_exception = e
            error_text = str(e)

            if is_permanent_error(error_text):
                # No point burning 2 more attempts (and 2 more rounds of
                # proxy bandwidth) on something retrying can't fix - fail
                # immediately instead of exhausting YT_DLP_MAX_ATTEMPTS.
                logger.warning(
                    f"Attempt {attempt}: permanent error detected (video "
                    f"unavailable/private/removed) - not retrying: {error_text}"
                )
                raise

            if is_bot_check_error(error_text):
                logger.warning(f"Attempt {attempt}: YouTube bot verification triggered.")
            else:
                logger.warning(f"Attempt {attempt} failed: {error_text}")

            if attempt < YT_DLP_MAX_ATTEMPTS:
                backoff = YT_DLP_BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.warning(f"Retrying in {backoff:.1f}s...")
                time.sleep(backoff)
            else:
                logger.error(f"All {YT_DLP_MAX_ATTEMPTS} attempts failed. Last error: {error_text}")

    raise last_exception