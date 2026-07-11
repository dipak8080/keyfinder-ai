"""
youtube.py - Everything the /download route needs for talking to yt_dlp:
retry-with-backoff wrapper and YouTube bot-check detection.
"""
import time
import yt_dlp

from config import logger, YT_DLP_MAX_ATTEMPTS, YT_DLP_BASE_BACKOFF_SECONDS, YT_BOT_CHECK_MARKERS

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