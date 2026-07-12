"""
youtube.py - Everything the /download route needs for talking to yt_dlp:
retry-with-backoff wrapper, YouTube bot-check detection, and the
direct-then-proxy fallback strategy (with a circuit breaker for when the
proxy runs out of credit).
"""
import re
import time
import threading
from typing import Optional

import yt_dlp

from config import (
    logger,
    YT_DLP_MAX_ATTEMPTS,
    YT_DLP_BASE_BACKOFF_SECONDS,
    YT_BOT_CHECK_MARKERS,
    MAX_VIDEO_DURATION_SECONDS,
    PROXY_CIRCUIT_BREAKER_COOLDOWN_SECONDS,
)
from monitoring import alert_now


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
    # Safety net for junk/garbage/non-YouTube URLs that slip past
    # is_valid_youtube_url() below for any reason - these are yt-dlp's own
    # error strings when it can't even recognize the URL, and retrying
    # them 3x with backoff (or worse, trying a proxy) is pure waste since
    # no amount of retrying fixes a URL that was never valid.
    "unsupported url",
    "is not a valid url",
    "unable to download webpage",
)

# Errors that indicate the PROXY itself is out of credit/quota, as opposed
# to a normal connection hiccup. Provider wording varies a lot, so this
# list is intentionally broad - false positives just mean the circuit
# breaker trips a bit eagerly (cheap: falls back to direct-only for a
# while), which is far better than false negatives (silently retrying a
# dead proxy on every single request).
PROXY_QUOTA_ERROR_MARKERS = (
    "insufficient balance",
    "insufficient funds",
    "insufficient credit",
    "quota exceeded",
    "no credit",
    "out of credit",
    "payment required",
    "account suspended",
    "proxy authentication failed",  # several providers reuse this for "balance = 0", not just bad creds
    "407",  # HTTP 407 Proxy Authentication Required - overloaded by some providers for "no balance"
)


def _normalize_error_text(error_text: str) -> str:
    """
    Lowercases AND normalizes "smart"/typographic punctuation to its plain
    ASCII equivalent before marker-matching.

    Real-world case that motivated this: YouTube's actual bot-check message
    is "Sign in to confirm you\u2019re not a bot" - a CURLY apostrophe
    (U+2019), not a straight one ('). Our marker strings use straight
    apostrophes. A plain substring match on the two silently NEVER matches
    even though they're visually identical, which meant is_bot_check_error()
    returned False for a textbook bot-check error - the proxy fallback
    never fired and the request surfaced as a raw 500 instead of a clean
    503. Normalizing both sides here prevents that entire class of bug for
    ANY marker list that matches against yt-dlp's error text, not just this
    one already-known case.
    """
    lowered = error_text.lower()
    return (
        lowered
        .replace("\u2019", "'")  # right single quotation mark -> straight apostrophe
        .replace("\u2018", "'")  # left single quotation mark -> straight apostrophe
        .replace("\u201c", '"')  # left double quotation mark -> straight quote
        .replace("\u201d", '"')  # right double quotation mark -> straight quote
    )


# Matches the handful of real YouTube URL shapes we actually expect:
# youtube.com/watch?v=, youtu.be/, youtube.com/shorts/, m.youtube.com,
# music.youtube.com. This is deliberately a cheap SHAPE check, not full
# validation - it's meant to catch obvious junk (empty strings, random
# text, non-YouTube domains, typos) BEFORE spending a single yt-dlp call,
# a download semaphore slot, or 3 retries-with-backoff on something that
# was never going to work. It is NOT meant to catch every edge case (e.g.
# a syntactically valid but deleted/private video) - those still get
# handled downstream by is_permanent_error() during the real yt-dlp call,
# same as before.
_YOUTUBE_URL_PATTERN = re.compile(
    r"^https?://(www\.|m\.|music\.)?(youtube\.com/(watch\?.*v=|shorts/|live/)|youtu\.be/)",
    re.IGNORECASE,
)


def is_valid_youtube_url(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False
    return bool(_YOUTUBE_URL_PATTERN.match(url.strip()))


def is_bot_check_error(error_text: str) -> bool:
    normalized = _normalize_error_text(error_text)
    return any(marker in normalized for marker in YT_BOT_CHECK_MARKERS)


def is_permanent_error(error_text: str) -> bool:
    """True if the error means the video itself can never be downloaded -
    no amount of retrying, cookie refreshing, or proxy switching helps."""
    normalized = _normalize_error_text(error_text)
    return any(marker in normalized for marker in PERMANENT_ERROR_MARKERS)


def is_proxy_quota_error(error_text: str) -> bool:
    normalized = _normalize_error_text(error_text)
    return any(marker in normalized for marker in PROXY_QUOTA_ERROR_MARKERS)


# ---------- PROXY CIRCUIT BREAKER ----------
# In-memory, per-instance (same pattern as monitoring.py). Once tripped,
# proxy_available() returns False until the cooldown passes, so
# download_with_fallback() skips straight to direct-only instead of paying
# the latency cost of trying (and failing against) a proxy that's known to
# be dead.
_proxy_lock = threading.Lock()
_proxy_disabled_until = 0.0


def proxy_available() -> bool:
    with _proxy_lock:
        return time.time() >= _proxy_disabled_until


def _trip_proxy_circuit_breaker():
    global _proxy_disabled_until
    with _proxy_lock:
        _proxy_disabled_until = time.time() + PROXY_CIRCUIT_BREAKER_COOLDOWN_SECONDS
    cooldown_min = PROXY_CIRCUIT_BREAKER_COOLDOWN_SECONDS // 60
    message = (
        f"[PROXY] Circuit breaker TRIPPED - proxy looks out of credit/quota. "
        f"Disabling proxy for {cooldown_min} min; falling back to direct "
        f"(no-proxy, cookies-only) requests in the meantime. Top up the "
        f"proxy provider balance to restore full bot-check resilience."
    )
    logger.critical(message)
    # Fired immediately - don't wait for monitoring.record_result()'s
    # failure-threshold/cooldown logic. This is a single distinct event
    # (proxy billing) worth knowing about the moment it happens, not after
    # N requests have already failed downstream of it.
    alert_now(message)


def reset_proxy_circuit_breaker():
    """Manual override - e.g. call this after topping up proxy credit,
    from a future admin endpoint, instead of waiting out the full cooldown."""
    global _proxy_disabled_until
    with _proxy_lock:
        _proxy_disabled_until = 0.0
    logger.info("[PROXY] Circuit breaker manually reset - proxy re-enabled.")


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


def download_with_fallback(base_ydl_opts: dict, url: str, proxy_url: Optional[str]):
    """
    Tiered download strategy, cheapest option first:

      Tier 1 - DIRECT (no proxy), cookies still attached if available.
               Free. Succeeds for most normal, spread-out traffic, since
               cookies alone clear the plain "sign in to confirm you're not
               a bot" check most of the time.

      Tier 2 - PROXY + cookies, tried ONLY if tier 1 failed with a
               bot-check / format-restriction error specifically (not on
               unrelated errors - is_permanent_error already short-circuits
               those inside extract_info_with_retry before we'd ever
               consider a proxy retry). This is the paid fallback, not the
               default, so normal traffic doesn't burn proxy bandwidth for
               no reason.

    If tier 2 itself fails with what looks like a proxy billing/quota
    error, trip the circuit breaker (see _trip_proxy_circuit_breaker) so
    subsequent requests skip proxy entirely during the cooldown instead of
    each one separately re-discovering the proxy is dead. The request
    still fails in that moment (nothing left to fall back to except a
    clean bot-check error), but every request AFTER it degrades gracefully
    to direct-only instead of paying the proxy's connection-failure
    latency every single time.
    """
    try:
        return extract_info_with_retry(base_ydl_opts, url)
    except Exception as e:
        first_error = str(e)

        if not is_bot_check_error(first_error):
            # Not a bot-check/format issue (e.g. permanent error, network
            # blip) - a proxy wouldn't fix this, don't spend money on it.
            raise

        if not proxy_url:
            logger.warning("[PROXY] Direct attempt hit bot-check and no proxy is configured - failing as-is.")
            raise

        if not proxy_available():
            logger.warning(
                "[PROXY] Direct attempt hit bot-check, but proxy circuit breaker is "
                "currently OPEN (likely out of credit) - failing as-is instead of "
                "retrying a proxy known to be down."
            )
            raise

        logger.warning("[PROXY] Direct attempt hit bot-check - retrying via proxy...")
        proxied_opts = {**base_ydl_opts, 'proxy': proxy_url}
        try:
            result = extract_info_with_retry(proxied_opts, url)
            logger.info("[PROXY] Proxy retry succeeded.")
            return result
        except Exception as proxy_error:
            proxy_error_text = str(proxy_error)
            if is_proxy_quota_error(proxy_error_text):
                _trip_proxy_circuit_breaker()
            else:
                logger.warning(f"[PROXY] Proxy retry also failed (non-quota error): {proxy_error_text}")
            # Whatever the proxy attempt raised is the most informative
            # error to surface - propagate it (routes.py still applies its
            # own is_bot_check_error() classification on top of this).
            raise