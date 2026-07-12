"""
youtube.py - Everything the /download route needs for talking to yt_dlp:
retry-with-backoff wrapper, YouTube bot-check / IP-block detection, the
direct-then-proxy fallback strategy (with a circuit breaker for when the
proxy runs out of credit), and a cookie-expiry Discord alert.
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
    IP_BLOCK_MARKERS,
    MAX_VIDEO_DURATION_SECONDS,
    PROXY_CIRCUIT_BREAKER_COOLDOWN_SECONDS,
    COOKIE_EXPIRY_MARKERS,
    COOKIE_ALERT_COOLDOWN_SECONDS,
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
# not anything transient about the network/bot-check/IP. Retrying these
# just burns proxy bandwidth (each attempt still fires 6-7 requests:
# webpage + 4 player-client API calls) and makes the user wait through
# backoff delays for a result that was never going to change. Fail fast
# on these instead.
#
# THIS IS THE ONLY CARVE-OUT that skips the proxy tier entirely (see
# download_with_fallback below) - every other kind of failure, including
# ones not on any list here, gets a proxy attempt. Keep this list narrow
# and only add things that are genuinely permanent/unfixable by any IP or
# retry - a false positive here means "give up early on something that
# maybe could have worked", which is the expensive direction to be wrong
# in.
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

# Errors meaning the video is blocked FOR A SPECIFIC REGION by the
# uploader/rights holder - distinct from PERMANENT_ERROR_MARKERS because a
# proxy exit node in an allowed country genuinely CAN fix this (unlike a
# deleted/private video, where no IP in the world helps). So this is NOT
# added to PERMANENT_ERROR_MARKERS - download_with_fallback still escalates
# to the proxy tier for these. It IS treated like an IP-block for the
# same-IP fail-fast optimization inside extract_info_with_retry, since
# retrying 3x from the SAME IP against a geo-block can never succeed - only
# switching IP (i.e. the proxy tier) has any chance.
GEO_RESTRICTED_MARKERS = (
    "not made this video available in your country",
    "the uploader has not made this video available",
    "content is not available in your country",
    "video is not available in your country",
    "not available in your country",
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
    returned False for a textbook bot-check error. Normalizing both sides
    here prevents that entire class of bug for ANY marker list that
    matches against yt-dlp's error text, not just this one already-known
    case.
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
    """
    Narrow check for the webpage-level bot-check UI page specifically.
    Used ONLY by routes.py to decide the user-facing 503 message wording -
    a raw CDN 403 on the media fetch shouldn't say "YouTube is requiring
    bot verification" since that's not literally what happened. Has NO
    effect on whether the proxy gets tried - see download_with_fallback,
    which now escalates on any non-permanent error.
    """
    normalized = _normalize_error_text(error_text)
    return any(marker in normalized for marker in YT_BOT_CHECK_MARKERS)


def is_geo_restricted_error(error_text: str) -> bool:
    """
    True if the uploader/rights holder has geo-blocked this video for the
    server's current exit country. Used by routes.py to give the user an
    accurate, actionable message instead of a generic failure, and by
    extract_info_with_retry as a same-IP fail-fast signal (see
    GEO_RESTRICTED_MARKERS above for why this is separate from
    is_permanent_error).
    """
    normalized = _normalize_error_text(error_text)
    return any(marker in normalized for marker in GEO_RESTRICTED_MARKERS)


def is_ip_block_error(error_text: str) -> bool:
    """
    True if this error looks like a KNOWN IP-reputation problem - the
    webpage-level bot-check page, a 403 on the actual media fetch (e.g.
    "unable to download video data: HTTP Error 403: Forbidden"), OR a
    geo-restriction (which is also fundamentally an "this IP/region is
    wrong" problem, just enforced by licensing instead of anti-bot
    detection). Used only inside extract_info_with_retry as a fail-fast
    optimization (skip the remaining same-IP retries and hand off to the
    caller faster) - it is NOT what decides whether the proxy tier gets
    tried overall. That decision is exclude-list based (see
    download_with_fallback) so error text NOT in this list still gets a
    proxy attempt, it just doesn't get the fail-fast speed-up.
    """
    normalized = _normalize_error_text(error_text)
    return (
        any(marker in normalized for marker in IP_BLOCK_MARKERS)
        or is_geo_restricted_error(error_text)
    )


def is_permanent_error(error_text: str) -> bool:
    """True if the error means the video itself can never be downloaded -
    no amount of retrying, cookie refreshing, or proxy switching helps.
    This is the ONLY thing that skips the proxy tier - see
    download_with_fallback. Geo-restriction is deliberately NOT here (see
    GEO_RESTRICTED_MARKERS) since a differently-located proxy CAN fix it."""
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


# ---------- COOKIE EXPIRY ALERTING ----------
# yt-dlp reports dead cookies as a WARNING, not an exception - downloads
# keep succeeding anyway via the direct/proxy fallback, so this would
# otherwise pass by completely silently: nothing in the existing
# failure-based alert system ever sees it. We hook into yt-dlp's own
# logger interface to catch the warning text as it's produced and fire a
# throttled Discord alert, without changing whether the download itself
# succeeds or fails.
_cookie_alert_lock = threading.Lock()
_cookie_alert_last_sent = 0.0


def _is_cookie_expiry_warning(message: str) -> bool:
    normalized = _normalize_error_text(message)
    return any(marker in normalized for marker in COOKIE_EXPIRY_MARKERS)


def _maybe_alert_cookie_expiry(message: str):
    """
    Throttled to one alert per COOKIE_ALERT_COOLDOWN_SECONDS regardless of
    how many times the warning fires in that window - yt-dlp repeats this
    exact warning once per player client (ios/android/mweb/web) it checks
    WITHIN a single download, so without this gate one dead-cookie
    download would fire 4+ Discord messages back to back.
    """
    global _cookie_alert_last_sent
    with _cookie_alert_lock:
        now = time.time()
        if now - _cookie_alert_last_sent < COOKIE_ALERT_COOLDOWN_SECONDS:
            return
        _cookie_alert_last_sent = now

    alert_message = (
        "[COOKIES] YouTube account cookies are no longer valid (expired/rotated). "
        "Downloads are still working via the direct/proxy fallback, but re-export "
        "cookies.txt from a logged-in browser session, base64-encode it, and update "
        "YT_COOKIES_B64 in Railway when you get a chance - this alert won't repeat "
        f"for {COOKIE_ALERT_COOLDOWN_SECONDS // 60} min regardless of how many "
        f"downloads hit the same stale cookies in the meantime."
    )
    logger.critical(alert_message)
    alert_now(alert_message)


class _YtdlpAlertLogger:
    """
    Passed as ydl_opts['logger'] so we can inspect yt-dlp's own log lines
    (which include the cookie-expiry warning) as they're produced, without
    yt-dlp raising an exception for it - downloads succeed anyway via
    fallback, so nothing would otherwise catch this. Every message is
    still printed exactly as before (Railway's log capture is unaffected,
    same verbose [debug]/[youtube]/WARNING output you're used to seeing) -
    this only ADDS a side-channel check on top, it doesn't suppress or
    alter yt-dlp's normal output in any way.
    """
    def debug(self, msg):
        print(msg)
        if _is_cookie_expiry_warning(msg):
            _maybe_alert_cookie_expiry(msg)

    def warning(self, msg):
        print(msg)
        if _is_cookie_expiry_warning(msg):
            _maybe_alert_cookie_expiry(msg)

    def error(self, msg):
        print(msg)


# Single shared instance - it holds no per-request state (the cooldown
# gate is already its own module-level lock/timestamp), so one instance
# passed into every ydl_opts dict is correct and avoids extra allocation
# per request.
ytdlp_alert_logger = _YtdlpAlertLogger()


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

            if is_ip_block_error(error_text):
                # Retrying from the SAME IP (direct-tier or already inside
                # a proxy-tier call) was never going to produce a different
                # result on a KNOWN bot-check/403/geo-block pattern - stop
                # burning attempts on this IP immediately (saves ~4.5s of
                # pointless backoff) instead of doing 2 more retries that
                # will fail identically. The CALLER (download_with_fallback)
                # decides whether to escalate to a different IP via the
                # proxy.
                logger.warning(
                    f"Attempt {attempt}: IP-block/bot-check/geo-restriction error "
                    f"detected - not retrying on the same IP: {error_text}"
                )
                raise

            # Unrecognized error text - could be a genuine transient blip
            # OR a new/unknown YouTube error pattern we haven't catalogued
            # yet. Either way, give it the full backoff-retry treatment
            # here (cheap, no proxy involved) before download_with_fallback
            # decides whether to also try the proxy tier.
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
               Free. Retries up to YT_DLP_MAX_ATTEMPTS with backoff first
               (see extract_info_with_retry) - genuine transient blips get
               a chance to self-resolve here before any money is spent.

      Tier 2 - PROXY + cookies, tried for ANY Tier 1 failure EXCEPT a
               confirmed permanent error (video deleted/private/copyright -
               see is_permanent_error). This is intentionally an
               EXCLUDE-list, not an allow-list: known IP-block errors
               (bot-check page, media-fetch 403s, geo-restriction) escalate,
               same as before, but so does ANY error text we haven't seen
               or catalogued yet. A hardcoded marker list can only ever
               recognize failure patterns we already know about - as new
               yt-dlp/YouTube error wording shows up over time, this way
               it still gets a proxy attempt instead of silently failing
               the same way every time until someone notices in the logs
               and adds a new marker string. Permanent errors are the one
               deliberate carve-out, since no IP change fixes a deleted
               video - that would just be pure wasted proxy spend.

    If tier 2 itself fails with what looks like a proxy billing/quota
    error, trip the circuit breaker (see _trip_proxy_circuit_breaker) so
    subsequent requests skip proxy entirely during the cooldown instead of
    each one separately re-discovering the proxy is dead. The request
    still fails in that moment (nothing left to fall back to), but every
    request AFTER it degrades gracefully to direct-only instead of paying
    the proxy's connection-failure latency every single time.
    """
    try:
        return extract_info_with_retry(base_ydl_opts, url)
    except Exception as e:
        first_error = str(e)

        if is_permanent_error(first_error):
            # The one deliberate carve-out - no IP in the world fixes a
            # deleted/private/copyright-blocked video, so don't spend
            # proxy money finding that out a second time.
            raise

        if not proxy_url:
            logger.warning("[PROXY] Direct attempt failed and no proxy is configured - failing as-is.")
            raise

        if not proxy_available():
            logger.warning(
                "[PROXY] Direct attempt failed, but proxy circuit breaker is "
                "currently OPEN (likely out of credit) - failing as-is instead of "
                "retrying a proxy known to be down."
            )
            raise

        logger.warning(f"[PROXY] Direct attempt failed ({first_error[:200]}) - retrying via proxy...")
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
            # own is_bot_check_error() classification on top of this for
            # the user-facing message).
            raise