"""
youtube.py - Everything the /download route needs for talking to yt_dlp:
retry-with-backoff wrapper, YouTube bot-check / IP-block detection, the
direct-then-proxy fallback strategy (with a circuit breaker for when the
proxy runs out of credit), and a per-account cookie-expiry Discord alert.
"""
import os
import re
import time
import base64
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
    COOKIE_EXPIRY_ALERT_THRESHOLD,
    COOKIE_EXPIRY_ALERT_WINDOW_SECONDS,
    COOKIE_ALERT_COOLDOWN_SECONDS,
    YT_COOKIES_PATH_DEFAULT,
    COOKIE_ACCOUNT_2_B64_ENV,
    COOKIE_ACCOUNT_3_B64_ENV,
    COOKIE_ACCOUNT_2_PATH,
    COOKIE_ACCOUNT_3_PATH,
    COOKIE_ACCOUNT_COOLDOWN_SECONDS,
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
    "copyright claim",  # narrowed from bare "copyright" - the bare word
                         # risks false-matching a message that merely
                         # MENTIONS copyright without actually being a
                         # takedown (e.g. a disclaimer sentence), which
                         # would wrongly give up on a retryable error.
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
    # Connection-level failures, not billing - a proxy provider outage
    # (their gateway can't reach the destination at all) looks
    # different from a quota error but is exactly as unrecoverable
    # within the current request. Without these, an outage like this
    # never trips the breaker, so every subsequent request keeps
    # paying the ~30s proxy-retry cost until the outage ends on its
    # own - broad-matching here is the same "false positive is cheap,
    # false negative is expensive" reasoning as the rest of this list.
    "no_host_connection",
    "tunnel connection failed",
    "unable to connect to proxy",
    "502",
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

# Errors meaning YouTube requires an age-verified account to view this
# specific video. Distinct from PERMANENT_ERROR_MARKERS because a
# DIFFERENT cookie account (if one happens to be age-verified) genuinely
# could fix this - unlike a deleted/private video, no IP or cookie in the
# world helps there. But also distinct from a normal retryable error:
# retrying the SAME account 3x with backoff, or trying via proxy, can
# never succeed either - only a different, age-verified account can. So
# this fails fast within extract_info_with_retry (no pointless same-
# account backoff) AND skips the proxy tier in download_with_fallback
# (proxy fixes IP reputation, not account identity), but still allows
# rotation to try the next cookie account, if any remain.
AGE_RESTRICTED_MARKERS = (
    "sign in to confirm your age",
    "age-restricted",
    "this video may be inappropriate for some users",
)

# Errors meaning this video is locked behind a YouTube channel membership
# (a paid subscription to that specific channel), not a general YouTube
# account issue. Same shape as age-restriction: a DIFFERENT cookie
# account (one that happens to be a paying member of that channel) could
# fix it, but the SAME account retried 3x can't, and a different IP via
# proxy does nothing for a membership requirement either.
MEMBERS_ONLY_MARKERS = (
    "join this channel to get access to members-only content",
    "this video is available to this channel's members",
    "members-only content",
)

# Errors meaning the video is a scheduled premiere/live stream that hasn't
# started yet. Genuinely unfixable RIGHT NOW by any cookie account, IP, or
# retry count - but not a "gone forever" error either (it will start
# working once the stream actually begins), so it gets its own fail-fast
# category rather than being lumped in with permanently-dead videos.
NOT_YET_LIVE_MARKERS = (
    "this live event will begin in",
    "premieres in",
    "this video is a live stream that has not yet started",
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


# Extracts just the 11-character video ID, used as the cache key in
# cache.py. Deliberately permissive about WHERE the ID appears (query
# param ?v=, or the path segment for youtu.be/shorts/live URLs) since
# is_valid_youtube_url() above already confirmed this is a real YouTube
# URL shape before this is ever called.
_YOUTUBE_ID_PATTERN = re.compile(
    r"(?:v=|youtu\.be/|shorts/|live/)([a-zA-Z0-9_-]{11})"
)


def extract_video_id(url: str) -> Optional[str]:
    """Returns the 11-character YouTube video ID, or None if it can't be
    found (caller should treat that as "not cacheable", not an error -
    the download itself doesn't depend on this)."""
    if not url:
        return None
    match = _YOUTUBE_ID_PATTERN.search(url)
    return match.group(1) if match else None


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


def is_age_restricted_error(error_text: str) -> bool:
    """
    True if YouTube is blocking this video behind an age-verification
    wall. Used by extract_info_with_retry as a fail-fast signal (same-
    account retries can't fix this) and by download_with_fallback to skip
    the proxy tier specifically (a different IP doesn't fix an account-
    identity requirement) while still allowing rotation to the next
    cookie account, if one happens to be age-verified.
    """
    normalized = _normalize_error_text(error_text)
    return any(marker in normalized for marker in AGE_RESTRICTED_MARKERS)


def is_members_only_error(error_text: str) -> bool:
    """
    True if this video is locked behind a YouTube channel membership.
    Same handling shape as age-restriction: fail fast on same-account
    retries, skip the proxy tier, but allow account rotation to try a
    different (possibly member) account.
    """
    normalized = _normalize_error_text(error_text)
    return any(marker in normalized for marker in MEMBERS_ONLY_MARKERS)


def is_not_yet_live_error(error_text: str) -> bool:
    """
    True if this is a scheduled premiere/live stream that hasn't started.
    No cookie account, IP, or retry count fixes this right now - it's
    fundamentally a timing issue, not an auth or availability one. Treated
    as fail-fast + skip-proxy, same as age-restriction/members-only, since
    none of those tools can make a stream start early.
    """
    normalized = _normalize_error_text(error_text)
    return any(marker in normalized for marker in NOT_YET_LIVE_MARKERS)


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
    download_with_fallback. Geo-restriction, age-restriction, members-only,
    and not-yet-live are deliberately NOT here (see their own marker lists
    above) since a different cookie account or timing CAN fix those."""
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


# ---------- COOKIE EXPIRY ALERTING (attributed to a specific account) ----------
# yt-dlp reports dead cookies as a WARNING, not an exception - downloads
# keep succeeding anyway via the direct/proxy fallback, so this would
# otherwise pass by completely silently: nothing in the existing
# failure-based alert system ever sees it. We hook into yt-dlp's own
# logger interface to catch the warning text as it's produced.
#
# IMPORTANT: yt-dlp's cookie-validity check is a HEURISTIC, not ground
# truth - it can flag cookies invalid for one specific video/client combo
# while the SAME cookies authenticate other downloads successfully
# moments later (observed in production logs). Alerting on the first
# occurrence produced false-alarm pings for cookies that were actually
# fine. Instead: track occurrences PER ACCOUNT in a rolling window and
# only alert once COOKIE_EXPIRY_ALERT_THRESHOLD occurrences happen for
# THAT SPECIFIC ACCOUNT within COOKIE_EXPIRY_ALERT_WINDOW_SECONDS - a
# single flaky check on an odd video won't cross that bar, but genuinely
# dead/rotated cookies will, since every subsequent authenticated download
# using that same account keeps hitting the same warning in quick
# succession.
#
# Tracking is PER ACCOUNT (a dict keyed by account path) rather than one
# shared global counter - with 3 cookie accounts in rotation, a global
# counter couldn't tell you WHICH account was actually the problem, only
# that "some cookie somewhere" looked dead. See _set_active_account /
# _get_active_account below for how the currently-in-use account gets
# attached to each warning as it's observed.
_cookie_alert_lock = threading.Lock()
_cookie_warning_events: dict = {}   # account_label -> list of timestamps (rolling window)
_cookie_alert_last_sent: dict = {}  # account_label -> timestamp of last alert (per-account cooldown)


def _is_cookie_expiry_warning(message: str) -> bool:
    normalized = _normalize_error_text(message)
    return any(marker in normalized for marker in COOKIE_EXPIRY_MARKERS)


# Per-attempt signal: was a "cookies are no longer valid" warning seen
# during THIS specific yt-dlp call, and which cookie account was active
# while it happened? Both are thread-local because extract_info_with_retry
# runs inside a worker thread from the pool (one thread per concurrent
# request), so this correctly scopes to the request currently running on
# that thread without concurrent downloads stepping on each other's state.
#
# _cookie_flagged_dead is what tells download_with_fallback "this failure
# is a cookie-IDENTITY problem, rotate accounts" versus "this is an
# IP-reputation problem, escalate to proxy instead" - both failure modes
# can produce the identical outer error text ("Sign in to confirm you're
# not a bot"), so the flag (set the moment the cookie-specific warning
# line appears) is the only reliable way to tell them apart. It's ALSO
# now used inside extract_info_with_retry itself (see below) to skip
# pointless same-account backoff retries the instant we already know this
# specific cookie session is dead - a pure cost optimization, changes no
# outcome, since retrying the identical dead cookiefile 2 more times
# within the same account attempt was never going to succeed anyway.
#
# _active_account is what lets _YtdlpAlertLogger (which is a single shared
# instance with no per-request state of its own) attribute a warning line
# to the specific account file that was in use when yt-dlp produced it -
# download_with_fallback sets this immediately before each attempt, for
# both the cookie-rotation loop and the proxy retry.
_thread_local = threading.local()


def _reset_cookie_flag():
    _thread_local.cookie_flagged_dead = False


def _mark_cookie_flagged():
    _thread_local.cookie_flagged_dead = True


def _was_cookie_flagged() -> bool:
    return getattr(_thread_local, "cookie_flagged_dead", False)


def _set_active_account(path: Optional[str]):
    _thread_local.active_account = path


def _get_active_account() -> Optional[str]:
    return getattr(_thread_local, "active_account", None)


def _maybe_alert_cookie_expiry(message: str):
    """
    Records this occurrence UNDER THE CURRENTLY ACTIVE ACCOUNT (see
    _get_active_account), prunes anything outside that account's rolling
    window, and only actually alerts once COOKIE_EXPIRY_ALERT_THRESHOLD
    occurrences are present for THAT ACCOUNT in that window - see the
    module-level comment above for why a single occurrence is deliberately
    not enough. After an alert fires for an account, that SAME account's
    COOKIE_ALERT_COOLDOWN_SECONDS suppresses repeats - a different account
    going bad later still alerts on its own schedule, it isn't silenced by
    an unrelated account's recent alert.
    """
    global _cookie_alert_last_sent
    account_path = _get_active_account()
    account_label = account_path if account_path else "unknown/cookie-less attempt"
    now = time.time()

    with _cookie_alert_lock:
        events = _cookie_warning_events.setdefault(account_label, [])
        events.append(now)
        cutoff = now - COOKIE_EXPIRY_ALERT_WINDOW_SECONDS
        while events and events[0] < cutoff:
            events.pop(0)
        recent_count = len(events)

        if recent_count < COOKIE_EXPIRY_ALERT_THRESHOLD:
            # Not enough occurrences yet FOR THIS ACCOUNT to distinguish
            # "genuinely dead cookies" from "yt-dlp's heuristic had one
            # flaky moment" - stay quiet.
            return

        last_sent = _cookie_alert_last_sent.get(account_label, 0)
        if now - last_sent < COOKIE_ALERT_COOLDOWN_SECONDS:
            # Already alerted recently for THIS account's ongoing issue -
            # don't repeat-ping. A different account is tracked separately
            # and unaffected by this cooldown.
            return

        _cookie_alert_last_sent[account_label] = now

    # Best-effort "what's still healthy" snapshot at alert time - this
    # account may not be formally disabled yet (that happens a moment
    # later in download_with_fallback once it sees _was_cookie_flagged()),
    # so it can still appear in get_cookie_accounts() briefly; excluded
    # here explicitly so the message doesn't call it "active" and "dead"
    # in the same breath.
    still_active = [p for p in get_cookie_accounts() if p != account_path]
    still_active_text = ", ".join(still_active) if still_active else "none currently active"

    env_hint = "YT_COOKIES_B64 (primary account)"
    if account_path == COOKIE_ACCOUNT_2_PATH:
        env_hint = f"{COOKIE_ACCOUNT_2_B64_ENV} (account 2)"
    elif account_path == COOKIE_ACCOUNT_3_PATH:
        env_hint = f"{COOKIE_ACCOUNT_3_B64_ENV} (account 3)"

    alert_message = (
        f"[COOKIES] '{account_label}' looks genuinely expired/rotated - "
        f"seen {recent_count} times in the last "
        f"{COOKIE_EXPIRY_ALERT_WINDOW_SECONDS // 60} min for THIS account "
        f"specifically (not just a one-off flaky check). "
        f"Still active: {still_active_text}. "
        f"Downloads are still working via the remaining account(s)/proxy "
        f"fallback, but re-export '{account_label}' from a logged-in browser "
        f"session, base64-encode it, and update {env_hint} in Railway when "
        f"you get a chance - this alert won't repeat for "
        f"{COOKIE_ALERT_COOLDOWN_SECONDS // 60} min for THIS account "
        f"regardless of how many more downloads hit it in the meantime."
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
            _mark_cookie_flagged()
            _maybe_alert_cookie_expiry(msg)

    def warning(self, msg):
        print(msg)
        if _is_cookie_expiry_warning(msg):
            _mark_cookie_flagged()
            _maybe_alert_cookie_expiry(msg)

    def error(self, msg):
        print(msg)


# Single shared instance - it holds no per-request state of its own (the
# thread-local flags/labels above carry the per-request context instead),
# so one instance passed into every ydl_opts dict is correct and avoids
# extra allocation per request.
ytdlp_alert_logger = _YtdlpAlertLogger()


# ---------- MULTI-ACCOUNT COOKIE ROTATION ----------
# Account 1 continues to use the EXISTING single-cookie mechanism
# (YT_COOKIES_PATH / utils.ensure_cookies_file(), untouched). Accounts 2
# and 3 are additional, OPTIONAL cookie sessions materialized here lazily
# on first use, independent of main.py's startup lifecycle - if
# YT_COOKIES_B64_2/_3 aren't set, those slots just don't exist and
# rotation works with whatever subset IS configured.
_cookie_accounts_lock = threading.Lock()
_cookie_accounts_materialized = False
_cookie_account_disabled_until: dict = {}  # path -> timestamp


def _materialize_extra_cookie_accounts():
    global _cookie_accounts_materialized
    if _cookie_accounts_materialized:
        return
    with _cookie_accounts_lock:
        if _cookie_accounts_materialized:
            return
        for env_name, path in (
            (COOKIE_ACCOUNT_2_B64_ENV, COOKIE_ACCOUNT_2_PATH),
            (COOKIE_ACCOUNT_3_B64_ENV, COOKIE_ACCOUNT_3_PATH),
        ):
            b64_value = os.environ.get(env_name)
            if b64_value and not os.path.exists(path):
                try:
                    with open(path, "wb") as f:
                        f.write(base64.b64decode(b64_value))
                    logger.info(f"[COOKIES] Materialized additional cookie account at {path}")
                except Exception as e:
                    logger.warning(f"[COOKIES] Failed to materialize account at {path} (non-fatal): {e}")
        _cookie_accounts_materialized = True


def get_cookie_accounts() -> list:
    """
    Returns paths of all CURRENTLY AVAILABLE cookie accounts (file exists
    on disk AND not on cooldown from a recent LOGIN_REQUIRED), primary
    account first. May return an empty list if every configured account
    is currently disabled - callers should treat that the same as "no
    cookies configured" and proceed cookie-less rather than erroring.
    """
    _materialize_extra_cookie_accounts()
    primary_path = os.environ.get("YT_COOKIES_PATH", YT_COOKIES_PATH_DEFAULT)
    candidate_paths = [
        p for p in (primary_path, COOKIE_ACCOUNT_2_PATH, COOKIE_ACCOUNT_3_PATH)
        if p and os.path.exists(p)
    ]
    now = time.time()
    with _cookie_accounts_lock:
        available = [p for p in candidate_paths if _cookie_account_disabled_until.get(p, 0) < now]
    return available


def _disable_cookie_account(path: str):
    with _cookie_accounts_lock:
        _cookie_account_disabled_until[path] = time.time() + COOKIE_ACCOUNT_COOLDOWN_SECONDS
    logger.warning(
        f"[COOKIES] Account '{path}' flagged LOGIN_REQUIRED - disabling it for "
        f"{COOKIE_ACCOUNT_COOLDOWN_SECONDS // 60} min, rotating to the next "
        f"available account (if any)."
    )


def extract_info_with_retry(ydl_opts: dict, url: str):
    """
    NOTE: this function is fully synchronous/blocking (yt_dlp + time.sleep
    backoff). It must always be called via utils.run_blocking() from an
    async endpoint - never awaited or called directly from `async def` code
    - or it will freeze the event loop for the whole server during the
    retry backoff sleeps and the download itself.

    Each attempt does ONE extraction pass (download=False - webpage, all
    player-client API calls, PO token generation, JS-challenge solving -
    the expensive part), checks the video's duration on that already-
    extracted result, and ONLY THEN reuses that same result to actually
    download + postprocess via process_ie_result(download=True). This
    replaces the old two-call pattern (a separate check_video_duration()
    extraction, immediately followed by a second, fully independent
    extraction inside the real download) which paid for the entire
    webpage/player-API/PO-token/JS-challenge sequence TWICE on every
    single request - the single biggest avoidable chunk of per-request
    latency. process_ie_result is yt-dlp's own supported API for exactly
    this "extract once, decide, then download" pattern.
    """
    last_exception = None

    for attempt in range(1, YT_DLP_MAX_ATTEMPTS + 1):
        try:
            logger.info(f"yt_dlp extract_info attempt {attempt}/{YT_DLP_MAX_ATTEMPTS} for URL: {url}")
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)

                duration = info.get('duration') if info else None
                if MAX_VIDEO_DURATION_SECONDS is not None and duration and duration > MAX_VIDEO_DURATION_SECONDS:
                    logger.warning(
                        f"Rejecting download: video duration {duration}s exceeds "
                        f"MAX_VIDEO_DURATION_SECONDS={MAX_VIDEO_DURATION_SECONDS}s for URL: {url}"
                    )
                    raise VideoTooLongError(duration, MAX_VIDEO_DURATION_SECONDS)

                info = ydl.process_ie_result(info, download=True)
            logger.info(f"yt_dlp extract_info succeeded on attempt {attempt}")
            return info
        except VideoTooLongError:
            # Not a network/bot-check/cookie/IP problem - no amount of
            # retrying, account rotation, or proxy switching changes a
            # video's actual duration. Propagate immediately, don't
            # consume a retry attempt or fall into the generic error
            # classification below.
            raise
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

            # COST OPTIMIZATION (no behavior/outcome change): if yt-dlp's
            # own logger already told us THIS SPECIFIC cookie session is
            # dead (_was_cookie_flagged(), set the instant the "cookies
            # are no longer valid" warning line appears - see
            # _YtdlpAlertLogger above), retrying with the IDENTICAL
            # cookiefile 1-2 more times within this same account attempt
            # cannot possibly succeed - nothing about the cookie changes
            # between backoff sleeps. Every subprocess/API round trip
            # (webpage + 4 player-client calls + Node PO-token + Deno
            # JS-challenge) that a retry would otherwise repeat is pure
            # wasted compute in this case. Failing fast here does NOT
            # change which account gets tried next, whether rotation or
            # proxy fallback happens, or what the user ultimately sees -
            # download_with_fallback's rotation/alerting logic (which
            # reads this same flag) is completely unchanged; this only
            # removes pointless retries BEFORE that logic ever runs.
            if _was_cookie_flagged():
                logger.warning(
                    f"Attempt {attempt}: this cookie account was confirmed dead "
                    f"by yt-dlp's own check - not retrying with the same "
                    f"cookiefile, handing off to account rotation/proxy instead: {error_text}"
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

            if is_age_restricted_error(error_text):
                # Retrying with the SAME cookie account can't fix an
                # age-verification requirement - nothing about the account
                # changes between backoff sleeps. Fail fast; the caller
                # (download_with_fallback) decides whether a different
                # cookie account should be tried, and skips the proxy tier
                # entirely for this error type since a different IP
                # doesn't fix an account-identity requirement.
                logger.warning(
                    f"Attempt {attempt}: age-restricted video, this cookie "
                    f"account isn't age-verified - not retrying: {error_text}"
                )
                raise

            if is_members_only_error(error_text):
                # Same reasoning as age-restriction: this account isn't a
                # channel member, and retrying won't change that.
                logger.warning(
                    f"Attempt {attempt}: members-only video, this cookie "
                    f"account isn't a member - not retrying: {error_text}"
                )
                raise

            if is_not_yet_live_error(error_text):
                # A scheduled premiere/stream that hasn't started - no
                # retry count, cookie, or IP makes it start early.
                logger.warning(
                    f"Attempt {attempt}: video is a premiere/live stream that "
                    f"hasn't started yet - not retrying: {error_text}"
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
    Multi-layer download strategy, cheapest/most-specific fix first:

      Layer 1 - COOKIE ACCOUNT ROTATION (free). Tries each currently
                available cookie account in turn (up to 3: primary + 2
                optional extras). If an attempt fails AND the thread-local
                flag confirms yt-dlp specifically flagged THIS cookie
                session as dead ("cookies are no longer valid" /
                LOGIN_REQUIRED), that account is disabled for a cooldown
                and the NEXT account is tried immediately - no point
                retrying the same dead session (this is now enforced
                doubly: extract_info_with_retry already stops retrying
                that account's own backoff loop the instant it's flagged,
                and this loop then moves on to the next account rather
                than trying the dead one again). If an attempt fails with
                an age-restriction, members-only, or not-yet-live error,
                rotation also continues to the next account (a different
                account might genuinely be verified/a member), WITHOUT
                disabling the current one (it isn't dead, just not
                privileged for this specific video). If an attempt fails
                for ANY OTHER reason (IP-block, permanent, unknown),
                rotation stops immediately - swapping cookie accounts
                won't fix an IP-reputation or availability problem, so we
                fall through to Layer 2 instead of wasting the remaining
                accounts on a failure they can't fix either.

      Layer 2 - PROXY, tried for any Layer 1 failure EXCEPT a confirmed
                permanent error, age-restriction, members-only, or
                not-yet-live error (see is_permanent_error and the three
                account-identity/timing checks below) - a different IP
                fixes IP-reputation problems, not account privilege or
                stream timing. Uses whichever cookie account is STILL
                available (not yet disabled) at this point, if any -
                proxy fixes IP reputation, cookies fix session identity,
                the two are independent problems that can combine (e.g. an
                IP-blocked request might still benefit from a valid
                cookie session once retried through a clean IP).

    Before EVERY attempt (both layers), _set_active_account() records
    which cookie file (if any) is in use for THIS attempt - this is what
    lets a "cookies are no longer valid" warning line get attributed to
    the correct account file in the Discord alert (see
    _maybe_alert_cookie_expiry), instead of a generic unattributed message.

    If Layer 2 itself fails with what looks like a proxy billing/quota
    error, trip the circuit breaker as before. If it fails because the
    cookie account used there ALSO turns out to be dead, that account gets
    disabled too - same accounting either way, just discovered a layer
    later.
    """
    accounts = get_cookie_accounts()
    if not accounts:
        # Every configured account is currently disabled (or none are
        # configured at all) - proceed cookie-less, same as the
        # historical no-cookies behavior. A single None entry means "one
        # attempt, no cookiefile".
        accounts = [None]

    last_error = None
    for account_path in accounts:
        opts = dict(base_ydl_opts)
        if account_path:
            opts["cookiefile"] = account_path
        else:
            opts.pop("cookiefile", None)

        _reset_cookie_flag()
        _set_active_account(account_path)
        try:
            result = extract_info_with_retry(opts, url)
            if account_path:
                logger.info(f"[COOKIES] Download succeeded using account: {account_path}")
            return result
        except Exception as e:
            last_error = e
            error_text = str(e)

            if isinstance(e, VideoTooLongError) or is_permanent_error(error_text):
                # No cookie swap, no proxy, no retry fixes a video that's
                # too long or genuinely unavailable - stop everywhere,
                # immediately.
                raise

            if account_path and _was_cookie_flagged():
                _disable_cookie_account(account_path)
                continue  # try the next account, if any remain

            if (
                is_age_restricted_error(error_text)
                or is_members_only_error(error_text)
                or is_not_yet_live_error(error_text)
            ):
                # This account isn't privileged for this specific video
                # (not age-verified, not a member) or the video simply
                # hasn't started - a DIFFERENT account might still work,
                # so keep rotating WITHOUT disabling this one (it's not
                # dead, just unprivileged/early for this particular
                # video).
                continue

            # Failure wasn't confirmed as THIS account's identity being
            # rejected (could be IP-block, transient, or cookie-less) -
            # rotating accounts further won't help. Stop here and let the
            # proxy tier below handle it instead.
            break

    first_error = str(last_error)

    if is_permanent_error(first_error):
        raise last_error

    if (
        is_age_restricted_error(first_error)
        or is_members_only_error(first_error)
        or is_not_yet_live_error(first_error)
    ):
        # Every available cookie account hit the same account-privilege
        # or timing wall. A different IP (the proxy) does nothing for
        # these, so skip that tier entirely rather than burning proxy
        # bandwidth and ~30s on a guaranteed repeat failure.
        logger.warning(
            "[PROXY] Skipping proxy tier - failure is an age-restriction/"
            "members-only/not-yet-live requirement, not an IP/bot-check "
            "problem: no available cookie account satisfies it for this video."
        )
        raise last_error

    if not proxy_url:
        logger.warning("[PROXY] Direct attempt(s) failed and no proxy is configured - failing as-is.")
        raise last_error

    if not proxy_available():
        logger.warning(
            "[PROXY] Direct attempt(s) failed, but proxy circuit breaker is "
            "currently OPEN (likely out of credit) - failing as-is instead of "
            "retrying a proxy known to be down."
        )
        raise last_error

    logger.warning(f"[PROXY] Direct attempt(s) failed ({first_error[:200]}) - retrying via proxy...")

    # Use whichever account is STILL available (not yet disabled by the
    # Layer 1 loop above) for the proxy attempt - if the direct failure
    # was an IP-block rather than a cookie problem, the same cookie
    # session is very likely still fine, just needs a cleaner IP.
    remaining_accounts = get_cookie_accounts()
    proxied_opts = dict(base_ydl_opts)
    proxied_opts["proxy"] = proxy_url
    proxy_account = remaining_accounts[0] if remaining_accounts else None
    if proxy_account:
        proxied_opts["cookiefile"] = proxy_account
    else:
        proxied_opts.pop("cookiefile", None)

    _reset_cookie_flag()
    _set_active_account(proxy_account)
    try:
        result = extract_info_with_retry(proxied_opts, url)
        logger.info("[PROXY] Proxy retry succeeded.")
        return result
    except Exception as proxy_error:
        proxy_error_text = str(proxy_error)
        if is_proxy_quota_error(proxy_error_text):
            _trip_proxy_circuit_breaker()
        elif proxy_account and _was_cookie_flagged():
            _disable_cookie_account(proxy_account)
            logger.warning(f"[PROXY] Cookie account '{proxy_account}' also failed via proxy - disabled.")
        else:
            logger.warning(f"[PROXY] Proxy retry also failed (non-quota error): {proxy_error_text}")
        # Whatever the proxy attempt raised is the most informative
        # error to surface - propagate it (routes.py still applies its
        # own is_bot_check_error()/is_geo_restricted_error() classification
        # on top of this for the user-facing message).
        raise