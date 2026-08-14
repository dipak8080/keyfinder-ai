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
    FORMAT_UNAVAILABLE_MARKERS,
    IP_BLOCK_MARKERS,
    MAX_VIDEO_DURATION_SECONDS,
    PROXY_CIRCUIT_BREAKER_COOLDOWN_SECONDS,
    CDN_DEGRADED_THRESHOLD,
    CDN_DEGRADED_WINDOW_SECONDS,
    CDN_DEGRADED_COOLDOWN_SECONDS,
    PROXY_BOTCHECK_THRESHOLD,
    PROXY_BOTCHECK_WINDOW_SECONDS,
    PROXY_BOTCHECK_COOLDOWN_SECONDS,
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

# ============================================================
# PLAYER CLIENT SELECTION (ADDED 2026-08-12)
#
# WHY THIS EXISTS: android and android_vr do NOT support cookies - yt-dlp
# silently SKIPS those clients whenever a cookiefile is attached to the
# request. Confirmed in production via direct --list-formats testing:
#
#   WITH cookies:    "Skipping client android_vr ... does not support
#                     cookies" / "Skipping client android ..." -> only
#                     "web" runs -> often returns ONLY storyboard/mhtml
#                     formats, no audio at all.
#   WITHOUT cookies: android_vr + android run normally -> real audio
#                     formats (139, 140, 249, 251, ...) come back fine.
#
# Since download_with_fallback almost always has a cookie account
# available and attaches it on the very first attempt, production was
# hitting the cookies+web-only path on nearly every request, surfacing as
# "Requested format is not available" - which is_ip_block_error() then
# mis-buckets as a bot-check, wrongly escalating to proxy and burning the
# proxy bot-check breaker for a problem no proxy exit can fix.
#
# FIX: pick the player_client list based on whether the CURRENT attempt
# actually has a cookiefile attached, not a fixed list for the whole
# process. Every attempt (each cookie-rotation loop iteration AND the
# proxy attempt) must call _apply_player_clients() right before use,
# since cookie presence differs attempt to attempt.
# ============================================================
PLAYER_CLIENTS_NO_COOKIES = ['android_vr', 'android', 'web']
PLAYER_CLIENTS_WITH_COOKIES = ['tv', 'web', 'web_safari']


def _apply_player_clients(opts: dict, has_cookies: bool) -> dict:
    """
    Overrides extractor_args.youtube.player_client on `opts` based on
    whether THIS specific attempt has a cookiefile attached, returning
    the same dict for convenient chaining. android/android_vr are dropped
    whenever cookies are present (yt-dlp would silently skip them anyway,
    which previously left "web" to run alone and frequently return no
    audio formats) in favor of clients that actually work with cookies
    attached.
    """
    extractor_args = dict(opts.get('extractor_args') or {})
    youtube_args = dict(extractor_args.get('youtube') or {})
    youtube_args['player_client'] = (
        PLAYER_CLIENTS_WITH_COOKIES if has_cookies else PLAYER_CLIENTS_NO_COOKIES
    )
    extractor_args['youtube'] = youtube_args
    opts['extractor_args'] = extractor_args
    return opts


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

# Errors meaning THIS SERVER has no working route to the destination -
# not a YouTube-side block, not an IP-reputation problem, not fixable by
# retrying, rotating cookie accounts, or switching to the proxy (the
# proxy doesn't help either, since the problem is this server's own
# outbound networking, not its IP reputation). Specific known cause on
# this VPS: IPv6 is enabled at the interface level but has no allocated
# global address or default route (confirmed via `ip -6 addr show` /
# `ip -6 route show` showing only link-local fe80:: addresses) - some
# googlevideo.com CDN edges are IPv6-only, and a request assigned to one
# of those can never succeed until the host allocates real IPv6
# connectivity. Retrying 3x with backoff against the SAME assigned edge
# wastes ~4.5s for a guaranteed identical failure each time - same
# fail-fast reasoning as IP_BLOCK_MARKERS below, just for a different
# root cause.
IPV6_UNROUTABLE_MARKERS = (
    "address family for hostname not supported",
    "network is unreachable",
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
# rotation to the next cookie account, if any remain.
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

# Errors meaning this video/track is locked behind a YouTube Music
# Premium subscription - distinct from MEMBERS_ONLY_MARKERS (a paid
# subscription to one specific CHANNEL) since this is a YouTube-wide
# Premium subscription instead. Same handling shape as age-restriction /
# members-only: a DIFFERENT cookie account that happens to have Music
# Premium active could fix it, the SAME account retried 3x can't, and a
# different IP via proxy does nothing for a subscription requirement
# either.
MUSIC_PREMIUM_MARKERS = (
    "only available to music premium members",
    "youtube music premium",
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
    # ADDED 2026-08-13: confirmed in production - this exact phrasing
    # ("Premiere will begin shortly") fell through every existing marker
    # here, so is_not_yet_live_error() returned False and the request
    # went through the full generic-error path instead: 3 retries with
    # backoff (~4.5s wasted), THEN correctly declined to escalate to
    # proxy via should_use_proxy() (a different IP can't start a
    # scheduled premiere early either way) - so the outcome was already
    # correct, this only removes the wasted retry time before reaching
    # it. Same fail-fast/skip-proxy handling as every other marker in
    # this tuple once matched.
    "premiere will begin shortly",
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


def is_format_unavailable_error(error_text: str) -> bool:
    """
    True if yt-dlp reached YouTube successfully and got a real manifest
    back, but none of the formats in it matched the player_client list
    that was active for THIS attempt - most commonly because cookies were
    attached, which drops android/android_vr from the client list (see
    _apply_player_clients above) and leaves only web-family clients,
    which don't always expose a usable audio format for every video.

    NOT an IP-reputation problem. Confirmed in production 2026-08-12: the
    SAME cookie account produced this IDENTICAL error on the direct
    attempt AND through the proxy, seconds apart, across 100+ occurrences
    in one evening. A different exit IP producing an identical failure is
    proof a different exit IP was never going to fix it - this marker
    used to live in YT_BOT_CHECK_MARKERS/IP_BLOCK_MARKERS, which is what
    caused every occurrence to pay for a wasted proxy round-trip and then
    trip the proxy bot-check breaker as a side effect (see
    FORMAT_UNAVAILABLE_MARKERS in config.py for the fuller incident note).

    Used the same way its age-restricted/members-only/Music-Premium
    siblings are: fails fast within extract_info_with_retry (retrying the
    same client/cookie combo can't change which formats exist), and skips
    the proxy tier entirely in download_with_fallback (a different exit
    IP doesn't change the manifest either) - but still allows account
    rotation to continue, since a different account may have different
    client eligibility (e.g. no cookies at all, unlocking
    android/android_vr).
    """
    normalized = _normalize_error_text(error_text)
    return any(marker in normalized for marker in FORMAT_UNAVAILABLE_MARKERS)


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


def is_music_premium_error(error_text: str) -> bool:
    """
    True if this video/track requires a YouTube Music Premium
    subscription. Same handling shape as age-restriction / members-only:
    fail fast on same-account retries (no plain retry grants a
    subscription), skip the proxy tier (a different IP doesn't either),
    but allow account rotation to try a different account that might
    already have Music Premium active.
    """
    normalized = _normalize_error_text(error_text)
    return any(marker in normalized for marker in MUSIC_PREMIUM_MARKERS)


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


def is_ipv6_unroutable_error(error_text: str) -> bool:
    """
    True if this failure means the SERVER has no route to the
    destination, not that YouTube blocked/restricted anything. Used only
    inside extract_info_with_retry as a fail-fast signal - retrying,
    rotating cookie accounts, or trying the proxy tier are all equally
    useless against this, since none of them change this server's own
    IPv6 connectivity. Deliberately excluded from is_ip_block_error() and
    should_use_proxy(): escalating to proxy for this would be actively
    wrong (implies "try a different IP", but the actual problem is this
    server has no path out over IPv6 at all - a different exit IP on the
    same broken interface doesn't fix that).
    """
    normalized = _normalize_error_text(error_text)
    return any(marker in normalized for marker in IPV6_UNROUTABLE_MARKERS)


def is_cdn_connect_timeout_error(error_text: str) -> bool:
    """
    True if this is a connect-timeout to a SPECIFIC googlevideo CDN media
    edge (e.g. "Connection to rr8---sn-xxxx.googlevideo.com timed out.
    (connect timeout=20.0)") - distinct from a generic/unrecognized
    timeout elsewhere in the chain (webpage fetch, DNS, proxy handshake,
    etc.), which has no particular reason to be fixed by a different IP.

    This shape specifically DOES have known evidence a different exit IP
    helps: YouTube assigns which CDN edge a client is routed to based on
    the requesting IP/geo, so a proxy exit node frequently lands on a
    different, reachable edge even when this server's direct IP keeps
    getting assigned the same dead one on every retry.

    Requiring BOTH "googlevideo.com" and "timed out" in the same message
    (rather than matching "timed out" alone) is deliberate - it keeps this
    narrow to the exact failure it's meant for, rather than escalating
    proxy for every unrelated timeout in the system.

    Used two places:
      - should_use_proxy(): this shape now DOES escalate to the proxy tier
      - extract_info_with_retry(): fail-fast, since retrying the SAME
        googlevideo edge from the SAME IP 3x with backoff produces the
        identical timeout every time - only a different exit IP (i.e. the
        proxy tier the caller escalates to next) has any chance.
    """
    normalized = _normalize_error_text(error_text)
    return "googlevideo.com" in normalized and "timed out" in normalized


def is_media_phase_error(error_text: str) -> bool:
    """
    True if this failure happened while fetching the actual audio bytes,
    rather than during extraction (webpage fetch, player API calls, PO
    token generation, JS challenge).

    WHY THIS MATTERS - it is the single most diagnostic signal available,
    and it settled a real production question on 2026-08-08:

      A media-phase failure PROVES extraction succeeded, which PROVES
      the cookies were accepted. yt-dlp cannot reach a googlevideo media
      URL without first completing an authenticated extraction.

    That day's logs showed the same cookie file failing at the MEDIA
    phase on the direct path (so: accepted) and then bot-checking at the
    EXTRACTION phase through the proxy seconds later (so: rejected) - same
    cookies, same minute, different exit IP. Without this distinction the
    obvious-looking conclusion is "cookies are stale, re-export all
    three", which would have been an hour of work fixing something that
    was never broken. The actual variable was the proxy's rotating exit
    IP presenting a Nepal-issued session from a different country.

    Detection: yt-dlp prefixes media-phase failures with "[download] Got
    error:" and they reference a googlevideo host. Extraction failures
    carry the "[youtube] <video_id>:" prefix instead.
    """
    normalized = _normalize_error_text(error_text)
    return "[download] got error" in normalized or "googlevideo.com" in normalized


def failure_phase(error_text: str) -> str:
    """Human-readable phase label for logs and /admin/status."""
    return "media" if is_media_phase_error(error_text) else "extraction"


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

    NOTE: "requested format is not available" deliberately does NOT match
    anything in IP_BLOCK_MARKERS as of 2026-08-12 - see
    is_format_unavailable_error() above for why that's a client/cookie
    mismatch, not an IP-reputation signal, and why conflating the two was
    burning proxy spend on a failure proxy could never fix.
    """
    normalized = _normalize_error_text(error_text)
    return (
        any(marker in normalized for marker in IP_BLOCK_MARKERS)
        or is_geo_restricted_error(error_text)
    )


def should_use_proxy(error_text: str) -> bool:
    """
    Narrows proxy escalation to failures that actually LOOK like an
    IP-reputation problem - a bot-check page, a 403 on the media fetch,
    a geo-restriction, or another marker in
    IP_BLOCK_MARKERS/YT_BOT_CHECK_MARKERS - rather than escalating on
    every non-permanent failure like the original behavior did. The goal
    is to stop paying proxy bandwidth on truly generic/unrecognized
    yt-dlp errors (network blips, a brand-new uncatalogued error string)
    where there's no actual evidence a different IP would help.

    CDN CONNECT-TIMEOUTS (is_cdn_connect_timeout_error) DO escalate here.
    This was briefly removed on 2026-08-07 on the theory that the proxy
    exit often reaches the same dead edge, then restored the same day once
    real data contradicted it. The evidence that settled it:

      - Host-level curl confirmed the dead edges are unreachable from this
        VPS on BOTH IPv4 and IPv6, so direct genuinely cannot recover.
      - The proxy provider's own 7-day usage log showed 190 requests to
        googlevideo.com edges through the proxy with a 100% success rate
        and multi-MB transfers (i.e. real audio fetched, not just a
        connection opened).

    One ambiguous app-log case suggested a proxy retry also timed out;
    190 successful fetches outweigh it. A different exit IP demonstrably
    lands on live edges.

    See also the direct-path degradation breaker below
    (record_cdn_timeout / direct_path_degraded): once these timeouts start
    clustering, download_with_fallback stops attempting direct at all for
    a cooldown window and goes straight to proxy, so the doomed ~10s
    socket_timeout isn't paid on every request during an episode. This
    function governs the per-request escalation decision; that breaker
    governs whether direct is even worth trying first.

    IMPORTANT CAVEAT: this does NOT reduce proxy usage for the current
    known mweb/web PO-token-bound-to-video-id 403 bug (tracked upstream in
    yt-dlp) - that error text ("unable to download video data: HTTP Error
    403: Forbidden") already matches IP_BLOCK_MARKERS, so this function
    still returns True for it, same as before this change. Proxy has
    already been confirmed NOT to fix that specific bug (the same 403
    occurs through the proxy too) - this function only reduces spend on
    failures OUTSIDE that known issue, it is not a cost-control measure
    for it. Toggle YT_PROXY_URL off entirely if you want to stop paying
    for proxy attempts against that specific bug until yt-dlp ships an
    upstream fix.

    Also does not escalate IPv6-unroutable errors (see
    is_ipv6_unroutable_error) - that failure means THIS server has no
    outbound path, which no proxy exit IP changes.

    Also does not escalate age-restriction, members-only, Music Premium,
    not-yet-live, or format-unavailable errors - those are account-
    identity/timing/client-eligibility problems, not IP-reputation
    problems, and are excluded upstream in download_with_fallback before
    this function is even consulted (see the skip-proxy block there);
    this function's own marker lists never match those error shapes in
    the first place, so this note is here only so a future reader doesn't
    wonder why they're absent from this list.

    Trade-off worth knowing: like every other classifier in this file,
    this is marker-list based. A genuinely new, not-yet-catalogued
    YouTube error that a different IP WOULD have fixed will now skip
    proxy entirely until its text is recognized and added to
    IP_BLOCK_MARKERS/YT_BOT_CHECK_MARKERS - watch the "[PROXY] Not
    escalating to proxy" log line for repeating unrecognized error text
    as the signal something needs adding.
    """
    return (
        is_ip_block_error(error_text)
        or is_bot_check_error(error_text)
        or is_cdn_connect_timeout_error(error_text)
    )


def is_permanent_error(error_text: str) -> bool:
    """True if the error means the video itself can never be downloaded -
    no amount of retrying, cookie refreshing, or proxy switching helps.
    This is the ONLY thing that skips the proxy tier - see
    download_with_fallback. Geo-restriction, age-restriction, members-only,
    Music Premium, and not-yet-live are deliberately NOT here (see their
    own marker lists above) since a different cookie account or timing CAN
    fix those."""
    normalized = _normalize_error_text(error_text)
    return any(marker in normalized for marker in PERMANENT_ERROR_MARKERS)


def is_proxy_quota_error(error_text: str) -> bool:
    normalized = _normalize_error_text(error_text)
    return any(marker in normalized for marker in PROXY_QUOTA_ERROR_MARKERS)


# Errors meaning the TLS handshake to the proxy (or through it to the
# destination) failed outright - a cipher/protocol mismatch, not a
# YouTube-side bot-check or block. Retrying the SAME proxy exit 2 more
# times with backoff produces the identical handshake failure every time
# (nothing about the negotiation changes between sleeps) - only a
# different exit (a new proxy session) has any chance. Narrow and
# specific on purpose: "ssl: " is broad enough to catch most OpenSSL
# error strings without accidentally matching unrelated text.
PROXY_TLS_ERROR_MARKERS = (
    "sslv3_alert_handshake_failure",
    "ssl: ",
    "handshake failure",
)


def is_proxy_tls_error(error_text: str) -> bool:
    normalized = _normalize_error_text(error_text)
    return any(marker in normalized for marker in PROXY_TLS_ERROR_MARKERS)


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
    _record_event("proxy_quota")
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


# ---------- DIRECT-PATH DEGRADATION BREAKER ----------
# The inverse of the proxy circuit breaker above. That one answers "is the
# proxy usable?"; this one answers "is going direct even worth trying?"
#
# Motivation, in one line: during a dead-edge episode EVERY direct attempt
# costs a guaranteed ~10s socket_timeout and then fails, so paying it once
# per request is pure waste once the pattern is established.
#
# Rolling-window counter rather than a single-failure trip, for the same
# reason the cookie-expiry alerting uses one: an isolated timeout is
# normal internet flakiness and shouldn't push all traffic (and cost) onto
# the proxy. A cluster of them inside CDN_DEGRADED_WINDOW_SECONDS is a
# real episode, and that's what trips it.
_cdn_lock = threading.Lock()
_cdn_timeout_events: list = []
_direct_degraded_until = 0.0


def record_cdn_timeout():
    """
    Called by download_with_fallback whenever a DIRECT attempt fails with
    a CDN connect-timeout. Trips the breaker once enough have accumulated
    inside the rolling window.

    Deliberately only records DIRECT-path timeouts. A timeout on the proxy
    path says nothing about whether direct is healthy, and counting it
    would keep the breaker latched on the very failures it's meant to
    route around.
    """
    _record_event("cdn_timeout")
    global _direct_degraded_until
    now = time.time()
    tripped_for = None

    with _cdn_lock:
        _cdn_timeout_events.append(now)
        cutoff = now - CDN_DEGRADED_WINDOW_SECONDS
        while _cdn_timeout_events and _cdn_timeout_events[0] < cutoff:
            _cdn_timeout_events.pop(0)
        count = len(_cdn_timeout_events)

        already_degraded = now < _direct_degraded_until
        if count >= CDN_DEGRADED_THRESHOLD and not already_degraded:
            _direct_degraded_until = now + CDN_DEGRADED_COOLDOWN_SECONDS
            _cdn_timeout_events.clear()  # fresh count for the next window
            tripped_for = CDN_DEGRADED_COOLDOWN_SECONDS

    if tripped_for is not None:
        logger.warning(
            f"[CDN] Direct path marked DEGRADED for {tripped_for // 60} min - "
            f"{CDN_DEGRADED_THRESHOLD} googlevideo connect-timeouts within "
            f"{CDN_DEGRADED_WINDOW_SECONDS // 60} min. YouTube is routing this "
            f"server's IP to unreachable CDN edges; downloads will go straight "
            f"to the proxy until this clears, skipping the doomed ~10s direct "
            f"attempt. Direct is retried automatically after the cooldown."
        )


def direct_path_degraded() -> bool:
    """True while the direct path is in its post-trip cooldown."""
    with _cdn_lock:
        return time.time() < _direct_degraded_until


def cdn_breaker_status() -> dict:
    """Snapshot for /admin/status, so an episode is visible without
    grepping logs."""
    now = time.time()
    with _cdn_lock:
        cutoff = now - CDN_DEGRADED_WINDOW_SECONDS
        recent = [t for t in _cdn_timeout_events if t >= cutoff]
        degraded_until = _direct_degraded_until
    return {
        "direct_path": "DEGRADED (routing via proxy)" if now < degraded_until else "healthy",
        "seconds_until_direct_retried": max(0, int(degraded_until - now)),
        "recent_cdn_timeouts": len(recent),
        "trip_threshold": CDN_DEGRADED_THRESHOLD,
        "window_seconds": CDN_DEGRADED_WINDOW_SECONDS,
    }


def reset_cdn_breaker():
    """Manual override, mirroring reset_proxy_circuit_breaker()."""
    global _direct_degraded_until
    with _cdn_lock:
        _direct_degraded_until = 0.0
        _cdn_timeout_events.clear()
    logger.info("[CDN] Direct-path degradation breaker manually reset.")


# ---------- PROXY BOT-CHECK BREAKER (cost control) ----------
# The proxy's entire job is to present a cleaner IP. When the proxy path
# itself starts returning bot-checks, that job is currently failing, and
# every further escalation is a PAID request with a known outcome.
#
# Distinct from the quota breaker above: that one means "out of money",
# this one means "the money works but YouTube is challenging these
# exits". Keeping them separate matters because the recovery is
# different - a quota trip needs a top-up, this one usually resolves on
# its own as the provider rotates to less-challenged exits.
_proxy_botcheck_lock = threading.Lock()
_proxy_botcheck_events: list = []
_proxy_botcheck_until = 0.0


def record_proxy_botcheck():
    """Called when a PROXY attempt fails with a bot-check specifically."""
    _record_event("proxy_botcheck")
    global _proxy_botcheck_until
    now = time.time()
    tripped = False

    with _proxy_botcheck_lock:
        _proxy_botcheck_events.append(now)
        cutoff = now - PROXY_BOTCHECK_WINDOW_SECONDS
        while _proxy_botcheck_events and _proxy_botcheck_events[0] < cutoff:
            _proxy_botcheck_events.pop(0)
        count = len(_proxy_botcheck_events)

        if count >= PROXY_BOTCHECK_THRESHOLD and now >= _proxy_botcheck_until:
            _proxy_botcheck_until = now + PROXY_BOTCHECK_COOLDOWN_SECONDS
            _proxy_botcheck_events.clear()
            tripped = True

    if tripped:
        logger.warning(
            f"[PROXY] Bot-check breaker TRIPPED - {PROXY_BOTCHECK_THRESHOLD} "
            f"bot-checks through the proxy within "
            f"{PROXY_BOTCHECK_WINDOW_SECONDS // 60} min. The proxy's current exit "
            f"pool is being challenged by YouTube, so further escalations are "
            f"paid requests with a known outcome. Pausing proxy escalation for "
            f"{PROXY_BOTCHECK_COOLDOWN_SECONDS // 60} min. If this trips "
            f"repeatedly, the usual cause is a ROTATING residential exit "
            f"presenting a cookie session from a different country each request "
            f"- pin a sticky session and a fixed country in YT_PROXY_URL."
        )


def proxy_botcheck_degraded() -> bool:
    """True while the proxy is in its post-bot-check cooldown."""
    with _proxy_botcheck_lock:
        return time.time() < _proxy_botcheck_until


def reset_proxy_botcheck_breaker():
    global _proxy_botcheck_until
    with _proxy_botcheck_lock:
        _proxy_botcheck_until = 0.0
        _proxy_botcheck_events.clear()
    logger.info("[PROXY] Bot-check breaker manually reset.")


# ---------- PER-ACCOUNT COOKIE HEALTH ----------
# Replaces "accounts_available: 3" - a single number that says nothing
# about WHICH account is degrading, or why. Without this the only way to
# answer "are my cookies actually bad?" was to paste logs somewhere and
# have someone read message prefixes by eye.
#
# Records outcomes per account, tagged with the PHASE the failure
# happened in (see failure_phase), because that's what separates
# "cookies rejected" from "cookies fine, network broke".
_account_health_lock = threading.Lock()
_account_health: dict = {}


def _health_entry(path: str) -> dict:
    return _account_health.setdefault(path, {
        "successes": 0,
        "failures": 0,
        "last_success_at": None,
        "last_failure_at": None,
        "last_failure_phase": None,
        "last_failure_kind": None,
        "last_used_via": None,   # "direct" or "proxy"
    })


def record_account_result(
    path: Optional[str],
    ok: bool,
    via: str,
    error_text: str = "",
):
    """
    Records one attempt's outcome against a cookie account.

    `via` ("direct" / "proxy") is recorded because the same account can
    succeed on one path and be rejected on the other in the same minute -
    which is exactly the pattern that proves the problem is the exit IP
    rather than the cookie. Losing that distinction would flatten the
    single most useful signal here back into an ambiguous failure count.
    """
    _record_event("account_result", path=path, ok=ok, via=via, error_text=error_text)
    if not path:
        return
    now = time.time()
    with _account_health_lock:
        entry = _health_entry(path)
        entry["last_used_via"] = via
        if ok:
            entry["successes"] += 1
            entry["last_success_at"] = now
        else:
            entry["failures"] += 1
            entry["last_failure_at"] = now
            entry["last_failure_phase"] = failure_phase(error_text)
            if is_bot_check_error(error_text):
                kind = "bot_check"
            elif is_cdn_connect_timeout_error(error_text):
                kind = "cdn_timeout"
            elif is_format_unavailable_error(error_text):
                kind = "format_unavailable"
            elif is_permanent_error(error_text):
                kind = "video_unavailable"
            else:
                kind = "other"
            entry["last_failure_kind"] = kind


def get_account_health() -> list:
    """
    Snapshot for /admin/status. One row per configured account with
    enough context to answer "is this account actually bad?" without
    reading raw logs.
    """
    now = time.time()
    _materialize_extra_cookie_accounts()
    primary_path = os.environ.get("YT_COOKIES_PATH", YT_COOKIES_PATH_DEFAULT)
    candidates = [p for p in (primary_path, COOKIE_ACCOUNT_2_PATH, COOKIE_ACCOUNT_3_PATH) if p]

    out = []
    with _cookie_accounts_lock:
        disabled_map = dict(_cookie_account_disabled_until)
    with _account_health_lock:
        for path in candidates:
            exists = os.path.exists(path)
            disabled_until = disabled_map.get(path, 0)
            entry = _account_health.get(path, {})
            total = entry.get("successes", 0) + entry.get("failures", 0)
            out.append({
                "path": path,
                "exists": exists,
                "status": (
                    "missing" if not exists
                    else "disabled" if now < disabled_until
                    else "active"
                ),
                "disabled_for_seconds": max(0, int(disabled_until - now)),
                "successes": entry.get("successes", 0),
                "failures": entry.get("failures", 0),
                "success_rate": (
                    round(entry.get("successes", 0) / total * 100, 1) if total else None
                ),
                "seconds_since_success": (
                    int(now - entry["last_success_at"])
                    if entry.get("last_success_at") else None
                ),
                "last_failure_phase": entry.get("last_failure_phase"),
                "last_failure_kind": entry.get("last_failure_kind"),
                "last_used_via": entry.get("last_used_via"),
            })
    return out


# ---------- PATH-LEVEL OUTCOME COUNTERS ----------
# Answers "what is my proxy success rate this week?" - previously
# unanswerable from /admin/status, which meant there was no way to tell
# whether a proxy change (sticky sessions, a new provider, a different
# country pin) actually helped.
_path_stats_lock = threading.Lock()
_path_stats: dict = {
    "direct": {"attempts": 0, "successes": 0},
    "proxy": {"attempts": 0, "successes": 0},
}


def record_path_attempt(via: str, ok: bool):
    _record_event("path_attempt", via=via, ok=ok)
    with _path_stats_lock:
        bucket = _path_stats.setdefault(via, {"attempts": 0, "successes": 0})
        bucket["attempts"] += 1
        if ok:
            bucket["successes"] += 1


def get_path_stats() -> dict:
    with _path_stats_lock:
        out = {}
        for via, b in _path_stats.items():
            out[via] = {
                **b,
                "success_rate": (
                    round(b["successes"] / b["attempts"] * 100, 1)
                    if b["attempts"] else None
                ),
            }
        return out


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
    if _record_events_enabled:
        # In worker mode: the rolling-window counter here is per-process
        # and would reset every download, so it could never reach the
        # alert threshold. Hand the occurrence to the parent instead.
        _record_event("cookie_warning", path=_get_active_account())
        return
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
        f"session, base64-encode it, and update {env_hint} in the server's "
        f".env (then restart the container) when "
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
    _record_event("cookie_dead", path=path)
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
            if _was_cookie_flagged() and not is_cdn_connect_timeout_error(error_text):
                logger.warning(
                    f"Attempt {attempt}: this cookie account was confirmed dead "
                    f"by yt-dlp's own check - not retrying with the same "
                    f"cookiefile, handing off to account rotation/proxy instead: {error_text}"
                )
                raise

            if is_ipv6_unroutable_error(error_text):
                # This server has no route to the assigned CDN edge - not
                # a YouTube-side restriction, not fixable by retrying the
                # same request. Fail immediately instead of sleeping
                # through 2 more backoff rounds for a guaranteed repeat
                # failure. download_with_fallback's should_use_proxy()
                # already correctly declines to escalate this to the
                # proxy tier (the error text doesn't match any
                # IP-reputation marker), so this only saves the wasted
                # retry time - it does not change the ultimate outcome.
                logger.warning(
                    f"Attempt {attempt}: server has no route to this CDN edge "
                    f"(IPv6-only host, no IPv6 connectivity on this VPS) - "
                    f"not retrying: {error_text}"
                )
                raise

            if is_cdn_connect_timeout_error(error_text):
                # A connect-timeout to a SPECIFIC googlevideo media edge -
                # retrying from the SAME IP 2 more times just re-dials the
                # same unreachable edge with the same result each time.
                # Only a different exit IP (the proxy tier the caller
                # escalates to next, since should_use_proxy() now returns
                # True for this shape) has any real chance of landing on
                # a different, reachable edge. Fail fast here instead of
                # burning ~4.5s of pointless backoff on a guaranteed
                # repeat timeout.
                logger.warning(
                    f"Attempt {attempt}: CDN connect-timeout to a googlevideo "
                    f"edge - not retrying on the same IP: {error_text}"
                )
                raise

            if is_proxy_tls_error(error_text):
                # TLS handshake to this specific proxy exit failed outright
                # - retrying the SAME exit 2 more times with backoff can't
                # change a cipher/protocol negotiation that already failed.
                # Only a fresh proxy session (a different exit) has any
                # chance. Fail fast instead of burning ~4.5s of pointless
                # backoff on a guaranteed repeat handshake failure.
                logger.warning(
                    f"Attempt {attempt}: TLS handshake failure - not retrying "
                    f"on the same connection: {error_text}"
                )
                raise

            if is_format_unavailable_error(error_text):
                # No format in the manifest matched the player_client list
                # active for THIS attempt - retrying with the IDENTICAL
                # client/cookie combo 2 more times produces the identical
                # empty format list every time. Only a DIFFERENT client
                # list (i.e. a different cookie state, handled by account
                # rotation in the caller) has any chance - a proxy exit IP
                # does not. See is_format_unavailable_error()'s docstring
                # for the 2026-08-12 production evidence that ruled out
                # this being an IP-reputation problem.
                logger.warning(
                    f"Attempt {attempt}: no format available for this "
                    f"client/cookie combo - not retrying on the same combo: {error_text}"
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

            if is_music_premium_error(error_text):
                # Same reasoning again: this account doesn't have Music
                # Premium, and retrying the identical account 2 more
                # times with backoff can't change that.
                logger.warning(
                    f"Attempt {attempt}: Music Premium required, this cookie "
                    f"account isn't subscribed - not retrying: {error_text}"
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

      Layer 1 - COOKIE ACCOUNT ROTATION (free). ANON-FIRST as of
                2026-08-12: the first entry in `accounts` is always None
                (no cookiefile), tried before any cookie account, because
                android/android_vr - the client set that actually exposes
                usable audio formats for most public videos - are only
                available when NO cookiefile is attached (see
                _apply_player_clients above). Attaching a cookie account
                on attempt 1, as this used to do, silently drops to the
                tv/web/web_safari client set and frequently returns no
                usable format at all - which is exactly the failure that
                triggered this reordering (100+ "Requested format is not
                available" errors in one evening, previously misrouted to
                the proxy tier - see is_format_unavailable_error()).

                After the anon attempt, tries each currently available
                cookie account in turn (up to 3: primary + 2 optional
                extras) - these are still needed for age/members/premium-
                gated videos the anon attempt can't reach. If an attempt
                fails AND the thread-local flag confirms yt-dlp
                specifically flagged THIS cookie session as dead ("cookies
                are no longer valid" / LOGIN_REQUIRED), that account is
                disabled for a cooldown and the NEXT account is tried
                immediately - no point retrying the same dead session
                (this is now enforced doubly: extract_info_with_retry
                already stops retrying that account's own backoff loop the
                instant it's flagged, and this loop then moves on to the
                next account rather than trying the dead one again). If an
                attempt fails with an age-restriction, members-only, Music
                Premium, not-yet-live, or format-unavailable error,
                rotation also continues to the next account (a different
                account might genuinely be verified/a member/subscribed,
                or simply carry no cookies and unlock android/android_vr),
                WITHOUT disabling the current one (it isn't dead, just not
                privileged/client-eligible for this specific video). If an
                attempt fails for ANY OTHER reason (IP-block, permanent,
                unknown), rotation stops immediately - swapping cookie
                accounts won't fix an IP-reputation or availability
                problem, so we fall through to Layer 2 instead of wasting
                the remaining accounts on a failure they can't fix either.

      Layer 2 - PROXY, tried only when should_use_proxy() confirms the
                failure actually LOOKS like an IP-reputation problem (a
                bot-check page, a 403 on the media fetch, a connect-
                timeout to a specific googlevideo CDN edge, or another
                IP_BLOCK_MARKERS/YT_BOT_CHECK_MARKERS match) - AND it
                isn't a confirmed permanent error, age-restriction,
                members-only, Music Premium, not-yet-live, or format-
                unavailable error (see is_permanent_error and the
                account-identity/timing/client-eligibility checks below),
                since a different IP fixes IP-reputation problems, not
                account privilege, subscription status, stream timing,
                which formats a manifest contains, or a truly generic/
                unrecognized failure with no evidence a different IP would
                help. Uses whichever cookie account is STILL available
                (not yet disabled) at this point, if any - proxy fixes IP
                reputation, cookies fix session identity, the two are
                independent problems that can combine (e.g. an IP-blocked
                request might still benefit from a valid cookie session
                once retried through a clean IP).

    Before EVERY attempt (both layers), _set_active_account() records
    which cookie file (if any) is in use for THIS attempt - this is what
    lets a "cookies are no longer valid" warning line get attributed to
    the correct account file in the Discord alert (see
    _maybe_alert_cookie_expiry), instead of a generic unattributed message.

    ALSO before every attempt (both layers), _apply_player_clients()
    overrides extractor_args.youtube.player_client to match whether THIS
    attempt has a cookiefile attached (see the ADDED block near the top
    of this module) - android/android_vr are dropped whenever cookies are
    present, since yt-dlp silently skips them in that case and leaving
    only "web" behind frequently returns no downloadable audio formats.

    If Layer 2 itself fails with what looks like a proxy billing/quota
    error, trip the circuit breaker as before. If it fails because the
    cookie account used there ALSO turns out to be dead, that account gets
    disabled too - same accounting either way, just discovered a layer
    later.
    """
    def _try_proxy(reason: str):
        """
        The proxy attempt, shared by two callers: the normal Layer-2
        fallback at the bottom of this function, and the degraded-path
        short-circuit just below (which skips direct entirely).

        Uses whichever cookie account is still available - proxy fixes IP
        reputation, cookies fix session identity; the two are independent
        problems that can combine.
        """
        remaining_accounts = get_cookie_accounts()
        proxied_opts = dict(base_ydl_opts)
        proxied_opts["proxy"] = proxy_url
        proxy_account = remaining_accounts[0] if remaining_accounts else None
        if proxy_account:
            proxied_opts["cookiefile"] = proxy_account
        else:
            proxied_opts.pop("cookiefile", None)
        proxied_opts = _apply_player_clients(proxied_opts, has_cookies=bool(proxy_account))  # <-- ADDED

        _reset_cookie_flag()
        _set_active_account(proxy_account)
        if proxy_account:
            logger.info(f"[PROXY] Using cookie account: {proxy_account}")
        else:
            # Worth a WARNING, not an info line: the proxy fixes IP
            # reputation, not session identity. Without a cookie the
            # bot-check is close to guaranteed, and during the
            # 2026-08-08 incident this state was completely silent -
            # the logs showed a bot-check with no hint that the real
            # cause was every account having been disabled moments
            # earlier by unrelated CDN timeouts.
            logger.warning(
                "[PROXY] No healthy cookie accounts available for the proxy "
                "attempt - a bot-check is near-certain. Check whether accounts "
                "were recently disabled (see [COOKIES] lines above)."
            )
        try:
            result = extract_info_with_retry(proxied_opts, url)
            logger.info(f"[PROXY] Proxy attempt succeeded ({reason}).")
            record_account_result(proxy_account, True, "proxy")
            record_path_attempt("proxy", True)
            return result
        except Exception as proxy_error:
            proxy_error_text = str(proxy_error)
            record_account_result(proxy_account, False, "proxy", proxy_error_text)
            record_path_attempt("proxy", False)

            if is_proxy_quota_error(proxy_error_text):
                _trip_proxy_circuit_breaker()
            elif is_bot_check_error(proxy_error_text):
                # Feed the cost-control breaker. Deliberately does NOT
                # disable the cookie account: a bot-check here says the
                # EXIT IP was challenged, not that the session is dead -
                # and on 2026-08-08 the same account had authenticated
                # successfully on the direct attempt seconds earlier.
                # Disabling on this signal would recreate exactly the
                # false-positive cascade that took all three accounts
                # offline that morning.
                record_proxy_botcheck()
                logger.warning(
                    f"[PROXY] Bot-check through proxy (phase="
                    f"{failure_phase(proxy_error_text)}). The exit IP was "
                    f"challenged - this does NOT mean the cookie is dead."
                )
            elif proxy_account and _was_cookie_flagged():
                _disable_cookie_account(proxy_account)
                logger.warning(f"[PROXY] Cookie account '{proxy_account}' also failed via proxy - disabled.")
            else:
                logger.warning(
                    f"[PROXY] Proxy attempt also failed (phase="
                    f"{failure_phase(proxy_error_text)}): {proxy_error_text[:200]}"
                )
            raise

    # DEGRADED-PATH SHORT CIRCUIT. When the direct-path breaker is
    # tripped, every direct attempt is a known ~10s socket_timeout
    # followed by a guaranteed failure (see record_cdn_timeout above for
    # the confirmed root cause). Skip it entirely and spend the request
    # on the path that actually works.
    #
    # Guarded on the proxy being both configured AND not circuit-broken:
    # if there is no usable proxy, degraded or not, direct is still the
    # only path available and attempting it beats refusing outright.
    if direct_path_degraded() and proxy_url and proxy_available() and not proxy_botcheck_degraded():
        logger.info(
            "[CDN] Direct path is degraded - going straight to proxy, "
            "skipping the direct attempt."
        )
        try:
            return _try_proxy("direct path degraded")
        except Exception as e:
            # Proxy failed too. Fall through to the normal direct flow
            # rather than giving up: the breaker is a heuristic about
            # recent history, and a working direct attempt is still
            # better than no attempt at all.
            logger.warning(
                f"[CDN] Proxy failed while direct was degraded - falling back "
                f"to a direct attempt anyway: {str(e)[:200]}"
            )

    # ANON-FIRST (2026-08-12): None (no cookiefile) always goes first,
    # ahead of every cookie account - see this function's own docstring
    # above for the full incident/reasoning. Previously this was
    # `accounts = get_cookie_accounts()` with a fallback to `[None]` only
    # when every account was disabled, which put a cookie account on
    # attempt 1 whenever any account was healthy - exactly backwards from
    # what testing showed actually works for public videos.
    accounts = [None] + get_cookie_accounts()

    last_error = None
    for account_path in accounts:
        opts = dict(base_ydl_opts)
        if account_path:
            opts["cookiefile"] = account_path
        else:
            opts.pop("cookiefile", None)
        opts = _apply_player_clients(opts, has_cookies=bool(account_path))  # <-- ADDED

        _reset_cookie_flag()
        _set_active_account(account_path)
        try:
            result = extract_info_with_retry(opts, url)
            if account_path:
                logger.info(f"[COOKIES] Download succeeded using account: {account_path}")
            record_account_result(account_path, True, "direct")
            record_path_attempt("direct", True)
            return result
        except Exception as e:
            last_error = e
            error_text = str(e)
            record_account_result(account_path, False, "direct", error_text)
            record_path_attempt("direct", False)

            if isinstance(e, VideoTooLongError) or is_permanent_error(error_text):
                # No cookie swap, no proxy, no retry fixes a video that's
                # too long or genuinely unavailable - stop everywhere,
                # immediately.
                raise

            if is_cdn_connect_timeout_error(error_text):
                # A DIRECT attempt just burned ~10s on an unreachable
                # googlevideo edge. Feed the breaker so a run of these
                # stops future requests from paying the same cost. Only
                # recorded here (direct path); the proxy attempt below
                # deliberately doesn't count toward it.
                record_cdn_timeout()

                # DO NOT fall through to the cookie-disable check below.
                #
                # Observed in production 2026-08-08: three CDN timeouts
                # in ~40 seconds disabled ALL THREE cookie accounts, and
                # the proxy tier then ran with no session at all and got
                # a guaranteed bot-check. The chain was:
                #
                #   direct attempt -> googlevideo connect timeout
                #   -> yt-dlp ALSO emits its "cookies are no longer
                #      valid" warning during the same run (that check is
                #      a heuristic, and it fires on unrelated network
                #      failures - see _maybe_alert_cookie_expiry's
                #      comment on exactly this)
                #   -> _was_cookie_flagged() is true
                #   -> account disabled for 15 min, rotate to next
                #   -> next account hits the SAME dead edge, same result
                #   -> repeat until no accounts remain
                #
                # A network-level timeout says nothing about whether a
                # cookie is valid. Breaking out here means: don't disable
                # anything, don't rotate (a different account cannot
                # reach a dead CDN edge either, and each rotation costs
                # another ~10s), go straight to the proxy tier - which is
                # the one thing that CAN fix this, and which now still
                # has healthy cookies to use when it gets there.
                break

            if account_path and _was_cookie_flagged():
                _disable_cookie_account(account_path)
                continue  # try the next account, if any remain

            if (
                is_age_restricted_error(error_text)
                or is_members_only_error(error_text)
                or is_music_premium_error(error_text)
                or is_not_yet_live_error(error_text)
                or is_format_unavailable_error(error_text)
            ):
                # This account isn't privileged for this specific video
                # (not age-verified, not a member, not a Music Premium
                # subscriber), the video simply hasn't started, or this
                # attempt's client/cookie combo exposed no usable format -
                # a DIFFERENT account might still work (different
                # privilege, or simply no cookies at all, unlocking
                # android/android_vr), so keep rotating WITHOUT disabling
                # this one (it's not dead, just unprivileged/early/
                # client-mismatched for this particular video).
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
        or is_music_premium_error(first_error)
        or is_not_yet_live_error(first_error)
        or is_format_unavailable_error(first_error)
    ):
        # Every available cookie/no-cookie combination hit the same
        # account-privilege/timing wall, or the same client/cookie combo
        # exposed no usable format. A different IP (the proxy) does
        # nothing for any of these, so skip that tier entirely rather
        # than burning proxy bandwidth and ~30s on a guaranteed repeat
        # failure. See is_format_unavailable_error()'s docstring for the
        # 2026-08-12 production evidence (identical failure through the
        # proxy) that proved this specific addition.
        logger.warning(
            "[PROXY] Skipping proxy tier - failure is an age-restriction/"
            "members-only/Music-Premium/not-yet-live/format-unavailable "
            "requirement, not an IP/bot-check problem: no available "
            "client/cookie combination satisfies it for this video."
        )
        raise last_error

    if is_ipv6_unroutable_error(first_error):
        # This server has no outbound path to the assigned CDN edge at
        # all - a proxy exit IP doesn't fix that, since the failure isn't
        # about IP reputation, it's about this server's own missing IPv6
        # route. Skip the proxy tier entirely rather than spending ~30s
        # confirming the identical failure through a different door.
        logger.warning(
            "[PROXY] Skipping proxy tier - failure means this server has no "
            "route to the destination (IPv6-only CDN edge, no IPv6 "
            "connectivity on this VPS), which a different exit IP cannot fix: "
            f"{first_error[:200]}"
        )
        raise last_error

    # IMPORTANT: paid proxy is only used for failures that actually match
    # a known IP-reputation/bot-check signal - see should_use_proxy() for
    # the reasoning, including the CDN connect-timeout case, and its
    # caveat about the current mweb 403 bug still qualifying here (proxy
    # just doesn't happen to fix that particular bug, even though it's a
    # legitimate IP-block-shaped error).
    if not should_use_proxy(first_error):
        logger.warning(
            f"[PROXY] Not escalating to proxy - failure doesn't match a known "
            f"IP-reputation/bot-check signal, so a different IP is unlikely to "
            f"help: {first_error[:200]}"
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

    if proxy_botcheck_degraded():
        # Cost control. The proxy's exits are currently being challenged
        # by YouTube (see record_proxy_botcheck), so this escalation is a
        # paid request with a known outcome. Failing here costs the user
        # nothing extra - they were going to get an error either way -
        # and saves both the money and the ~5-15s the attempt would burn.
        logger.warning(
            f"[PROXY] Skipping escalation - proxy bot-check breaker is active "
            f"(exits currently challenged). Failing fast instead of paying for "
            f"a request that is very likely to bot-check too. Direct failure "
            f"was phase={failure_phase(first_error)}: {first_error[:150]}"
        )
        raise last_error

    logger.warning(
        f"[PROXY] Direct attempt(s) failed "
        f"(phase={failure_phase(first_error)}: {first_error[:200]}) - retrying via proxy..."
    )

    # Whatever the proxy attempt raises propagates - it's the most
    # informative error to surface (routes.py still applies its own
    # is_bot_check_error()/is_geo_restricted_error()/etc. classification on
    # top of it for the user-facing message).
    return _try_proxy("direct attempt failed")


# ============================================================
# CROSS-PROCESS BREAKER STATE (added 2026-08-14)
#
# WHY THIS EXISTS: since downloads moved into download_worker.py (see
# utils.run_in_killable_subprocess), every download runs in a FRESH
# process that imports this module from scratch. Every breaker, counter
# and cookie-cooldown above is a module-level global - so in a worker
# they all start empty and die when the process exits. Concretely, this
# silently disabled EVERY cost-control mechanism in this file:
#
#   _proxy_disabled_until          quota breaker never trips
#   _cdn_timeout_events            CDN degradation breaker never trips
#   _proxy_botcheck_events         proxy bot-check breaker never trips
#   _cookie_account_disabled_until dead cookie accounts never disabled
#   _account_health / _path_stats  /admin/status permanently reports zeros
#   _cookie_warning_events         cookie-expiry alerts never fire
#
# The fix is two-way, and both directions are required:
#
#   PARENT -> WORKER (export/import_breaker_state): the worker must SEE
#   the parent's current breakers, or it will happily hammer a proxy the
#   parent already knows is out of credit, or retry a cookie account the
#   parent already disabled.
#
#   WORKER -> PARENT (enable_event_recording/drain_events/apply_events):
#   whatever tripped inside the worker must be replayed into the
#   parent's long-lived state, or it evaporates when the process exits.
#
# Events, not state, on the way back: the parent may have handled other
# concurrent downloads while this worker ran, so replaying raw state
# would clobber theirs. Replaying events composes correctly with
# whatever else happened.
# ============================================================

_events_lock = threading.Lock()
_recorded_events: list = []
_record_events_enabled = False


def enable_event_recording():
    """Called ONCE by download_worker.py at startup. The parent process
    never calls this, so _record_event is a no-op there and apply_events
    below cannot recurse into the recorder."""
    global _record_events_enabled
    _record_events_enabled = True


def _record_event(kind: str, **payload):
    if not _record_events_enabled:
        return
    with _events_lock:
        _recorded_events.append({"kind": kind, **payload})


def drain_events() -> list:
    """Worker-side: everything that tripped during this download."""
    with _events_lock:
        out = list(_recorded_events)
        _recorded_events.clear()
    return out


def apply_events(events: list):
    """
    Parent-side: replay a worker's events into THIS process's state.
    Never raises - a malformed event must not break a download that
    otherwise succeeded.
    """
    if not events:
        return
    for ev in events:
        try:
            kind = ev.get("kind")
            if kind == "cdn_timeout":
                record_cdn_timeout()
            elif kind == "proxy_botcheck":
                record_proxy_botcheck()
            elif kind == "proxy_quota":
                _trip_proxy_circuit_breaker()
            elif kind == "cookie_dead" and ev.get("path"):
                _disable_cookie_account(ev["path"])
            elif kind == "account_result":
                record_account_result(
                    ev.get("path"), ev.get("ok", False),
                    ev.get("via", "direct"), ev.get("error_text", ""),
                )
            elif kind == "path_attempt":
                record_path_attempt(ev.get("via", "direct"), ev.get("ok", False))
            elif kind == "cookie_warning":
                _set_active_account(ev.get("path"))
                _maybe_alert_cookie_expiry("cookies are no longer valid")
        except Exception as e:
            logger.warning(f"[BREAKER] Failed to apply worker event {ev}: {e}")


def export_breaker_state() -> dict:
    """
    Parent-side snapshot handed to the worker. All values are absolute
    time.time() deadlines, so they survive the process boundary without
    any relative-time recalculation.
    """
    with _proxy_lock:
        proxy_until = _proxy_disabled_until
    with _cdn_lock:
        cdn_until = _direct_degraded_until
    with _proxy_botcheck_lock:
        botcheck_until = _proxy_botcheck_until
    with _cookie_accounts_lock:
        disabled = dict(_cookie_account_disabled_until)
    return {
        "proxy_disabled_until": proxy_until,
        "direct_degraded_until": cdn_until,
        "proxy_botcheck_until": botcheck_until,
        "cookie_disabled": disabled,
    }


def import_breaker_state(state: dict):
    """Worker-side: adopt the parent's breakers before doing any work."""
    global _proxy_disabled_until, _direct_degraded_until, _proxy_botcheck_until
    if not state:
        return
    try:
        with _proxy_lock:
            _proxy_disabled_until = float(state.get("proxy_disabled_until") or 0.0)
        with _cdn_lock:
            _direct_degraded_until = float(state.get("direct_degraded_until") or 0.0)
        with _proxy_botcheck_lock:
            _proxy_botcheck_until = float(state.get("proxy_botcheck_until") or 0.0)
        with _cookie_accounts_lock:
            _cookie_account_disabled_until.clear()
            _cookie_account_disabled_until.update(state.get("cookie_disabled") or {})
    except Exception as e:
        logger.warning(f"[BREAKER] Failed to import parent breaker state: {e}")